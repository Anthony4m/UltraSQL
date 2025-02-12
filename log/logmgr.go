package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"ultraSQL/utils"
)

// Sentinel error for an inserted cell that is too large to fit in the current page.
// This value should ideally be defined in the kfile package.
var ErrCellTooLarge = errors.New("cell too large full")

// Error wraps an underlying error with an operation context.
type Error struct {
	Op  string
	Err error
}

func (e *Error) Error() string {
	return fmt.Sprintf("log operation %s failed: %v", e.Op, e.Err)
}

func (e *Error) Unwrap() error {
	return e.Err
}

// LogMgr is responsible for managing the write-ahead log.
type LogMgr struct {
	fm             *kfile.FileMgr
	mu             sync.RWMutex
	bm             *buffer.BufferMgr
	logBuffer      *buffer.Buffer
	logFile        string
	currentBlock   *kfile.BlockId
	latestLSN      int
	latestSavedLSN int
	logSize        int32
}

// NewLogMgr creates a new LogMgr using the provided file and buffer managers.
func NewLogMgr(fm *kfile.FileMgr, bm *buffer.BufferMgr, logFile string) (*LogMgr, error) {
	if fm == nil {
		return nil, &Error{Op: "new", Err: fmt.Errorf("file manager cannot be nil")}
	}

	lm := &LogMgr{
		fm:      fm,
		bm:      bm,
		logFile: logFile,
	}

	var err error
	if lm.logSize, err = fm.Length(logFile); err != nil {
		return nil, &Error{Op: "new", Err: fmt.Errorf("failed to get log file length: %w", err)}
	}

	// Create a new slotted page for the log.
	logPage := kfile.NewSlottedPage(fm.BlockSize())
	if lm.logSize == 0 {
		// No log file yet; append a new block.
		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil || lm.currentBlock == nil {
			return nil, &Error{Op: "new", Err: fmt.Errorf("failed to append initial block: %w", err)}
		}
		// Inform the buffer manager that this block is in use.
		lm.bm.Policy().AllocateBufferForBlock(*lm.currentBlock)
	} else {
		// Otherwise, set the current block as the last block.
		lm.currentBlock = kfile.NewBlockId(logFile, lm.logSize-1)
	}

	// Pin the current block.
	buff, err := bm.Pin(lm.currentBlock)
	if err != nil {
		return nil, &Error{Op: "new", Err: fmt.Errorf("failed to pin initial block: %w", err)}
	}
	// Initialize the log page's contents.
	buff.SetContents(logPage)
	lm.logBuffer = buff

	// Flush the initial block.
	if err := lm.logBuffer.Flush(); err != nil {
		return nil, &Error{Op: "new", Err: fmt.Errorf("failed to flush initial block: %w", err)}
	}

	return lm, nil
}

// FlushAsync flushes the log buffer to disk asynchronously.
func (lm *LogMgr) FlushAsync() <-chan error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- lm.Flush()
		close(errChan)
	}()
	return errChan
}

// Iterator returns an iterator over the log records.
// It first flushes the log to disk.
func (lm *LogMgr) Iterator() (utils.Iterator[[]byte], error) {
	if err := lm.Flush(); err != nil {
		return nil, &Error{Op: "iterator", Err: err}
	}
	return utils.NewLogIterator(lm.fm, lm.bm, lm.currentBlock)
}

// Flush writes the contents of the log buffer to disk and updates the saved LSN.
func (lm *LogMgr) Flush() error {
	// Flush the log buffer.
	if err := lm.logBuffer.LogFlush(lm.currentBlock); err != nil {
		return err
	}
	// Unpin the buffer if needed.
	lm.bm.Unpin(lm.logBuffer)
	lm.latestSavedLSN = lm.latestLSN
	return nil
}

// appendNewBlock appends a new block to the log file.
func (lm *LogMgr) appendNewBlock() (*kfile.BlockId, error) {
	blkNum, err := lm.fm.LengthLocked(lm.logFile)
	if err != nil {
		return nil, &Error{Op: "appendNewBlock", Err: err}
	}
	blk := kfile.NewBlockId(lm.logFile, blkNum)
	return blk, nil
}

// Append adds a new log record to the log and returns the LSN and key.
func (lm *LogMgr) Append(logrec []byte) (int, []byte, error) {
	if len(logrec) == 0 {
		return 0, nil, &Error{Op: "append", Err: fmt.Errorf("empty log record")}
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Generate a unique key for the log record.
	cellKey := lm.GenerateKey()
	// Create a new key-value cell with the generated key.
	cell := kfile.NewKVCell(cellKey)
	if err := cell.SetValue(logrec); err != nil {
		return 0, nil, &Error{Op: "append", Err: fmt.Errorf("failed to set log record value: %w", err)}
	}

	// Retrieve the current log page.
	logPage := lm.logBuffer.Contents()
	err := logPage.InsertCell(cell)
	if err != nil {
		// If the cell does not fit in the current page, flush the current block and start a new one.
		if errors.Is(err, ErrCellTooLarge) {
			if flushErr := lm.Flush(); flushErr != nil {
				return 0, nil, &Error{Op: "append", Err: fmt.Errorf("failed to flush current block: %w", flushErr)}
			}
			lm.currentBlock, err = lm.appendNewBlock()
			if err != nil || lm.currentBlock == nil {
				return 0, nil, &Error{Op: "append", Err: fmt.Errorf("failed to append new block: %w", err)}
			}
			// You may want to inform the buffer manager about the new block.
			lm.bm.Policy().AllocateBufferForBlock(*lm.currentBlock)
			// Try inserting again into the new log page.
			logPage = lm.logBuffer.Contents()
			if err = logPage.InsertCell(cell); err != nil {
				return 0, nil, &Error{Op: "append", Err: fmt.Errorf("failed to insert cell after appending new block: %w", err)}
			}
		} else {
			return 0, nil, &Error{Op: "append", Err: fmt.Errorf("failed to insert cell: %w", err)}
		}
	}

	// Update the log buffer with the modified log page.
	lm.logBuffer.SetContents(logPage)
	lm.latestLSN++
	// Mark the buffer as modified with the new LSN.
	lm.logBuffer.MarkModified(-1, lm.latestLSN)
	return lm.latestLSN, cellKey, nil
}

// Checkpoint forces a flush of the log.
func (lm *LogMgr) Checkpoint() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if err := lm.Flush(); err != nil {
		return &Error{Op: "checkpoint", Err: err}
	}
	return nil
}

// GenerateKey creates a unique key for a new log record.
func (lm *LogMgr) GenerateKey() []byte {
	const prefix = "log_"
	var lsnBytes [8]byte
	binary.BigEndian.PutUint64(lsnBytes[:], uint64(lm.latestLSN+1))
	var keyBuffer bytes.Buffer
	keyBuffer.WriteString(prefix)
	keyBuffer.Write(lsnBytes[:])
	return keyBuffer.Bytes()
}

// ValidateKey checks whether the provided key matches the expected generated key.
func (lm *LogMgr) ValidateKey(key []byte) bool {
	// In this simple implementation, we compare the generated key with the provided key.
	// Depending on your requirements, you might instead check for format, prefix, etc.
	generatedKey := lm.GenerateKey()
	return bytes.Compare(key, generatedKey) == 0
}

func (lm *LogMgr) Buffer() *buffer.Buffer {
	return lm.logBuffer
}
