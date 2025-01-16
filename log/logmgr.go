package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"ultraSQL/utils"
)

type Error struct {
	Op  string
	Err error
}

func (e *Error) Error() string {
	return fmt.Sprintf("log operation %s failed: %v", e.Op, e.Err)
}

type LogMgr struct {
	fm             *kfile.FileMgr
	mu             sync.RWMutex
	bm             *buffer.BufferMgr
	logBuffer      *buffer.Buffer
	logFile        string
	currentBlock   *kfile.BlockId
	latestLSN      int
	latestSavedLSN int
	logsize        int
}

func NewLogMgr(fm *kfile.FileMgr, bm *buffer.BufferMgr, logFile string) (*LogMgr, error) {
	if fm == nil {
		return nil, &Error{Op: "new", Err: fmt.Errorf("file manager cannot be nil")}
	}

	logMgr := &LogMgr{
		fm:      fm,
		bm:      bm,
		logFile: logFile,
	}
	var err error
	if logMgr.logsize, err = fm.Length(logFile); err != nil {
		return nil, &Error{Op: "new", Err: fmt.Errorf("failed to get log file length: %v", err)}
	}
	logPage := kfile.NewSlottedPage(fm.BlockSize())
	if logMgr.logsize == 0 {
		if logMgr.currentBlock, err = logMgr.appendNewBlock(); logMgr.currentBlock == nil {
			return nil, &Error{Op: "new", Err: fmt.Errorf("failed to append initial block")}
		}
		logMgr.bm.Insert(logMgr.currentBlock)
	} else {
		logMgr.currentBlock = kfile.NewBlockId(logFile, logMgr.logsize-1)
	}
	buff, err := bm.Pin(logMgr.currentBlock)
	buff.SetContents(logPage)
	if err != nil {
		return nil, &Error{Op: "Pin", Err: fmt.Errorf("failed to pin initial block")}
	}

	logMgr.logBuffer = buff
	//if err := logMgr.logBuffer.GetContents().SetInt(0, logMgr.fm.BlockSize()); err != nil {
	//	return nil, &Error{Op: "Pin", Err: fmt.Errorf("failed to append initial block")}
	//}

	if err := logMgr.logBuffer.Flush(); err != nil {
		return nil, &Error{Op: "Pin", Err: fmt.Errorf("failed to flush initial block")}
	}
	return logMgr, nil
}

func (lm *LogMgr) FlushAsync() <-chan error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- lm.Flush()
		close(errChan)
	}()
	return errChan
}

func (lm *LogMgr) Iterator() (utils.Iterator[[]byte], error) {
	if err := lm.Flush(); err != nil {
		return nil, &Error{Op: "iterator", Err: err}
	}
	return utils.NewLogIterator(lm.fm, lm.bm, lm.currentBlock), nil
}

func (lm *LogMgr) Flush() error {

	// Flush the log buffer to disk
	if err := lm.logBuffer.LogFlush(lm.currentBlock); err != nil {
		return err
	}
	if lm.logBuffer != nil {
		lm.bm.UnPin(lm.logBuffer)
	}
	lm.latestSavedLSN = lm.latestLSN
	return nil
}

func (lm *LogMgr) appendNewBlock() (*kfile.BlockId, error) {
	blkNum, err := lm.fm.LengthLocked(lm.logFile)
	if err != nil {
		return nil, &Error{Op: "appendNewBlock", Err: err}
	}

	blk := kfile.NewBlockId(lm.logFile, blkNum)
	return blk, nil
}

func (lm *LogMgr) Append(logrec []byte) (int, []byte, error) {
	if len(logrec) == 0 {
		return 0, nil, &Error{Op: "append", Err: fmt.Errorf("empty log record")}
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()
	cellKey := lm.GenerateKey()
	//create a new key value cell and pass in the key
	cell := kfile.NewKVCell(cellKey)
	err := cell.SetValue(logrec)
	if err != nil {
		return 0, nil, err
	}
	// append the cell to the slotted page
	logpage := lm.logBuffer.GetContents()
	err = logpage.InsertCell(cell)
	if err != nil {
		if err.Error() == "cell too large full" {
			if err := lm.Flush(); err != nil {
				return 0, nil, &Error{Op: "append", Err: fmt.Errorf("failed to flush: %v", err)}
			}

			if lm.currentBlock, _ = lm.appendNewBlock(); lm.currentBlock == nil {
				return 0, nil, &Error{Op: "append", Err: fmt.Errorf("failed to append new block")}
			}
		} else {
			return 0, nil, &Error{Op: "Append", Err: fmt.Errorf("failed to insert cell %s", err)}
		}

	}

	lm.logBuffer.SetContents(logpage)
	lm.latestLSN++
	lm.logBuffer.MarkModified(-1, lm.latestLSN)
	return lm.latestLSN, cellKey, nil
}

func (lm *LogMgr) Checkpoint() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if err := lm.Flush(); err != nil {
		return &Error{Op: "checkpoint", Err: err}
	}

	return nil
}

func (lm *LogMgr) GenerateKey() []byte {
	prefix := "log_"
	var lsnBytes [8]byte
	binary.BigEndian.PutUint64(lsnBytes[:], uint64(lm.latestLSN+1))

	var keyBuffer bytes.Buffer

	keyBuffer.WriteString(prefix)
	keyBuffer.Write(lsnBytes[:])
	return keyBuffer.Bytes()
}

func (lm *LogMgr) ValidateKey(key []byte) bool {
	generatedKey := lm.GenerateKey()
	if comp := bytes.Compare(key, generatedKey); comp == 1 {
		return false
	}
	return true
}
