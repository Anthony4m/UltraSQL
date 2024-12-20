package log

import (
	"awesomeDB/kfile"
	"awesomeDB/utils"
	"fmt"
	"sync"
	"unsafe"
)

type LogError struct {
	Op  string
	Err error
}

func (e *LogError) Error() string {
	return fmt.Sprintf("log operation %s failed: %v", e.Op, e.Err)
}

type LogMgr struct {
	fm             *kfile.FileMgr
	mu             sync.RWMutex
	logFile        string
	currentBlock   *kfile.BlockId
	logPage        *kfile.Page
	latestLSN      int
	latestSavedLSN int
	logsize        int
}

func NewLogMgr(fm *kfile.FileMgr, logFile string) (*LogMgr, error) {
	if fm == nil {
		return nil, &LogError{Op: "new", Err: fmt.Errorf("file manager cannot be nil")}
	}
	logMgr := &LogMgr{
		fm:      fm,
		logFile: logFile,
	}

	var err error
	if logMgr.logsize, err = fm.Length(logFile); err != nil {
		return nil, &LogError{Op: "new", Err: fmt.Errorf("failed to get log file length: %v", err)}
	}

	b := make([]byte, fm.BlockSize())
	logMgr.logPage = kfile.NewPageFromBytes(b)

	if logMgr.logsize == 0 {
		if logMgr.currentBlock = logMgr.appendNewBlock(); logMgr.currentBlock == nil {
			return nil, &LogError{Op: "new", Err: fmt.Errorf("failed to append initial block")}
		}
	} else {
		logMgr.currentBlock = kfile.NewBlockId(logFile, logMgr.logsize-1)
		if err := fm.Read(logMgr.currentBlock, logMgr.logPage); err != nil {
			return nil, &LogError{Op: "new", Err: fmt.Errorf("failed to read current block: %v", err)}
		}
	}

	return logMgr, nil
}

func (lm *LogMgr) FlushLSN(lsn int) error {
	if lsn >= lm.latestLSN {
		return lm.Flush()
	}
	return nil
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
		return nil, &LogError{Op: "iterator", Err: err}
	}
	return utils.NewLogIterator(lm.fm, lm.currentBlock), nil
}

func (lm *LogMgr) Flush() error {
	if err := lm.fm.Write(lm.currentBlock, lm.logPage); err != nil {
		return &LogError{Op: "flush", Err: fmt.Errorf("failed to write block %s: %v",
			lm.currentBlock.FileName(), err)}
	}
	lm.latestSavedLSN = lm.latestLSN
	return nil
}

func (lm *LogMgr) appendNewBlock() *kfile.BlockId {
	newBlock, err := lm.fm.Append(lm.logFile)
	if err != nil {
		return nil
	}

	if err := lm.logPage.SetInt(0, lm.fm.BlockSize()); err != nil {
		return nil
	}

	if err := lm.fm.Write(newBlock, lm.logPage); err != nil {
		return nil
	}

	return newBlock
}

func (lm *LogMgr) Append(logrec []byte) (int, error) {
	if len(logrec) == 0 {
		return 0, &LogError{Op: "append", Err: fmt.Errorf("empty log record")}
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary, err := lm.logPage.GetInt(0)
	if err != nil {
		return 0, &LogError{Op: "append", Err: fmt.Errorf("failed to get boundary: %v", err)}
	}

	recsize := len(logrec)
	intBytes := int(unsafe.Sizeof(0))
	bytesNeeded := recsize + intBytes

	if (boundary - bytesNeeded) < intBytes {
		if err := lm.Flush(); err != nil {
			return 0, &LogError{Op: "append", Err: fmt.Errorf("failed to flush: %v", err)}
		}

		if lm.currentBlock = lm.appendNewBlock(); lm.currentBlock == nil {
			return 0, &LogError{Op: "append", Err: fmt.Errorf("failed to append new block")}
		}

		boundary, _ = lm.logPage.GetInt(0)
	}

	recpos := boundary - bytesNeeded
	if err := lm.logPage.SetBytes(recpos, logrec); err != nil {
		return 0, &LogError{Op: "append", Err: fmt.Errorf("failed to set bytes: %v", err)}
	}

	if err := lm.logPage.SetInt(0, recpos); err != nil {
		return 0, &LogError{Op: "append", Err: fmt.Errorf("failed to update boundary: %v", err)}
	}

	lm.latestLSN++
	return lm.latestLSN, nil
}

func (lm *LogMgr) Checkpoint() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if err := lm.Flush(); err != nil {
		return &LogError{Op: "checkpoint", Err: err}
	}

	return nil
}
