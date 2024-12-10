package log

import (
	"awesomeDB/kfile"
	"awesomeDB/utils"
	"fmt"
	"sync"
	"unsafe"
)

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
	logMgr := &LogMgr{
		fm:      fm,
		logFile: logFile,
	}

	logMgr.logsize, _ = fm.Length(logFile)
	b := make([]byte, fm.BlockSize())
	newPageBytes := kfile.NewPageFromBytes(b)
	logMgr.logPage = newPageBytes
	if logMgr.logsize == 0 {
		logMgr.currentBlock = logMgr.appendNewBlock()
	} else {
		logMgr.currentBlock = kfile.NewBlockId(logFile, logMgr.logsize-1)
		err := fm.Read(logMgr.currentBlock, logMgr.logPage)
		if err != nil {
			return nil, err
		}
	}
	return logMgr, nil
}

func (lm *LogMgr) FlushLsn(lsn int) {
	if lsn >= lm.latestLSN {
		lm.Flush()
	}
}

func (lm *LogMgr) FlushAsync() {
	go func() {
		if err := lm.Flush(); err != nil {
			fmt.Printf("Async Flush failed: %v\n", err)
		}
	}()
}

func (lm *LogMgr) Iterator() utils.Iterator[[]byte] {
	err := lm.Flush()
	if err != nil {
		panic(err)
	}
	lg := utils.NewLogIterator(lm.fm, lm.currentBlock)
	return lg
}

func (lm *LogMgr) Flush() error {
	err := lm.fm.Write(lm.currentBlock, lm.logPage)
	if err != nil {
		return fmt.Errorf("failed to Flush log block %s: %v", lm.currentBlock.FileName(), err)
	}
	return nil
}

func (lm *LogMgr) appendNewBlock() *kfile.BlockId {
	newBlock, err := lm.fm.Append(lm.logFile)
	if err != nil {
		_ = fmt.Errorf("error occurred when appending block %s", err)
	}
	err = lm.logPage.SetInt(0, lm.fm.BlockSize())
	if err != nil {
		fmt.Printf("the error is %s", err)
		return nil
	}
	err = lm.fm.Write(newBlock, lm.logPage)
	if err != nil {
		fmt.Printf("the second error is %s", err)
		return nil
	}
	return newBlock
}

func (lm *LogMgr) Append(logrec []byte) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary, _ := lm.logPage.GetInt(0)
	recsize := len(logrec)
	intBytes := int(unsafe.Sizeof(0))
	bytesNeeded := recsize + intBytes

	if (boundary - bytesNeeded) < intBytes {
		lm.Flush()
		lm.currentBlock = lm.appendNewBlock()
		boundary, _ = lm.logPage.GetInt(0)
	}

	recpos := boundary - bytesNeeded
	err := lm.logPage.SetBytes(recpos, logrec)
	if err != nil {
		_ = fmt.Errorf("error while settng byte %s", err)
		return 0
	}
	lm.logPage.SetInt(0, recpos)
	lm.latestLSN += 1
	return lm.latestLSN
}

func (lm *LogMgr) Checkpoint() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	err := lm.Flush()
	if err != nil {
		return fmt.Errorf("failed to create checkpoint: %v", err)
	}
	fmt.Println("Checkpoint created.")
	return nil
}
