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

func newLogMgr(fm *kfile.FileMgr, logFile string) (*LogMgr, error) {
	logMgr := &LogMgr{
		fm:      fm,
		logFile: logFile,
	}

	logMgr.logsize = fm.NewLength(logFile)
	pageManager := kfile.NewPageManager(fm.BlockSize())
	if logMgr.logsize == 0 {
		logMgr.currentBlock = logMgr.appendNewBlock()
	} else {
		b := make([]byte, fm.BlockSize())
		logMgr.currentBlock = kfile.NewBlockId(logFile, logMgr.logsize-1)
		newPageBytes := kfile.NewPageFromBytes(b)
		pageID := kfile.NewPageId(*logMgr.currentBlock)
		pageManager.SetPage(pageID, newPageBytes)
		err := fm.Read(logMgr.currentBlock, pageManager, pageID)
		if err != nil {
			return nil, fmt.Errorf("failed to read log block: %w", err)
		}
		logMgr.logPage, err = pageManager.GetPage(pageID)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve page from page manager: %w", err)
		}
	}
	return logMgr, nil
}

func (lm *LogMgr) flushLsn(lsn int) {
	if lsn >= lm.latestLSN {
		lm.flush()
	}
}

func (lm *LogMgr) Iterator() utils.Iterator[[]byte] {
	lm.flush()
	return utils.NewLogIterator(lm.fm, lm.currentBlock)
}

func (lm *LogMgr) flush() {
	lm.fm.Write(lm.currentBlock, lm.logPage)
}

func (lm *LogMgr) appendNewBlock() *kfile.BlockId {
	newBlock := kfile.NewBlockId(lm.logFile, lm.logsize)
	lm.logsize++
	return newBlock
}

func (lm *LogMgr) append(logrec []byte) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	boundary, _ := lm.logPage.GetInt(0)
	recsize := int32(len(logrec))
	intBytes := int32(unsafe.Sizeof(int32(0)))
	bytesNeeded := recsize + intBytes

	if (boundary - bytesNeeded) < intBytes {
		lm.flush()
		lm.currentBlock = lm.appendNewBlock()
		boundary, _ = lm.logPage.GetInt(0)
	}

	recpos := boundary - bytesNeeded
	lm.logPage.SetBytes(int(recpos), logrec)
	lm.logPage.SetInt(0, int(recpos))
	lm.latestLSN += 1
	return lm.latestLSN
}
