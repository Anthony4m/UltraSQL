package log

import (
	"awesomeDB/kfile"
	"fmt"
	"sync"
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
		newPageBytes := kfile.NewPageFromBytes(b, logFile, logMgr.currentBlock.Blknum)
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

func (lm *LogMgr) appendNewBlock() *kfile.BlockId {
	newBlock := kfile.NewBlockId(lm.logFile, lm.logsize)
	lm.logsize++
	return newBlock
}
