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
		logMgr.currentBlock = kfile.NewBlockId(logFile, logMgr.logsize-1)
		pageID := kfile.NewPageId(*logMgr.currentBlock)
		newPage := kfile.NewPage(fm.BlockSize(), logFile)
		pageManager.SetPage(pageID, newPage)
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
