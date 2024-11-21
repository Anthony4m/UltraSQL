package kfile

import (
	"fmt"
	"sync"
)

type PageID struct {
	Filename    string
	BlockNumber int
}

func NewPageId(id BlockId) PageID {
	return PageID{
		Filename:    id.Filename,
		BlockNumber: id.Blknum,
	}
}

type PageManager struct {
	pageSize int
	Pages    map[PageID]*Page
	mu       sync.RWMutex
}

func NewPageManager(pageSize int) *PageManager {
	return &PageManager{
		pageSize: pageSize,
		Pages:    make(map[PageID]*Page),
	}
}

func (pid *PageID) Equals(other PageID) bool {
	return pid.Filename == other.Filename && pid.BlockNumber == other.BlockNumber
}

func (pid PageID) String() string {
	return fmt.Sprintf("Filename: %s, BlockNumber: %d", pid.Filename, pid.BlockNumber)
}

func (pm *PageManager) SetPage(id PageID, page *Page) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.Pages[id] = page
}

func (pm *PageManager) GetPage(id PageID) (*Page, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if page, exists := pm.Pages[id]; exists {
		return page, nil
	}
	return nil, fmt.Errorf("page not found")
}
