package kfile

import (
	"fmt"
	"sync"
)

type PageManager struct {
	pageSize int
	Pages    map[uint64]*Page
	mu       sync.RWMutex
}

func NewPageManager(pageSize int) *PageManager {
	return &PageManager{
		pageSize: pageSize,
		Pages:    make(map[uint64]*Page),
	}
}

func (pm *PageManager) SetPage(id uint64, page *Page) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.Pages[id] = page
}

func (pm *PageManager) GetPage(id uint64) (*Page, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if page, exists := pm.Pages[id]; exists {
		return page, nil
	}
	return nil, fmt.Errorf("page not found")
}
