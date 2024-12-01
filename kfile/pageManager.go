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

var (
	managerInstance *PageManager
	once            sync.Once
)

// GetPageManager returns the singleton instance of the PageManager
func GetPageManager(blockSize int) *PageManager {
	once.Do(func() {
		managerInstance = &PageManager{
			pageSize: blockSize,
			Pages:    make(map[uint64]*Page),
		}
	})
	return managerInstance
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

func RegisterPage(pm *PageManager, page *Page) error {
	pageId := page.PageID()
	pm.SetPage(pageId, page)
	return nil
}

func FindPage(pm *PageManager, pageId uint64) (*Page, bool) {
	page, err := pm.GetPage(pageId)
	if err != nil {
		return nil, false
	}
	return page, true
}
