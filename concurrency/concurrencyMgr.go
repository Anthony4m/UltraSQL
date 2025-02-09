package concurrency

import (
	"fmt"
	"sync"
	"ultraSQL/kfile"
)

type ConcurrencyMgr struct {
	lTble *LockTable
	locks map[kfile.BlockId]string
	mu    sync.RWMutex // Protect shared map access
}

func NewConcurrencyMgr() *ConcurrencyMgr {
	return &ConcurrencyMgr{
		locks: make(map[kfile.BlockId]string),
	}
}

func (cM *ConcurrencyMgr) SLock(blk kfile.BlockId) error {
	cM.mu.Lock()
	defer cM.mu.Unlock()

	// If we already have any lock (S or X), no need to acquire again
	if _, exists := cM.locks[blk]; exists {
		return nil
	}

	err := cM.lTble.sLock(blk)
	if err != nil {
		return fmt.Errorf("failed to acquire shared lock: %w", err)
	}

	cM.locks[blk] = "S"
	return nil
}

func (cM *ConcurrencyMgr) XLock(blk kfile.BlockId) error {
	cM.mu.Lock()
	defer cM.mu.Unlock()

	// If we already have an X lock, no need to acquire again
	if cM.hasXLock(blk) {
		return nil
	}

	// Following the two-phase locking protocol:
	// 1. First acquire S lock if we don't have any lock
	if _, exists := cM.locks[blk]; !exists {
		err := cM.lTble.sLock(blk)
		if err != nil {
			return fmt.Errorf("failed to acquire initial shared lock: %w", err)
		}
		cM.locks[blk] = "S"
	}

	// 2. Then upgrade to X lock
	err := cM.lTble.xLock(blk)
	if err != nil {
		return fmt.Errorf("failed to upgrade to exclusive lock: %w", err)
	}

	cM.locks[blk] = "X"
	return nil
}

func (cM *ConcurrencyMgr) Release() error {
	cM.mu.Lock()
	defer cM.mu.Unlock()

	var errs []error
	for blk := range cM.locks {
		if err := cM.lTble.unlock(blk); err != nil {
			errs = append(errs, fmt.Errorf("failed to release lock for block %v: %w", blk, err))
		}
	}

	// Clear the locks map regardless of errors
	cM.locks = make(map[kfile.BlockId]string)

	if len(errs) > 0 {
		return fmt.Errorf("errors during release: %v", errs)
	}
	return nil
}

func (cM *ConcurrencyMgr) hasXLock(blk kfile.BlockId) bool {
	// Note: Caller must hold mutex
	lockType, ok := cM.locks[blk]
	return ok && lockType == "X"
}

// Helper method to check current lock status
func (cM *ConcurrencyMgr) GetLockType(blk kfile.BlockId) (string, bool) {
	cM.mu.RLock()
	defer cM.mu.RUnlock()

	lockType, exists := cM.locks[blk]
	return lockType, exists
}
