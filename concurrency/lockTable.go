package concurrency

import (
	"fmt"
	"sync"
	"time"
	"ultraSQL/kfile"
)

const MaxWaitTime = 10 * time.Second

type LockTable struct {
	locks map[kfile.BlockId]int // positive: number of shared locks, negative: exclusive lock
	mu    sync.RWMutex
	cond  *sync.Cond
}

func NewLockTable() *LockTable {
	lt := &LockTable{
		locks: make(map[kfile.BlockId]int),
	}
	lt.cond = sync.NewCond(&lt.mu)
	return lt
}

func (lT *LockTable) sLock(blk kfile.BlockId) error {
	lT.mu.Lock()
	defer lT.mu.Unlock()

	deadline := time.Now().Add(MaxWaitTime)

	// Wait while there's an exclusive lock on the block
	for lT.hasXLock(blk) {
		if time.Now().After(deadline) {
			return fmt.Errorf("shared lock acquisition timed out for block %v", blk)
		}
		lT.cond.Wait()
	}

	// Increment the number of shared locks (or initialize to 1)
	val := lT.getLockVal(blk)
	lT.locks[blk] = val + 1
	return nil
}

func (lT *LockTable) xLock(blk kfile.BlockId) error {
	lT.mu.Lock()
	defer lT.mu.Unlock()

	deadline := time.Now().Add(MaxWaitTime)

	// Wait while there are other locks (shared or exclusive)
	for lT.hasOtherLocks(blk) {
		if time.Now().After(deadline) {
			return fmt.Errorf("exclusive lock acquisition timed out for block %v", blk)
		}
		lT.cond.Wait()
	}

	// Set to -1 to indicate exclusive lock
	lT.locks[blk] = -1
	return nil
}

func (lT *LockTable) hasXLock(blk kfile.BlockId) bool {
	return lT.getLockVal(blk) < 0
}

func (lT *LockTable) getLockVal(blk kfile.BlockId) int {
	val, exists := lT.locks[blk]
	if !exists {
		return 0
	}
	return val
}

func (lT *LockTable) hasOtherLocks(blk kfile.BlockId) bool {
	val := lT.getLockVal(blk)
	return val != 0 && val != 1 // Allow upgrade from single shared lock
}

func (lT *LockTable) unlock(blk kfile.BlockId) error {
	lT.mu.Lock()
	defer lT.mu.Unlock()

	val := lT.getLockVal(blk)
	if val == 0 {
		return fmt.Errorf("attempting to unlock block %v which is not locked", blk)
	}

	if val > 1 {
		// Decrement shared lock count
		lT.locks[blk] = val - 1
	} else {
		// Remove last shared lock or exclusive lock
		delete(lT.locks, blk)
		lT.cond.Broadcast() // Wake up waiting goroutines
	}
	return nil
}

// Helper method to get lock information
func (lT *LockTable) GetLockInfo(blk kfile.BlockId) (lockType string, count int) {
	lT.mu.RLock()
	defer lT.mu.RUnlock()

	val := lT.getLockVal(blk)
	if val < 0 {
		return "exclusive", 1
	} else if val > 0 {
		return "shared", val
	}
	return "none", 0
}
