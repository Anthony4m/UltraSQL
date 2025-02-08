package transaction

import (
	"fmt"
	"sync"
	"time"
	"ultraSQL/kfile"
)

const MaxTime = 10000

type LockTable struct {
	locks map[kfile.BlockId]int
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

	deadline := time.Now().Add(MaxTime)

	for lT.hasXLock(blk) {
		if time.Now().After(deadline) {
			return fmt.Errorf("lock acquisition timed out")
		}
		lT.cond.Wait()
	}

	val := lT.getLockVal(blk)
	lT.locks[blk] = val + 1
	return nil
}

func (lT *LockTable) xLock(blk kfile.BlockId) error {
	lT.mu.Lock()
	defer lT.mu.Unlock()

	deadline := time.Now().Add(MaxTime)

	for lT.hasOtherSLocks(blk) {
		if time.Now().After(deadline) {
			return fmt.Errorf("lock acquisition timed out")
		}
		lT.cond.Wait()
	}

	lT.locks[blk] = -1
	return nil
}

func (lT *LockTable) hasXLock(blk kfile.BlockId) bool {
	return lT.getLockVal(blk) < 0
}

func (lT *LockTable) getLockVal(blk kfile.BlockId) int {
	val, exist := lT.locks[blk]
	if exist {
		return 0
	}
	return val
}

func (lT *LockTable) hasOtherSLocks(blk kfile.BlockId) bool {
	return lT.getLockVal(blk) > 1
}
