package transaction

import "ultraSQL/kfile"

type ConcurrencyMgr struct {
	lTble *LockTable
	locks map[kfile.BlockId]string
}

func NewConcurrencyMgr() *ConcurrencyMgr {
	return &ConcurrencyMgr{
		locks: make(map[kfile.BlockId]string),
	}
}

func (cM *ConcurrencyMgr) sLock(blk kfile.BlockId) {
	if _, exists := cM.locks[blk]; !exists {
		cM.lTble.sLock
	}
}
