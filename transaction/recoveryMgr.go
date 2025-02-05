package transaction

import (
	"ultraSQL/buffer"
	"ultraSQL/log"
)

type RecoveryMgr struct {
}

func NewRecoveryMgr(txMgr *TransactionMgr, txtnum int64, lm *log.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	return &RecoveryMgr{}
}

func (r *RecoveryMgr) Commit() {

}
