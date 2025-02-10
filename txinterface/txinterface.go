package txinterface

import "ultraSQL/kfile"

// TxInterface is what RecoveryMgr needs from TransactionMgr.
type TxInterface interface {
	GetTxNum() int64
	Pin(blk kfile.BlockId) error
	UnPin(blk kfile.BlockId) error
}
