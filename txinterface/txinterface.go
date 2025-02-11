package txinterface

import "ultraSQL/kfile"

type TxInterface interface {
	GetTxNum() int64
	Pin(blk kfile.BlockId) error
	UnPin(blk kfile.BlockId) error
	InsertCell(blk kfile.BlockId, key []byte, val any, okToLog bool) error
}
