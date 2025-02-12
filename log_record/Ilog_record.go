package log_record

import (
	"ultraSQL/txinterface"
)

const (
	CHECKPOINT = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

type Ilog_record interface {
	Op() int32
	TxNumber() int64
	Undo(tx txinterface.TxInterface) error
	ToBytes() []byte
}
