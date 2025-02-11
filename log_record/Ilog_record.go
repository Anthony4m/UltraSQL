package log_record

import (
	"ultraSQL/txinterface"
)

type Ilog_record interface {
	Op() int
	TxNumber() int64
	Undo(tx txinterface.TxInterface)
	//ToBytes() []byte
}
