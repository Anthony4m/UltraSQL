package log

type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(txnum int) error
}
