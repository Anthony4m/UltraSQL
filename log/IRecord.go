package log

const (
	CHECKPOINT = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(txNum int)
	// Optionally: a method to serialize or convert to a Cell
}
