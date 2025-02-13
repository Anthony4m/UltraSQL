package transaction

type TransactionInterface interface {
	Commit() error
	Rollback() error
	Recover() error
}
