package transaction

type TransactionInterface interface {
	Commit()
	Rollback()
	Recover()
}
