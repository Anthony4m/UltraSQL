package transaction_interface

type TransactionInterface interface {
	Commit()
	Rollback()
	Recover()
}
