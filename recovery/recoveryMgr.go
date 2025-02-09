package recovery

import (
	"fmt"
	"ultraSQL/buffer"
	"ultraSQL/log"
	"ultraSQL/log_record"
	"ultraSQL/transaction"
)

// RecoveryMgr manages the logging and recovery for a given transaction.
type RecoveryMgr struct {
	lm    *log.LogMgr
	bm    *buffer.BufferMgr
	tx    *transaction.TransactionMgr
	txNum int64
}

// NewRecoveryMgr is analogous to the Java constructor:
//
//	public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm)
func NewRecoveryMgr(tx *transaction.TransactionMgr, txNum int64, lm *log.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	rm := &RecoveryMgr{
		tx:    tx,
		txNum: txNum,
		lm:    lm,
		bm:    bm,
	}
	// Write a START record to the log
	StartRecordWriteToLog(lm, txNum) // e.g., StartRecord.writeToLog(lm, txNum)
	return rm
}

// Commit is analogous to public void commit()
func (r *RecoveryMgr) Commit() {
	// 1. Flush all buffers associated with this transaction
	r.bm.Policy().FlushAll(r.txNum)

	// 2. Write COMMIT record to the log
	lsn := CommitRecordWriteToLog(r.lm, r.txNum) // e.g., CommitRecord.writeToLog(r.lm, r.txNum)

	// 3. Force the log up to that LSN
	flushErr := r.lm.Buffer().FlushLSN(lsn)
	if flushErr != nil {
		fmt.Printf("error occurred during commit flush: %v\n", flushErr)
	}
}

// Rollback is analogous to public void rollback()
func (r *RecoveryMgr) Rollback() {
	r.doRollback()
	r.bm.Policy().FlushAll(r.txNum)
	lsn := RollbackRecordWriteToLog(r.lm, r.txNum)
	flushErr := r.lm.Buffer().FlushLSN(lsn)
	if flushErr != nil {
		fmt.Printf("error occurred during rollback flush: %v\n", flushErr)
	}
}

// Recover is analogous to public void recover()
func (r *RecoveryMgr) Recover() {
	r.doRecover()
	r.bm.Policy().FlushAll(r.txNum)
	lsn := CheckpointRecordWriteToLog(r.lm) // e.g., CheckpointRecord.writeToLog(lm)
	flushErr := r.lm.Buffer().FlushLSN(lsn)
	if flushErr != nil {
		fmt.Printf("error occurred during recovery flush: %v\n", flushErr)
	}
}

// SetCellValue updates the cell in a slotted page, then writes a unified log record
// that stores the old/new serialized cell bytes for undo/redo.
func (r *RecoveryMgr) SetCellValue(buff *buffer.Buffer, key []byte, newVal any) (int, error) {
	// 1. Get the slotted page from the buffer.
	sp := buff.Contents()

	// 2. Retrieve the cell at the given slot.
	cell, _, err := sp.FindCell(key)
	if err != nil {
		return -1, fmt.Errorf("failed to get cell at slot %d: %w", key, err)
	}

	// 3. Serialize the current (old) cell state.
	oldBytes := cell.ToBytes()

	// 4. Update the cell with the new value (the cell handles type encoding).
	if err := cell.SetValue(newVal); err != nil {
		return -1, fmt.Errorf("failed to set cell value: %w", err)
	}

	// 5. Serialize the new cell state.
	newBytes := cell.ToBytes()

	// 6. Write a unified update record to the log: includes txNum, block ID, slotIndex, oldBytes, newBytes.
	blk := buff.Block() // or any *BlockId if your Buffer returns it
	lsn := log_record.WriteUnifiedUpdateLogRecord(r.lm, r.txNum, blk, key, oldBytes, newBytes)

	// 7. Return the LSN so the caller can handle further flush or keep track of it.
	return lsn, nil
}

// doRollback performs a backward scan of the log to undo any record belonging to this transaction.
func (r *RecoveryMgr) doRollback() {
	iter, err := r.lm.Iterator()
	if err != nil {
		fmt.Printf("error occurred creating log iterator: %v\n", err)
		return
	}
	for iter.HasNext() {
		data, err := iter.Next()
		if err != nil {
			fmt.Printf("error occurred reading next log record: %v\n", err)
			return
		}
		rec := CreateLogRecord(data) // e.g. UnifiedUpdateRecord or other record
		if rec == nil {
			continue
		}
		if rec.TxNumber() == r.txNum {
			if rec.Op() == START {
				// Once we reach the START record for our transaction, we stop
				return
			}
			rec.Undo(r.tx) // "Undo" is record-specific logic
		}
	}
}

// doRecover replays the log from the end, undoing updates for transactions that never committed.
func (r *RecoveryMgr) doRecover() {
	finishedTxs := make(map[int64]bool)

	iter, err := r.lm.Iterator()
	if err != nil {
		fmt.Printf("error occurred creating log iterator: %v\n", err)
		return
	}
	for iter.HasNext() {
		data, err := iter.Next()
		if err != nil {
			fmt.Printf("error occurred reading next log record: %v\n", err)
			return
		}
		rec := CreateLogRecord(data)
		if rec == nil {
			continue
		}
		switch rec.Op() {
		case CHECKPOINT:
			return
		case COMMIT, ROLLBACK:
			finishedTxs[rec.TxNumber()] = true
		default:
			if !finishedTxs[rec.TxNumber()] {
				rec.Undo(r.tx)
			}
		}
	}
}
