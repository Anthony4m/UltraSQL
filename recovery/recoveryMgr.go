package recovery

import (
	"fmt"
	"ultraSQL/buffer"
	"ultraSQL/log"
	"ultraSQL/log_record"
	"ultraSQL/txinterface"
)

// Mgr manages the logging and recovery for a given transaction.
type Mgr struct {
	lm    *log.LogMgr
	bm    *buffer.BufferMgr
	tx    txinterface.TxInterface
	txNum int64
}

func NewRecoveryMgr(tx txinterface.TxInterface, txNum int64, lm *log.LogMgr, bm *buffer.BufferMgr) *Mgr {
	rm := &Mgr{
		tx:    tx,
		txNum: txNum,
		lm:    lm,
		bm:    bm,
	}

	_, err := log_record.StartRecordWriteToLog(lm, txNum)
	if err != nil {
		return nil
	}
	return rm
}

func (r *Mgr) Commit() error {

	r.bm.Policy().FlushAll(r.txNum)
	lsn, err := log_record.CommitRecordWriteToLog(r.lm, r.txNum)
	if err != nil {
		return fmt.Errorf("error occurred during commit: %v\n", err)
	}
	flushErr := r.lm.Buffer().FlushLSN(lsn)
	if flushErr != nil {
		return fmt.Errorf("error occurred during commit flush: %v\n", flushErr)
	}
	return nil
}

func (r *Mgr) Rollback() error {
	r.doRollback()
	r.bm.Policy().FlushAll(r.txNum)
	lsn, err := log_record.RollbackRecordWriteToLog(r.lm, r.txNum)
	if err != nil {
		return fmt.Errorf("error occurred during rollback: %v\n", err)
	}
	flushErr := r.lm.Buffer().FlushLSN(lsn)
	if flushErr != nil {
		return fmt.Errorf("error occurred during rollback flush: %v\n", flushErr)
	}
	return nil
}

func (r *Mgr) Recover() error {
	r.doRecover()
	r.bm.Policy().FlushAll(r.txNum)
	lsn, err := log_record.CheckpointRecordWriteToLog(r.lm)
	if err != nil {
		return fmt.Errorf("error occurred during recovery checkpoint: %v\n", err)
	}
	flushErr := r.lm.Buffer().FlushLSN(lsn)
	if flushErr != nil {
		return fmt.Errorf("error occurred during recovery flush: %v\n", flushErr)
	}
	return nil
}

// SetCellValue updates the cell in a slotted page, then writes a unified log record
// that stores the old/new serialized cell bytes for undo/redo.
func (r *Mgr) SetCellValue(buff *buffer.Buffer, key []byte, newVal any) (int, error) {
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
	lsn := log_record.WriteToLog(r.lm, r.txNum, *blk, key, oldBytes, newBytes)

	// 7. Return the LSN so the caller can handle further flush or keep track of it.
	return lsn, nil
}

// doRollback performs a backward scan of the log to undo any record belonging to this transaction.
func (r *Mgr) doRollback() {
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
		rec := log_record.CreateLogRecord(data) // e.g. UnifiedUpdateRecord or other record
		if rec == nil {
			continue
		}
		if rec.TxNumber() == r.txNum {
			if rec.Op() == log_record.START {
				// Once we reach the START record for our transaction, we stop
				return
			}
			err := rec.Undo(r.tx)
			if err != nil {
				return
			}
		}
	}
}

// doRecover replays the log from the end, undoing updates for transactions that never committed.
func (r *Mgr) doRecover() {
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
		rec := log_record.CreateLogRecord(data)
		if rec == nil {
			continue
		}
		switch rec.Op() {
		case log_record.CHECKPOINT:
			return
		case log_record.COMMIT, log_record.ROLLBACK:
			finishedTxs[rec.TxNumber()] = true
		default:
			if !finishedTxs[rec.TxNumber()] {
				err := rec.Undo(r.tx)
				if err != nil {
					return
				}
			}
		}
	}
}
