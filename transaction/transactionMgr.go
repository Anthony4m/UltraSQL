package transaction

import (
	"fmt"
	"sync/atomic"
	"ultraSQL/buffer"
	"ultraSQL/concurrency"
	"ultraSQL/kfile"
	"ultraSQL/log"
	"ultraSQL/recovery"
)

type TransactionMgr struct {
	nextTxNum  int64
	EndOfFile  int
	rm         *recovery.RecoveryMgr
	cm         *concurrency.ConcurrencyMgr
	bm         *buffer.BufferMgr
	fm         *kfile.FileMgr
	txtnum     int64
	bufferList *BufferList
}

func NewTransaction(fm *kfile.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *TransactionMgr {
	tx := &TransactionMgr{
		fm: fm,
		bm: bm,
	}
	tx.nextTxNum = tx.nextTxNumber()
	tx.rm = recovery.NewRecoveryMgr(tx, tx.txtnum, lm, bm)
	tx.cm = concurrency.NewConcurrencyMgr()
	tx.bufferList = NewBufferList(bm)
	return tx
}

func (t *TransactionMgr) Commit() {
	t.rm.Commit()
	t.cm.Release()
	t.bufferList.UnpinAll()
}

func (t *TransactionMgr) Rollback() {
	t.rm.Rollback()
	t.cm.Release()
	t.bufferList.UnpinAll()
}

func (t *TransactionMgr) Recover() {
	t.bm.Policy().FlushAll(t.txtnum)
	t.rm.Recover()
}

func (t *TransactionMgr) Pin(blk kfile.BlockId) error {
	err := t.bufferList.Pin(blk)
	if err != nil {
		return fmt.Errorf("failed to pin block %v: %w", blk, err)
	}
	return nil
}
func (t *TransactionMgr) UnPin(blk kfile.BlockId) error {
	err := t.bufferList.Unpin(blk)
	if err != nil {
		return fmt.Errorf("failed to pin block %v: %w", blk, err)
	}
	return nil
}

func (t *TransactionMgr) Size(filename string) (int, error) {
	dummyblk := kfile.NewBlockId(filename, t.EndOfFile)
	err := t.cm.SLock(*dummyblk)
	if err != nil {
		return 0, fmt.Errorf("an error occured when acquiring lock %s", err)
	}
	fileLength, err := t.fm.LengthLocked(filename)
	if err != nil {
		return 0, fmt.Errorf("an error occured when acquiring file length %s", err)
	}
	return fileLength, nil
}

func (t *TransactionMgr) append(filename string) *kfile.BlockId {
	dummyblk := kfile.NewBlockId(filename, t.EndOfFile)
	t.cm.XLock(*dummyblk)
	blk, err := t.fm.Append(filename)
	if err != nil {
		return nil
	}
	return blk
}
func (t *TransactionMgr) blockSize() int {
	return t.fm.BlockSize()
}
func (t *TransactionMgr) AvailableBuffs() int {
	return t.bm.Available()
}

func (t *TransactionMgr) nextTxNumber() int64 {
	return atomic.AddInt64(&t.nextTxNum, 1)
}

func (t *TransactionMgr) FindCell(blk kfile.BlockId, key []byte) *kfile.Cell {
	t.cm.SLock(blk)
	buff := t.bufferList.Buffer(blk)
	cell, _, err := buff.Contents().FindCell(key)
	if err != nil {
		return nil
	}
	return cell
}

func (t *TransactionMgr) InsertCell(blk kfile.BlockId, key []byte, val any, okToLog bool) error {
	t.cm.XLock(blk)
	buff := t.bufferList.Buffer(blk)
	lsn := -1
	var err error
	if okToLog {
		lsn, err = t.rm.SetCellValue(buff, key, val)
		if err != nil {
			return nil
		}
	}
	cellKey := key
	cell := kfile.NewKVCell(cellKey)
	p := buff.Contents()
	err = p.InsertCell(cell)
	if err != nil {
		return fmt.Errorf("failed to pin block %v: %w", blk, err)
	}
	buff.MarkModified(t.txtnum, lsn)
	return nil
}
