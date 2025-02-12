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

type Mgr struct {
	nextTxNum  int64
	EndOfFile  int32
	rm         *recovery.Mgr
	cm         *concurrency.Mgr
	bm         *buffer.BufferMgr
	fm         *kfile.FileMgr
	txNum      int64
	bufferList *BufferList
}

func NewTransaction(fm *kfile.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Mgr {
	tx := &Mgr{
		fm: fm,
		bm: bm,
	}
	tx.nextTxNum = tx.nextTxNumber()
	tx.rm = recovery.NewRecoveryMgr(tx, tx.txNum, lm, bm)
	tx.cm = concurrency.NewConcurrencyMgr()
	tx.bufferList = NewBufferList(bm)
	return tx
}

func (t *Mgr) Commit() error {
	err := t.rm.Commit()
	if err != nil {
		return err
	}
	err = t.cm.Release()
	if err != nil {
		return err
	}
	t.bufferList.UnpinAll()
	return nil
}

func (t *Mgr) Rollback() error {
	err := t.rm.Rollback()
	if err != nil {
		return err
	}
	err = t.cm.Release()
	if err != nil {
		return err
	}
	t.bufferList.UnpinAll()
	return nil
}

func (t *Mgr) Recover() error {
	t.bm.Policy().FlushAll(t.txNum)
	err := t.rm.Recover()
	if err != nil {
		return err
	}
	return nil
}

func (t *Mgr) Pin(blk kfile.BlockId) error {
	err := t.bufferList.Pin(blk)
	if err != nil {
		return fmt.Errorf("failed to pin block %v: %w", blk, err)
	}
	return nil
}
func (t *Mgr) UnPin(blk kfile.BlockId) error {
	err := t.bufferList.Unpin(blk)
	if err != nil {
		return fmt.Errorf("failed to pin block %v: %w", blk, err)
	}
	return nil
}

func (t *Mgr) Size(filename string) (int32, error) {
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

func (t *Mgr) append(filename string) *kfile.BlockId {
	dummyblk := kfile.NewBlockId(filename, t.EndOfFile)
	t.cm.XLock(*dummyblk)
	blk, err := t.fm.Append(filename)
	if err != nil {
		return nil
	}
	return blk
}
func (t *Mgr) blockSize() int {
	return t.fm.BlockSize()
}
func (t *Mgr) AvailableBuffs() int {
	return t.bm.Available()
}

func (t *Mgr) nextTxNumber() int64 {
	return atomic.AddInt64(&t.nextTxNum, 1)
}

func (t *Mgr) FindCell(blk kfile.BlockId, key []byte) *kfile.Cell {
	t.cm.SLock(blk)
	buff := t.bufferList.Buffer(blk)
	cell, _, err := buff.Contents().FindCell(key)
	if err != nil {
		return nil
	}
	return cell
}

func (t *Mgr) InsertCell(blk kfile.BlockId, key []byte, val any, okToLog bool) error {
	t.cm.XLock(blk)
	var err error
	err = t.Pin(blk)
	if err != nil {
		return err
	}
	buff := t.bufferList.Buffer(blk)
	lsn := -1
	cellKey := key
	cell := kfile.NewKVCell(cellKey)
	p := buff.Contents()
	err = p.InsertCell(cell)
	if err != nil {
		return fmt.Errorf("failed to pin block %v: %w", blk, err)
	}
	buff.MarkModified(t.txNum, lsn)
	if okToLog {
		lsn, err = t.rm.SetCellValue(buff, key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetTxNum is required by the TxInterface.
func (t *Mgr) GetTxNum() int64 {
	return t.nextTxNum
}
