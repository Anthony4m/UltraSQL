package transaction

import (
	"sync/atomic"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"ultraSQL/log"
)

type TransactionMgr struct {
	nextTxNum  int64
	EndOfFile  int
	rm         *RecoveryMgr
	cm         *ConcurrencyMgr
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
	tx.rm = NewRecoveryMgr(tx, tx.txtnum, lm, bm)
	tx.cm = NewConcurrencyMgr()
	tx.bufferList = NewBufferList(bm)
	return tx
}

func (t *TransactionMgr) Commit() {
	t.rm.Commit()
	t.cm.Release()
	t.bufferlist.UnpinAll()
}

func (t *TransactionMgr) Rollback() {
	t.rm.Rollback()
	t.cm.Release()
	t.bufferList.UnpinAll()
}

func (t *TransactionMgr) Recover() {
	t.bm.Policy().FlushAll(t.txtnum)
	t.recoveryMgr.recover()
}

func (t *TransactionMgr) Pin(blk *kfile.BlockId) {
	bufferList.Pin(blk)
}
func (t *TransactionMgr) UnPin(blk *kfile.BlockId) {
	bufferList.UnPin(blk)
}

func (t *TransactionMgr) Size(filename string) int {
	dummyblk := kfile.NewBlockId(filename, t.EndOfFile)
	t.cm.sLock(dummyblk)
	fileLength, err := t.fm.LengthLocked(filename)
	if err != nil {
		return 0
	}
	return fileLength
}

func (t *TransactionMgr) append(filename string) *kfile.BlockId {
	dummyblk := kfile.NewBlockId(filename, t.EndOfFile)
	t.cm.xLock(dummyblk)
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
	t.cm.sLock(blk)
	buff := t.bufferList.getBuffer(blk)
	return buff.Contents().FindCell(key)
}

func (t *TransactionMgr) InsertCell(blk kfile.BlockId, key []byte, val any, okToLog bool) {
	t.cm.xLock(blk)
	buff := t.bufferList.getBuffer(blk)
	lsn := -1
	if okToLog {
		lsn = t.rm.setValue(buff, key, val)
	}
	cellKey := t.txtnum
	cell := kfile.NewKVCell(cellKey)
	p := buff.Contents()
	p.InsertCell(cell)
	buff.setModified(txnum, lsn)
}
