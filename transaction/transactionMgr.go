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
		fm:     fm,
		bm:     bm,
		txtnum: nextTxNumber(),
	}
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

func (t *TransactionMgr) Size(filename string)int {
	dummyblk := kfile.NewBlockId(filename, t.EndOfFile);
	t.cm.sLock(dummyblk);
	fileLength,err := t.fm.LengthLocked(filename);
	if err != nil{
		return 0
	}
	return fileLength;
}

func (t *TransactionMgr) append(filename string) *kfile.BlockId {
	BlockId dummyblk = new BlockId(filename, END_OF_FILE);
	concurMgr.xLock(dummyblk);
	return fm.append(filename);
}
func (t *TransactionMgr)  blockSize() int {
	return t.fm.BlockSize();
}
func (t *TransactionMgr)  AvailableBuffs() int {
	return t.bm.Available();
}

func (t *TransactionMgr) nextTxNumber() int64 {
	return atomic.AddInt64(&t.nextTxNum, 1)
}