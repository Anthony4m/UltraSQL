package buffer

import (
	"errors"
	"ultraSQL/kfile"
	"ultraSQL/log"
)

type Buffer struct {
	fm       *kfile.FileMgr
	lm       *log.LogMgr
	contents *kfile.Page
	blk      *kfile.BlockId
	pins     int
	txnum    int
	lsn      int
}

func NewBuffer(fm *kfile.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: kfile.NewPage(fm.BlockSize()),
		blk:      nil,
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) GetContents() *kfile.Page {
	return b.contents
}

func (b *Buffer) Block() *kfile.BlockId {
	return b.blk
}

func (b *Buffer) MarkModified(txtnum int, lsn int) {
	b.txnum = txtnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) modifyingTx() int {
	return b.txnum
}

func (b *Buffer) assignToBlock(block *kfile.BlockId) error {
	if err := b.flush(); err != nil {
		return err
	}
	b.blk = block
	if err := b.fm.Read(b.blk, b.contents); err != nil {
		return err
	}
	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	if b.txnum > 0 && b.blk != nil {
		b.lm.FlushLsn(b.lsn)
		if err := b.fm.Write(b.blk, b.contents); err != nil {
			return err
		}
		b.txnum = -1
	}
	return nil
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() error {
	if b.pins <= 0 {
		return errors.New("unpin operation failed: buffer is not pinned")
	}
	b.pins--
	return nil
}
