package utils

import (
	"fmt"
	"ultraSQL/kfile"
	"unsafe"
)

type LogIterator struct {
	fm         *kfile.FileMgr
	blk        *kfile.BlockId
	p          *kfile.Page
	currentPos int
	boundary   int
}

func NewLogIterator(fm *kfile.FileMgr, blk *kfile.BlockId) *LogIterator {
	b := make([]byte, fm.BlockSize())
	p := kfile.NewPageFromBytes(b)
	iterator := &LogIterator{fm: fm, blk: blk, p: p}
	iterator.moveToBlock(blk)
	return iterator
}

func (it *LogIterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() && it.blk.Number() > 0
}

func (it *LogIterator) Next() ([]byte, error) {
	if it.currentPos == it.fm.BlockSize() {
		it.blk = kfile.NewBlockId(it.blk.GetFileName(), it.blk.Number()-1)
		it.moveToBlock(it.blk)
	}
	rec, err := it.p.GetBytesWithLen(it.currentPos)
	if err != nil {
		_ = fmt.Errorf("error while getting bytes %s", err)
	}
	recLen := string(rec)
	npos := MaxLength(len(recLen))
	it.currentPos += int(unsafe.Sizeof(0)) + npos

	return rec, nil
}

func (it *LogIterator) moveToBlock(blk *kfile.BlockId) {
	it.fm.Read(blk, it.p)
	it.boundary, _ = it.p.GetInt(0)
	it.currentPos = it.boundary
}
