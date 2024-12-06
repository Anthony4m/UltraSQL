package utils

import (
	"awesomeDB/kfile"
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
		it.blk = kfile.NewBlockId(it.blk.FileName(), it.blk.Number()-1)
		it.moveToBlock(it.blk)
	}
	rec, err := it.p.GetBytes(it.currentPos)
	recLen := string(rec)
	npos := MaxLength(len(recLen))
	b := make([]byte, npos+int(unsafe.Sizeof(0)))
	copy(b, rec)
	if err != nil {
		panic(err)
	}
	it.currentPos += int(unsafe.Sizeof(0)) + len(b)

	return b, nil
}

func (it *LogIterator) moveToBlock(blk *kfile.BlockId) {
	it.fm.Read(blk, it.p)
	it.boundary, _ = it.p.GetInt(0)
	it.currentPos = it.boundary
}
