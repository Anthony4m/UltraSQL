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
	boundary   int32
}

func NewLogIterator(fm *kfile.FileMgr, blk *kfile.BlockId) Iterator[[]byte] {
	b := make([]byte, fm.BlockSize())
	logIterator := &LogIterator{
		fm:  fm,
		blk: blk,
		p:   kfile.NewPageFromBytes(b),
	}
	logIterator.moveToBlock(blk)
	return logIterator
}

func (lg *LogIterator) hasNext() bool {
	return lg.currentPos < lg.fm.BlockSize() || lg.blk.Number() > 0
}

func (lg *LogIterator) next() []byte {
	if lg.currentPos == lg.fm.BlockSize() {
		lg.blk = kfile.NewBlockId(lg.blk.FileName(), lg.blk.Number()-1)
		lg.moveToBlock(lg.blk)
	}

	rec, _ := lg.p.GetBytes(lg.currentPos)
	lg.currentPos += int(unsafe.Sizeof(0)) + len(rec)
	return rec
}

func (lg *LogIterator) moveToBlock(blk *kfile.BlockId) {
	lg.fm.Read(blk, lg.p)
	lg.boundary, _ = lg.p.GetInt(0)
	lg.currentPos = int(lg.boundary)

}

func (lg *LogIterator) HasNext() bool {
	return lg.currentPos < int(lg.boundary)
}

func (lg *LogIterator) Next() []byte {
	if !lg.HasNext() {
		panic("No more elements")
	}
	reclen, _ := lg.p.GetInt(lg.currentPos)
	lg.currentPos += 4
	record, _ := lg.p.GetBytes(lg.currentPos)
	lg.currentPos += int(reclen)
	return record
}
