package utils

import (
	"awesomeDB/kfile"
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
