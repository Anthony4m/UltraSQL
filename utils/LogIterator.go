package utils

import (
	"fmt"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"unsafe"
)

type LogIterator struct {
	fm         *kfile.FileMgr
	bm         *buffer.BufferMgr
	blk        *kfile.BlockId
	buff       *buffer.Buffer
	currentPos int
	boundary   int
}

func NewLogIterator(fm *kfile.FileMgr, bm *buffer.BufferMgr, blk *kfile.BlockId) *LogIterator {

	iterator := &LogIterator{fm: fm, bm: bm, blk: blk}
	err := iterator.moveToBlock(blk)
	defer func() {
		if err != nil && iterator != nil {
			iterator.Close()
		}
	}()
	return iterator
}

func (it *LogIterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() || it.blk.Number() > 0
}

func (it *LogIterator) Next() ([]byte, error) {
	if it.currentPos == it.fm.BlockSize() {
		it.blk = kfile.NewBlockId(it.blk.GetFileName(), it.blk.Number()-1)
		if err := it.moveToBlock(it.blk); err != nil {
			return nil, err
		}
	}
	rec, err := it.buff.GetContents().GetBytesWithLen(it.currentPos)
	if err != nil {
		return nil, fmt.Errorf("error while getting bytes: %v", err)
	}
	recLen := string(rec)
	npos := MaxLength(len(recLen))
	it.currentPos += int(unsafe.Sizeof(0)) + npos

	return rec, nil
}

func (it *LogIterator) moveToBlock(blk *kfile.BlockId) error {
	if it.buff != nil {
		err := it.buff.UnPin()
		if err != nil {
			return err
		}
	}
	var err error
	it.buff, err = it.bm.Pin(blk)
	if err != nil {
		return err
	}
	it.boundary, _ = it.buff.GetContents().GetInt(0)
	it.currentPos = it.boundary
	return nil
}

func (it *LogIterator) Close() {
	if it.buff != nil {
		err := it.buff.UnPin()
		if err != nil {
			panic(err)
		}
	}

}
