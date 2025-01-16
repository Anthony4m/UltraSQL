package utils

import (
	"fmt"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
)

type LogIterator struct {
	fm         *kfile.FileMgr
	bm         *buffer.BufferMgr
	blk        *kfile.BlockId
	buff       *buffer.Buffer
	currentPos int
	boundary   int
	slots      []int
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
	return it.currentPos >= 0 || it.blk.Number() > 0
}

func (it *LogIterator) Next() ([]byte, error) {
	if it.currentPos < 0 {
		it.blk = kfile.NewBlockId(it.blk.GetFileName(), it.blk.Number()-1)
		if err := it.moveToBlock(it.blk); err != nil {
			return nil, err
		}
	}
	cell, err := it.buff.GetContents().GetCellBySlot(it.currentPos)
	if err != nil {
		return nil, fmt.Errorf("error while getting bytes: %v", err)
	}
	cellVal, err := cell.GetValue()
	if err != nil {
		return nil, fmt.Errorf("error while getting value: %v", err)
	}
	rec, ok := cellVal.([]byte)

	if !ok {
		panic("value is not byte")
	}

	//recLen := string(rec)
	//npos := MaxLength(len(recLen))
	//it.currentPos += int(unsafe.Sizeof(0)) + npos
	it.currentPos--

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
	it.slots = it.buff.GetContents().GetAllSlots()
	it.currentPos = len(it.slots) - 1
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
