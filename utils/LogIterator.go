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
	slots      []int
}

// NewLogIterator returns a LogIterator and an error if something goes wrong.
func NewLogIterator(fm *kfile.FileMgr, bm *buffer.BufferMgr, blk *kfile.BlockId) (*LogIterator, error) {
	if blk == nil {
		return nil, fmt.Errorf("cannot create LogIterator with nil block")
	}
	it := &LogIterator{fm: fm, bm: bm, blk: blk}
	if err := it.moveToBlock(blk); err != nil {
		it.Close()
		return nil, err
	}
	return it, nil
}

// HasNext indicates whether there's another record to read.
func (it *LogIterator) HasNext() bool {
	// If we're in the current block and have >= 0 slots left, we have a record.
	// Otherwise, if we have more blocks (blk.Number() > 0), we can move to the previous block.
	return it.currentPos >= 0 || it.blk.Number() > 0
}

// Next fetches the next record (backwards in blocks/slots).
func (it *LogIterator) Next() ([]byte, error) {
	// If the current position is out of slots, move to the previous block.
	if it.currentPos < 0 {
		if it.blk.Number() == 0 {
			// strictly speaking, we have no next record
			return nil, fmt.Errorf("no more records in block 0")
		}
		newBlk := kfile.NewBlockId(it.blk.GetFileName(), it.blk.Number()-1)
		if err := it.moveToBlock(newBlk); err != nil {
			return nil, err
		}
	}

	// Now currentPos should be valid
	cell, err := it.buff.Contents().GetCellBySlot(it.currentPos)
	if err != nil {
		return nil, fmt.Errorf("error while getting cell: %w", err)
	}
	cellVal, err := cell.GetValue()
	if err != nil {
		return nil, fmt.Errorf("error while getting value: %w", err)
	}
	rec, ok := cellVal.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected []byte but got %T", cellVal)
	}

	it.currentPos--
	return rec, nil
}

// moveToBlock pins the new block and updates the current slot to the last slot in that block.
func (it *LogIterator) moveToBlock(blk *kfile.BlockId) error {
	// If we already have a buffer pinned, unpin it first
	if it.buff != nil {
		if err := it.buff.Unpin(); err != nil {
			return fmt.Errorf("moveToBlock: unpin error: %w", err)
		}
	}
	b, err := it.bm.Pin(blk)
	if err != nil {
		return fmt.Errorf("moveToBlock: pin error: %w", err)
	}
	it.buff = b
	it.blk = blk

	it.slots = it.buff.Contents().GetAllSlots()
	it.currentPos = len(it.slots) - 1
	return nil
}

// Close unpins the current buffer (if any).
func (it *LogIterator) Close() {
	if it.buff != nil {
		if err := it.buff.Unpin(); err != nil {
			// Normally you'd log or handle the error, but you might not want to panic.
			fmt.Printf("warning: error unpinning buffer in Close: %v\n", err)
		}
		it.buff = nil
	}
}
