package transaction

import (
	"fmt"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
)

type BufferList struct {
	bm      *buffer.BufferMgr
	buffers map[kfile.BlockId]*buffer.Buffer
}

func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{
		bm:      bm,
		buffers: make(map[kfile.BlockId]*buffer.Buffer),
	}
}

// Buffer retrieves a pinned Buffer (if any) for the given block
func (bl *BufferList) Buffer(blk kfile.BlockId) *buffer.Buffer {
	return bl.buffers[blk]
}

// Pin pins the specified block if it isn't already pinned in this BufferList
func (bl *BufferList) Pin(blk kfile.BlockId) error {
	if _, exists := bl.buffers[blk]; exists {
		// already pinned in this transaction
		return nil
	}
	buff, err := bl.bm.Pin(&blk)
	if err != nil {
		return fmt.Errorf("failed to pin block %v: %w", blk, err)
	}
	bl.buffers[blk] = buff
	return nil
}

// Unpin unpins the specified block
func (bl *BufferList) Unpin(blk kfile.BlockId) error {
	buff, exists := bl.buffers[blk]
	if !exists {
		// not pinned in this transaction
		return nil
	}
	bl.bm.Unpin(buff)
	delete(bl.buffers, blk)
	return nil
}

// UnpinAll unpins all blocks pinned by this BufferList
func (bl *BufferList) UnpinAll() {
	for _, buff := range bl.buffers {
		bl.bm.Unpin(buff)
	}
	// reset map
	bl.buffers = make(map[kfile.BlockId]*buffer.Buffer)
}
