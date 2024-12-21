package buffer

import (
	"fmt"
	"strings"
	"sync"
	"time"
	"ultraSQL/kfile"
	"ultraSQL/log"
)

type BufferMgr struct {
	bufferpool   []*Buffer
	numAvailable int
	mu           sync.RWMutex
	cond         *sync.Cond
}

const MAX_TIME = 1000 * time.Millisecond

func NewBufferMgr(fm *kfile.FileMgr, lm *log.LogMgr, numbuffs int) *BufferMgr {
	bm := &BufferMgr{
		bufferpool:   make([]*Buffer, numbuffs),
		numAvailable: numbuffs,
	}
	bm.cond = sync.NewCond(&bm.mu)

	for i := 0; i < numbuffs; i++ {
		bm.bufferpool[i] = NewBuffer(fm, lm)
	}

	return bm
}

func (bM *BufferMgr) available() int {
	bM.mu.RLock()
	defer bM.mu.RUnlock()
	return bM.numAvailable
}

func (bM *BufferMgr) FlushAll(txtnum int) {
	bM.mu.RLock()
	defer bM.mu.RUnlock()
	for _, buff := range bM.bufferpool {
		if buff.modifyingTx() == txtnum {
			buff.flush()
		}
	}
}

func (bM *BufferMgr) unpin(buff *Buffer) {
	bM.mu.Lock()
	defer bM.mu.Unlock()

	err := buff.unpin()
	if err != nil {
		panic(err)
	}
	if !buff.IsPinned() {
		bM.numAvailable++
		bM.cond.Broadcast()
	}
}

func (bM *BufferMgr) pin(blk *kfile.BlockId) (*Buffer, error) {
	bM.mu.Lock()
	defer bM.mu.Unlock()

	starttime := time.Now()
	buff := bM.tryToPin(blk)

	for buff == nil && !bM.waitingTooLong(starttime) {
		bM.cond.Wait()
		buff = bM.tryToPin(blk)
	}

	if buff == nil {
		return nil, fmt.Errorf("BufferAbortException: Could not pin the buffer")
	}

	return buff, nil
}

func (bM *BufferMgr) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > MAX_TIME
}

func (bM *BufferMgr) tryToPin(blk *kfile.BlockId) *Buffer {
	buff := bM.findExistingBuffer(blk)
	if buff == nil {
		buff = bM.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		err := buff.assignToBlock(blk)
		if err != nil {
			if !strings.Contains(err.Error(), "EOF") {
				// Panic for errors that do not involve "EOF"
				panic(err)
			}
		}
	}
	if !buff.IsPinned() {
		bM.numAvailable--
	}
	buff.pin()
	return buff
}

func (bM *BufferMgr) findExistingBuffer(blk *kfile.BlockId) *Buffer {
	for i := range bM.bufferpool {
		if bM.bufferpool[i].Block() != nil && bM.bufferpool[i].Block().Equals(blk) {
			return bM.bufferpool[i]
		}
	}
	return nil
}

func (bM *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bM.bufferpool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}
