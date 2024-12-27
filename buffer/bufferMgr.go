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
	bufferPool    map[*kfile.BlockId]*Buffer
	numAvailable  int
	mu            sync.RWMutex
	cond          *sync.Cond
	lruHead       *Buffer
	lruTail       *Buffer
	fm            *kfile.FileMgr
	lm            *log.LogMgr
	accessCounter uint64
	frequency     int
	hitCounter    int
	missCounter   int
	availableCh   chan struct{}
}

const MAX_TIME = 1000 * time.Millisecond

var (
	ErrNoUnpinnedBuffers = fmt.Errorf("no unpinned buffers available for eviction")
)

func NewBufferMgr(fm *kfile.FileMgr, lm *log.LogMgr, numbuffs int) *BufferMgr {
	bm := &BufferMgr{
		bufferPool:    make(map[*kfile.BlockId]*Buffer, numbuffs),
		numAvailable:  numbuffs,
		fm:            fm,
		lm:            lm,
		accessCounter: 0,
		frequency:     0,
		hitCounter:    0,
		missCounter:   0,
	}
	bm.availableCh = make(chan struct{}, numbuffs)
	bm.cond = sync.NewCond(&bm.mu)
	return bm
}

func (bM *BufferMgr) moveToHead(buff *Buffer) {
	if buff == bM.lruHead {
		return
	}

	if buff.prev != nil {
		buff.prev.next = buff.next
	}
	if buff.next != nil {
		buff.next.prev = buff.prev
	}
	if buff == bM.lruTail {
		bM.lruTail = buff.prev
	}

	buff.next = bM.lruHead
	buff.prev = nil
	if bM.lruHead != nil {
		bM.lruHead.prev = buff
	}
	bM.lruHead = buff
	if bM.lruTail == nil {
		bM.lruTail = buff
	}
}

func (bM *BufferMgr) available() int {
	bM.mu.RLock()
	defer bM.mu.RUnlock()
	return bM.numAvailable
}

func (bM *BufferMgr) FlushAll(txtnum int) {
	bM.mu.RLock()
	defer bM.mu.RUnlock()
	for _, buff := range bM.bufferPool {
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
		select {
		case bM.availableCh <- struct{}{}:
		default:
		}
	}
}

func (bM *BufferMgr) pin(blk *kfile.BlockId) (*Buffer, error) {
	bM.mu.Lock()
	defer bM.mu.Unlock()

	startTime := time.Now()

	buff := bM.Get(blk)
	if buff == nil && bM.numAvailable > 0 {
		buff = bM.Insert(blk)
	}
	for buff == nil && !bM.waitingTooLong(startTime) {
		bM.mu.Unlock()

		select {
		case <-bM.availableCh:
		case <-time.After(MAX_TIME):
			bM.mu.Lock()
			return nil, fmt.Errorf("BufferAbortException: No buffers available after waiting %v", MAX_TIME)
		}

		bM.mu.Lock()
		buff = bM.Get(blk)
		if buff == nil && bM.numAvailable > 0 {
			buff = bM.Insert(blk)
		}
	}

	if buff == nil {
		return nil, fmt.Errorf("BufferAbortException: No buffers available after waiting %v", MAX_TIME)
	}

	return buff, nil
}

func (bM *BufferMgr) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > MAX_TIME
}

func (bm *BufferMgr) Insert(blk *kfile.BlockId) *Buffer {
	if buff, exists := bm.bufferPool[blk]; exists {
		bm.moveToHead(buff)
		buff.pin()
		return buff
	}

	if bm.numAvailable == 0 {
		evictedBuff, err := bm.findAndEvictBuffer()
		if err != nil {
			_ = fmt.Errorf("failed to evict buffer: %w", err)
			return nil
		}

		delete(bm.bufferPool, evictedBuff.blk)
	}

	buff := NewBuffer(bm.fm, bm.lm)

	// Add new buffer to pool
	bm.bufferPool[blk] = buff
	err := buff.assignToBlock(blk)
	if err != nil {
		if !strings.Contains(err.Error(), "EOF") {
			panic(err)
		}
	}
	bm.moveToHead(buff)
	buff.pin()
	bm.numAvailable--

	return buff
}

func (bM *BufferMgr) findAndEvictBuffer() (*Buffer, error) {
	current := bM.lruTail

	for current != nil && current.IsPinned() {
		current = current.prev
	}

	if current == nil {
		return nil, ErrNoUnpinnedBuffers
	}

	if current.isDirty() {
		if err := current.flush(); err != nil {
			return nil, fmt.Errorf("failed to flush dirty buffer: %w", err)
		}
	}

	return current, nil
}

func (bM *BufferMgr) Get(blk *kfile.BlockId) *Buffer {
	if _, exists := bM.bufferPool[blk]; exists {
		buff := bM.bufferPool[blk]
		buff.pin()
		bM.updateAccessTime(buff)
		return buff
	}
	return nil
}

func (bM *BufferMgr) updateAccessTime(buff *Buffer) {
	bM.accessCounter++
	buff.lastAccessTime = bM.accessCounter
}
