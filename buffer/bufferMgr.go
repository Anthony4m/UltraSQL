package buffer

import (
	"fmt"
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
}

const MAX_TIME = 1000 * time.Millisecond

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
		bM.cond.Broadcast()
	}
}

func (bM *BufferMgr) pin(blk *kfile.BlockId) (*Buffer, error) {
	bM.mu.Lock()
	defer bM.mu.Unlock()

	starttime := time.Now()

	buff := bM.Get(blk)
	if buff == nil && bM.numAvailable > 0 {
		buff = bM.Insert(blk)
	}

	for buff == nil && !bM.waitingTooLong(starttime) {
		timer := time.NewTimer(MAX_TIME)
		waiting := make(chan struct{})
		done := make(chan struct{})

		go func() {
			bM.cond.L.Lock()
			defer bM.cond.L.Unlock()

			select {
			case <-done:
				return
			default:
				bM.cond.Wait()
				select {
				case <-done:
				default:
					close(waiting)
				}
			}
		}()

		select {
		case <-waiting:
			timer.Stop()
			close(done)
		case <-timer.C:
			close(done)
			return nil, fmt.Errorf("BufferAbortException: No buffers available after waiting %v", MAX_TIME)
		}

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

func (bM *BufferMgr) Insert(blk *kfile.BlockId) *Buffer {
	if buff, exists := bM.bufferPool[blk]; exists {
		bM.moveToHead(buff)
		buff.pin()
		return buff
	}

	if bM.numAvailable == 0 {
		for bM.lruTail != nil && bM.lruTail.IsPinned() {
			bM.lruTail = bM.lruTail.prev
		}
		if bM.lruTail == nil {
			return nil
		}
		if bM.lruTail.isDirty() {
			bM.lruTail.flush()
		}
		delete(bM.bufferPool, bM.lruTail.blk)
	}

	buff := NewBuffer(bM.fm, bM.lm)
	bM.bufferPool[blk] = buff
	bM.moveToHead(buff)
	buff.pin()
	bM.numAvailable--
	return buff
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
