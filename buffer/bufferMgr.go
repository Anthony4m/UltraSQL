package buffer

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
	"ultraSQL/kfile"
)

const MaxTime = 1000 * time.Millisecond

// ErrNoUnpinnedBuffers is returned when no unpinned buffers are available for eviction.
var ErrNoUnpinnedBuffers = errors.New("no unpinned buffers available for eviction")

type BufferMgr struct {
	mu           sync.RWMutex
	bufferPool   map[*kfile.BlockId]*Buffer
	fm           *kfile.FileMgr
	numAvailable int
	availableCh  chan struct{}

	// LRU fields
	lruHead       *Buffer
	lruTail       *Buffer
	accessCounter uint64

	// if used or remove them
	hitCounter  int
	missCounter int
}

// NewBufferMgr creates a new BufferMgr with the specified number of buffers.
func NewBufferMgr(fm *kfile.FileMgr, numBuffs int) *BufferMgr {
	bm := &BufferMgr{
		bufferPool:   make(map[*kfile.BlockId]*Buffer, numBuffs),
		fm:           fm,
		numAvailable: numBuffs,
		availableCh:  make(chan struct{}, numBuffs),
	}
	return bm
}

// Pin retrieves (or creates) a Buffer for blk, possibly blocking until available.
func (bm *BufferMgr) Pin(blk *kfile.BlockId) (*Buffer, error) {
	startTime := time.Now()

	bm.mu.Lock()
	defer bm.mu.Unlock()

	for {
		buff := bm.Get(blk) // returns pinned buffer if found
		if buff == nil && bm.numAvailable > 0 {
			buff = bm.AllocateBufferForBlock(blk)
		}
		if buff != nil {
			return buff, nil
		}

		// none available; check for timeout
		if time.Since(startTime) > MaxTime {
			return nil, fmt.Errorf("BufferAbortException: No buffers after %v", MaxTime)
		}

		// wait for a buffer to become free
		bm.mu.Unlock()
		select {
		case <-bm.availableCh:
		case <-time.After(MaxTime - time.Since(startTime)):
		}
		bm.mu.Lock()
	}
}

// Get returns the buffer if it exists in the pool, else nil. The buffer is pinned if found.
func (bm *BufferMgr) Get(blk *kfile.BlockId) *Buffer {
	if buff, ok := bm.bufferPool[blk]; ok {
		buff.Pin()
		bm.updateAccessTime(buff)
		return buff
	}
	return nil
}

// Unpin decrements the pin count. If the buffer becomes unpinned, increment bm.numAvailable
// and signal bm.availableCh that a buffer is free.
func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if err := buff.Unpin(); err != nil {
		// better to log than panic
		fmt.Printf("warning: Unpin called on unpinned buffer: %v\n", err)
		return
	}
	if !buff.Pinned() {
		bm.numAvailable++
		select {
		case bm.availableCh <- struct{}{}:
		default:
		}
	}
}

// FlushAll flushes any buffer belonging to txnum.  Must hold at least a read lock if
// the map is never modified concurrently. Otherwise, use a write lock.
func (bm *BufferMgr) FlushAll(txnum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buff := range bm.bufferPool {
		if buff.ModifyingTxID() == txnum {
			_ = buff.Flush()
		}
	}
}

func (bm *BufferMgr) AllocateBufferForBlock(blk *kfile.BlockId) *Buffer {
	// See if block is already in the pool
	if existing, ok := bm.bufferPool[blk]; ok {
		bm.moveToHead(existing)
		existing.Pin()
		return existing
	}

	// Evict if no available
	if bm.numAvailable == 0 {
		evicted, err := bm.findAndEvictBuffer()
		if err != nil {
			// in real code, handle or return error
			fmt.Printf("error evicting buffer: %v\n", err)
			return nil
		}
		delete(bm.bufferPool, evicted.blk)
	}

	newBuff := NewBuffer(bm.fm)
	bm.bufferPool[blk] = newBuff
	if err := newBuff.assignToBlock(blk); err != nil {
		// if not EOF, log or return
		if !errors.Is(err, io.EOF) {
			fmt.Printf("assignToBlock error: %v\n", err)
			return nil
		}
	}
	bm.moveToHead(newBuff)
	newBuff.Pin()
	bm.numAvailable--
	return newBuff
}

// findAndEvictBuffer finds an unpinned buffer from the LRU tail. Flush if dirty.
func (bm *BufferMgr) findAndEvictBuffer() (*Buffer, error) {
	b := bm.lruTail
	for b != nil && b.Pinned() {
		b = b.prev
	}
	if b == nil {
		return nil, ErrNoUnpinnedBuffers
	}
	if b.Dirty {
		if err := b.Flush(); err != nil {
			return nil, fmt.Errorf("failed to flush: %w", err)
		}
	}
	return b, nil
}

func (bm *BufferMgr) moveToHead(buff *Buffer) {
	if buff == bm.lruHead {
		return
	}
	// unlink
	if buff.prev != nil {
		buff.prev.next = buff.next
	}
	if buff.next != nil {
		buff.next.prev = buff.prev
	}
	if buff == bm.lruTail {
		bm.lruTail = buff.prev
	}
	// put at head
	buff.next = bm.lruHead
	buff.prev = nil
	if bm.lruHead != nil {
		bm.lruHead.prev = buff
	}
	bm.lruHead = buff
	if bm.lruTail == nil {
		bm.lruTail = buff
	}
}

func (bm *BufferMgr) updateAccessTime(buff *Buffer) {
	bm.accessCounter++
	buff.lastAccessTime = bm.accessCounter
}

// available returns the number of available (unpinned) buffers.
func (bM *BufferMgr) available() int {
	bM.mu.RLock()
	defer bM.mu.RUnlock()
	return bM.numAvailable
}
