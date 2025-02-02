package buffer

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"ultraSQL/kfile"
)

const MaxTime = 1000 * time.Millisecond

// ErrNoUnpinnedBuffers is returned when no unpinned buffers are available for eviction.
var ErrNoUnpinnedBuffers = errors.New("no unpinned buffers available for eviction")

// BufferMgr manages a pool of buffers and applies an eviction policy.
type BufferMgr struct {
	mu           sync.RWMutex
	fm           *kfile.FileMgr
	Policy       EvictionPolicy
	numAvailable int
	availableCh  chan struct{}

	// Access tracking fields (for LRU or similar).
	accessCounter uint64

	// Optional statistics.
	hitCounter  int
	missCounter int
}

// NewBufferMgr creates a new BufferMgr with the specified number of buffers and eviction policy.
func NewBufferMgr(fm *kfile.FileMgr, numBuffs int, policy EvictionPolicy) *BufferMgr {
	return &BufferMgr{
		Policy:       policy,
		fm:           fm,
		numAvailable: numBuffs,
		availableCh:  make(chan struct{}, numBuffs),
	}
}

// Pin attempts to retrieve a buffer for the given block, possibly blocking until a buffer becomes available.
// If no buffers become available within MaxTime, an error is returned.
func (bm *BufferMgr) Pin(blk *kfile.BlockId) (*Buffer, error) {
	startTime := time.Now()

	// Main loop: retry until success or timeout.
	for {
		bm.mu.Lock()

		buff, getErr := bm.Policy.Get(*blk)
		switch {
		case getErr != nil:
			// Log the error from Policy.Get but don’t necessarily return unless it's critical.
			// The 'not found' scenario might not be an error per se; it could simply return (nil, nil).
			fmt.Printf("debug: Policy.Get returned an error: %v\n", getErr)

		case buff != nil:
			// We found the buffer in the policy -> It's a "hit".
			bm.hitCounter++
			bm.mu.Unlock()
			return buff, nil
		}

		// Not found in the policy, so we need a new buffer if one is available.
		if buff == nil && bm.numAvailable > 0 {
			bm.missCounter++
			newBuff, allocErr := bm.Policy.AllocateBufferForBlock(*blk)
			if allocErr != nil {
				bm.mu.Unlock()
				return nil, fmt.Errorf("failed to allocate buffer: %w", allocErr)
			}
			bm.numAvailable--
			bm.mu.Unlock()
			return newBuff, nil
		}

		// If we reach here, it means buff == nil and bm.numAvailable == 0.

		// Check if we’ve timed out.
		remaining := MaxTime - time.Since(startTime)
		if remaining <= 0 {
			bm.mu.Unlock()
			return nil, fmt.Errorf("no buffers available after waiting %v", MaxTime)
		}

		// Wait for a buffer to become free. Unlock while waiting.
		bm.mu.Unlock()
		select {
		case <-bm.availableCh:
			// A buffer might have been freed; loop again.
		case <-time.After(remaining):
			return nil, fmt.Errorf("no buffers available after waiting %v", MaxTime)
		}
	}
}

// Unpin decrements the pin count of the given buffer. If it becomes unpinned,
// bm.numAvailable is incremented, and a signal is sent on bm.availableCh to notify waiters.
func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if err := buff.Unpin(); err != nil {
		// Log a warning rather than panicking.
		fmt.Printf("warning: Unpin called on an unpinned buffer: %v\n", err)
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

// updateAccessTime sets a buffer’s lastAccessTime using a global counter,
// which can be used by LRU or other replacement policies.
func (bm *BufferMgr) updateAccessTime(buff *Buffer) {
	bm.accessCounter++
	buff.lastAccessTime = bm.accessCounter
}

// available returns the current count of available (unpinned) buffers.
func (bm *BufferMgr) available() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.numAvailable
}
