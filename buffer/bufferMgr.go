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

	// LRU/Access tracking fields
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

// Pin retrieves (or creates) a Buffer for the given block, possibly blocking until one is available.
// The block is looked up (or allocated) using the provided eviction policy.
func (bm *BufferMgr) Pin(blk *kfile.BlockId) (*Buffer, error) {
	startTime := time.Now()

	// Main loop: try to obtain a buffer until success or timeout.
	for {
		bm.mu.Lock()

		// Try to get the buffer from the policy.
		buff, err := bm.Policy.Get(*blk) // returns pinned buffer if found.
		if err != nil {
			// Log the error (here we simply print it).
			fmt.Printf("warning: error getting buffer from policy: %v\n", err)
		} else {
			bm.hitCounter++
		}
		// If not found and there is availability, allocate a new buffer.
		if buff == nil && bm.numAvailable > 0 {
			bm.missCounter++
			buff, err = bm.Policy.AllocateBufferForBlock(*blk)
			if err != nil {
				bm.mu.Unlock()
				return nil, fmt.Errorf("failed to allocate buffer: %w", err)
			}
			bm.numAvailable--
		}
		if buff != nil {
			bm.mu.Unlock()
			return buff, nil
		}

		// Calculate remaining wait time.
		remaining := MaxTime - time.Since(startTime)
		if remaining <= 0 {
			bm.mu.Unlock()
			return nil, fmt.Errorf("no buffers available after waiting %v", MaxTime)
		}
		// Release lock and wait for a buffer to become free.
		bm.mu.Unlock()
		select {
		case <-bm.availableCh:
			// A signal indicates a buffer is available; retry.
		case <-time.After(remaining):
			return nil, fmt.Errorf("no buffers available after waiting %v", MaxTime)
		}
	}
}

// Unpin decrements the pin count of the given buffer. If the buffer becomes unpinned,
// it increments bm.numAvailable and signals availableCh that a buffer is free.
func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Unpin the buffer.
	if err := buff.Unpin(); err != nil {
		// Log a warning rather than panicking.
		fmt.Printf("warning: Unpin called on an unpinned buffer: %v\n", err)
		return
	}
	// If the buffer is now unpinned, update availability.
	if !buff.Pinned() {
		bm.numAvailable++
		select {
		case bm.availableCh <- struct{}{}:
		default:
		}
	}
}

// updateAccessTime updates a buffer's last access time using the global access counter.
func (bm *BufferMgr) updateAccessTime(buff *Buffer) {
	bm.accessCounter++
	buff.lastAccessTime = bm.accessCounter
}

// available returns the number of available (unpinned) buffers.
func (bm *BufferMgr) available() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.numAvailable
}
