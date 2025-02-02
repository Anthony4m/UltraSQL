package buffer

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"ultraSQL/kfile"
)

// Clock implements the Clock (Second Chance) replacement algorithm.
// It maintains a circular buffer of frames with a reference bit for each frame.
type Clock struct {
	fm         *kfile.FileMgr
	capacity   int
	bufferPool map[kfile.BlockId]*Buffer // Maps BlockId to Buffer
	frames     []*Buffer                 // Circular buffer of frames
	clockHand  int                       // Current position of clock hand
	mu         sync.Mutex                // Ensures thread safety
}

// InitClock creates a new Clock replacement policy with the given capacity.
func InitClock(capacity int, fm *kfile.FileMgr) *Clock {
	return &Clock{
		fm:         fm,
		capacity:   capacity,
		bufferPool: make(map[kfile.BlockId]*Buffer),
		frames:     make([]*Buffer, capacity),
		clockHand:  0,
	}
}

// AllocateBufferForBlock implements the buffer allocation strategy for the Clock algorithm.
func (c *Clock) AllocateBufferForBlock(block kfile.BlockId) (*Buffer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if block already exists
	if buff, exists := c.bufferPool[block]; exists {
		buff.setReferenced(true) // Set reference bit
		buff.Pin()
		return buff, nil
	}

	// Find an empty frame or evict one
	buff := NewBuffer(c.fm)
	var err error

	// First, try to find an empty frame
	for i, frame := range c.frames {
		if frame == nil {
			buff = NewBuffer(c.fm)
			c.frames[i] = buff
			break
		}
	}

	// If no empty frame, need to evict
	if buff == nil {
		buff, err = c.evictLocked()
		if err != nil {
			return nil, fmt.Errorf("failed to evict buffer: %w", err)
		}
	}

	// Assign the new block to the buffer
	if err := buff.assignToBlock(&block); err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("failed to assign block to buffer: %w", err)
		}
	}

	buff.setReferenced(true) // Set reference bit for new buffer
	buff.Pin()
	c.bufferPool[block] = buff

	return buff, nil
}

// Get retrieves a buffer containing the specified block.
func (c *Clock) Get(block kfile.BlockId) (*Buffer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if buff, exists := c.bufferPool[block]; exists {
		buff.setReferenced(true) // Set reference bit
		buff.Pin()
		return buff, nil
	}
	return nil, fmt.Errorf("buffer for block %v does not exist", block)
}

// evictLocked implements the clock algorithm's eviction strategy.
// The caller must hold c.mu.
func (c *Clock) evictLocked() (*Buffer, error) {
	startingHand := c.clockHand

	// Make up to two passes:
	// First pass: Look for unreferenced page
	// Second pass: Clear references and try again
	for pass := 0; pass < 2; pass++ {
		for {
			buff := c.frames[c.clockHand]

			// Advance clock hand
			c.clockHand = (c.clockHand + 1) % c.capacity

			// Skip if buffer is nil or pinned
			if buff == nil || buff.Pinned() {
				if c.clockHand == startingHand {
					break // Completed full circle
				}
				continue
			}

			if pass == 0 {
				// First pass: if referenced, clear bit and continue
				if buff.referenced() {
					buff.setReferenced(false)
					if c.clockHand == startingHand {
						break // Completed full circle
					}
					continue
				}
			}

			// Found a victim: unreferenced and unpinned
			if block := buff.Block(); block != nil {
				delete(c.bufferPool, *block)
			}
			return buff, nil
		}
	}

	return nil, ErrNoUnpinnedBuffers
}

// Evict implements the EvictionPolicy interface.
func (c *Clock) Evict() (*Buffer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictLocked()
}

// FlushAll implements the EvictionPolicy interface.
func (c *Clock) FlushAll(txnum int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, buff := range c.frames {
		if buff != nil && buff.ModifyingTxID() == txnum {
			_ = buff.Flush()
		}
	}
}
