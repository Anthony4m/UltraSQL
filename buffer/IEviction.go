package buffer

import "ultraSQL/kfile"

// EvictionPolicy defines the methods required for buffer eviction policies.
type EvictionPolicy interface {
	// Insert adds a block to the buffer.
	AllocateBufferForBlock(block kfile.BlockId) (*Buffer, error)

	// Get retrieves a block from the buffer.
	Get(block kfile.BlockId) (*Buffer, error)

	// Evict removes a block from the buffer based on the eviction policy.
	Evict() (*Buffer, error)

	FlushAll(txnum int)
}
