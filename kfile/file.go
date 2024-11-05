package kfile

import (
	"fmt"
	"hash/fnv"
)

type BlockId struct {
	filename string
	blknum   int
}

func NewBlockId(filename string, blknum int) *BlockId {
	return &BlockId{
		filename: filename,
		blknum:   blknum,
	}
}

func (b *BlockId) Filename() string {
	return b.filename
}

func (b *BlockId) Number() int {
	return b.blknum
}

func (b *BlockId) Equals(other *BlockId) bool {
	if other == nil {
		return false
	}
	return b.filename == other.filename && b.blknum == other.blknum
}

func (b *BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blknum)
}

func (b *BlockId) HashCode() uint32 {
	h := fnv.New32a()
	h.Write([]byte(b.filename))

	blknumBytes := []byte{
		byte(b.blknum >> 24),
		byte(b.blknum >> 16),
		byte(b.blknum >> 8),
		byte(b.blknum),
	}
	h.Write(blknumBytes)

	return h.Sum32()
}
