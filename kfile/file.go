package kfile

import (
	"fmt"
	"hash/fnv"
)

type BlockId struct {
	Filename string
	Blknum   int
}

func NewBlockId(filename string, blknum int) *BlockId {
	return &BlockId{
		Filename: filename,
		Blknum:   blknum,
	}
}

func (b *BlockId) FileName() string {
	return b.Filename
}

func (b *BlockId) Number() int {
	return b.Blknum
}

func (b *BlockId) Equals(other *BlockId) bool {
	if other == nil {
		return false
	}
	return b.Filename == other.Filename && b.Blknum == other.Blknum
}

func (b *BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Blknum)
}

func (b *BlockId) HashCode() uint32 {
	h := fnv.New32a()
	h.Write([]byte(b.Filename))

	blknumBytes := []byte{
		byte(b.Blknum >> 24),
		byte(b.Blknum >> 16),
		byte(b.Blknum >> 8),
		byte(b.Blknum),
	}
	h.Write(blknumBytes)

	return h.Sum32()
}
