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
	if err := ValidateFilename(filename); err != nil {
		panic(err)
	}
	if err := ValidateBlockNumber(blknum); err != nil {
		panic(err)
	}
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

func (b *BlockId) Copy() *BlockId {
	return NewBlockId(b.Filename, b.Blknum)
}

func (b *BlockId) NextBlock() *BlockId {
	return NewBlockId(b.Filename, b.Blknum+1)
}

func (b *BlockId) PrevBlock() *BlockId {
	if b.Blknum > 0 {
		return NewBlockId(b.Filename, b.Blknum-1)
	}
	return nil
}

func (b *BlockId) IsFirst() bool {
	return b.Blknum == 0
}

func ValidateBlockNumber(blknum int) error {
	if blknum < 0 {
		return fmt.Errorf("block number cannot be negative: %d", blknum)
	}
	return nil
}

func ValidateFilename(filename string) error {
	if filename == "" {
		return fmt.Errorf("filename cannot be empty")
	}
	return nil
}

func (b *BlockId) IsValid() bool {
	return b != nil &&
		b.Filename != "" &&
		b.Blknum >= 0
}
