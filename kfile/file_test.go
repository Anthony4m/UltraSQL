package kfile

import (
	"testing"
)

func TestPage(t *testing.T) {
	Filename := "test.db"
	t.Run("NewPage creates page with correct size", func(t *testing.T) {

		blockSize := 4096
		page := NewPage(blockSize, Filename)
		if len(page.data) != blockSize {
			t.Errorf("expected page size %d, got %d", blockSize, len(page.data))
		}
	})

	t.Run("Integer operations work correctly", func(t *testing.T) {
		page := NewPage(100, Filename)
		testVal := int32(42)

		err := page.SetInt(0, int(testVal))
		if err != nil {
			t.Fatalf("SetInt failed: %v", err)
		}

		got, err := page.GetInt(0)
		if err != nil {
			t.Fatalf("GetInt failed: %v", err)
		}
		if got != testVal {
			t.Errorf("expected %d, got %d", testVal, got)
		}
	})
}

func TestBlock(t *testing.T) {
	t.Run("Creation and basic properties", func(t *testing.T) {
		Filename := "test.db"
		Blknum := 5
		blk := NewBlockId(Filename, Blknum)

		if blk.FileName() != Filename {
			t.Errorf("Expected Filename %s, got %s", Filename, blk.FileName())
		}

		if blk.Number() != Blknum {
			t.Errorf("Expected block number %d, got %d", Blknum, blk.Number())
		}
	})

	t.Run("Equality", func(t *testing.T) {
		blk1 := NewBlockId("test.db", 1)
		blk2 := NewBlockId("test.db", 1)
		blk3 := NewBlockId("test.db", 2)
		blk4 := NewBlockId("other.db", 1)

		testCases := []struct {
			name     string
			a, b     *BlockId
			expected bool
		}{
			{"Same block", blk1, blk2, true},
			{"Different number", blk1, blk3, false},
			{"Different file", blk1, blk4, false},
			{"With nil", blk1, nil, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if result := tc.a.Equals(tc.b); result != tc.expected {
					t.Errorf("Expected Equals to return %v for %v and %v",
						tc.expected, tc.a, tc.b)
				}
			})
		}
	})

	t.Run("String representation", func(t *testing.T) {
		blk := NewBlockId("test.db", 5)
		expected := "[file test.db, block 5]"
		if s := blk.String(); s != expected {
			t.Errorf("Expected string %q, got %q", expected, s)
		}
	})

	t.Run("Hash code consistency", func(t *testing.T) {
		blk1 := NewBlockId("test.db", 1)
		blk2 := NewBlockId("test.db", 1)
		blk3 := NewBlockId("test.db", 2)

		// Same blocks should have same hash
		if blk1.HashCode() != blk2.HashCode() {
			t.Error("Hash codes different for equal blocks")
		}

		// Different blocks should have different hash
		if blk1.HashCode() == blk3.HashCode() {
			t.Error("Hash codes same for different blocks")
		}
	})

	t.Run("Copy", func(t *testing.T) {
		original := NewBlockId("test.db", 1)
		copy := original.Copy()

		if !original.Equals(copy) {
			t.Error("Copy not equal to original")
		}

		// Verify it's a deep copy
		copy.Filename = "other.db"
		if original.Filename == copy.Filename {
			t.Error("Copy seems to share data with original")
		}
	})

	t.Run("Block navigation", func(t *testing.T) {
		blk := NewBlockId("test.db", 5)

		// Test NextBlock
		next := blk.NextBlock()
		if next.Number() != 6 || next.FileName() != "test.db" {
			t.Error("NextBlock returned incorrect block")
		}

		// Test PrevBlock
		prev := blk.PrevBlock()
		if prev.Number() != 4 || prev.FileName() != "test.db" {
			t.Error("PrevBlock returned incorrect block")
		}

		// Test PrevBlock on first block
		first := NewBlockId("test.db", 0)
		if first.PrevBlock() != nil {
			t.Error("PrevBlock on first block should return nil")
		}

		// Test IsFirst
		if !first.IsFirst() {
			t.Error("IsFirst returned false for block 0")
		}
		if blk.IsFirst() {
			t.Error("IsFirst returned true for non-zero block")
		}
	})
}

// Let's also add some benchmarks to ensure our hash function performs well:

func BenchmarkBlockId(b *testing.B) {
	b.Run("HashCode", func(b *testing.B) {
		blk := NewBlockId("test.db", 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = blk.HashCode()
		}
	})

	b.Run("Equals", func(b *testing.B) {
		blk1 := NewBlockId("test.db", 1000)
		blk2 := NewBlockId("test.db", 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = blk1.Equals(blk2)
		}
	})
}
