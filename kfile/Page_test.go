package kfile

import (
	"sync"
	"testing"
	"time"
)

func TestBlockId(t *testing.T) {
	t.Run("Creation and basic properties", func(t *testing.T) {
		filename := "test.db"
		blknum := 5
		blk := NewBlockId(filename, blknum)

		if blk.FileName() != filename {
			t.Errorf("Expected Filename %s, got %s", filename, blk.FileName())
		}

		if blk.Number() != blknum {
			t.Errorf("Expected block number %d, got %d", blknum, blk.Number())
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

		if blk1.HashCode() != blk2.HashCode() {
			t.Error("Hash codes different for equal blocks")
		}

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

		copy.Filename = "other.db"
		if original.Filename == copy.Filename {
			t.Error("Copy seems to share data with original")
		}
	})

	t.Run("Block navigation", func(t *testing.T) {
		blk := NewBlockId("test.db", 5)

		next := blk.NextBlock()
		if next.Number() != 6 || next.FileName() != "test.db" {
			t.Error("NextBlock returned incorrect block")
		}

		prev := blk.PrevBlock()
		if prev.Number() != 4 || prev.FileName() != "test.db" {
			t.Error("PrevBlock returned incorrect block")
		}

		first := NewBlockId("test.db", 0)
		if first.PrevBlock() != nil {
			t.Error("PrevBlock on first block should return nil")
		}

		if !first.IsFirst() {
			t.Error("IsFirst returned false for block 0")
		}
		if blk.IsFirst() {
			t.Error("IsFirst returned true for non-zero block")
		}
	})
}

func TestPage_SetDateAndGetDate(t *testing.T) {
	blockSize := 128
	page := NewPage(blockSize)
	if page == nil {
		t.Fatalf("Failed to create page")
	}

	// Test valid date set and get
	offset := 16
	expectedDate := time.Date(2023, time.December, 1, 10, 30, 0, 0, time.UTC)
	err := page.SetDate(offset, expectedDate)
	if err != nil {
		t.Fatalf("SetDate failed: %v", err)
	}

	retrievedDate, err := page.GetDate(offset)
	if err != nil {
		t.Fatalf("GetDate failed: %v", err)
	}
	if !retrievedDate.Equal(expectedDate) {
		t.Errorf("Expected date %v, got %v", expectedDate, retrievedDate)
	}

	// Test out-of-bounds set
	outOfBoundsOffset := blockSize - 4 // Not enough space for 8 bytes
	err = page.SetDate(outOfBoundsOffset, expectedDate)
	if err == nil {
		t.Errorf("Expected error for out-of-bounds SetDate, got nil")
	}

	// Test out-of-bounds get
	_, err = page.GetDate(outOfBoundsOffset)
	if err == nil {
		t.Errorf("Expected error for out-of-bounds GetDate, got nil")
	}
}

func TestPage_SetDate_ThreadSafety(t *testing.T) {
	blockSize := 128
	page := NewPage(blockSize)
	if page == nil {
		t.Fatalf("Failed to create page")
	}

	offset := 16
	expectedDate := time.Date(2023, time.December, 1, 10, 30, 0, 0, time.UTC)

	// Run concurrent SetDate operations
	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = page.SetDate(offset, expectedDate)
		}()
	}
	wg.Wait()

	// Ensure the final value is correct
	retrievedDate, err := page.GetDate(offset)
	if err != nil {
		t.Fatalf("GetDate failed after concurrent SetDate: %v", err)
	}
	if !retrievedDate.Equal(expectedDate) {
		t.Errorf("Expected date %v, got %v after concurrent operations", expectedDate, retrievedDate)
	}
}

func TestPage_GetDate_InvalidOffset(t *testing.T) {
	blockSize := 128
	page := NewPage(blockSize)
	if page == nil {
		t.Fatalf("Failed to create page")
	}

	// Test invalid offset for GetDate
	invalidOffset := blockSize + 1
	_, err := page.GetDate(invalidOffset)
	if err == nil {
		t.Errorf("Expected error for invalid GetDate offset, got nil")
	}
}
