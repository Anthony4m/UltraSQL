package kfile

import (
	"bytes"
	"fmt"
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

// Test GetBytes method
func TestGetBytes(t *testing.T) {
	testCases := []struct {
		name           string
		initialData    []byte
		offset         int
		expectedResult []byte
		expectedError  error
	}{
		{
			name:           "Normal retrieval",
			initialData:    []byte{1, 2, 3, 4, 5},
			offset:         2,
			expectedResult: []byte{3, 4, 5},
			expectedError:  nil,
		},
		{
			name:           "Retrieval from start",
			initialData:    []byte{1, 2, 3, 4, 5},
			offset:         0,
			expectedResult: []byte{1, 2, 3, 4, 5},
			expectedError:  nil,
		},
		{
			name:           "Out of bounds offset",
			initialData:    []byte{1, 2, 3},
			offset:         4,
			expectedResult: nil,
			expectedError:  fmt.Errorf("%s: getting bytes", ErrOutOfBounds),
		},
		{
			name:           "Empty slice retrieval",
			initialData:    []byte{},
			offset:         0,
			expectedResult: []byte{},
			expectedError:  fmt.Errorf("%s: getting bytes", ErrOutOfBounds),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := &Page{
				data: make([]byte, len(tc.initialData)),
			}
			copy(p.data, tc.initialData)

			result, err := p.GetBytes(tc.offset)

			// Check error
			if tc.expectedError != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tc.expectedError)
				}
				if err.Error() != tc.expectedError.Error() {
					t.Fatalf("Expected error %v, got %v", tc.expectedError, err)
				}
			} else if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Check result
			if !bytes.Equal(result, tc.expectedResult) {
				t.Fatalf("Expected %v, got %v", tc.expectedResult, result)
			}

			// Ensure original data is unchanged
			originalData := make([]byte, len(tc.initialData))
			copy(originalData, tc.initialData)
			if !bytes.Equal(p.data, originalData) {
				t.Fatalf("Original data modified: expected %v, got %v", originalData, p.data)
			}
		})
	}
}

// Test SetBytes method
func TestSetBytes(t *testing.T) {
	testCases := []struct {
		name           string
		initialData    []byte
		offset         int
		valueToSet     []byte
		expectedResult []byte
		expectedError  error
	}{
		{
			name:           "Normal setting",
			initialData:    []byte{1, 2, 3, 4, 0},
			offset:         2,
			valueToSet:     []byte{10, 11},
			expectedResult: []byte{1, 2, 10, 11, 0},
			expectedError:  nil,
		},
		{
			name:           "Setting at start",
			initialData:    []byte{1, 2, 3, 4, 5},
			offset:         0,
			valueToSet:     []byte{10, 11},
			expectedResult: []byte{10, 11, 0, 4, 5},
			expectedError:  nil,
		},
		{
			name:           "Out of bounds setting",
			initialData:    []byte{1, 2, 3},
			offset:         2,
			valueToSet:     []byte{10, 11, 12},
			expectedResult: nil,
			expectedError:  fmt.Errorf("%s: setting bytes", ErrOutOfBounds),
		},
		{
			name:           "Empty slice setting",
			initialData:    []byte{},
			offset:         0,
			valueToSet:     []byte{},
			expectedResult: []byte{},
			expectedError:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := &Page{
				data: make([]byte, len(tc.initialData)),
			}
			copy(p.data, tc.initialData)

			err := p.SetBytes(tc.offset, tc.valueToSet)

			// Check error
			if tc.expectedError != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tc.expectedError)
				}
				if err.Error() != tc.expectedError.Error() {
					t.Fatalf("Expected error %v, got %v", tc.expectedError, err)
				}
				return
			} else if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Check result
			if tc.expectedResult != nil && !bytes.Equal(p.data, tc.expectedResult) {
				t.Fatalf("Expected %v, got %v", tc.expectedResult, p.data)
			}
		})
	}
}

// Concurrency test for SetBytes and GetBytes
func TestConcurrentAccess(t *testing.T) {
	p := &Page{
		data: make([]byte, 100),
	}

	// Fill with initial data
	for i := range p.data {
		p.data[i] = byte(i)
	}

	// Number of concurrent operations
	numOperations := 1000

	// Use wait group to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(numOperations * 2)

	// Concurrent setters
	for i := 0; i < numOperations; i++ {
		go func(idx int) {
			defer wg.Done()
			val := []byte{byte(idx), byte(idx + 1)}
			offset := idx % (len(p.data) - 2)
			_ = p.SetBytes(offset, val)
		}(i)
	}

	// Concurrent getters
	for i := 0; i < numOperations; i++ {
		go func(idx int) {
			defer wg.Done()
			offset := idx % len(p.data)
			_, _ = p.GetBytes(offset)
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
}
