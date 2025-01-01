package kfile

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPage(t *testing.T) {
	t.Run("NewPage creates page with correct size", func(t *testing.T) {

		blockSize := 4096
		page := NewPage(blockSize)
		if len(page.data) != blockSize {
			t.Errorf("expected page size %d, got %d", blockSize, len(page.data))
		}
	})

	t.Run("Integer operations work correctly", func(t *testing.T) {
		page := NewPage(100)
		testVal := int(42)

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

		if blk.GetFileName() != Filename {
			t.Errorf("Expected Filename %s, got %s", Filename, blk.GetFileName())
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
		if next.Number() != 6 || next.GetFileName() != "test.db" {
			t.Error("NextBlock returned incorrect block")
		}

		// Test PrevBlock
		prev := blk.PrevBlock()
		if prev.Number() != 4 || prev.GetFileName() != "test.db" {
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

func TestFileMgr(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))

	t.Run("Basic FileMgr operations", func(t *testing.T) {
		// Setup
		blockSize := 400
		fm, err := NewFileMgr(tempDir, blockSize)
		if err != nil {
			t.Fatalf("Failed to create FileMgr: %v", err)
		}
		defer func() {
			fm.Close()
			os.RemoveAll(tempDir)
		}()

		// Test file creation and appending
		filename := "test.db"
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("Failed to append block: %v", err)
		}

		// Write data
		data := "Hello, SimpleDB!"
		p := NewPage(blockSize)
		err = p.SetString(0, data)
		if err != nil {
			t.Fatalf("Failed to set string in page: %v", err)
		}

		err = fm.Write(blk, p)
		if err != nil {
			t.Fatalf("Failed to write block: %v", err)
		}

		// Read data back
		p2 := NewPage(blockSize)
		err = fm.Read(blk, p2)
		if err != nil {
			t.Fatalf("Failed to read block: %v", err)
		}

		readData, err := p2.GetString(0)
		if err != nil {
			t.Fatalf("Failed to get string from page: %v", err)
		}

		if readData != data {
			t.Errorf("data mismatch: expected %s, got %s", data, readData)
		}
	})

	t.Run("File length and multiple blocks", func(t *testing.T) {
		fm, _ := NewFileMgr(tempDir, 100)
		defer fm.Close()

		filename := "multiblock.db"

		// Append multiple blocks
		for i := 0; i < 5; i++ {
			_, err := fm.Append(filename)
			if err != nil {
				t.Fatalf("Failed to append block %d: %v", i, err)
			}
		}

		length, err := fm.Length(filename)
		if err != nil {
			t.Fatalf("Failed to get file length: %v", err)
		}

		if length != 5 {
			t.Errorf("Expected length 5, got %d", length)
		}
	})

	t.Run("Statistics tracking", func(t *testing.T) {
		fm, _ := NewFileMgr(tempDir, 100)
		defer fm.Close()

		filename := "stats.db"
		blk, _ := fm.Append(filename)
		p := NewPage(100)

		// Perform some reads and writes
		fm.Write(blk, p)
		fm.Read(blk, p)

		if fm.BlocksWritten() != 1 {
			t.Errorf("Expected 1 block written, got %d", fm.BlocksWritten())
		}

		if fm.BlocksRead() != 1 {
			t.Errorf("Expected 1 block read, got %d", fm.BlocksRead())
		}

		// Check logs
		writeLog := fm.WriteLog()
		if len(writeLog) != 1 {
			t.Errorf("Expected 1 write log entry, got %d", len(writeLog))
		}

		readLog := fm.ReadLog()
		if len(readLog) != 1 {
			t.Errorf("Expected 1 read log entry, got %d", len(readLog))
		}
	})
}

func TestLengthLocked(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "filemgr-test-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test cases
	testCases := []struct {
		name           string
		initialContent []byte
		blockSize      int
		expectedBlocks int
		expectedError  bool
	}{
		{
			name:           "Empty File",
			initialContent: []byte{},
			blockSize:      512,
			expectedBlocks: 0,
			expectedError:  false,
		},
		{
			name:           "Empty File",
			initialContent: make([]byte, 512),
			blockSize:      512,
			expectedBlocks: 1,
			expectedError:  false,
		},
		{
			name:           "Empty File",
			initialContent: make([]byte, 256),
			blockSize:      512,
			expectedBlocks: 0,
			expectedError:  false,
		},
		{
			name:           "Empty File",
			initialContent: make([]byte, 1536),
			blockSize:      512,
			expectedBlocks: 3,
			expectedError:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test file with specific content
			filename := filepath.Join(tempDir, tc.name+".dat")
			err := os.WriteFile(filename, tc.initialContent, 0644)
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Create FileMgr instance
			fm := &FileMgr{
				dbDirectory: tempDir,
				blocksize:   tc.blockSize,
				openFiles:   make(map[string]*os.File),
				isNew:       false,
			}

			// Call LengthLocked
			numBlocks, err := fm.LengthLocked(tc.name + ".dat")

			// Check for unexpected errors
			if tc.expectedError && err == nil {
				t.Errorf("Expected an error, but got none")
			}

			// Check number of blocks
			if numBlocks != tc.expectedBlocks {
				t.Errorf("Unexpected number of blocks. Expected %d, got %d",
					tc.expectedBlocks, numBlocks)
			}

			// Ensure file is closed after the test
			if f, exists := fm.openFiles[tc.name+".dat"]; exists {
				f.Close()
				delete(fm.openFiles, tc.name+".dat")
			}
		})
	}
}

func TestBlockId(t *testing.T) {
	t.Run("Creation and basic properties", func(t *testing.T) {
		filename := "test.db"
		blknum := 5
		blk := NewBlockId(filename, blknum)

		if blk.GetFileName() != filename {
			t.Errorf("Expected Filename %s, got %s", filename, blk.GetFileName())
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
		if next.Number() != 6 || next.GetFileName() != "test.db" {
			t.Error("NextBlock returned incorrect block")
		}

		prev := blk.PrevBlock()
		if prev.Number() != 4 || prev.GetFileName() != "test.db" {
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
			expectedError:  nil,
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

func TestFileRename(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	fm, err := NewFileMgr(tempDir, 512)
	if err != nil {
		t.Errorf("Could not create directory %s", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	file := "test_file"
	blk := NewBlockId(file, 0)
	p := NewPage(fm.BlockSize())
	new_file := "test_new_file"
	fm.Write(blk, p)
	err = fm.RenameFile(blk, new_file)
	if err != nil {
		t.Errorf("Could not rename file %s", err)
	}
	want := new_file
	got := blk.GetFileName()
	if want != got {
		t.Errorf("want %s but got %s", want, got)
	}
}

func TestPreallocateFile(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	fm, err := NewFileMgr(tempDir, 512)
	if err != nil {
		t.Errorf("Could not create directory %s", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	file := "test_file"
	blk := NewBlockId(file, 0)
	err = fm.PreallocateFile(blk, 512)
	if err != nil {
		t.Errorf("Could not preallocate file %s", err)
	}
}

func TestPreallocateFileNonAlignedSize(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	fm, err := NewFileMgr(tempDir, 512)
	if err != nil {
		t.Errorf("Could not create directory %s", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	file := "test_file"
	blk := NewBlockId(file, 0)
	err = fm.PreallocateFile(blk, 100) // Not multiple of 512
	if err == nil {
		t.Error("Expected error for non-block-aligned size, got nil")
	}
}

func TestPreallocateLargeFile(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	fm, err := NewFileMgr(tempDir, 512)
	if err != nil {
		t.Errorf("Could not create directory %s", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	file := "test_file"
	blk := NewBlockId(file, 0)
	size := int64(512 * 100) // 100 blocks
	err = fm.PreallocateFile(blk, size)
	if err != nil {
		t.Errorf("Failed to preallocate large file: %v", err)
	}

	// Verify file size
	f, _ := os.Stat(filepath.Join(tempDir, file))
	if f.Size() != size {
		t.Errorf("Expected file size %d, got %d", size, f.Size())
	}
}

func TestPreallocateExistingFile(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	fm, err := NewFileMgr(tempDir, 512)
	if err != nil {
		t.Errorf("Could not create directory %s", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	file := "test_file"
	blk := NewBlockId(file, 0)
	// First allocation
	err = fm.PreallocateFile(blk, 1024) // 2 blocks
	if err != nil {
		t.Errorf("First preallocation failed: %v", err)
	}

	// Second smaller allocation (should be no-op)
	err = fm.PreallocateFile(blk, 512) // 1 block
	if err != nil {
		t.Errorf("Second preallocation failed: %v", err)
	}

	// Verify size stayed at larger allocation
	f, _ := os.Stat(filepath.Join(tempDir, file))
	if f.Size() != 1024 {
		t.Errorf("Expected file size 1024, got %d", f.Size())
	}
}

func TestPreallocateFileErrors(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	fm, err := NewFileMgr(tempDir, 512)
	if err != nil {
		t.Errorf("Could not create directory %s", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	file := "test_file"
	blk := NewBlockId(file, 0)
	if err != nil {
		t.Errorf("An error occured %s", err)
	}
	invalidBlk := NewBlockId("", 0)
	err = fm.PreallocateFile(invalidBlk, 512)
	if err == nil {
		t.Error("Expected error for invalid block, got nil")
	}

	// Test with read-only directory
	if err := os.Chmod(tempDir, 0444); err != nil {
		t.Fatalf("Failed to set directory readonly: %v", err)
	}
	err = fm.PreallocateFile(blk, 512)
	if err == nil {
		t.Error("Expected error for readonly directory, got nil")
	}
}
