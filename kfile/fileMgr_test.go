package kfile

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileMgr(t *testing.T) {
	// Create temporary directory for tests
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

		readData, err := p2.GetString(0, len(data))
		if err != nil {
			t.Fatalf("Failed to get string from page: %v", err)
		}

		if readData != data {
			t.Errorf("Data mismatch: expected %s, got %s", data, readData)
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
