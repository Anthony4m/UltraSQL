package log

import (
	"awesomeDB/kfile"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewLogMgr(t *testing.T) {
	// Setup
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	blockSize := 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	filename := "new_log.db"
	logMgr, err := newLogMgr(fm, filename)
	if err != nil {
		t.Fatalf("Failed to create LogMgr for new log file: %v", err)
	}
	if logMgr.logsize != 0 {
		t.Errorf("Expected logsize 0 for new log file, got %d", logMgr.logsize)
	}

	// Test for an existing log file
	logMgr.Append([]byte("test record"))
	logMgr.Flush()

	logMgr2, err := newLogMgr(fm, filename)
	if err != nil {
		t.Fatalf("Failed to create LogMgr for existing log file: %v", err)
	}
	if logMgr2.logsize == 0 {
		t.Errorf("Expected logsize > 0 for existing log file, got %d", logMgr2.logsize)
	}
}

func TestAppend(t *testing.T) {
	// Setup
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	var blockSize int = 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	logMgr, err := newLogMgr(fm, "append_test.db")
	if err != nil {
		t.Fatalf("Failed to initialize LogMgr: %v", err)
	}

	// Append records and check LSN
	record := []byte("test record")
	for i := 0; i < 10; i++ {
		lsn := logMgr.Append(record)
		if lsn != i+1 {
			t.Errorf("Expected LSN %d, got %d", i+1, lsn)
		}
	}

	// Verify boundary updates correctly
	boundary, _ := logMgr.logPage.GetInt(0)
	if boundary <= 0 || boundary >= blockSize {
		t.Errorf("Invalid boundary after append: %d", boundary)
	}
}

func TestFlush(t *testing.T) {
	// Setup
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	blockSize := 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	logMgr, err := newLogMgr(fm, "flush_test.db")
	if err != nil {
		t.Fatalf("Failed to initialize LogMgr: %v", err)
	}

	// Append a record
	record := []byte("flush record")
	logMgr.Append(record)

	// Flush and verify
	err = logMgr.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read the block to confirm data was written
	page := kfile.NewPage(blockSize)
	err = fm.Read(logMgr.currentBlock, page)
	if err != nil {
		t.Fatalf("Failed to read block after flush: %v", err)
	}
	recpos, err := logMgr.logPage.GetInt(0)
	if err != nil {
		t.Errorf("Error getting recpos %s", err)
	}
	readRecord, _ := page.GetBytes(int(recpos))
	readRecordStr := string(readRecord)
	readRecordStr = strings.TrimRight(readRecordStr, "\x00 ") // Trim nulls and spacesZZ
	if string(readRecordStr) != string(record) {
		t.Errorf("Expected record '%s', got '%s'", string(record), string(readRecord))
	}
}

func TestAppendBoundary(t *testing.T) {
	// Setup
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	blockSize := 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	logMgr, err := newLogMgr(fm, "boundary_test.db")
	if err != nil {
		t.Fatalf("Failed to initialize LogMgr: %v", err)
	}

	// Append records to fill the block
	record := make([]byte, 50) // Record size
	for i := 0; i < blockSize/len(record)-1; i++ {
		logMgr.Append(record)
	}

	initialBlock := logMgr.currentBlock
	logMgr.Append(record)

	if logMgr.currentBlock == initialBlock {
		t.Errorf("Expected new block after boundary overflow, but block did not change")
	}
}
