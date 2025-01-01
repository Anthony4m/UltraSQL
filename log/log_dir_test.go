package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"ultraSQL/utils"
	"unsafe"
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
	bm := buffer.NewBufferMgr(fm, 3)
	logMgr, err := NewLogMgr(fm, bm, filename)
	if err != nil {
		t.Fatalf("Failed to create LogMgr for new log file: %v", err)
	}
	if logMgr.logsize != 0 {
		t.Errorf("Expected logsize 0 for new log file, got %d", logMgr.logsize)
	}

	// Test for an existing log file
	logMgr.Append([]byte("test record"))
	logMgr.Flush()

	logMgr2, err := NewLogMgr(fm, bm, filename)
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

	bm := buffer.NewBufferMgr(fm, 3)
	logMgr, err := NewLogMgr(fm, bm, "append_test.db")
	if err != nil {
		t.Fatalf("Failed to initialize LogMgr: %v", err)
	}

	// Append records and check LSN
	record := []byte("test record")
	for i := 0; i < 10; i++ {
		lsn, _ := logMgr.Append(record)
		if lsn != i+1 {
			t.Errorf("Expected LSN %d, got %d", i+1, lsn)
		}
	}

	// Verify boundary updates correctly
	boundary, _ := logMgr.logBuffer.GetContents().GetInt(0)
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

	bm := buffer.NewBufferMgr(fm, 3)
	logMgr, err := NewLogMgr(fm, bm, "flush_test.db")
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
	recpos, err := logMgr.logBuffer.GetContents().GetInt(0)
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
	bm := buffer.NewBufferMgr(fm, 3)
	logMgr, err := NewLogMgr(fm, bm, "boundary_test.db")
	if err != nil {
		t.Fatalf("Failed to initialize LogMgr: %v", err)
	}

	// Append records to fill the block
	record := make([]byte, 50) // Record size
	for i := 0; i < blockSize/len(record)-2; i++ {
		logMgr.Append(record)
	}

	initialBlock := logMgr.currentBlock
	logMgr.Append(record)

	if logMgr.currentBlock == initialBlock {
		t.Errorf("Expected new block after boundary overflow, but block did not change")
	}
}

func TestLogMgr(t *testing.T) {
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

	bm := buffer.NewBufferMgr(fm, 3)
	// Test file creation and appending
	filename := "test.db"
	_, err = fm.Append(filename)
	if err != nil {
		t.Fatalf("Failed to append block: %v", err)
	}
	lm, _ := NewLogMgr(fm, bm, filename)

	createRecords(t, lm, 1, 3)
	printLogRecords(t, lm, "The log file now has these records:")

	// Create and append additional records
	createRecords(t, lm, 4, 7)
	err = lm.logBuffer.FlushLSN(5)
	if err != nil {
		return
	}
	printLogRecords(t, lm, "The log file now has these records:")
}

func createRecords(t *testing.T, lm *LogMgr, start, end int) {
	t.Logf("Creating records:")
	for i := start; i <= end; i++ {
		record := createLogRecord(fmt.Sprintf("record %d", i), i+100)
		lsn, _ := lm.Append(record)
		t.Logf("Record LSN: %d,i is %d", lsn, i+100)
	}
}

func printLogRecords(t *testing.T, lm *LogMgr, msg string) {
	t.Log(msg)
	iter, _ := lm.Iterator()
	for iter.HasNext() {
		rec, err := iter.Next()
		if err != nil {
			panic(err)
		}
		page := kfile.NewPageFromBytes(rec)
		s, err := page.GetStringWithOffset(0)
		if err != nil {
			panic(err)
		}
		npos := utils.MaxLength(len(s))
		val, _ := page.GetInt(npos)
		t.Logf("[%s, %d]", s, val)
	}
	t.Log()
}

func createLogRecord(s string, n int) []byte {
	npos := utils.MaxLength(len(s))
	record := make([]byte, npos+int(unsafe.Sizeof(0))) // String + Integer
	page := kfile.NewPageFromBytes(record)

	if err := page.SetString(0, s); err != nil {
		panic(fmt.Sprintf("Failed to set string: %v", err))
	}
	if err := page.SetInt(npos, n); err != nil {
		panic(fmt.Sprintf("Failed to set int: %v", err))
	}

	// Log serialized record details
	//fmt.Printf("Serialized record [%s, %d]: npos=%d, recordLen=%d\n", s, n, npos, len(record))
	return record
}
