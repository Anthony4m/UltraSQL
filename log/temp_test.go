package log

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"ultraSQL/utils"
)

func TestLogMgrAppend(t *testing.T) {
	// Setup
	tempDir := filepath.Join(os.TempDir(), "logmgr_test_"+time.Now().Format("20060102150405"))
	blockSize := 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	policy := buffer.InitLRU(3, fm)
	bm := buffer.NewBufferMgr(fm, 3, policy)
	filename := "test.db"
	_, err = fm.Append(filename)
	if err != nil {
		t.Fatalf("Failed to append block: %v", err)
	}
	lm, err := NewLogMgr(fm, bm, filename)
	if err != nil {
		t.Fatalf("Failed to initialize LogMgr: %v", err)
	}

	// Test cases
	t.Run("AppendMultipleRecordsWithinSingleBlock", func(t *testing.T) {
		verifyMultipleRecordsInSingleBlock(t, lm, blockSize)
	})

	//t.Run("AppendRecordsAcrossMultipleBlocks", func(t *testing.T) {
	//	verifyRecordsAcrossBlocks(t, lm, blockSize)
	//})
}

func verifyMultipleRecordsInSingleBlock(t *testing.T, lm *LogMgr, blockSize int) {
	t.Log("Testing appending multiple records within a single block...")

	// Append multiple small records
	record1 := "record2"
	record2 := "record1"

	lsn1, _, err := lm.Append([]byte(record1))
	lsn2, _, err := lm.Append([]byte(record2))
	if err != nil {
		t.Errorf("Error occured %s", err)
	}

	// Assert LSNs
	if lsn1 != 1 || lsn2 != 2 {
		t.Errorf("Expected LSNs 1 and 2, got %d and %d", lsn1, lsn2)
	}

	// Read back records to verify correctness
	iter, err := lm.Iterator()
	if err != nil {
		t.Errorf("Error occured %s", err)
	}
	records := readAllRecords(t, iter)
	expected := []string{"record1", "record2"}
	compareRecords(t, records, expected)
}

func verifyRecordsAcrossBlocks(t *testing.T, lm *LogMgr, blockSize int) {
	t.Log("Testing appending records across multiple blocks...")

	// Append enough records to exceed block size
	// Each record is 1/5 of the block
	records := []string{}
	for i := 1; i <= 10; i++ {
		str := string(rune(i))
		record := []byte(str)
		_, _, err := lm.Append(record)
		if err != nil {
			return
		}
		records = append(records, fmt.Sprintf("record%d, %d", i, i*10))
	}

	// Verify all records
	iter, err := lm.Iterator()
	if err != nil {
		t.Errorf("Error occured %s", err)
	}
	readRecords := readAllRecords(t, iter)
	compareRecords(t, readRecords, records)
}

func readAllRecords(t *testing.T, iter utils.Iterator[[]byte]) []string {
	var records []string
	for iter.HasNext() {
		rec, err := iter.Next()
		if err != nil {
			t.Fatalf("Error reading record: %v", err)
		}

		s := string(rec)

		record := fmt.Sprintf("%s", s)
		records = append(records, record)
	}
	return records
}

func compareRecords(t *testing.T, actual, expected []string) {
	if len(actual) != len(expected) {
		t.Errorf("Expected %d records, but got %d", len(expected), len(actual))
	}
	for i, rec := range actual {
		if rec != expected[i] {
			t.Errorf("Expected record %d to be %q, but got %q", i+1, expected[i], rec)
		}
	}
}
