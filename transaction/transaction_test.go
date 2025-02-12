package transaction

import (
	"os"
	"path/filepath"
	"testing"
	"time"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"ultraSQL/log"
	"ultraSQL/log_record"
)

func TestLogRecordLifecycle(t *testing.T) {
	// Setup: create a temporary directory for the file manager.
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	blockSize := 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	// Clean up after the test.
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	// Initialize a small LRU policy and a BufferMgr with capacity 2.
	policy := buffer.InitLRU(2, fm)
	bm := buffer.NewBufferMgr(fm, 2, policy)

	// Create a LogMgr (assuming it takes a FileMgr, BufferMgr, and log file name).
	lm, err := log.NewLogMgr(fm, bm, "log_test.db")
	if err != nil {
		t.Fatalf("Failed to create LogMgr: %v", err)
	}

	// Define expected op codes.
	const (
		CHECKPOINT = iota
		START
		COMMIT
		ROLLBACK
		SETINT
		SETSTRING
		UNIFIEDUPDATE
	)

	// Test the full log record lifecycle.
	t.Run("Full log record lifecycle", func(t *testing.T) {
		// 1. Write several records.
		// Here we create a slice of expected records with their txNum and op code.
		expectedRecords := []struct {
			txNum int64
			op    int32
		}{
			{1, START},
			{1, COMMIT},
			{2, START},
			{2, ROLLBACK},
		}

		// For each expected record, create a log record (using your log_record package)
		// and append it to the log manager.
		for _, expected := range expectedRecords {
			var record log_record.Ilog_record // assuming LogRecord is your interface
			switch expected.op {
			case START:
				record = log_record.NewStartRecord(expected.txNum)
			case COMMIT:
				record = log_record.NewCommitRecord(expected.txNum)
			case ROLLBACK:
				record = log_record.NewRollbackRecord(expected.txNum)
				// You can add additional cases if needed.
			}

			// Append the serialized record.
			_, _, err := lm.Append(record.ToBytes())
			if err != nil {
				t.Fatalf("Failed to append record: %v", err)
			}
		}

		// 2. Flush the log so that all records are persisted.
		if err := lm.Flush(); err != nil {
			t.Fatalf("Failed to flush log: %v", err)
		}

		// 3. Create an iterator and read back the records.
		iter, err := lm.Iterator()
		if err != nil {
			t.Fatalf("Failed to create log iterator: %v", err)
		}

		// We expect the records in reverse order (assuming the log iterator returns records in LIFO order).
		recordCount := len(expectedRecords) - 1
		for iter.HasNext() {
			recordData, err := iter.Next()
			if err != nil {
				t.Fatalf("Failed to get next record: %v", err)
			}

			record := log_record.CreateLogRecord(recordData)
			if record == nil {
				t.Fatalf("Failed to create record from data")
			}

			if recordCount < 0 {
				t.Errorf("Found more records than expected")
				break
			}

			expected := expectedRecords[recordCount]
			if record.Op() != expected.op {
				t.Errorf("Record %d: expected op %v, got %v", recordCount, expected.op, record.Op())
			}
			if record.TxNumber() != expected.txNum {
				t.Errorf("Record %d: expected txNum %v, got %v", recordCount, expected.txNum, record.TxNumber())
			}
			recordCount--
		}

		if recordCount >= 0 {
			t.Errorf("Expected %d records, found %d", len(expectedRecords), len(expectedRecords)-recordCount-1)
		}
	})
}

func TestTransactionManagerLifecycle(t *testing.T) {
	// Create a temporary directory for file storage.
	tempDir := filepath.Join(os.TempDir(), "ultraSQL_test_"+time.Now().Format("20060102150405"))
	blockSize := 8192

	// Create a file manager.
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	// Create a small buffer pool (capacity 2) using an LRU policy.
	policy := buffer.InitLRU(2, fm)
	bm := buffer.NewBufferMgr(fm, 2, policy)

	// Create a log manager.
	lm, err := log.NewLogMgr(fm, bm, "log_test.db")
	if err != nil {
		t.Fatalf("Failed to create LogMgr: %v", err)
	}

	// Create the transaction manager.
	txMgr := NewTransaction(fm, lm, bm)
	if txMgr == nil {
		t.Fatal("Transaction manager is nil")
	}

	// Optionally, print initial txMgr state.
	t.Logf("Created TransactionMgr with txnum=%d", txMgr.txNum)

	// Test Commit: it should not return an error.
	if err := txMgr.Commit(); err != nil {
		t.Errorf("Commit returned error: %v", err)
	}

	// Test Rollback: it should not return an error.
	if err := txMgr.Rollback(); err != nil {
		t.Errorf("Rollback returned error: %v", err)
	}

	// Test InsertCell:
	// Create a dummy block (for example, "testfile" and block number 0).
	blk := kfile.NewBlockId("testfile", 0)
	// Lock the block using the concurrency manager.
	if err := txMgr.cm.XLock(*blk); err != nil {
		t.Errorf("Failed to acquire XLock on block %v: %v", blk, err)
	}

	// Insert a cell with a dummy key and value.
	key := []byte("testkey")
	val := "testvalue"
	if err := txMgr.InsertCell(*blk, key, val, true); err != nil {
		t.Errorf("InsertCell returned error: %v", err)
	} else {
		t.Log("InsertCell succeeded.")
	}

	// Optionally, test finding the cell.
	cell := txMgr.FindCell(*blk, key)
	if cell == nil {
		t.Errorf("Failed to find cell with key %s in block %v", key, blk)
	} else {
		t.Logf("Found cell with key %s in block %v", key, blk)
	}

	// Additional tests (Recover, Pin/Unpin, etc.) can be added here.
}
