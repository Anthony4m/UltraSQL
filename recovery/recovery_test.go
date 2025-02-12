package recovery_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"
	"ultraSQL/transaction"

	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"ultraSQL/log"
	"ultraSQL/log_record"
	"ultraSQL/recovery"
)

// 1) A minimal dummy Tx that implements txinterface.TxInterface.
type dummyTx struct {
	txNum int64
}

// GetTxNum is the only method required by our interface here.
func (d *dummyTx) GetTxNum() int64 {
	return d.txNum
}

// TestRecoveryMgrLifecycle verifies that RecoveryMgr appends
// the correct log records (START, COMMIT, ROLLBACK, CHECKPOINT)
// and flushes them to disk.
func TestRecoveryMgrLifecycle(t *testing.T) {
	// Setup: Create a temporary directory for file storage.
	tempDir := filepath.Join(os.TempDir(), "ultraSQL_test_"+time.Now().Format("20060102150405"))
	blockSize := 4096

	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	// Create a small LRU-based buffer manager.
	policy := buffer.InitLRU(2, fm)
	bm := buffer.NewBufferMgr(fm, 2, policy)

	// Create a log manager.
	lm, err := log.NewLogMgr(fm, bm, "log_test.db")
	if err != nil {
		t.Fatalf("Failed to create LogMgr: %v", err)
	}
	// Create the transaction manager.
	txMgr := transaction.NewTransaction(fm, lm, bm)
	if txMgr == nil {
		t.Fatal("Transaction manager is nil")
	}

	// 3) Create the RecoveryMgr for our dummy transaction.
	rm := recovery.NewRecoveryMgr(txMgr, txMgr.GetTxNum(), lm, bm)
	if rm == nil {
		t.Fatal("Expected a new RecoveryMgr, got nil")
	}

	// Start record should have been written. We'll verify it shortly.

	// 4) Test Commit
	if err := rm.Commit(); err != nil {
		t.Errorf("Commit returned error: %v", err)
	}

	// 5) Test Rollback
	if err := rm.Rollback(); err != nil {
		t.Errorf("Rollback returned error: %v", err)
	}

	// 6) Test Recover
	if err := rm.Recover(); err != nil {
		t.Errorf("Recover returned error: %v", err)
	}

	// 7) Now read the log records in reverse order (iterator is typically LIFO).
	iter, err := lm.Iterator()
	if err != nil {
		t.Fatalf("Failed to create log iterator: %v", err)
	}

	// We'll collect the operations we see, in the order we see them.
	var ops []int32

	for iter.HasNext() {
		recordData, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to read log record: %v", err)
		}
		rec := log_record.CreateLogRecord(recordData)
		if rec == nil {
			t.Fatalf("Failed to parse log record from data")
		}
		ops = append(ops, rec.Op())
	}

	// Because the iterator is LIFO, the last written record is first in ops.
	// We expect (in write order): START, COMMIT, ROLLBACK, CHECKPOINT
	// So in LIFO order: CHECKPOINT, ROLLBACK, COMMIT, START

	if len(ops) < 4 {
		t.Fatalf("Expected at least 4 records (START, COMMIT, ROLLBACK, CHECKPOINT), found %d", len(ops))
	}

	if ops[0] != log_record.CHECKPOINT {
		t.Errorf("Expected first log record to be CHECKPOINT, got %v", ops[0])
	}
	if ops[1] != log_record.ROLLBACK {
		t.Errorf("Expected second log record to be ROLLBACK, got %v", ops[1])
	}
	if ops[2] != log_record.COMMIT {
		t.Errorf("Expected third log record to be COMMIT, got %v", ops[2])
	}
	if ops[3] != log_record.START {
		t.Errorf("Expected fourth log record to be START, got %v", ops[3])
	}
}
