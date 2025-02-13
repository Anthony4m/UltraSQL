package concurrency

import (
	"sync"
	"testing"
	"time"
	"ultraSQL/kfile"
)

// TestConcurrencyManagerConcurrent demonstrates a "better" test that
// actually exercises concurrency: multiple readers, then an exclusive writer.
func TestConcurrencyManagerConcurrent(t *testing.T) {
	cm := NewConcurrencyMgr()
	blk := kfile.NewBlockId("testfile", 42)

	var wg sync.WaitGroup

	// Number of concurrent readers
	numReaders := 3

	// Start multiple reader goroutines (each one acquires SLock, simulates read, then releases).
	for i := 1; i <= numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Acquire shared lock
			if err := cm.SLock(*blk); err != nil {
				t.Errorf("[Reader %d] Failed to SLock: %v", readerID, err)
				return
			}
			t.Logf("[Reader %d] Acquired SLock", readerID)

			// Simulate reading
			time.Sleep(100 * time.Millisecond)

			// Release
			if err := cm.Release(); err != nil {
				t.Errorf("[Reader %d] Failed to release: %v", readerID, err)
				return
			}
			t.Logf("[Reader %d] Released SLock", readerID)
		}(i)
	}

	// Give readers a moment to start and (likely) acquire their SLocks
	time.Sleep(50 * time.Millisecond)

	// Start one writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Attempt to acquire an exclusive lock
		if err := cm.XLock(*blk); err != nil {
			t.Errorf("[Writer] Failed to XLock: %v", err)
			return
		}
		t.Logf("[Writer] Acquired XLock")

		// Simulate writing
		time.Sleep(200 * time.Millisecond)

		// Release
		if err := cm.Release(); err != nil {
			t.Errorf("[Writer] Failed to release after XLock: %v", err)
			return
		}
		t.Logf("[Writer] Released XLock")
	}()

	// Wait for all goroutines to finish
	wg.Wait()

	// At this point, the test completes successfully if no deadlock has occurred
	// and if all lock acquisitions/release calls returned successfully.
	t.Log("All readers and writer completed without deadlock.")
}

// TestLockTableDirect tests LockTable directly if desired.
func TestLockTableDirect(t *testing.T) {
	lt := NewLockTable()
	blk := kfile.NewBlockId("testfile", 1)

	// Acquire shared lock
	if err := lt.SLock(*blk); err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}
	lockType, count := lt.GetLockInfo(*blk)
	if lockType != "shared" || count != 1 {
		t.Errorf("Expected shared lock count=1, got type=%s count=%d", lockType, count)
	}

	// Acquire exclusive lock (upgrade)
	if err := lt.XLock(*blk); err != nil {
		t.Fatalf("Failed to upgrade to exclusive lock: %v", err)
	}
	lockType, count = lt.GetLockInfo(*blk)
	if lockType != "exclusive" {
		t.Errorf("Expected exclusive lock, got %s", lockType)
	}

	// Unlock
	if err := lt.Unlock(*blk); err != nil {
		t.Fatalf("Failed to Unlock: %v", err)
	}
	lockType, count = lt.GetLockInfo(*blk)
	if lockType != "none" || count != 0 {
		t.Errorf("Expected no lock after Unlock, got type=%s count=%d", lockType, count)
	}
}
