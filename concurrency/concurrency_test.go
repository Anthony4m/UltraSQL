package concurrency

import (
	"testing"
	"ultraSQL/kfile"
)

// TestConcurrencyManager provides a simple, single-threaded test of the
// ConcurrencyMgr and LockTable logic.
func TestConcurrencyManager(t *testing.T) {
	// 1) Create a concurrency manager.
	cm := NewConcurrencyMgr()
	if cm == nil {
		t.Fatalf("Failed to create ConcurrencyMgr")
	}

	// 2) Create a dummy block to lock.
	blk := kfile.NewBlockId("testfile", 0)

	// -------------------------------------------------------------------------
	// Test: Acquire a shared lock (SLock)
	// -------------------------------------------------------------------------
	if err := cm.SLock(*blk); err != nil {
		t.Errorf("SLock failed: %v", err)
	}

	lockType, exists := cm.GetLockType(*blk)
	if !exists {
		t.Errorf("Expected a lock to exist, but none found for block %v", blk)
	} else if lockType != "S" {
		t.Errorf("Expected shared lock (S), got %s", lockType)
	}

	if err := cm.SLock(*blk); err != nil {
		t.Errorf("SLock failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// Test: Acquire exclusive lock (XLock) on same block
	// (Should upgrade from S to X if there's a single shared lock.)
	// -------------------------------------------------------------------------
	if err := cm.XLock(*blk); err == nil {
		t.Errorf("XLock failed (upgrade from S): %v", err)
	}

	lockType, exists = cm.GetLockType(*blk)
	if !exists {
		t.Errorf("Expected a lock to exist after XLock, but none found for block %v", blk)
	} else if lockType != "X" {
		t.Errorf("Expected exclusive lock (X), got %s", lockType)
	}

	if err := cm.SLock(*blk); err != nil {
		t.Errorf("SLock failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// Test: Release all locks.
	// -------------------------------------------------------------------------
	if err := cm.Release(); err != nil {
		t.Errorf("Release failed: %v", err)
	}

	lockType, exists = cm.GetLockType(*blk)
	if exists {
		t.Errorf("Expected no lock after Release, found lock type %s", lockType)
	}
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
