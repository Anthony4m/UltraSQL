package buffer

import (
	"awesomeDB/kfile"
	"awesomeDB/log"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestBuffer(t *testing.T) {
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

	filename := "bufferTest.db"
	lm, _ := log.NewLogMgr(fm, "logfile.db")
	bm := NewBufferMgr(fm, lm, 3)
	//blk1, _ := fm.Append("bufferTest.db")

	buff1, _ := bm.pin(kfile.NewBlockId("bufferTest.db", 1))
	p := buff1.GetContents()
	n, err := p.GetInt(80)
	if err != nil {
		fmt.Printf("An error occurred %s", err)
	}
	p.SetInt(80, n+1)
	buff1.MarkModified(1, 0)
	fmt.Printf("The new value is %d", n+1)
	bm.unpin(buff1)

	buff2, _ := bm.pin(kfile.NewBlockId("bufferTest", 2))
	buff3, _ := bm.pin(kfile.NewBlockId("bufferTest", 3))
	buff4, _ := bm.pin(kfile.NewBlockId("bufferTest", 4))
	fmt.Println(buff3, buff4)

	bm.unpin(buff2)
	buff2, _ = bm.pin(kfile.NewBlockId(filename, 1))
	p2 := buff2.GetContents()
	p2.SetInt(80, 9999)
	buff2.MarkModified(1, 0)
	bm.unpin(buff2)
}

// MockBuffer is a mock implementation of the Buffer interface.
type MockBuffer struct {
	block      *kfile.BlockId
	pinned     bool
	pinnedLock sync.Mutex
}

func (mb *MockBuffer) assignToBlock(blk *kfile.BlockId) {
	mb.block = blk
}

func (mb *MockBuffer) unpin() {
	mb.pinnedLock.Lock()
	defer mb.pinnedLock.Unlock()
	mb.pinned = false
}

func (mb *MockBuffer) IsPinned() bool {
	mb.pinnedLock.Lock()
	defer mb.pinnedLock.Unlock()
	return mb.pinned
}

func (mb *MockBuffer) Block() *kfile.BlockId {
	return mb.block
}

func (mb *MockBuffer) flush() {}

func (mb *MockBuffer) modifyingTx() int {
	return 0
}

// TestNewBufferMgr tests the initialization of a BufferMgr.
func TestNewBufferMgr(t *testing.T) {
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

	lm, _ := log.NewLogMgr(fm, "logfile.db")
	bufferMgr := NewBufferMgr(fm, lm, 3)

	if bufferMgr.available() != 3 {
		t.Errorf("Expected 3 available buffers, got %d", bufferMgr.available())
	}
}

// TestPinAndUnpin tests the pinning and unpinning of buffers.
func TestPinAndUnpin(t *testing.T) {
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

	lm, _ := log.NewLogMgr(fm, "logfile.db")
	bufferMgr := NewBufferMgr(fm, lm, 2)

	//blk1 := &kfile.BlockId{Filename: "file1", Blknum: 1}
	//blk2 := &kfile.BlockId{Filename: "file1", Blknum: 2}

	blk1, err := fm.Append("file1")
	blk2, err := fm.Append("file2")

	// Pin first block
	buf1, _ := bufferMgr.pin(blk1)
	if buf1 == nil {
		t.Fatal("Failed to pin buffer for block 1")
	}
	if bufferMgr.available() != 1 {
		t.Errorf("Expected 1 available buffer, got %d", bufferMgr.available())
	}

	// Pin second block
	buf2, _ := bufferMgr.pin(blk2)
	if buf2 == nil {
		t.Fatal("Failed to pin buffer for block 2")
	}
	if bufferMgr.available() != 0 {
		t.Errorf("Expected 0 available buffers, got %d", bufferMgr.available())
	}

	// Unpin first block
	bufferMgr.unpin(buf1)
	if bufferMgr.available() != 1 {
		t.Errorf("Expected 1 available buffer after unpin, got %d", bufferMgr.available())
	}
}

// TestPinTimeout tests pinning with a timeout.
//func TestPinTimeout(t *testing.T) {
//	// Setup
//	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
//	blockSize := 400
//	fm, err := kfile.NewFileMgr(tempDir, blockSize)
//	if err != nil {
//		t.Fatalf("Failed to create FileMgr: %v", err)
//	}
//	defer func() {
//		fm.Close()
//		os.RemoveAll(tempDir)
//	}()
//
//	lm, _ := log.NewLogMgr(fm, "logfile.db")
//	bufferMgr := NewBufferMgr(fm, lm, 1)
//
//	blk1, err := fm.Append("file1")
//	blk2, err := fm.Append("file2")
//	blk3, err := fm.Append("file3")
//
//	// Pin the only available buffer
//	buf1, _ := bufferMgr.pin(blk1)
//	if buf1 == nil {
//		t.Fatal("Failed to pin buffer for block 1")
//	}
//
//	// Attempt to pin another block, which should time out
//	start := time.Now()
//	buf2, _ := bufferMgr.pin(blk2)
//	buf3, _ := bufferMgr.pin(blk3)
//	fmt.Println(buf2)
//	if buf3 != nil {
//		t.Error("Expected nil buffer due to timeout, but got a buffer")
//	}
//	if time.Since(start) < MAX_TIME {
//		t.Errorf("Expected wait time to be at least %v, but got %v", MAX_TIME, time.Since(start))
//	}
//}

// TestFlushAll tests flushing buffers for a specific transaction.
func TestFlushAll(t *testing.T) {
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

	lm, _ := log.NewLogMgr(fm, "logfile.db")
	bufferMgr := NewBufferMgr(fm, lm, 2)

	blk1 := &kfile.BlockId{Filename: "file1", Blknum: 1}

	// Pin and modify a buffer
	buf1, _ := bufferMgr.pin(blk1)
	if buf1 == nil {
		t.Fatal("Failed to pin buffer for block 1")
	}

	bufferMgr.FlushAll(0) // Mock logic to flush based on txid

	// Verify no crash and potential mock flush calls
}
