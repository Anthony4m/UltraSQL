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

// DeterministicBufferSimulator wraps BufferMgr to provide controlled testing
type DeterministicBufferSimulator struct {
	bufferMgr *BufferMgr
	testLog   []string
	mu        sync.Mutex
}

// NewDeterministicBufferSimulator creates a simulator for testing
func NewDeterministicBufferSimulator(fm *kfile.FileMgr, lm *log.LogMgr, numbuffs int) *DeterministicBufferSimulator {
	return &DeterministicBufferSimulator{
		bufferMgr: NewBufferMgr(fm, lm, numbuffs),
		testLog:   make([]string, 0),
	}
}

// logEvent adds a thread-safe event to the test log
func (ds *DeterministicBufferSimulator) logEvent(event string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.testLog = append(ds.testLog, event)
}

// Scenario: Basic Buffer Allocation and Deallocation
func TestDeterministicBufferAllocation(t *testing.T) {
	// Mock dependencies (these would typically be created with proper mocks)
	fm := &kfile.FileMgr{} // Mock file manager
	lm := &log.LogMgr{}    // Mock log manager

	// Create a simulator with a fixed number of buffers
	simulator := NewDeterministicBufferSimulator(fm, lm, 5)
	bufferMgr := simulator.bufferMgr

	// Test initial availability
	initialAvailable := bufferMgr.available()
	if initialAvailable != 5 {
		t.Fatalf("Expected 5 initial available buffers, got %d", initialAvailable)
	}

	// Simulate pinning and unpinning
	testBlocks := []*kfile.BlockId{
		{}, {}, {},
	}

	// Pin multiple blocks
	pinnedBuffers := make([]*Buffer, len(testBlocks))
	for i, blk := range testBlocks {
		buff, err := bufferMgr.pin(blk)
		if err != nil {
			t.Fatalf("Failed to pin block %d: %v", i, err)
		}
		pinnedBuffers[i] = buff
	}

	// Check availability decreased
	currentAvailable := bufferMgr.available()
	if currentAvailable != 2 {
		t.Fatalf("Expected 2 available buffers after pinning, got %d", currentAvailable)
	}

	// Unpin buffers
	for _, buff := range pinnedBuffers {
		bufferMgr.unpin(buff)
	}

	// Verify availability is back to initial state
	finalAvailable := bufferMgr.available()
	if finalAvailable != 5 {
		t.Fatalf("Expected 5 available buffers after unpinning, got %d", finalAvailable)
	}
}

// Scenario: Concurrent Buffer Access Simulation
func TestDeterministicConcurrentBufferAccess(t *testing.T) {
	fm := &kfile.FileMgr{} // Mock file manager
	lm := &log.LogMgr{}    // Mock log manager

	// Create a simulator with a limited number of buffers
	simulator := NewDeterministicBufferSimulator(fm, lm, 3)
	bufferMgr := simulator.bufferMgr

	// Concurrent pinning and unpinning simulation
	var wg sync.WaitGroup
	concurrentPins := 10

	for i := 0; i < concurrentPins; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create a unique block for each goroutine
			blk := &kfile.BlockId{}

			// Attempt to pin
			buff, err := bufferMgr.pin(blk)
			if err != nil {
				simulator.logEvent(fmt.Sprintf("Pin failed for goroutine %d", id))
				return
			}

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			// Unpin
			bufferMgr.unpin(buff)
			simulator.logEvent(fmt.Sprintf("Goroutine %d completed pin/unpin", id))
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify final buffer availability
	finalAvailable := bufferMgr.available()
	if finalAvailable != 3 {
		t.Fatalf("Expected 3 available buffers at end, got %d", finalAvailable)
	}
}

// Scenario: Buffer Overflow Handling
func TestDeterministicBufferOverflow(t *testing.T) {
	fm := &kfile.FileMgr{} // Mock file manager
	lm := &log.LogMgr{}    // Mock log manager

	// Create a simulator with very limited buffers
	bufferCount := 2
	bufferMgr := NewBufferMgr(fm, lm, bufferCount)

	// Create more pin requests than available buffers
	blocks := make([]*kfile.BlockId, bufferCount+2)
	for i := range blocks {
		blocks[i] = &kfile.BlockId{}
	}

	// First two pins should succeed
	firstBuffers := make([]*Buffer, bufferCount)
	for i := 0; i < bufferCount; i++ {
		buff, err := bufferMgr.pin(blocks[i])
		if err != nil {
			t.Fatalf("Failed to pin block %d: %v", i, err)
		}
		firstBuffers[i] = buff
	}

	// Next pin should timeout or fail
	_, err := bufferMgr.pin(blocks[bufferCount])
	if err == nil {
		t.Fatal("Expected an error when pinning beyond buffer limit")
	}

	// Unpin first buffers
	for _, buff := range firstBuffers {
		bufferMgr.unpin(buff)
	}
}

// Benchmark Buffer Manager Performance
func BenchmarkBufferManagerConcurrency(b *testing.B) {
	fm := &kfile.FileMgr{} // Mock file manager
	lm := &log.LogMgr{}    // Mock log manager

	bufferMgr := NewBufferMgr(fm, lm, 10)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		blk := &kfile.BlockId{}
		for pb.Next() {
			buff, err := bufferMgr.pin(blk)
			if err == nil {
				bufferMgr.unpin(buff)
			}
		}
	})
}
