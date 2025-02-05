package buffer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"ultraSQL/kfile"
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
	//policy := InitLRU(3, fm)
	policy := InitClock(3, fm)
	bm := NewBufferMgr(fm, 3, policy)
	filename := "bufferTest.db"
	//blk1, _ := fm.Append("bufferTest.db")

	buff1, _ := bm.Pin(kfile.NewBlockId("bufferTest.db", 1))
	p := buff1.Contents()
	n, err := p.GetInt(80)
	if err != nil {
		fmt.Printf("An error occurred %s", err)
	}
	p.SetInt(80, n+1)
	buff1.MarkModified(1, 0)
	fmt.Printf("The new value is %d", n+1)
	bm.Unpin(buff1)

	buff2, _ := bm.Pin(kfile.NewBlockId("bufferTest", 2))
	buff3, _ := bm.Pin(kfile.NewBlockId("bufferTest", 3))
	buff4, _ := bm.Pin(kfile.NewBlockId("bufferTest", 4))
	fmt.Println(buff3, buff4)

	bm.Unpin(buff2)
	buff2, _ = bm.Pin(kfile.NewBlockId(filename, 1))
	p2 := buff2.Contents()
	p2.SetInt(80, 9999)
	buff2.MarkModified(1, 0)
	bm.Unpin(buff2)
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
	//policy := InitLRU(3, fm)
	policy := InitClock(3, fm)
	bufferMgr := NewBufferMgr(fm, 3, policy)

	if bufferMgr.Available() != 3 {
		t.Errorf("Expected 3 Available buffers, got %d", bufferMgr.Available())
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
	policy := InitClock(2, fm)
	//policy := InitLRU(2, fm)
	bufferMgr := NewBufferMgr(fm, 2, policy)

	//blk1 := &kfile.BlockId{Filename: "file1", Blknum: 1}
	//blk2 := &kfile.BlockId{Filename: "file1", Blknum: 2}

	blk1, err := fm.Append("file1")
	blk2, err := fm.Append("file2")

	// Pin first block
	buf1, _ := bufferMgr.Pin(blk1)
	if buf1 == nil {
		t.Fatal("Failed to Pin blk for block 1")
	}
	if bufferMgr.Available() != 1 {
		t.Errorf("Expected 1 Available blk, got %d", bufferMgr.Available())
	}

	// Pin second block
	buf2, _ := bufferMgr.Pin(blk2)
	if buf2 == nil {
		t.Fatal("Failed to Pin blk for block 2")
	}
	if bufferMgr.Available() != 0 {
		t.Errorf("Expected 0 Available buffers, got %d", bufferMgr.Available())
	}

	// Unpin first block
	bufferMgr.Unpin(buf1)
	if bufferMgr.Available() != 1 {
		t.Errorf("Expected 1 Available blk after UnPin, got %d", bufferMgr.Available())
	}
}

// TestPinTimeout tests pinning with a timeout.
func TestPinTimeout(t *testing.T) {
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
	policy := InitLRU(1, fm)
	bufferMgr := NewBufferMgr(fm, 1, policy)

	blk1, err := fm.Append("file1")
	blk2, err := fm.Append("file2")
	blk3, err := fm.Append("file3")

	// Pin the only Available blk
	buf1, _ := bufferMgr.Pin(blk1)
	if buf1 == nil {
		t.Fatal("Failed to Pin blk for block 1")
	}

	// Attempt to Pin another block, which should time out
	start := time.Now()
	buf2, _ := bufferMgr.Pin(blk2)
	buf3, _ := bufferMgr.Pin(blk3)
	fmt.Println(buf2)
	if buf3 != nil {
		t.Error("Expected nil blk due to timeout, but got a blk")
	}
	if time.Since(start) < MaxTime {
		t.Errorf("Expected wait time to be at least %v, but got %v", MaxTime, time.Since(start))
	}
}

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
	//policy := InitLRU(2, fm)
	policy := InitClock(2, fm)
	bufferMgr := NewBufferMgr(fm, 2, policy)

	blk1 := &kfile.BlockId{Filename: "file1", Blknum: 1}

	// Pin and modify a blk
	buf1, _ := bufferMgr.Pin(blk1)
	if buf1 == nil {
		t.Fatal("Failed to Pin blk for block 1")
	}

	bufferMgr.Policy().FlushAll(0) // Mock logic to Flush based on txid

	// Verify no crash and potential mock Flush calls
}

// DeterministicBufferSimulator wraps BufferMgr to provide controlled testing
type DeterministicBufferSimulator struct {
	bufferMgr *BufferMgr
	testLog   []string
	mu        sync.Mutex
}

// NewDeterministicBufferSimulator creates a simulator for testing
func NewDeterministicBufferSimulator(fm *kfile.FileMgr, numbuffs int) *DeterministicBufferSimulator {
	policy := InitLRU(numbuffs, fm)
	return &DeterministicBufferSimulator{
		bufferMgr: NewBufferMgr(fm, numbuffs, policy),
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

	// Create a simulator with a fixed number of buffers
	simulator := NewDeterministicBufferSimulator(fm, 5)
	bufferMgr := simulator.bufferMgr

	// Test initial availability
	initialAvailable := bufferMgr.Available()
	if initialAvailable != 5 {
		t.Fatalf("Expected 5 initial Available buffers, got %d", initialAvailable)
	}
	// Simulate pinning and unpinning
	testBlocks := []*kfile.BlockId{
		{Filename: "file1", Blknum: 1}, {Filename: "file1", Blknum: 2}, {Filename: "file1", Blknum: 3},
	}

	// Pin multiple blocks
	pinnedBuffers := make([]*Buffer, len(testBlocks))
	for i, blk := range testBlocks {
		buff, err := bufferMgr.Pin(blk)
		if err != nil {
			t.Fatalf("Failed to Pin block %d: %v", i, err)
		}
		pinnedBuffers[i] = buff
	}

	// Check availability decreased
	currentAvailable := bufferMgr.Available()
	if currentAvailable != 2 {
		t.Fatalf("Expected 2 Available buffers after pinning, got %d", currentAvailable)
	}

	// Unpin buffers
	for _, buff := range pinnedBuffers {
		bufferMgr.Unpin(buff)
	}

	// Verify availability is back to initial state
	finalAvailable := bufferMgr.Available()
	if finalAvailable != 5 {
		t.Fatalf("Expected 5 Available buffers after unpinning, got %d", finalAvailable)
	}
}

// Benchmark Buffer Manager Performance
func BenchmarkBufferManagerConcurrency(b *testing.B) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	blockSize := 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		b.Fatalf("Failed to create FileMgr: %v", err)
	}
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()
	policy := InitLRU(10, fm)
	bufferMgr := NewBufferMgr(fm, 10, policy)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		blk := &kfile.BlockId{Filename: "file1", Blknum: 1}
		for pb.Next() {
			buff, err := bufferMgr.Pin(blk)
			if err == nil {
				bufferMgr.Unpin(buff)
			}
		}
	})
}

// Scenario: Concurrent Buffer Access Simulation
func TestDeterministicConcurrentBufferAccess(t *testing.T) {
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
	// Create a simulator with a limited number of buffers
	simulator := NewDeterministicBufferSimulator(fm, 3)
	bufferMgr := simulator.bufferMgr

	// Concurrent pinning and unpinning simulation
	var wg sync.WaitGroup
	concurrentPins := 10

	for i := 0; i < concurrentPins; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create a unique block for each goroutine
			blk := &kfile.BlockId{Filename: "file1", Blknum: 1}

			// Attempt to Pin
			buff, err := bufferMgr.Pin(blk)
			if err != nil {
				simulator.logEvent(fmt.Sprintf("Pin failed for goroutine %d", id))
				return
			}

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			// Unpin
			bufferMgr.Unpin(buff)
			simulator.logEvent(fmt.Sprintf("Goroutine %d completed Pin/UnPin", id))
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify final blk availability
	finalAvailable := bufferMgr.Available()
	if finalAvailable != 3 {
		t.Fatalf("Expected 3 Available buffers at end, got %d", finalAvailable)
	}
}

// Scenario: Buffer Overflow Handling
func TestDeterministicBufferOverflow(t *testing.T) {
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

	// Create a simulator with very limited buffers
	bufferCount := 2
	policy := InitLRU(bufferCount, fm)
	bufferMgr := NewBufferMgr(fm, bufferCount, policy)

	// Create more Pin requests than Available buffers
	blocks := make([]*kfile.BlockId, bufferCount+6)
	for i := range blocks {
		blocks[i] = &kfile.BlockId{
			Filename: "file2",
			Blknum:   i,
		}
	}

	// First two pins should succeed
	firstBuffers := make([]*Buffer, bufferCount)
	for i := 0; i < bufferCount; i++ {
		buff, err := bufferMgr.Pin(blocks[i])
		if err != nil {
			t.Fatalf("Failed to Pin block %d: %v", i, err)
		}
		firstBuffers[i] = buff
	}

	_, pinErr := bufferMgr.Pin(blocks[bufferCount])
	if pinErr == nil {
		t.Errorf("Expected an abortion got a block: %v", pinErr)
	}

	bufferMgr.Unpin(firstBuffers[0])
}
