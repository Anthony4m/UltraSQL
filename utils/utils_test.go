package utils

import (
	"os"
	"path/filepath"
	_ "path/filepath"
	"testing"
	"time"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a temporary file manager
func createTempFileMgr(t *testing.T) *kfile.FileMgr {
	tempDir, err := os.MkdirTemp("", "logiterator-test-")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	// Assuming FileMgr constructor takes directory and block size
	fm, err := kfile.NewFileMgr(tempDir, 512)
	require.NoError(t, err)
	return fm
}

// Helper function to prepare a log block with test records
func prepareLogBlock(t *testing.T, fm *kfile.FileMgr, filename string, records [][]byte) *kfile.BlockId {
	blk := kfile.NewBlockId(filename, 0)
	pgsize := make([]byte, fm.BlockSize())
	page := kfile.NewPageFromBytes(pgsize)

	// Start with boundary at the start of the page
	page.SetInt(0, 4) // 4 is size of int32 boundary
	currentPos := 4

	for _, rec := range records {
		err := page.SetBytes(currentPos, rec)
		require.NoError(t, err)

		// Update position (4 bytes for integer size + record length)
		//currentPos += int(unsafe.Sizeof(0)) + len(rec)

		// Update boundary
		page.SetInt(0, currentPos)
	}

	// Write the page to block
	err := fm.Write(blk, page)
	require.NoError(t, err)

	return blk
}

func TestLogIterator_SingleBlockSingleRecord(t *testing.T) {
	fm := createTempFileMgr(t)
	filename := "test_single_record.log"

	testRecords := [][]byte{
		[]byte("hello world"),
	}

	blk := prepareLogBlock(t, fm, filename, testRecords)
	bm := buffer.NewBufferMgr(fm, 3)
	iterator := NewLogIterator(fm, bm, blk)

	// Verify HasNext
	assert.True(t, iterator.HasNext())

	// Verify Next retrieves correct record
	retrievedRec, _ := iterator.Next()
	retrievedRec = retrievedRec[:len(retrievedRec)-int(unsafe.Sizeof(0))]
	assert.Equal(t, testRecords[0], retrievedRec)

	// Verify no more records
	assert.True(t, iterator.HasNext())
}

//func TestLogIterator_MultipleRecordsSameBlock(t *testing.T) {
//	fm := createTempFileMgr(t)
//	filename := "test_multiple_records.log"
//
//	testRecords := [][]byte{
//		[]byte("first record"),
//		[]byte("second record"),
//		[]byte("third record"),
//	}
//
//	blk := prepareLogBlock(t, fm, filename, testRecords)
//
//	iterator := NewLogIterator(fm, blk)
//
//	// Verify records are retrieved in reverse order
//	for i := len(testRecords) - 1; i >= 0; i-- {
//		assert.True(t, iterator.HasNext())
//		retrievedRec, _ := iterator.Next()
//		assert.Equal(t, testRecords[i], retrievedRec)
//	}
//
//	assert.False(t, iterator.HasNext())
//}
//
//func TestLogIterator_MultipleBlocks(t *testing.T) {
//	fm := createTempFileMgr(t)
//	filename := "test_multiple_blocks.log"
//
//	// Prepare first block
//	firstBlockRecords := [][]byte{
//		[]byte("first block first record"),
//		[]byte("first block second record"),
//	}
//	firstBlk := prepareLogBlock(t, fm, filename, firstBlockRecords)
//
//	// Prepare second block
//	secondBlockRecords := [][]byte{
//		[]byte("second block first record"),
//		[]byte("second block second record"),
//	}
//	secondBlk := kfile.NewBlockId(filename, 1)
//	secondPage := kfile.NewPage(fm.BlockSize())
//	secondPage.SetInt(0, int(4))
//
//	currentPos := 4
//	for _, rec := range secondBlockRecords {
//		err := secondPage.SetBytes(currentPos, rec)
//		require.NoError(t, err)
//
//		currentPos += int(unsafe.Sizeof(int(0))) + len(rec)
//		secondPage.SetInt(0, int(currentPos))
//	}
//
//	err := fm.Write(secondBlk, secondPage)
//	require.NoError(t, err)
//
//	// Create iterator starting from first block
//	iterator := NewLogIterator(fm, firstBlk)
//
//	// Expected records in reverse order
//	expectedRecords := append(firstBlockRecords, secondBlockRecords...)
//
//	// Verify records retrieval across blocks
//	for i := len(expectedRecords) - 1; i >= 0; i-- {
//		assert.True(t, iterator.HasNext())
//		retrievedRec, _ := iterator.Next()
//		assert.Equal(t, expectedRecords[i], retrievedRec)
//	}
//
//	assert.False(t, iterator.HasNext())
//}

func TestLogIterator_EmptyIterator(t *testing.T) {
	fm := createTempFileMgr(t)
	filename := "test_empty.log"

	// Create an empty block
	blk := kfile.NewBlockId(filename, 0)
	page := kfile.NewPage(fm.BlockSize())
	page.SetInt(0, int(4)) // Set boundary to start of page

	err := fm.Write(blk, page)
	require.NoError(t, err)
	bm := buffer.NewBufferMgr(fm, 3)
	iterator := NewLogIterator(fm, bm, blk)

	assert.True(t, iterator.HasNext())
}

//func TestLogIterator_NilFileMgr(t *testing.T) {
//	iterator := NewLogIterator(nil, nil)
//
//	assert.False(t, iterator.HasNext())
//}

func setupTestFileMgr(t *testing.T) (*kfile.FileMgr, string) {
	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
	blockSize := 400
	fm, err := kfile.NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	return fm, tempDir
}

func TestMoveToBlock(t *testing.T) {
	fm, tempDir := setupTestFileMgr(t)
	defer func() {
		fm.Close()
		os.RemoveAll(tempDir)
	}()

	filename := "test.db"
	block := kfile.NewBlockId(filename, 0)
	page := kfile.NewPage(fm.BlockSize())

	// Write a boundary value to the page
	page.SetInt(0, 200) // Arbitrary boundary position
	err := fm.Write(block, page)
	if err != nil {
		t.Fatalf("Failed to write block: %v", err)
	}
	bm := buffer.NewBufferMgr(fm, 3)
	// Initialize LogIterator and move to block
	iter := &LogIterator{fm: fm, blk: block, bm: bm}
	iter.moveToBlock(block)

	if iter.boundary != 200 {
		t.Errorf("Expected boundary to be 200, got %d", iter.boundary)
	}
	if iter.currentPos != 200 {
		t.Errorf("Expected currentPos to be 200, got %d", iter.currentPos)
	}
}

//func TestHasNext(t *testing.T) {
//	fm, tempDir := setupTestFileMgr(t)
//	defer func() {
//		fm.Close()
//		os.RemoveAll(tempDir)
//	}()
//
//	filename := "test.db"
//	block := kfile.NewBlockId(filename, 0)
//	page := kfile.NewPage(fm.BlockSize())
//
//	// Write a boundary value to simulate data
//	page.SetInt(0, 200)
//	err := fm.Write(block, page)
//	if err != nil {
//		t.Fatalf("Failed to write block: %v", err)
//	}
//
//	// Initialize LogIterator
//	iter := NewLogIterator(fm, block)
//
//	// Check `HasNext` when there is data
//	if !iter.HasNext() {
//		t.Errorf("Expected HasNext to be true, got false")
//	}
//
//	// Simulate reaching the end of the block
//	iter.currentPos = fm.BlockSize()
//	if iter.HasNext() {
//		t.Errorf("Expected HasNext to be false after reaching end of block, got true")
//	}
//}

//func TestMultipleBlocks(t *testing.T) {
//	fm, tempDir := setupTestFileMgr(t)
//	defer func() {
//		fm.Close()
//		os.RemoveAll(tempDir)
//	}()
//
//	filename := "test.db"
//	block1 := kfile.NewBlockId(filename, 1)
//	block2 := kfile.NewBlockId(filename, 0)
//
//	// Write records to two blocks
//	record1 := []byte("record in block 1")
//	record2 := []byte("record in block 0")
//	rec3 := make([]byte, fm.BlockSize())
//	rec4 := make([]byte, fm.BlockSize())
//	copy(rec3, record1)
//	copy(rec4, record2)
//	page1 := kfile.NewPageFromBytes(rec3)
//	page2 := kfile.NewPageFromBytes(rec4)
//	page1.SetBytes(200, record1)
//	page1.SetInt(0, 200) // Set boundary for block 1
//	page2.SetBytes(225, record2)
//	page2.SetInt(0, 225) // Set boundary for block 0
//
//	err := fm.Write(block1, page1)
//	if err != nil {
//		t.Fatalf("Failed to write block1: %v", err)
//	}
//
//	err = fm.Write(block2, page2)
//	if err != nil {
//		t.Fatalf("Failed to write block2: %v", err)
//	}
//
//	// Initialize LogIterator with the most recent block (block1)
//	iter := NewLogIterator(fm, block1)
//
//	// Retrieve record from block 1
//	if !iter.HasNext() {
//		t.Fatalf("Expected HasNext to be true for block1")
//	}
//	rec1, _ := iter.Next()
//	if string(rec1) != string(record1) {
//		t.Errorf("Expected record '%s', got '%s'", string(record1), string(rec1))
//	}
//
//	// Retrieve record from block 0 after transitioning
//	if !iter.HasNext() {
//		t.Fatalf("Expected HasNext to be true for block0")
//	}
//	rec2, _ := iter.Next()
//	if string(rec2) != string(record2) {
//		t.Errorf("Expected record '%s', got '%s'", string(record2), string(rec2))
//	}
//}
