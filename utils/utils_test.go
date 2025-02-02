package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path/filepath"
	_ "path/filepath"
	"testing"
	"time"
	"ultraSQL/buffer"
	"ultraSQL/kfile"
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
	page := kfile.NewSlottedPage(fm.BlockSize())

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

func randomCellKeyGenerator() []byte {
	randNum := rand.Int31n(5)
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, randNum)
	if err != nil {
		fmt.Println("Binary write failed:", err)
		return nil
	}
	return buf.Bytes()
}
func TestLogIterator_EmptyIterator(t *testing.T) {
	fm := createTempFileMgr(t)
	filename := "test_empty.log"

	// Create an empty block

	blk := kfile.NewBlockId(filename, 0)
	page := kfile.NewSlottedPage(fm.BlockSize())
	err := fm.Write(blk, page)
	require.NoError(t, err)
	policy := buffer.InitLRU(3, fm)
	bm := buffer.NewBufferMgr(fm, 3, policy)
	iterator, _ := NewLogIterator(fm, bm, blk)

	assert.False(t, iterator.HasNext())
}

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
	page := kfile.NewSlottedPage(fm.BlockSize())
	cellKey := randomCellKeyGenerator()
	cell := kfile.NewKVCell(cellKey)
	err := cell.SetValue(200)
	require.NoError(t, err)
	err = page.InsertCell(cell)
	buff := buffer.NewBuffer(fm)
	buff.SetContents(page)
	policy := buffer.InitLRU(3, fm)
	bm := buffer.NewBufferMgr(fm, 3, policy)
	// Initialize LogIterator and move to block
	iter, _ := NewLogIterator(fm, bm, block)
	err = iter.moveToBlock(block)
	require.NoError(t, err)

	if iter.currentPos == 0 {
		t.Errorf("Expected currentPos to be 0, got %d", iter.currentPos)
	}
}
