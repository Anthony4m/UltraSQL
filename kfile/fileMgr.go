package kfile

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileMgr struct {
	dbDirectory   string
	blocksize     int
	isNew         bool
	openFiles     map[string]*os.File
	mutex         sync.Mutex
	blocksRead    int
	blocksWritten int
	readLog       []ReadWriteLogEntry
	writeLog      []ReadWriteLogEntry
}

type ReadWriteLogEntry struct {
	Timestamp   time.Time
	BlockId     *BlockId
	BytesAmount int
}

// NewFileMgr creates a new FileMgr instance.
// dbDirectory: Path to the database directory.
// blocksize: Size of each block.
func NewFileMgr(dbDirectory string, blocksize int) (*FileMgr, error) {
	fm := &FileMgr{
		dbDirectory: dbDirectory,
		blocksize:   blocksize,
		openFiles:   make(map[string]*os.File),
	}

	// Check if the directory exists
	info, err := os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		fm.isNew = true
		// Create the directory
		err = os.MkdirAll(dbDirectory, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dbDirectory, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to access directory %s: %v", dbDirectory, err)
	} else if info.IsDir() {
		fm.isNew = false
	} else {
		return nil, fmt.Errorf("path %s is not a directory", dbDirectory)
	}

	// Remove temporary files
	files, err := os.ReadDir(dbDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %s: %v", dbDirectory, err)
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".tmp" { // Adjust condition as needed
			tempPath := filepath.Join(dbDirectory, file.Name())
			err := os.Remove(tempPath)
			if err != nil {
				return nil, fmt.Errorf("failed to remove temporary file %s: %v", tempPath, err)
			}
		}
	}

	return fm, nil
}

// getFile retrieves the file associated with the filename.
// If the file is not already open, it opens the file in read-write mode,
// creates it if it doesn't exist, and caches it in openFiles.
func (fm *FileMgr) getFile(filename string) (*os.File, error) {

	// Check if file is already open
	if f, exists := fm.openFiles[filename]; exists {
		return f, nil
	}

	// Open the file in read-write mode, create if not exists
	filePath := filepath.Join(fm.dbDirectory, filename)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}

	// Cache the open file
	fm.openFiles[filename] = f
	return f, nil
}

// Read reads the block specified by blk into the Page p.
func (fm *FileMgr) Read(blk *BlockId, p *Page) error {

	f, err := fm.getFile(blk.Filename())
	if err != nil {
		return fmt.Errorf("failed to get file for block %v: %v", blk, err)
	}

	// Calculate the byte offset
	offset := int64(blk.Number() * fm.blocksize)

	// Seek to the offset
	_, err = f.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to offset %d in file %s: %v", offset, blk.Filename(), err)
	}

	// Read block data into Page
	bytesRead, err := f.Read(p.Contents())
	if err != nil {
		return fmt.Errorf("failed to read block %v: %v", blk, err)
	}

	if bytesRead != fm.blocksize {
		return fmt.Errorf("incomplete read: expected %d bytes, got %d", fm.blocksize, bytesRead)
	}

	// Increment read counter and log the read operation
	fm.blocksRead++
	fm.readLog = append(fm.readLog, ReadWriteLogEntry{
		Timestamp:   time.Now(),
		BlockId:     blk,
		BytesAmount: bytesRead,
	})

	return nil
}

// Write writes the content of Page p to the block specified by blk.
func (fm *FileMgr) Write(blk *BlockId, p *Page) error {

	f, err := fm.getFile(blk.Filename())
	if err != nil {
		return fmt.Errorf("failed to get file for block %v: %v", blk, err)
	}

	// Calculate the byte offset
	offset := int64(blk.Number() * fm.blocksize)

	// Seek to the offset
	_, err = f.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to offset %d in file %s: %v", offset, blk.Filename(), err)
	}

	// Write block data from Page
	bytesWritten, err := f.Write(p.Contents())
	if err != nil {
		return fmt.Errorf("failed to write block %v: %v", blk, err)
	}

	if bytesWritten != fm.blocksize {
		return fmt.Errorf("incomplete write: expected %d bytes, wrote %d", fm.blocksize, bytesWritten)
	}

	// Ensure data is flushed to disk
	err = f.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync file %s: %v", blk.Filename(), err)
	}

	// Increment write counter and log the write operation
	fm.blocksWritten++
	fm.writeLog = append(fm.writeLog, ReadWriteLogEntry{
		Timestamp:   time.Now(),
		BlockId:     blk,
		BytesAmount: bytesWritten,
	})

	return nil
}

// Append appends a new block to the specified file and returns its BlockId.
func (fm *FileMgr) Append(filename string) (*BlockId, error) {

	// Determine the new block number based on the current length
	newblknum, err := fm.lengthLocked(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to determine length for file %s: %v", filename, err)
	}

	blk := NewBlockId(filename, newblknum)
	emptyBlock := make([]byte, fm.blocksize)

	f, err := fm.getFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get file for append: %v", err)
	}

	// Seek to the end of the file
	offset := int64(newblknum * fm.blocksize)
	_, err = f.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d in file %s: %v", offset, filename, err)
	}

	// Write empty block
	bytesWritten, err := f.Write(emptyBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to write new block %v: %v", blk, err)
	}

	if bytesWritten != fm.blocksize {
		return nil, fmt.Errorf("incomplete write: expected %d bytes, wrote %d", fm.blocksize, bytesWritten)
	}

	// Ensure data is flushed to disk
	err = f.Sync()
	if err != nil {
		return nil, fmt.Errorf("failed to sync file %s: %v", filename, err)
	}

	return blk, nil
}

// Length returns the number of blocks in the specified file.
func (fm *FileMgr) Length(filename string) (int, error) {
	return fm.lengthLocked(filename)
}

// lengthLocked is a helper method that assumes the mutex is already locked.
func (fm *FileMgr) lengthLocked(filename string) (int, error) {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to get file %s: %v", filename, err)
	}

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %v", filename, err)
	}

	// Calculate number of blocks
	numBlocks := int(stat.Size() / int64(fm.blocksize))
	return numBlocks, nil
}

// IsNew returns whether the database is new.
func (fm *FileMgr) IsNew() bool {
	return fm.isNew
}

// BlockSize returns the size of each block.
func (fm *FileMgr) BlockSize() int {
	return fm.blocksize
}

// Close closes all open files managed by FileMgr.
func (fm *FileMgr) Close() error {
	var firstErr error
	for filename, f := range fm.openFiles {
		err := f.Close()
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close file %s: %v", filename, err)
		}
		delete(fm.openFiles, filename)
	}
	return firstErr
}

// BlocksRead returns the number of blocks read so far.
func (fm *FileMgr) BlocksRead() int {
	return fm.blocksRead
}

// returns the number of blocks written so far.
func (fm *FileMgr) BlocksWritten() int {
	return fm.blocksWritten
}

// ReadLog returns the read log entries.
func (fm *FileMgr) ReadLog() []ReadWriteLogEntry {
	return fm.readLog
}

// WriteLog returns the write log entries.
func (fm *FileMgr) WriteLog() []ReadWriteLogEntry {
	return fm.writeLog
}
