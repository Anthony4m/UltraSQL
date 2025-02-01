package kfile

import (
	"fmt"
	"io"
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
	openFilesLock sync.Mutex
	mutex         sync.RWMutex
	blocksRead    int
	blocksWritten int
	readLog       []ReadWriteLogEntry
	writeLog      []ReadWriteLogEntry
	metaData      FileMetadata
}

// FileMetadata contains metadata for the database files.
type FileMetadata struct {
	CreatedAt    time.Time
	ModifiedAt   time.Time
	SizeLimit    int64
	FileSize     int64
	BlockCount   int
	LastAccessed time.Time
}

// ReadWriteLogEntry logs a read or write operation.
type ReadWriteLogEntry struct {
	Timestamp   time.Time
	BlockId     *BlockId
	BytesAmount int
}

const (
	maxLogEntries = 1000
)

var seekErrFormat = "failed to seek to offset %d in file %s: %w"

func NewFileMgr(dbDirectory string, blocksize int) (*FileMgr, error) {
	fm := &FileMgr{
		dbDirectory: dbDirectory,
		blocksize:   blocksize,
		openFiles:   make(map[string]*os.File),
	}

	// Ensure the directory exists.
	info, err := os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		fm.isNew = true
		if err = os.MkdirAll(dbDirectory, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dbDirectory, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to access directory %s: %w", dbDirectory, err)
	} else if info.IsDir() {
		fm.isNew = false
	} else {
		return nil, fmt.Errorf("path %s is not a directory", dbDirectory)
	}

	// Remove any leftover temporary files.
	files, err := os.ReadDir(dbDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %s: %w", dbDirectory, err)
	}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".tmp" {
			tempPath := filepath.Join(dbDirectory, file.Name())
			if err := os.Remove(tempPath); err != nil {
				return nil, fmt.Errorf("failed to remove temporary file %s: %w", tempPath, err)
			}
		}
	}

	metadata := NewMetaData(time.Now())
	fm.metaData = metadata
	return fm, nil
}

// addMetaData updates the metadata.
func (fm *FileMgr) addMetaData(metaData FileMetadata) {
	fm.metaData = FileMetadata{
		CreatedAt:    metaData.CreatedAt,
		ModifiedAt:   metaData.ModifiedAt,
		SizeLimit:    metaData.SizeLimit,
		FileSize:     metaData.FileSize,
		BlockCount:   metaData.BlockCount,
		LastAccessed: metaData.LastAccessed,
	}
}

// NewMetaData creates new metadata with the given creation time.
func NewMetaData(createdAt time.Time) FileMetadata {
	return FileMetadata{
		CreatedAt: createdAt,
	}
}

// PreallocateFile reserves space in the file corresponding to blk.
func (fm *FileMgr) PreallocateFile(blk *BlockId, size int64) error {
	if err := fm.validatePreallocationParams(blk, size); err != nil {
		return err
	}

	filename := blk.GetFileName()
	if err := fm.validatePermissions(); err != nil {
		return err
	}

	return fm.performPreallocation(filename, size)
}

// validatePreallocationParams checks that the parameters are valid.
func (fm *FileMgr) validatePreallocationParams(blk *BlockId, size int64) error {
	if size%int64(fm.blocksize) != 0 {
		return fmt.Errorf("size must be a multiple of blocksize %d", fm.blocksize)
	}
	if blk.GetFileName() == "" {
		return fmt.Errorf("invalid filename")
	}
	return nil
}

// validatePermissions ensures that the directory is writable.
func (fm *FileMgr) validatePermissions() error {
	dirStat, err := os.Stat(fm.dbDirectory)
	if err != nil {
		return fmt.Errorf("failed to stat directory: %w", err)
	}
	if dirStat.Mode()&0200 == 0 {
		return fmt.Errorf("directory is not writable")
	}
	return nil
}

// performPreallocation opens the file and grows it if necessary.
func (fm *FileMgr) performPreallocation(filename string, size int64) error {
	f, err := fm.getFile(filename)
	if err != nil {
		return fmt.Errorf("failed to get file for preallocation: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	if stat.Mode()&0200 == 0 {
		return fmt.Errorf("file is not writable")
	}
	if stat.Size() >= size {
		return nil
	}

	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("failed to preallocate sparse file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync preallocated file: %w", err)
	}
	return nil
}

// getFile returns an open file handle for the given filename,
// caching the result. It uses a separate lock for thread safety.
func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	fm.openFilesLock.Lock()
	defer fm.openFilesLock.Unlock()

	if f, exists := fm.openFiles[filename]; exists {
		return f, nil
	}
	filePath := filepath.Join(fm.dbDirectory, filename)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	fm.openFiles[filename] = f
	return f, nil
}

// Read reads a block from disk into the given slotted page.
func (fm *FileMgr) Read(blk *BlockId, p *SlottedPage) error {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	f, err := fm.getFile(blk.GetFileName())
	if err != nil {
		return fmt.Errorf("failed to get file for block %v: %w", blk, err)
	}

	offset := int64(blk.Number() * fm.blocksize)
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf(seekErrFormat, offset, blk.GetFileName(), err)
	}
	bytesRead, err := f.Read(p.Contents())
	if err != nil {
		return fmt.Errorf("failed to read block %v: %w", blk, err)
	}
	if bytesRead != fm.blocksize {
		return fmt.Errorf("incomplete read: expected %d bytes, got %d", fm.blocksize, bytesRead)
	}

	fm.blocksRead++
	fm.addToReadLog(ReadWriteLogEntry{
		Timestamp:   time.Now(),
		BlockId:     blk,
		BytesAmount: bytesRead,
	})
	return nil
}

// Write writes the contents of a slotted page to disk.
func (fm *FileMgr) Write(blk *BlockId, p *SlottedPage) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	f, err := fm.getFile(blk.GetFileName())
	if err != nil {
		return fmt.Errorf("failed to get file for block %v: %w", blk, err)
	}

	offset := int64(blk.Number() * fm.blocksize)
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf(seekErrFormat, offset, blk.GetFileName(), err)
	}
	bytesWritten, err := f.Write(p.Contents())
	if err != nil {
		return fmt.Errorf("failed to write block %v: %w", blk, err)
	}
	if bytesWritten != fm.blocksize {
		return fmt.Errorf("incomplete write: expected %d bytes, wrote %d", fm.blocksize, bytesWritten)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file %s: %w", blk.GetFileName(), err)
	}

	fm.blocksWritten++
	fm.addToWriteLog(ReadWriteLogEntry{
		Timestamp:   time.Now(),
		BlockId:     blk,
		BytesAmount: bytesWritten,
	})
	return nil
}

// Append adds an empty block to the file and returns its BlockId.
func (fm *FileMgr) Append(filename string) (*BlockId, error) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	newBlkNum, err := fm.LengthLocked(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to determine length for file %s: %w", filename, err)
	}
	blk := NewBlockId(filename, newBlkNum)
	emptyBlock := make([]byte, fm.blocksize)

	f, err := fm.getFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get file for append: %w", err)
	}
	offset := int64(newBlkNum * fm.blocksize)
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d in file %s: %w", offset, filename, err)
	}
	bytesWritten, err := f.Write(emptyBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to write new block %v: %w", blk, err)
	}
	if bytesWritten != fm.blocksize {
		return nil, fmt.Errorf("incomplete write: expected %d bytes, wrote %d", fm.blocksize, bytesWritten)
	}
	if err = f.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync file %s: %w", filename, err)
	}
	return blk, nil
}

// Length returns the number of blocks in the file.
func (fm *FileMgr) Length(filename string) (int, error) {
	return fm.LengthLocked(filename)
}

// NewLength is a helper that returns the length or 0 on error.
func (fm *FileMgr) NewLength(filename string) int {
	n, err := fm.LengthLocked(filename)
	if err != nil {
		return 0
	}
	return n
}

// LengthLocked returns the number of blocks in the file; the caller must hold fm.mutex.
func (fm *FileMgr) LengthLocked(filename string) (int, error) {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to get file %s: %w", filename, err)
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %w", filename, err)
	}
	numBlocks := int(stat.Size() / int64(fm.blocksize))
	return numBlocks, nil
}

// IsNew returns whether the FileMgr was created with a new directory.
func (fm *FileMgr) IsNew() bool {
	return fm.isNew
}

// BlockSize returns the configured block size.
func (fm *FileMgr) BlockSize() int {
	return fm.blocksize
}

// Close closes all open files.
func (fm *FileMgr) Close() error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	var firstErr error
	fm.openFilesLock.Lock()
	defer fm.openFilesLock.Unlock()
	for filename, f := range fm.openFiles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close file %s: %w", filename, err)
		}
		delete(fm.openFiles, filename)
	}
	return firstErr
}

// BlocksRead returns the total number of blocks read.
func (fm *FileMgr) BlocksRead() int {
	return fm.blocksRead
}

// BlocksWritten returns the total number of blocks written.
func (fm *FileMgr) BlocksWritten() int {
	return fm.blocksWritten
}

// addToReadLog adds an entry to the read log.
func (fm *FileMgr) addToReadLog(entry ReadWriteLogEntry) {
	if len(fm.readLog) >= maxLogEntries {
		fm.readLog = fm.readLog[1:]
	}
	fm.readLog = append(fm.readLog, entry)
}

// addToWriteLog adds an entry to the write log.
func (fm *FileMgr) addToWriteLog(entry ReadWriteLogEntry) {
	if len(fm.writeLog) >= maxLogEntries {
		fm.writeLog = fm.writeLog[1:]
	}
	fm.writeLog = append(fm.writeLog, entry)
}

// ReadLog returns the current read log.
func (fm *FileMgr) ReadLog() []ReadWriteLogEntry {
	return fm.readLog
}

// WriteLog returns the current write log.
func (fm *FileMgr) WriteLog() []ReadWriteLogEntry {
	return fm.writeLog
}

// ensureFileSize ensures the file has at least the required number of blocks.
func (fm *FileMgr) ensureFileSize(blk *BlockId, requiredBlocks int) error {
	currentBlocks, err := fm.Length(blk.GetFileName())
	if err != nil {
		return err
	}
	if currentBlocks < requiredBlocks {
		size := int64(requiredBlocks * fm.blocksize)
		return fm.PreallocateFile(blk, size)
	}
	return nil
}

// RenameFile renames the file corresponding to blk to newFileName.
func (fm *FileMgr) RenameFile(blk *BlockId, newFileName string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	if newFileName == "" {
		return fmt.Errorf("invalid new filename: %s", newFileName)
	}

	oldFileName := blk.GetFileName()

	// Close the old file if it is open.
	fm.openFilesLock.Lock()
	if f, exists := fm.openFiles[oldFileName]; exists {
		if err := f.Close(); err != nil {
			fm.openFilesLock.Unlock()
			return fmt.Errorf("failed to close file before rename: %w", err)
		}
		delete(fm.openFiles, oldFileName)
	}
	fm.openFilesLock.Unlock()

	oldPath := filepath.Join(fm.dbDirectory, oldFileName)
	newPath := filepath.Join(fm.dbDirectory, newFileName)

	if _, err := os.Stat(newPath); err == nil {
		return fmt.Errorf("target file already exists: %s", newFileName)
	}

	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("failed to rename file from %s to %s: %w", oldFileName, newFileName, err)
	}

	newFile, err := os.OpenFile(newPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen renamed file: %w", err)
	}

	// Update metadata and cache.
	blk.SetFileName(newFileName)
	metadata := fm.metaData
	metadata.ModifiedAt = time.Now()
	metadata.LastAccessed = time.Now()
	fm.addMetaData(metadata)

	fm.openFilesLock.Lock()
	fm.openFiles[newFileName] = newFile
	fm.openFilesLock.Unlock()

	return nil
}

// DeleteFile closes and removes the specified file.
func (fm *FileMgr) DeleteFile(filename string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	fm.openFilesLock.Lock()
	if f, exists := fm.openFiles[filename]; exists {
		if err := f.Close(); err != nil {
			fm.openFilesLock.Unlock()
			return fmt.Errorf("failed to close file before deletion: %w", err)
		}
		delete(fm.openFiles, filename)
	}
	fm.openFilesLock.Unlock()

	path := filepath.Join(fm.dbDirectory, filename)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to delete file %s: %w", filename, err)
	}
	return nil
}

// checkSizeLimit verifies that adding additionalBytes will not exceed the size limit.
func (fm *FileMgr) checkSizeLimit(filename string, additionalBytes int64) error {
	if fm.metaData.SizeLimit <= 0 {
		return nil
	}
	f, err := fm.getFile(filename)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size()+additionalBytes > fm.metaData.SizeLimit {
		return fmt.Errorf("operation would exceed size limit of %d bytes", fm.metaData.SizeLimit)
	}
	return nil
}

// ValidateFile checks that the file size is a multiple of blocksize and that permissions are sufficient.
func (fm *FileMgr) ValidateFile(filename string) error {
	f, err := fm.getFile(filename)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size()%int64(fm.blocksize) != 0 {
		return fmt.Errorf("file size %d is not a multiple of blocksize %d", stat.Size(), fm.blocksize)
	}
	if stat.Mode().Perm()&0600 != 0600 {
		return fmt.Errorf("insufficient file permissions")
	}
	return nil
}
