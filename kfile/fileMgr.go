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
	mutex         sync.RWMutex
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

const maxLogEntries = 1000
const format = "failed to seek to offset %d in file %s: %v"

func NewFileMgr(dbDirectory string, blocksize int) (*FileMgr, error) {
	fm := &FileMgr{
		dbDirectory: dbDirectory,
		blocksize:   blocksize,
		openFiles:   make(map[string]*os.File),
	}

	info, err := os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		fm.isNew = true
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

	files, err := os.ReadDir(dbDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %s: %v", dbDirectory, err)
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".tmp" {
			tempPath := filepath.Join(dbDirectory, file.Name())
			err := os.Remove(tempPath)
			if err != nil {
				return nil, fmt.Errorf("failed to remove temporary file %s: %v", tempPath, err)
			}
		}
	}

	return fm, nil
}

func (fm *FileMgr) getFile(filename string) (*os.File, error) {

	if f, exists := fm.openFiles[filename]; exists {
		return f, nil
	}
	filePath := filepath.Join(fm.dbDirectory, filename)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	fm.openFiles[filename] = f
	return f, nil
}

func (fm *FileMgr) Read(blk *BlockId, p *Page) error {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	f, err := fm.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("failed to get file for block %v: %v", blk, err)
	}

	offset := int64(blk.Number() * fm.blocksize)
	_, err = f.Seek(offset, 0)
	if err != nil {

		return fmt.Errorf(format, offset, blk.FileName(), err)
	}
	bytesRead, err := f.Read(p.Contents())
	if err != nil {
		return fmt.Errorf("failed to read block %v: %v", blk, err)
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

func (fm *FileMgr) Write(blk *BlockId, p *Page) error {

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	f, err := fm.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("failed to get file for block %v: %v", blk, err)
	}

	offset := int64(blk.Number() * fm.blocksize)
	_, err = f.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf(format, offset, blk.FileName(), err)
	}
	bytesWritten, err := f.Write(p.Contents())
	if err != nil {
		return fmt.Errorf("failed to write block %v: %v", blk, err)
	}

	if bytesWritten != fm.blocksize {
		return fmt.Errorf("incomplete write: expected %d bytes, wrote %d", fm.blocksize, bytesWritten)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync file %s: %v", blk.FileName(), err)
	}
	fm.blocksWritten++
	fm.addToWriteLog(ReadWriteLogEntry{
		Timestamp:   time.Now(),
		BlockId:     blk,
		BytesAmount: bytesWritten,
	})

	return nil
}
func (fm *FileMgr) Append(filename string) (*BlockId, error) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
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
	offset := int64(newblknum * fm.blocksize)
	_, err = f.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d in file %s: %v", offset, filename, err)
	}
	bytesWritten, err := f.Write(emptyBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to write new block %v: %v", blk, err)
	}

	if bytesWritten != fm.blocksize {
		return nil, fmt.Errorf("incomplete write: expected %d bytes, wrote %d", fm.blocksize, bytesWritten)
	}
	err = f.Sync()
	if err != nil {
		return nil, fmt.Errorf("failed to sync file %s: %v", filename, err)
	}

	return blk, nil
}

func (fm *FileMgr) Length(filename string) (int, error) {
	return fm.lengthLocked(filename)
}
func (fm *FileMgr) NewLength(filename string) int {
	locked, err := fm.lengthLocked(filename)
	if err != nil {
		return 0
	}
	return locked
}
func (fm *FileMgr) lengthLocked(filename string) (int, error) {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to get file %s: %v", filename, err)
	}

	stat, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %v", filename, err)
	}
	numBlocks := int(stat.Size() / int64(fm.blocksize))
	return numBlocks, nil
}

func (fm *FileMgr) IsNew() bool {
	return fm.isNew
}

func (fm *FileMgr) BlockSize() int {
	return fm.blocksize
}

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

func (fm *FileMgr) BlocksRead() int {
	return fm.blocksRead
}

func (fm *FileMgr) BlocksWritten() int {
	return fm.blocksWritten
}

func (fm *FileMgr) addToReadLog(entry ReadWriteLogEntry) {
	if len(fm.readLog) >= maxLogEntries {
		fm.readLog = fm.readLog[1:]
	}
	fm.readLog = append(fm.readLog, entry)
}

func (fm *FileMgr) addToWriteLog(entry ReadWriteLogEntry) {
	if len(fm.writeLog) >= maxLogEntries {
		fm.writeLog = fm.writeLog[1:]
	}
	fm.writeLog = append(fm.writeLog, entry)
}

func (fm *FileMgr) ReadLog() []ReadWriteLogEntry {
	return fm.readLog
}

func (fm *FileMgr) WriteLog() []ReadWriteLogEntry {
	return fm.writeLog
}
