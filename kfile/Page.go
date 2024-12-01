package kfile

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type PageIDGenerator struct {
	nextID uint64
}

func (g *PageIDGenerator) Next() uint64 {
	return atomic.AddUint64(&g.nextID, 1)
}

var DefaultPageIDGenerator = &PageIDGenerator{
	nextID: uint64(time.Now().UnixNano()), // Initialize with current timestamp
}

type Page struct {
	data   []byte
	pageId int
	mu     sync.RWMutex
}

const (
	ErrOutOfBounds = "offset out of bounds"
	bytesPerChar   = 1 //TODO: make this configurable or using UTF-8 aware methods

)

const pageIdOffset = 0 // First 8 bytes of the page are reserved for ID

// TODO: Implement the syncronized equivalent in Java
func NewPage(blockSize int) *Page {
	page := &Page{
		data: make([]byte, blockSize),
	}
	pm := GetPageManager(blockSize)
	pageId := DefaultPageIDGenerator.Next()
	err := page.SetPageID(pageId)
	if err != nil {
		return nil
	}
	pm.SetPage(pageId, page)
	return page
}

func NewPageFromBytes(b []byte) *Page {
	dataCopy := make([]byte, len(b))
	copy(dataCopy, b)
	page := &Page{
		data: dataCopy,
	}
	pm := GetPageManager(len(b))
	pageId := DefaultPageIDGenerator.Next()
	err := page.SetPageID(pageId)
	if err != nil {
		return nil
	}
	pm.SetPage(pageId, page)
	return page
}

func (p *Page) PageID() uint64 {
	return binary.BigEndian.Uint64(p.data[:8])
}

// SetPageID writes the page ID to the first 8 bytes
func (p *Page) SetPageID(id uint64) error {
	if len(p.data) < 8 {
		return fmt.Errorf("page too small to store ID")
	}
	binary.BigEndian.PutUint64(p.data[:8], id)
	return nil
}

func (p *Page) GetInt(offset int) (int32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+4 > len(p.data) {
		return 0, fmt.Errorf("%s: getting int", ErrOutOfBounds)
	}
	return int32(binary.BigEndian.Uint32(p.data[offset:])), nil
}

func (p *Page) SetInt(offset int, val int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+4 > len(p.data) {
		return fmt.Errorf("%s: setting int", ErrOutOfBounds)
	}
	binary.BigEndian.PutUint32(p.data[offset:], uint32(val))
	return nil
}

func (p *Page) GetBytes(offset int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset > len(p.data) {
		return nil, fmt.Errorf("%s: getting bytes", ErrOutOfBounds)
	}
	dataCopy := make([]byte, len(p.data[offset:]))
	copy(dataCopy, p.data[offset:])
	return dataCopy, nil
}

func (p *Page) SetBytes(offset int, val []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	length := len(val)
	if offset+length > len(p.data) {
		return fmt.Errorf("%s: setting bytes", ErrOutOfBounds)
	}
	copy(p.data[offset:], val)
	return nil
}

func (p *Page) GetString(offset int, length int) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+length > len(p.data) {
		return "", fmt.Errorf("%s: getting string", ErrOutOfBounds)
	}

	str := string(trimZero(p.data[offset : offset+length]))
	return str, nil
}

func (p *Page) SetString(offset int, val string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	length := len(val)
	strBytes := make([]byte, length)
	copy(strBytes, val)

	if offset+length > len(p.data) {
		return fmt.Errorf("%s: setting string", ErrOutOfBounds)
	}
	copy(p.data[offset:], strBytes)
	return nil
}

func (p *Page) SetBool(offset int, val bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+1 > len(p.data) {
		return fmt.Errorf("%s: setting bool", ErrOutOfBounds)
	}
	if val {
		p.data[offset] = 1
	} else {
		p.data[offset] = 0
	}
	return nil
}

func (p *Page) GetBool(offset int) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+1 > len(p.data) {
		return false, fmt.Errorf("%s: getting bool", ErrOutOfBounds)
	}
	if p.data[offset] == 1 {
		return true, nil
	}
	return false, nil
}

func (p *Page) SetDate(offset int, val time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+8 > len(p.data) {
		return fmt.Errorf("%s: setting date", ErrOutOfBounds)
	}
	convertedVal := uint64(val.Unix())
	binary.BigEndian.PutUint64(p.data[offset:], convertedVal)
	return nil
}

func (p *Page) GetDate(offset int) (time.Time, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+8 > len(p.data) {
		return time.Unix(0, 0), fmt.Errorf("%s: getting date", ErrOutOfBounds)
	}
	timestamp := binary.BigEndian.Uint64(p.data[offset:])
	return time.Unix(int64(timestamp), 0), nil
}

func MaxLength(strlen int) int {
	bytesPerChar := 1
	return 4 + (strlen * bytesPerChar)
}

func (p *Page) Contents() []byte {
	return p.data
}

func trimZero(s []byte) []byte {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0 {
			return s[:i+1]
		}
	}
	return []byte{}
}
