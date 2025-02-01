package kfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// Page represents a block of data in memory.
type Page struct {
	data         []byte
	pageId       uint64
	mu           sync.RWMutex
	IsCompressed bool
	isDirty      bool
}

const (
	ErrOutOfBounds = "offset out of bounds"
)

// pageIdOffset is where the page ID stored.
const pageIdOffset = 0

// NewPage creates a new page with the given block size.
func NewPage(blockSize int) *Page {
	return &Page{
		data: make([]byte, blockSize),
	}
}

// NewPageFromBytes creates a new page from the given byte slice.
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		data: b,
	}
}

// GetInt reads a 4-byte big-endian integer from the given offset.
func (p *Page) GetInt(offset int) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 || offset+4 > len(p.data) {
		return 0, fmt.Errorf("%s: getting int", ErrOutOfBounds)
	}
	return int(binary.BigEndian.Uint32(p.data[offset:])), nil
}

// SetInt writes a 4-byte big-endian integer at the given offset.
func (p *Page) SetInt(offset int, val int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset < 0 || offset+4 > len(p.data) {
		return fmt.Errorf("%s: setting int", ErrOutOfBounds)
	}
	binary.BigEndian.PutUint32(p.data[offset:], uint32(val))
	p.setIsDirty(true)
	return nil
}

// GetBytes reads a length-prefixed byte slice from the given offset.
// The length prefix is a 4-byte big-endian integer.
func (p *Page) GetBytes(offset int) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 || offset+4 > len(p.data) {
		return nil, fmt.Errorf("%s: getting bytes", ErrOutOfBounds)
	}

	length := int(binary.BigEndian.Uint32(p.data[offset : offset+4]))
	if offset+4+length > len(p.data) {
		return nil, fmt.Errorf("%s: invalid length", ErrOutOfBounds)
	}

	// Return a copy of the data so that internal state isn’t modified.
	result := make([]byte, length)
	copy(result, p.data[offset+4:offset+4+length])
	return result, nil
}

// GetBytesWithLen is kept for compatibility and behaves the same as GetBytes.
func (p *Page) GetBytesWithLen(offset int) ([]byte, error) {
	return p.GetBytes(offset)
}

// SetBytes writes a length-prefixed byte slice at the given offset.
func (p *Page) SetBytes(offset int, val []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	length := len(val)
	totalSize := 4 + length // length prefix + data

	if offset < 0 || offset+totalSize > len(p.data) {
		return fmt.Errorf("%s: setting bytes", ErrOutOfBounds)
	}

	// Write length prefix.
	binary.BigEndian.PutUint32(p.data[offset:], uint32(length))
	// Write data.
	copy(p.data[offset+4:], val)

	p.setIsDirty(true)
	return nil
}

// GetString reads a string from a length-prefixed byte slice starting at offset.
func (p *Page) GetString(offset int) (string, error) {
	b, err := p.GetBytes(offset)
	if err != nil {
		return "", fmt.Errorf("getting string: %w", err)
	}
	return string(b), nil
}

// GetStringWithOffset reads a string from a length-prefixed byte slice, then
// removes a 4-byte suffix (if any) and trims trailing zero bytes.
func (p *Page) GetStringWithOffset(offset int) (string, error) {
	b, err := p.GetBytesWithLen(offset)
	if err != nil {
		return "", fmt.Errorf("getting string with offset: %w", err)
	}
	if len(b) < 4 {
		return "", fmt.Errorf("insufficient bytes to extract string")
	}
	stringBytes := b[:len(b)-4]
	trimmedBytes := bytes.TrimRight(stringBytes, "\x00")
	return string(trimmedBytes), nil
}

// SetString writes a string as a length-prefixed byte slice at the given offset.
func (p *Page) SetString(offset int, val string) error {
	strBytes := []byte(val)
	if err := p.SetBytes(offset, strBytes); err != nil {
		return err
	}
	p.setIsDirty(true)
	return nil
}

// SetBool writes a single byte (0 or 1) at the given offset.
func (p *Page) SetBool(offset int, val bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset < 0 || offset+1 > len(p.data) {
		return fmt.Errorf("%s: setting bool", ErrOutOfBounds)
	}
	if val {
		p.data[offset] = 1
	} else {
		p.data[offset] = 0
	}
	p.setIsDirty(true)
	return nil
}

// GetBool reads a boolean value (0 or 1) from the given offset.
func (p *Page) GetBool(offset int) (bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 || offset+1 > len(p.data) {
		return false, fmt.Errorf("%s: getting bool", ErrOutOfBounds)
	}
	return p.data[offset] == 1, nil
}

// SetDate writes an 8-byte big-endian integer (Unix timestamp) at the given offset.
func (p *Page) SetDate(offset int, val time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset < 0 || offset+8 > len(p.data) {
		return fmt.Errorf("%s: setting date", ErrOutOfBounds)
	}
	binary.BigEndian.PutUint64(p.data[offset:], uint64(val.Unix()))
	p.setIsDirty(true)
	return nil
}

// GetDate reads an 8-byte big-endian integer from the given offset and returns a time.Time.
func (p *Page) GetDate(offset int) (time.Time, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 || offset+8 > len(p.data) {
		return time.Unix(0, 0), fmt.Errorf("%s: getting date", ErrOutOfBounds)
	}
	timestamp := binary.BigEndian.Uint64(p.data[offset:])
	return time.Unix(int64(timestamp), 0), nil
}

// setIsDirty sets the dirty flag.
// It uses a write lock internally if not already held.
func (p *Page) setIsDirty(dirt bool) {
	// Assume caller holds the proper lock; if not, this can be wrapped with Lock/Unlock.
	p.isDirty = dirt
}

// GetIsDirty returns whether the page has been modified.
func (p *Page) GetIsDirty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isDirty
}

// Contents returns the underlying page data.
func (p *Page) Contents() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.data
}

// SetContents replaces the underlying page data.
func (p *Page) SetContents(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.data = data
}

// Size returns the size (in bytes) of the page.
func (p *Page) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.data)
}

// Available returns the number of unused bytes on the page.
// Note: GetUsedSpace() should be implemented per page type.
func (p *Page) Available() int {
	return p.Size() - p.GetUsedSpace()
}

// GetUsedSpace returns the amount of space currently used in the page.
// TODO: Implement this based on page type (e.g. slotted pages include header, slots, cells).
func (p *Page) GetUsedSpace() int {
	// Placeholder implementation.
	return 0
}

// trimTrailingZeros removes trailing zero bytes from the given slice.
func trimTrailingZeros(s []byte) []byte {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0 {
			return s[:i+1]
		}
	}
	return []byte{}
}
