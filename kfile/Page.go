package kfile

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

type Page struct {
	data   []byte
	pageId uint64
	mu     sync.RWMutex
}

const (
	ErrOutOfBounds = "offset out of bounds"
)

const pageIdOffset = 0

func NewPage(blockSize int) *Page {
	page := &Page{
		data: make([]byte, blockSize),
	}
	return page
}

func NewPageFromBytes(b []byte) *Page {
	page := &Page{
		data: b,
	}
	return page
}

func (p *Page) GetInt(offset int) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset > len(p.data) {
		return 0, fmt.Errorf("%s: getting int", ErrOutOfBounds)
	}
	return int(binary.BigEndian.Uint32(p.data[offset:])), nil
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

	// Find the end of the segment (delimiter)
	end := offset
	for end < len(p.data) && p.data[end] != 0 {
		end++
	}

	// Copy data between offset and end
	dataCopy := make([]byte, end-offset)
	copy(dataCopy, p.data[offset:end])

	return dataCopy, nil
}

func (p *Page) SetBytes(offset int, val []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	length := len(val)
	// Check if there's enough space for the data and the delimiter
	if length != 0 {
		if offset+length+1 > len(p.data) { // +1 for the delimiter
			return fmt.Errorf("%s: setting bytes", ErrOutOfBounds)
		}

		// Clear the buffer in the target range
		for i := 0; i < length+1; i++ { // +1 to clear the delimiter space
			p.data[offset+i] = 0
		}

		// Copy the new value
		copy(p.data[offset:], val)

		// Set the delimiter
		p.data[offset+length] = 0 // Null byte as a delimiter
	}

	return nil
}

func (p *Page) GetString(offset int) (string, error) {
	if offset > len(p.data) {
		return "", fmt.Errorf("%s: getting string", ErrOutOfBounds)
	}

	b, err := p.GetBytes(offset)
	if err != nil {
		return "", fmt.Errorf("error occured %s", err)
	}

	str := string(b) // Convert bytes to string
	return str, nil
}

func (p *Page) SetString(offset int, val string) error {
	strBytes := append([]byte(val))

	p.SetBytes(offset, strBytes)
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
