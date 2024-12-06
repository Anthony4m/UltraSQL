package kfile

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

type Page struct {
	Data   []byte
	pageId uint64
	mu     sync.RWMutex
}

const (
	ErrOutOfBounds = "offset out of bounds"
)

const pageIdOffset = 0

// TODO: Implement the syncronized equivalent in Java
func NewPage(blockSize int) *Page {
	page := &Page{
		Data: make([]byte, blockSize),
	}
	return page
}

func NewPageFromBytes(b []byte) *Page {
	page := &Page{
		Data: b,
	}
	return page
}

func (p *Page) GetInt(offset int) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset > len(p.Data) {
		return 0, fmt.Errorf("%s: getting int", ErrOutOfBounds)
	}
	return int(binary.BigEndian.Uint32(p.Data[offset:])), nil
}

func (p *Page) SetInt(offset int, val int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+4 > len(p.Data) {
		return fmt.Errorf("%s: setting int", ErrOutOfBounds)
	}
	binary.BigEndian.PutUint32(p.Data[offset:], uint32(val))
	return nil
}

func (p *Page) GetBytes(offset int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset > len(p.Data) {
		return nil, fmt.Errorf("%s: getting bytes", ErrOutOfBounds)
	}

	// Find the end of the segment (delimiter)
	end := offset
	for end < len(p.Data) && p.Data[end] != 0 {
		end++
	}

	// Copy data between offset and end
	dataCopy := make([]byte, end-offset)
	copy(dataCopy, p.Data[offset:end])

	return dataCopy, nil
}

func (p *Page) SetBytes(offset int, val []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	length := len(val)
	// Check if there's enough space for the data and the delimiter
	if length != 0 {
		if offset+length+1 > len(p.Data) { // +1 for the delimiter
			return fmt.Errorf("%s: setting bytes", ErrOutOfBounds)
		}

		// Clear the buffer in the target range
		for i := 0; i < length+1; i++ { // +1 to clear the delimiter space
			p.Data[offset+i] = 0
		}

		// Copy the new value
		copy(p.Data[offset:], val)

		// Set the delimiter
		p.Data[offset+length] = 0 // Null byte as a delimiter
	}

	return nil
}

func (p *Page) GetString(offset int) (string, error) {
	if offset > len(p.Data) {
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
	if offset+1 > len(p.Data) {
		return fmt.Errorf("%s: setting bool", ErrOutOfBounds)
	}
	if val {
		p.Data[offset] = 1
	} else {
		p.Data[offset] = 0
	}
	return nil
}

func (p *Page) GetBool(offset int) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+1 > len(p.Data) {
		return false, fmt.Errorf("%s: getting bool", ErrOutOfBounds)
	}
	if p.Data[offset] == 1 {
		return true, nil
	}
	return false, nil
}

func (p *Page) SetDate(offset int, val time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+8 > len(p.Data) {
		return fmt.Errorf("%s: setting date", ErrOutOfBounds)
	}
	convertedVal := uint64(val.Unix())
	binary.BigEndian.PutUint64(p.Data[offset:], convertedVal)
	return nil
}

func (p *Page) GetDate(offset int) (time.Time, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+8 > len(p.Data) {
		return time.Unix(0, 0), fmt.Errorf("%s: getting date", ErrOutOfBounds)
	}
	timestamp := binary.BigEndian.Uint64(p.Data[offset:])
	return time.Unix(int64(timestamp), 0), nil
}

func (p *Page) Contents() []byte {
	return p.Data
}

func trimZero(s []byte) []byte {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0 {
			return s[:i+1]
		}
	}
	return []byte{}
}
