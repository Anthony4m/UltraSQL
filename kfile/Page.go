package kfile

import (
	"encoding/binary"
	"fmt"
	"time"
)

type Page struct {
	data []byte
}

const OUTOFBOUNDS = "offset out of bounds"

//TODO: Implement the syncronized equivalent in Java

// NewPage creates a new Page with a byte slice of the given block size.
func NewPage(blockSize int) *Page {
	return &Page{
		data: make([]byte, blockSize),
	}
}

// NewPageFromBytes creates a new Page by wrapping the provided byte slice.
func NewPageFromBytes(b []byte) *Page {
	dataCopy := make([]byte, len(b))
	copy(dataCopy, b)
	return &Page{
		data: dataCopy,
	}
}

func (p *Page) GetInt(offset int) (int32, error) {
	if offset+4 > len(p.data) {
		return 0, fmt.Errorf(OUTOFBOUNDS)
	}
	return int32(binary.BigEndian.Uint32(p.data[offset:])), nil
}

func (p *Page) SetInt(offset int, val int) error {
	if offset+4 > len(p.data) {
		return fmt.Errorf(OUTOFBOUNDS)
	}
	binary.BigEndian.PutUint32(p.data[offset:], uint32(val))
	return nil
}

func (p *Page) GetBytes(offset int, length int) ([]byte, error) {
	if offset+length > len(p.data) {
		return nil, fmt.Errorf(OUTOFBOUNDS)
	}
	dataCopy := make([]byte, length)
	copy(dataCopy, p.data[offset:offset+length])
	return dataCopy, nil
}

func (p *Page) SetBytes(offset int, val []byte) error {
	length := len(val)
	if offset+length > len(p.data) {
		return fmt.Errorf(OUTOFBOUNDS)
	}
	copy(p.data[offset:], val)
	return nil
}

func (p *Page) GetString(offset int, length int) (string, error) {
	if offset+length > len(p.data) {
		return "", fmt.Errorf(OUTOFBOUNDS)
	}
	return string(p.data[offset : offset+length]), nil
}

func (p *Page) SetString(offset int, val string) error {
	length := len(val)
	if offset+length > len(p.data) {
		return fmt.Errorf(OUTOFBOUNDS)
	}
	copy(p.data[offset:], val)
	return nil
}

func (p *Page) SetBool(offset int, val bool) error {
	if offset+1 > len(p.data) {
		return fmt.Errorf(OUTOFBOUNDS)
	}
	if val {
		p.data[offset] = 1
	} else {
		p.data[offset] = 0
	}
	return nil
}

func (p *Page) GetBool(offset int) (bool, error) {
	if offset+1 > len(p.data) {
		return false, fmt.Errorf(OUTOFBOUNDS)
	}
	if p.data[offset] == 1 {
		return true, nil
	}
	return false, nil
}

func (p *Page) SetDate(offset int, val time.Time) error {
	if offset+8 > len(p.data) {
		return fmt.Errorf("offset out of bounds")
	}
	convertedVal := uint64(val.Unix())
	binary.BigEndian.PutUint64(p.data[offset:], convertedVal) // Use PutUint64 here
	return nil
}

func (p *Page) GetDate(offset int) (time.Time, error) {
	if offset+8 > len(p.data) {
		return time.Unix(0, 0), fmt.Errorf("offset out of bounds")
	}
	timestamp := binary.BigEndian.Uint64(p.data[offset:])
	return time.Unix(int64(timestamp), 0), nil
}

// MaxLength calculates the maximum number of bytes needed to store a string of length strlen.
func MaxLength(strlen int) int {
	// Assuming US-ASCII encoding (1 byte per character)
	bytesPerChar := 1
	// 4 bytes for the length of the string (int32), plus bytes for each character
	return 4 + (strlen * bytesPerChar)
}

// Contents returns the data slice of the Page.
func (p *Page) Contents() []byte {
	// No need to reset position; just return the data slice.
	return p.data
}
