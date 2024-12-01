package kfile

import (
	"encoding/binary"
	"fmt"
	"time"
)

type Page struct {
	data []byte
}

const OUTBOUNDS = "offset out of bounds"

// TODO: Implement the syncronized equivalent in Java
func NewPage(blockSize int) *Page {

	return &Page{
		data: make([]byte, blockSize),
	}
}

func NewPageFromBytes(b []byte) *Page {
	dataCopy := make([]byte, len(b))
	copy(dataCopy, b)
	return &Page{
		data: dataCopy,
	}
}

func (p *Page) GetInt(offset int) (int32, error) {
	if offset+4 > len(p.data) {
		return 0, fmt.Errorf(OUTBOUNDS)
	}
	return int32(binary.BigEndian.Uint32(p.data[offset:])), nil
}

func (p *Page) SetInt(offset int, val int) error {
	if offset+4 > len(p.data) {
		return fmt.Errorf(OUTBOUNDS)
	}
	binary.BigEndian.PutUint32(p.data[offset:], uint32(val))
	return nil
}

func (p *Page) GetBytes(offset int) ([]byte, error) {
	if offset > len(p.data) {
		return nil, fmt.Errorf(OUTBOUNDS)
	}
	dataCopy := make([]byte, 0)
	copy(dataCopy, p.data[offset:offset])
	return dataCopy, nil
}

func (p *Page) SetBytes(offset int, val []byte) error {
	length := len(val)
	if offset+length > len(p.data) {
		return fmt.Errorf(OUTBOUNDS)
	}
	copy(p.data[offset:], val)
	return nil
}

func (p *Page) GetString(offset int, length int) (string, error) {
	if offset+length > len(p.data) {
		return "", fmt.Errorf(OUTBOUNDS)
	}

	str := string(trimZero(p.data[offset : offset+length]))
	return str, nil
}

func (p *Page) SetString(offset int, val string) error {
	length := len(val)
	strBytes := make([]byte, length)
	copy(strBytes, val)

	if offset+length > len(p.data) {
		return fmt.Errorf(OUTBOUNDS)
	}
	copy(p.data[offset:], strBytes)
	return nil
}

func (p *Page) SetBool(offset int, val bool) error {
	if offset+1 > len(p.data) {
		return fmt.Errorf(OUTBOUNDS)
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
		return false, fmt.Errorf(OUTBOUNDS)
	}
	if p.data[offset] == 1 {
		return true, nil
	}
	return false, nil
}

func (p *Page) SetDate(offset int, val time.Time) error {
	if offset+8 > len(p.data) {
		return fmt.Errorf(OUTBOUNDS)
	}
	convertedVal := uint64(val.Unix())
	binary.BigEndian.PutUint64(p.data[offset:], convertedVal)
	return nil
}

func (p *Page) GetDate(offset int) (time.Time, error) {
	if offset+8 > len(p.data) {
		return time.Unix(0, 0), fmt.Errorf(OUTBOUNDS)
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
