package kfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	// Cell Types
	KEY_CELL = 1 // Internal node cell (key + page pointer)
	KV_CELL  = 2 // Leaf node cell (key + value)

	// Data Types
	INTEGER_TYPE = 1
	STRING_TYPE  = 2
	BOOL_TYPE    = 3
	DATE_TYPE    = 4
	BYTES_TYPE   = 5

	// Flag bits
	FLAG_DELETED  = 1 << 0
	FLAG_OVERFLOW = 1 << 1 // If record doesn't fit in page
)

type Cell struct {
	flags     byte   // Cell metadata flags
	keySize   int    // Size of key in bytes
	valueSize int    // Size of value/record in bytes
	keyType   byte   // Type of key data
	valueType byte   // Type of value data
	key       []byte // Key bytes
	value     []byte // Value/record bytes
	pageId    uint64 // For internal nodes - points to child page
	offset    int    // Physical offset in page
}

// NewKeyCell new key-only cell (internal node)
func NewKeyCell(key []byte, childPageId uint64) *Cell {
	return &Cell{
		flags:   KEY_CELL,
		keySize: len(key),
		key:     key,
		pageId:  childPageId,
	}
}

// NewKVCell new key-value cell (leaf node)
func NewKVCell(key []byte) *Cell {
	return &Cell{
		flags:   KV_CELL,
		keySize: len(key),
		key:     key,
	}
}

// SetValue Set the value for a KV cell
func (c *Cell) SetValue(val interface{}) error {
	if c.flags != KV_CELL {
		return fmt.Errorf("cannot set value on key-only cell")
	}

	switch v := val.(type) {
	case int:
		c.valueType = INTEGER_TYPE
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))
		c.value = buf
		c.valueSize = 4

	case string:
		c.valueType = STRING_TYPE
		c.value = []byte(v)
		c.valueSize = len(c.value)

	case bool:
		c.valueType = BOOL_TYPE
		if v {
			c.value = []byte{1}
		} else {
			c.value = []byte{0}
		}
		c.valueSize = 1

	case time.Time:
		c.valueType = DATE_TYPE
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v.Unix()))
		c.value = buf
		c.valueSize = 8

	case []byte:
		c.valueType = BYTES_TYPE
		c.value = v
		c.valueSize = len(v)

	default:
		return fmt.Errorf("unsupported type")
	}
	return nil
}

// GetValue Get the value from a KV cell
func (c *Cell) GetValue() (interface{}, error) {
	if c.flags != KV_CELL {
		return nil, fmt.Errorf("cannot get value from key-only cell")
	}

	switch c.valueType {
	case INTEGER_TYPE:
		return int(binary.BigEndian.Uint32(c.value)), nil
	case STRING_TYPE:
		return string(c.value), nil
	case BOOL_TYPE:
		return c.value[0] == 1, nil
	case DATE_TYPE:
		timestamp := binary.BigEndian.Uint64(c.value)
		return time.Unix(int64(timestamp), 0), nil
	case BYTES_TYPE:
		return c.value, nil
	default:
		return nil, fmt.Errorf("unknown type")
	}
}

// Size Calculate total cell size in bytes
func (c *Cell) Size() int {
	size := 1 + 4 + 4 // flags + keySize + valueSize
	size += c.keySize
	if c.flags == KV_CELL {
		size += c.valueSize
	} else {
		size += 8 // pageId for key-only cells
	}
	return size
}

func (c *Cell) FitsInPage(remainingSpace int) bool {
	return c.Size() <= remainingSpace
}

func (c *Cell) MarkDeleted() {
	c.flags |= FLAG_DELETED
}

func (c *Cell) IsDeleted() bool {
	return (c.flags & FLAG_DELETED) != 0
}

// ToBytes Serialize cell from bytes
func (c *Cell) ToBytes() []byte {
	buf := new(bytes.Buffer)

	// Write header
	buf.WriteByte(c.flags)
	err := binary.Write(buf, binary.BigEndian, uint32(c.keySize))
	if err != nil {
		return nil
	}

	if c.flags == KV_CELL {
		err := binary.Write(buf, binary.BigEndian, uint32(c.valueSize))
		if err != nil {
			return nil
		}
		buf.WriteByte(c.valueType)
	}

	// Write key
	buf.Write(c.key)

	// Write value or pageId
	if c.flags == KV_CELL {
		buf.Write(c.value)
	} else {
		binary.Write(buf, binary.BigEndian, c.pageId)
	}

	return buf.Bytes()
}

// CellFromBytes Deserialize cell from bytes
func CellFromBytes(data []byte) (*Cell, error) {
	buf := bytes.NewBuffer(data)

	cell := &Cell{}

	// Read header
	flags, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	cell.flags = flags

	var keySize uint32
	err = binary.Read(buf, binary.BigEndian, &keySize)
	if err != nil {
		return nil, err
	}
	cell.keySize = int(keySize)

	if cell.flags == KV_CELL {
		var valueSize uint32
		err = binary.Read(buf, binary.BigEndian, &valueSize)
		if err != nil {
			return nil, err
		}
		cell.valueSize = int(valueSize)

		valueType, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		cell.valueType = valueType
	}

	// Read key
	cell.key = make([]byte, cell.keySize)
	_, err = buf.Read(cell.key)
	if err != nil {
		return nil, err
	}

	// Read value or pageId
	if cell.flags == KV_CELL {
		cell.value = make([]byte, cell.valueSize)
		_, err = buf.Read(cell.value)
		if err != nil {
			return nil, err
		}
	} else {
		err = binary.Read(buf, binary.BigEndian, &cell.pageId)
		if err != nil {
			return nil, err
		}
	}

	return cell, nil
}
