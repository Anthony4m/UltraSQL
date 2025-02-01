package kfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// Constants for cell types and flags.
// Reserve the lower 4 bits for the cell type and the upper 4 bits for flags.
const (
	// Cell types (lower nibble)
	CellTypeKey = 1 // Internal node cell (key + page pointer)
	CellTypeKV  = 2 // Leaf node cell (key + value)

	// Flag bits (upper nibble)
	FlagDeleted  = 1 << 4 // Mark cell as deleted
	FlagOverflow = 1 << 5 // Record doesn’t fit in page
)

// Data types for values.
const (
	IntegerType = 1
	StringType  = 2
	BoolType    = 3
	DateType    = 4
	BytesType   = 5
)

type Cell struct {
	// The cell type is stored in the lower nibble.
	cellType byte
	// Flags (e.g. deleted, overflow) are stored in the upper nibble.
	flags byte

	key       []byte
	value     []byte
	keySize   int
	valueSize int

	pageId    uint64
	keyType   byte
	valueType byte
	offset    int
}

func NewKeyCell(key []byte, childPageId uint64) *Cell {
	return &Cell{
		cellType: CellTypeKey,
		flags:    0,
		key:      key,
		keySize:  len(key),
		pageId:   childPageId,
	}
}

func NewKVCell(key []byte) *Cell {
	return &Cell{
		cellType: CellTypeKV,
		flags:    0,
		key:      key,
		keySize:  len(key),
	}
}

func (c *Cell) SetValue(val any) error {
	if c.cellType != CellTypeKV {
		return fmt.Errorf("cannot set value on a non-KV (leaf) cell")
	}

	switch v := val.(type) {
	case int:
		c.valueType = IntegerType
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))
		c.value = buf
		c.valueSize = 4

	case string:
		c.valueType = StringType
		c.value = []byte(v)
		c.valueSize = len(c.value)

	case bool:
		c.valueType = BoolType
		if v {
			c.value = []byte{1}
		} else {
			c.value = []byte{0}
		}
		c.valueSize = 1

	case time.Time:
		c.valueType = DateType
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v.Unix()))
		c.value = buf
		c.valueSize = 8

	case []byte:
		c.valueType = BytesType
		c.value = v
		c.valueSize = len(v)

	default:
		return fmt.Errorf("unsupported value type: %T", val)
	}
	return nil
}

func (c *Cell) GetValue() (any, error) {
	if c.cellType != CellTypeKV {
		return nil, fmt.Errorf("cannot get value from a non-KV (leaf) cell")
	}

	switch c.valueType {
	case IntegerType:
		if len(c.value) < 4 {
			return nil, fmt.Errorf("invalid data for integer")
		}
		return int(binary.BigEndian.Uint32(c.value)), nil
	case StringType:
		return string(c.value), nil
	case BoolType:
		if len(c.value) < 1 {
			return nil, fmt.Errorf("invalid data for bool")
		}
		return c.value[0] == 1, nil
	case DateType:
		if len(c.value) < 8 {
			return nil, fmt.Errorf("invalid data for date")
		}
		timestamp := binary.BigEndian.Uint64(c.value)
		return time.Unix(int64(timestamp), 0), nil
	case BytesType:
		return c.value, nil
	default:
		return nil, fmt.Errorf("unknown value type: %d", c.valueType)
	}
}

func (c *Cell) Size() int {
	// 1 byte for header, 4 bytes each for keySize and (if KV) valueSize.
	size := 1 + 4
	if c.cellType == CellTypeKV {
		size += 4 + 1 // additional 4 for valueSize and 1 for valueType
	}
	size += c.keySize
	if c.cellType == CellTypeKV {
		size += c.valueSize
	} else {
		size += 8 // for pageId in key-only cells
	}
	return size
}

func (c *Cell) FitsInPage(remainingSpace int) bool {
	return c.Size() <= remainingSpace
}

func (c *Cell) MarkDeleted() {
	c.flags |= FlagDeleted
}

func (c *Cell) IsDeleted() bool {
	return (c.flags & FlagDeleted) != 0
}

func (c *Cell) GetKey() []byte {
	return c.key
}

func (c *Cell) ToBytes() []byte {
	buf := new(bytes.Buffer)

	// Compose header: upper nibble is flags, lower nibble is cell type.
	headerByte := (c.flags & 0xF0) | (c.cellType & 0x0F)
	if err := buf.WriteByte(headerByte); err != nil {
		return nil
	}

	// Write key size.
	if err := binary.Write(buf, binary.BigEndian, uint32(c.keySize)); err != nil {
		return nil
	}

	if c.cellType == CellTypeKV {
		// Write value size and value type.
		if err := binary.Write(buf, binary.BigEndian, uint32(c.valueSize)); err != nil {
			return nil
		}
		if err := buf.WriteByte(c.valueType); err != nil {
			return nil
		}
	}

	// Write key.
	if _, err := buf.Write(c.key); err != nil {
		return nil
	}

	// Write value or pageId.
	if c.cellType == CellTypeKV {
		if _, err := buf.Write(c.value); err != nil {
			return nil
		}
	} else {
		if err := binary.Write(buf, binary.BigEndian, c.pageId); err != nil {
			return nil
		}
	}

	return buf.Bytes()
}

// CellFromBytes deserializes a cell from the given byte slice.
func CellFromBytes(data []byte) (*Cell, error) {
	buf := bytes.NewBuffer(data)
	cell := &Cell{}

	// Read header.
	headerByte, err := buf.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	// Lower 4 bits: cell type; upper 4 bits: flags.
	cell.cellType = headerByte & 0x0F
	cell.flags = headerByte & 0xF0

	// Read key size.
	var keySize uint32
	if err := binary.Read(buf, binary.BigEndian, &keySize); err != nil {
		return nil, fmt.Errorf("failed to read key size: %w", err)
	}
	cell.keySize = int(keySize)

	if cell.cellType == CellTypeKV {
		// For KV cells, read value size and value type.
		var valueSize uint32
		if err := binary.Read(buf, binary.BigEndian, &valueSize); err != nil {
			return nil, fmt.Errorf("failed to read value size: %w", err)
		}
		cell.valueSize = int(valueSize)

		valueType, err := buf.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read value type: %w", err)
		}
		cell.valueType = valueType
	}

	// Read key.
	cell.key = make([]byte, cell.keySize)
	if n, err := buf.Read(cell.key); err != nil || n != cell.keySize {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	// Read value or pageId.
	if cell.cellType == CellTypeKV {
		cell.value = make([]byte, cell.valueSize)
		if n, err := buf.Read(cell.value); err != nil || n != cell.valueSize {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}
	} else {
		if err := binary.Read(buf, binary.BigEndian, &cell.pageId); err != nil {
			return nil, fmt.Errorf("failed to read pageId: %w", err)
		}
	}

	return cell, nil
}
