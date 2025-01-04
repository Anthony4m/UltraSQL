package kfile

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestCell_Basic(t *testing.T) {
	// Test creating a basic key-value cell
	key := []byte("testKey")
	cell := NewKVCell(key)

	if cell.flags != KV_CELL {
		t.Errorf("Expected KV_CELL flag, got %d", cell.flags)
	}

	if cell.keySize != len(key) {
		t.Errorf("Expected key size %d, got %d", len(key), cell.keySize)
	}

	if !bytes.Equal(cell.key, key) {
		t.Error("Key mismatch")
	}
}

func TestCell_SetValue(t *testing.T) {
	tests := []struct {
		name    string
		value   interface{}
		valType byte
		wantErr bool
	}{
		{"Integer", 42, INTEGER_TYPE, false},
		{"String", "test", STRING_TYPE, false},
		{"Boolean", true, BOOL_TYPE, false},
		{"Date", time.Now(), DATE_TYPE, false},
		{"Bytes", []byte{1, 2, 3}, BYTES_TYPE, false},
		{"Invalid", struct{}{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cell := NewKVCell([]byte("key"))
			err := cell.SetValue(tt.value)

			if (err != nil) != tt.wantErr {
				t.Errorf("SetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if cell.valueType != tt.valType {
					t.Errorf("Expected value type %d, got %d", tt.valType, cell.valueType)
				}

				// Verify GetValue returns same value
				val, err := cell.GetValue()
				if err != nil {
					t.Errorf("GetValue() error = %v", err)
					return
				}

				switch v := val.(type) {
				case int:
					if v != tt.value.(int) {
						t.Errorf("Value mismatch: got %v, want %v", v, tt.value)
					}
				case string:
					if v != tt.value.(string) {
						t.Errorf("Value mismatch: got %v, want %v", v, tt.value)
					}
				case bool:
					if v != tt.value.(bool) {
						t.Errorf("Value mismatch: got %v, want %v", v, tt.value)
					}
				case time.Time:
					if !v.Equal(tt.value.(time.Time)) {
						t.Errorf("Value mismatch: got %v, want %v", v, tt.value)
					}
				case []byte:
					if !bytes.Equal(v, tt.value.([]byte)) {
						t.Errorf("Value mismatch: got %v, want %v", v, tt.value)
					}
				}
			}
		})
	}
}

func TestCell_Serialization(t *testing.T) {
	// Create test cell with various types
	tests := []struct {
		name  string
		setup func() *Cell
	}{
		{
			name: "KV Cell with String",
			setup: func() *Cell {
				cell := NewKVCell([]byte("key"))
				cell.SetValue("value")
				return cell
			},
		},
		{
			name: "KV Cell with Integer",
			setup: func() *Cell {
				cell := NewKVCell([]byte("key"))
				cell.SetValue(42)
				return cell
			},
		},
		{
			name: "Key Cell",
			setup: func() *Cell {
				return NewKeyCell([]byte("key"), 123)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := tt.setup()

			// Serialize
			data := original.ToBytes()

			// Deserialize
			restored, err := CellFromBytes(data)
			if err != nil {
				t.Fatalf("Failed to deserialize: %v", err)
			}

			// Compare
			if original.flags != restored.flags {
				t.Errorf("Flags mismatch: got %d, want %d", restored.flags, original.flags)
			}

			if !bytes.Equal(original.key, restored.key) {
				t.Errorf("Key mismatch: got %v, want %v", restored.key, original.key)
			}

			if original.flags == KV_CELL {
				if !bytes.Equal(original.value, restored.value) {
					t.Errorf("Value mismatch: got %v, want %v", restored.value, original.value)
				}
			} else {
				if original.pageId != restored.pageId {
					t.Errorf("PageId mismatch: got %d, want %d", restored.pageId, original.pageId)
				}
			}
		})
	}
}

func TestSlottedPage_Basic(t *testing.T) {
	page := NewSlottedPage(DefaultPageSize)

	if page.freeSpace != DefaultPageSize {
		t.Errorf("Expected free space %d, got %d", DefaultPageSize, page.freeSpace)
	}

	if len(page.slots) != 0 {
		t.Errorf("Expected empty slots, got %d slots", len(page.slots))
	}
}

func TestSlottedPage_InsertCell(t *testing.T) {
	page := NewSlottedPage(DefaultPageSize)

	// Insert cells with increasing keys
	for i := 0; i < 10; i++ {
		cell := NewKVCell([]byte(fmt.Sprintf("key%d", i)))
		cell.SetValue(fmt.Sprintf("value%d", i))

		err := page.InsertCell(cell)
		if err != nil {
			t.Fatalf("Failed to insert cell %d: %v", i, err)
		}

		// Verify cell count
		if page.cellCount != i+1 {
			t.Errorf("Expected cell count %d, got %d", i+1, page.cellCount)
		}

		// Verify slot array order
		if len(page.slots) != i+1 {
			t.Errorf("Expected slot count %d, got %d", i+1, len(page.slots))
		}
	}

	// Verify retrieval
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		cell, slot, err := page.FindCell(key)

		if err != nil {
			t.Errorf("Failed to find cell %d: %v", i, err)
			continue
		}

		if slot != i {
			t.Errorf("Expected slot %d, got %d", i, slot)
		}

		val, err := cell.GetValue()
		if err != nil {
			t.Errorf("Failed to get value for cell %d: %v", i, err)
			continue
		}

		expected := fmt.Sprintf("value%d", i)
		if val != expected {
			t.Errorf("Expected value %s, got %s", expected, val)
		}
	}
}

func TestSlottedPage_DeleteAndCompact(t *testing.T) {
	page := NewSlottedPage(DefaultPageSize)

	// Insert cells
	for i := 0; i < 5; i++ {
		cell := NewKVCell([]byte(fmt.Sprintf("key%d", i)))
		cell.SetValue(fmt.Sprintf("value%d", i))
		if err := page.InsertCell(cell); err != nil {
			t.Fatalf("Failed to insert cell %d: %v", i, err)
		}
	}

	originalFreeSpace := page.freeSpace

	// Delete middle cell
	err := page.DeleteCell(2)
	if err != nil {
		t.Fatalf("Failed to delete cell: %v", err)
	}

	if page.cellCount != 4 {
		t.Errorf("Expected cell count 4, got %d", page.cellCount)
	}

	// Verify cell is marked as deleted
	cell, err := page.GetCell(page.slots[2])
	if err != nil {
		t.Fatalf("Failed to get cell: %v", err)
	}

	if !cell.IsDeleted() {
		t.Error("Cell should be marked as deleted")
	}

	// Compact page
	err = page.Compact()
	if err != nil {
		t.Fatalf("Failed to compact page: %v", err)
	}

	// Verify space was reclaimed
	if page.freeSpace <= originalFreeSpace {
		t.Error("Compaction did not reclaim space")
	}

	// Verify remaining cells are intact and in order
	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if i >= 2 {
			key = []byte(fmt.Sprintf("key%d", i+1))
		}

		cell, _, err := page.FindCell(key)
		if err != nil {
			t.Errorf("Failed to find cell for key %s: %v", key, err)
			continue
		}

		val, err := cell.GetValue()
		if err != nil {
			t.Errorf("Failed to get value for key %s: %v", key, err)
			continue
		}

		expected := fmt.Sprintf("value%d", i)
		if i >= 2 {
			expected = fmt.Sprintf("value%d", i+1)
		}
		if val != expected {
			t.Errorf("Expected value %s, got %s", expected, val)
		}
	}
}

func TestSlottedPage_SpaceManagement(t *testing.T) {
	page := NewSlottedPage(100) // Small page size to test space management

	// Try to insert cell that's too large
	largeCell := NewKVCell([]byte("key"))
	largeCell.SetValue(bytes.Repeat([]byte("x"), 90)) // Almost fill the page

	err := page.InsertCell(largeCell)
	if err == nil {
		t.Error("Expected error when inserting cell too large for page")
	}

	// Insert cells until page is full
	i := 0
	for {
		cell := NewKVCell([]byte(fmt.Sprintf("k%d", i)))
		cell.SetValue(fmt.Sprintf("v%d", i))

		err := page.InsertCell(cell)
		if err != nil {
			break
		}
		i++
	}

	// Verify we can't insert any more cells
	cell := NewKVCell([]byte("final"))
	cell.SetValue("value")
	err = page.InsertCell(cell)
	if err == nil {
		t.Error("Expected error when inserting into full page")
	}
}