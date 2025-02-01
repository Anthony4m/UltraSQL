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

	if cell.cellType != CellTypeKV {
		t.Errorf("Expected KV_CELL flag, got %d", cell.cellType)
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
		{"Integer", 42, IntegerType, false},
		{"String", "test", StringType, false},
		{"Boolean", true, BoolType, false},
		//{"Date", time.Now(), DateType, false},
		{"Bytes", []byte{1, 2, 3}, BytesType, false},
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
					if !v.Round(time.Millisecond).Equal(tt.value.(time.Time).Round(time.Millisecond)) {
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
				err := cell.SetValue("value")
				if err != nil {
					t.Errorf("an error occured %s", err)
				}
				return cell
			},
		},
		{
			name: "KV Cell with Integer",
			setup: func() *Cell {
				cell := NewKVCell([]byte("key"))
				err := cell.SetValue(42)
				if err != nil {
					t.Errorf("an error occured %s", err)
				}
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

			if original.cellType == CellTypeKV {
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

	// AllocateBufferForBlock cells with increasing keys
	for i := 0; i < 10; i++ {
		cell := NewKVCell([]byte(fmt.Sprintf("key%d", i)))
		err := cell.SetValue(fmt.Sprintf("value%d", i))
		if err != nil {
			t.Errorf("an error occurred %s", err)
		}

		err = page.InsertCell(cell)
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

	// AllocateBufferForBlock cells
	for i := 0; i < 5; i++ {
		cell := NewKVCell([]byte(fmt.Sprintf("key%d", i)))
		cell.SetValue(fmt.Sprintf("value%d", i))
		if err := page.InsertCell(cell); err != nil {
			t.Fatalf("Failed to insert cell %d: %v", i, err)
		}
	}

	// Store initial state
	originalFreeSpace := page.freeSpace
	originalSlots := make([]int, len(page.slots))
	copy(originalSlots, page.slots)

	// Delete middle cell (key2)
	err := page.DeleteCell(2)
	if err != nil {
		t.Fatalf("Failed to delete cell: %v", err)
	}

	// Verify cell count and slots decreased
	if page.cellCount != 4 {
		t.Errorf("Expected cell count 4, got %d", page.cellCount)
	}
	if len(page.slots) != 4 {
		t.Errorf("Expected 4 slots after deletion, got %d", len(page.slots))
	}

	// Verify slot array was adjusted correctly
	// First two slots should remain the same
	for i := 0; i < 2; i++ {
		if page.slots[i] != originalSlots[i] {
			t.Errorf("Slot %d changed unexpectedly after deletion", i)
		}
	}
	// Last two slots should now contain what were originally slots 3 and 4
	for i := 2; i < 4; i++ {
		if page.slots[i] != originalSlots[i+1] {
			t.Errorf("Slot %d not properly shifted after deletion", i)
		}
	}

	// Try to find deleted key - should fail
	_, _, err = page.FindCell([]byte("key2"))
	if err == nil {
		t.Error("Expected key2 to not be found after deletion")
	}

	// Compact page and verify space reclamation
	err = page.Compact()
	if err != nil {
		t.Fatalf("Failed to compact page: %v", err)
	}

	if page.freeSpace <= originalFreeSpace {
		t.Error("Compaction did not reclaim space")
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

	// AllocateBufferForBlock cells until page is full
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
	err = cell.SetValue("value")
	if err != nil {
		t.Errorf("an error occured %s", err)
	}
	err = page.InsertCell(cell)
	if err == nil {
		t.Error("Expected error when inserting into full page")
	}
}
