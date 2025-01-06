package kfile

import (
	"bytes"
	"fmt"
)

type SlottedPage struct {
	*Page            // Embed base Page
	headerSize int   // Size of header including slot array
	cellCount  int   // Number of cells in page
	freeSpace  int   // Offset where free space begins
	maxCells   int   // Maximum cells this page can hold
	slots      []int // Array of offsets to cells (sorted by key)
}

const (
	PageHeaderSize  = 24   // Fixed header size
	DefaultPageSize = 8196 // 4KB default page size
)

func NewSlottedPage(pageSize int) *SlottedPage {
	if pageSize == 0 {
		pageSize = DefaultPageSize
	}

	sp := &SlottedPage{
		Page:       NewPage(pageSize),
		headerSize: PageHeaderSize,
		freeSpace:  pageSize,
		slots:      make([]int, 0),
	}

	// Initialize header
	err := sp.SetInt(0, pageSize)      // Page size
	err = sp.SetInt(4, PageHeaderSize) // Header size
	err = sp.SetInt(8, 0)              // Cell count
	err = sp.SetInt(12, pageSize)      // Free space pointer
	if err != nil {
		return nil
	}
	return sp
}

// InsertCell Insert a cell into the page
func (sp *SlottedPage) InsertCell(cell *Cell) error {
	cellBytes := cell.ToBytes()
	cellSize := len(cellBytes)

	// Check if we have enough space
	if !cell.FitsInPage(sp.freeSpace) {
		return fmt.Errorf("cell too large full")
	}

	// Calculate new cell offset (from end of page)
	newOffset := (sp.freeSpace - cellSize) - 4

	// Write cell data
	err := sp.SetBytes(newOffset, cellBytes)
	if err != nil {
		return err
	}

	// Find insertion point in slot array (binary search by key)
	insertPos := sp.findSlotPosition(cell.key)

	// Insert new offset into slot array
	sp.slots = append(sp.slots, 0)
	copy(sp.slots[insertPos+1:], sp.slots[insertPos:])
	sp.slots[insertPos] = newOffset

	// Update header
	sp.cellCount++
	sp.freeSpace = newOffset
	err = sp.SetInt(8, sp.cellCount)
	err = sp.SetInt(12, sp.freeSpace)
	if err != nil {
		return err
	}

	return nil
}

// Find position to insert new cell based on key
func (sp *SlottedPage) findSlotPosition(key []byte) int {
	// Binary search through slots
	low, high := 0, len(sp.slots)-1

	for low <= high {
		mid := (low + high) / 2

		// Get cell at this slot
		cell, err := sp.GetCell(sp.slots[mid])
		if err != nil {
			return low // Default to beginning on error
		}

		// Compare keys
		comp := bytes.Compare(key, cell.key)
		if comp == 0 {
			return mid
		} else if comp < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return low
}

// GetCell Get cell at given offset
func (sp *SlottedPage) GetCell(offset int) (*Cell, error) {
	// Read cell bytes
	cellBytes, err := sp.GetBytes(offset)
	if err != nil {
		return nil, err
	}

	// Deserialize cell
	return CellFromBytes(cellBytes)
}

// GetCellBySlot Get cell by slot index
func (sp *SlottedPage) GetCellBySlot(slot int) (*Cell, error) {
	if slot >= len(sp.slots) {
		return nil, fmt.Errorf("invalid slot")
	}
	return sp.GetCell(sp.slots[slot])
}

// DeleteCell Delete cell at given slot
func (sp *SlottedPage) DeleteCell(slot int) error {
	if slot >= len(sp.slots) {
		return fmt.Errorf("invalid slot")
	}

	// Mark cell as deleted
	cell, err := sp.GetCell(sp.slots[slot])
	if err != nil {
		return err
	}
	cell.MarkDeleted()

	// Remove slot
	sp.slots = append(sp.slots[:slot], sp.slots[slot+1:]...)
	sp.cellCount--
	err = sp.SetInt(8, sp.cellCount)
	if err != nil {
		return err
	}
	return nil
}

// FindCell Find cell by key
func (sp *SlottedPage) FindCell(key []byte) (*Cell, int, error) {
	// Binary search through slots
	low, high := 0, len(sp.slots)-1

	for low <= high {
		mid := (low + high) / 2

		cell, err := sp.GetCell(sp.slots[mid])
		if err != nil {
			return nil, -1, err
		}

		comp := bytes.Compare(key, cell.key)
		if comp == 0 {
			return cell, mid, nil
		} else if comp < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return nil, -1, fmt.Errorf("key not found")
}

// Compact page by removing deleted cells and defragmentation space
func (sp *SlottedPage) Compact() error {
	newPage := NewSlottedPage(len(sp.data))

	// Copy all non-deleted cells to new page
	for _, offset := range sp.slots {
		cell, err := sp.GetCell(offset)
		if err != nil {
			return err
		}

		if !cell.IsDeleted() {
			err = newPage.InsertCell(cell)
			if err != nil {
				return err
			}
		}
	}

	// Replace current page data
	sp.data = newPage.data
	sp.slots = newPage.slots
	sp.cellCount = newPage.cellCount
	sp.freeSpace = newPage.freeSpace

	return nil
}
