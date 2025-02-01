package kfile

import (
	"bytes"
	"fmt"
)

// Header field offsets (in bytes)
const (
	pageSizeOffset   = 0  // Page size stored at offset 0
	headerSizeOffset = 4  // Header size stored at offset 4
	cellCountOffset  = 8  // Number of cells stored at offset 8
	freeSpaceOffset  = 12 // Free space pointer stored at offset 12
	PageHeaderSize   = 24 // Fixed header size (may include additional metadata)
	DefaultPageSize  = 8196
	slotPointerSize  = 4 // Size reserved for a slot pointer (used in cell offset calculations)
)

// SlottedPage represents a page with a slotted structure
type SlottedPage struct {
	*Page            // Embeds the underlying Page
	headerSize int   // Fixed header size (including slot array)
	cellCount  int   // Number of cells in the page
	freeSpace  int   // Offset where free space begins
	slots      []int // Array of offsets to cells (sorted by key)
}

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

	// Initialize header fields.
	if err := sp.SetInt(pageSizeOffset, pageSize); err != nil {
		return nil
	}
	if err := sp.SetInt(headerSizeOffset, PageHeaderSize); err != nil {
		return nil
	}
	if err := sp.SetInt(cellCountOffset, 0); err != nil {
		return nil
	}
	if err := sp.SetInt(freeSpaceOffset, pageSize); err != nil {
		return nil
	}

	return sp
}

// GetFreeSpace returns the current free space pointer.
func (sp *SlottedPage) GetFreeSpace() int {
	return sp.freeSpace
}

func (sp *SlottedPage) InsertCell(cell *Cell) error {
	cellBytes := cell.ToBytes()
	cellSize := len(cellBytes)

	// Ensure there is enough free space (header is reserved at the beginning).
	usableSpace := sp.freeSpace - sp.headerSize
	if usableSpace < cellSize {
		return fmt.Errorf("not enough space: need %d bytes but only %d bytes available", cellSize, usableSpace)
	}

	// Check if the cell itself fits within the available free space.
	if !cell.FitsInPage(sp.freeSpace) {
		return fmt.Errorf("cell too large for remaining page space")
	}

	// Calculate the new cell offset.
	// Reserve extra space (slotPointerSize bytes) for internal bookkeeping if needed.
	newOffset := sp.freeSpace - cellSize - slotPointerSize

	// Write the cell data to the underlying page.
	if err := sp.SetBytes(newOffset, cellBytes); err != nil {
		return fmt.Errorf("failed to write cell bytes: %w", err)
	}

	// Find the insertion index for the cell in the sorted slot array.
	insertPos := sp.FindSlotPosition(cell.key)

	// AllocateBufferForBlock new cell offset into the slot array.
	sp.slots = append(sp.slots, 0)                     // Grow slice by one.
	copy(sp.slots[insertPos+1:], sp.slots[insertPos:]) // Shift slots to the right.
	sp.slots[insertPos] = newOffset

	// Update page header values.
	sp.cellCount++
	sp.freeSpace = newOffset

	if err := sp.SetInt(cellCountOffset, sp.cellCount); err != nil {
		return fmt.Errorf("failed to update cell count: %w", err)
	}
	if err := sp.SetInt(freeSpaceOffset, sp.freeSpace); err != nil {
		return fmt.Errorf("failed to update free space pointer: %w", err)
	}

	return nil
}

// FindSlotPosition returns the insertion index for a new cell (by key) using binary search.
func (sp *SlottedPage) FindSlotPosition(key []byte) int {
	low, high := 0, len(sp.slots)-1
	for low <= high {
		mid := (low + high) / 2
		cell, err := sp.GetCell(sp.slots[mid])
		if err != nil {
			// In case of error reading the cell, default to inserting at the beginning.
			return low
		}
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

// GetCell retrieves the cell stored at the specified offset.
func (sp *SlottedPage) GetCell(offset int) (*Cell, error) {
	cellBytes, err := sp.GetBytes(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get cell bytes at offset %d: %w", offset, err)
	}
	return CellFromBytes(cellBytes)
}

// GetCellBySlot retrieves the cell at the given slot index.
func (sp *SlottedPage) GetCellBySlot(slot int) (*Cell, error) {
	if slot < 0 || slot >= len(sp.slots) {
		return nil, fmt.Errorf("invalid slot index: %d", slot)
	}
	return sp.GetCell(sp.slots[slot])
}

// DeleteCell marks the cell at the given slot as deleted and removes its slot entry.
func (sp *SlottedPage) DeleteCell(slot int) error {
	if slot < 0 || slot >= len(sp.slots) {
		return fmt.Errorf("invalid slot index: %d", slot)
	}

	cell, err := sp.GetCell(sp.slots[slot])
	if err != nil {
		return fmt.Errorf("failed to get cell for deletion: %w", err)
	}
	cell.MarkDeleted()

	// Remove the slot from the sorted slot array.
	sp.slots = append(sp.slots[:slot], sp.slots[slot+1:]...)
	sp.cellCount--

	if err := sp.SetInt(cellCountOffset, sp.cellCount); err != nil {
		return fmt.Errorf("failed to update cell count after deletion: %w", err)
	}
	return nil
}

// FindCell performs a binary search for a cell by key.
// Returns the cell, its slot index, or an error if not found.
func (sp *SlottedPage) FindCell(key []byte) (*Cell, int, error) {
	low, high := 0, len(sp.slots)-1
	for low <= high {
		mid := (low + high) / 2
		cell, err := sp.GetCell(sp.slots[mid])
		if err != nil {
			return nil, -1, fmt.Errorf("failed to retrieve cell at slot %d: %w", mid, err)
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

// Compact defragments the page by removing deleted cells and re-packing live cells.
func (sp *SlottedPage) Compact() error {
	// Create a new slotted page with the same underlying size.
	newPage := NewSlottedPage(len(sp.data))
	if newPage == nil {
		return fmt.Errorf("failed to create new page for compaction")
	}

	// Re-insert all non-deleted cells into the new page.
	for _, offset := range sp.slots {
		cell, err := sp.GetCell(offset)
		if err != nil {
			return fmt.Errorf("failed to retrieve cell during compaction: %w", err)
		}
		if !cell.IsDeleted() {
			if err := newPage.InsertCell(cell); err != nil {
				return fmt.Errorf("failed to insert cell during compaction: %w", err)
			}
		}
	}

	// Replace the current page data and metadata with the compacted version.
	sp.data = newPage.data
	sp.slots = newPage.slots
	sp.cellCount = newPage.cellCount
	sp.freeSpace = newPage.freeSpace

	return nil
}

// GetAllSlots returns the list of cell offsets (slots) in the page.
func (sp *SlottedPage) GetAllSlots() []int {
	return sp.slots
}
