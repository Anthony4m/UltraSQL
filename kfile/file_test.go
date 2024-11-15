package kfile

import (
	"testing"
)

func TestPage(t *testing.T) {
	t.Run("NewPage creates page with correct size", func(t *testing.T) {
		blockSize := 4096
		page := NewPage(blockSize)
		if len(page.data) != blockSize {
			t.Errorf("expected page size %d, got %d", blockSize, len(page.data))
		}
	})

	t.Run("Integer operations work correctly", func(t *testing.T) {
		page := NewPage(100)
		testVal := int32(42)

		err := page.SetInt(0, int(testVal))
		if err != nil {
			t.Fatalf("SetInt failed: %v", err)
		}

		got, err := page.GetInt(0)
		if err != nil {
			t.Fatalf("GetInt failed: %v", err)
		}
		if got != testVal {
			t.Errorf("expected %d, got %d", testVal, got)
		}
	})
}
