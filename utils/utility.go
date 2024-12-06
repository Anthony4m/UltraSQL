package utils

import "unicode/utf8"

func MaxLength(strlen int) int {
	if strlen < 0 {
		panic("String length cannot be negative")
	}
	// Estimate bytes required for UTF-8 encoding
	bytesPerChar := utf8.RuneLen('a') // Assuming single-character equivalence
	return 4 + (strlen * bytesPerChar)
}
