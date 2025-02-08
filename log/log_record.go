package log

import (
	"fmt"
	"ultraSQL/kfile"
)

const (
	CHECKPOINT = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

// CreateLogRecord is a package-level function that inspects the bytes
// to figure out which concrete record to instantiate.
func CreateLogRecord(data []byte) (LogRecord, error) {
	// Suppose we have some helper "Page" type to interpret 'data'.
	cell := kfile.NewKVCell(data) // you'll define or import this
	cell.SetValue(data)
	p := kfile.NewSlottedPage(0) // you'll define or import this

	recordType := p.InsertCell(cell)
	switch recordType {
	case CHECKPOINT:
		return NewCheckpointRecord(), nil
	case START:
		return NewStartRecord(p), nil
	case COMMIT:
		return NewCommitRecord(p), nil
	case ROLLBACK:
		return NewRollbackRecord(p), nil
	case SETINT:
		return NewSetIntRecord(p), nil
	case SETSTRING:
		return NewSetStringRecord(p), nil
	default:
		return nil, fmt.Errorf("unknown log record type: %d", recordType)
	}
}
