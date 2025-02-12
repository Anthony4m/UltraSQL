package log_record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"ultraSQL/log"
	"ultraSQL/txinterface"
)

// StartRecord represents a transaction start log record
type StartRecord struct {
	txnum int64
}

// CommitRecord represents a transaction commit log record
type CommitRecord struct {
	txnum int64
}

// RollbackRecord represents a transaction rollback log record
type RollbackRecord struct {
	txnum int64
}

// CheckpointRecord represents a checkpoint in the log
type CheckpointRecord struct{}

// Constructor functions
func NewStartRecord(txnum int64) *StartRecord {
	return &StartRecord{txnum: txnum}
}

func NewCommitRecord(txnum int64) *CommitRecord {
	return &CommitRecord{txnum: txnum}
}

func NewRollbackRecord(txnum int64) *RollbackRecord {
	return &RollbackRecord{txnum: txnum}
}

func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

// ToBytes implementations
func (r *StartRecord) ToBytes() []byte {
	var buf bytes.Buffer

	// Write record type
	if err := binary.Write(&buf, binary.BigEndian, int32(START)); err != nil {
		return nil
	}

	// Write transaction number
	if err := binary.Write(&buf, binary.BigEndian, r.txnum); err != nil {
		return nil
	}

	return buf.Bytes()
}

func (r *CommitRecord) ToBytes() []byte {
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, int32(COMMIT)); err != nil {
		return nil
	}
	if err := binary.Write(&buf, binary.BigEndian, r.txnum); err != nil {
		return nil
	}

	return buf.Bytes()
}

func (r *RollbackRecord) ToBytes() []byte {
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, int32(ROLLBACK)); err != nil {
		return nil
	}
	if err := binary.Write(&buf, binary.BigEndian, r.txnum); err != nil {
		return nil
	}

	return buf.Bytes()
}

func (r *CheckpointRecord) ToBytes() []byte {
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, int32(CHECKPOINT)); err != nil {
		return nil
	}

	return buf.Bytes()
}

// FromBytes functions
func NewStartRecordFromBytes(data []byte) (*StartRecord, error) {
	buf := bytes.NewBuffer(data)

	// Skip past record type
	if err := binary.Read(buf, binary.BigEndian, new(int32)); err != nil {
		return nil, fmt.Errorf("failed to read record type: %w", err)
	}

	var txnum int64
	if err := binary.Read(buf, binary.BigEndian, &txnum); err != nil {
		return nil, fmt.Errorf("failed to read transaction number: %w", err)
	}

	return NewStartRecord(txnum), nil
}

// Write functions with improved error handling
func StartRecordWriteToLog(lm *log.LogMgr, txnum int64) (int, error) {
	record := NewStartRecord(txnum)
	lsn, _, err := lm.Append(record.ToBytes())
	if err != nil {
		return -1, fmt.Errorf("failed to write start record to log: %w", err)
	}
	return lsn, nil
}

func NewCommitRecordFromBytes(data []byte) (*CommitRecord, error) {
	buf := bytes.NewBuffer(data)

	// Skip past record type
	if err := binary.Read(buf, binary.BigEndian, new(int32)); err != nil {
		return nil, fmt.Errorf("failed to read record type: %w", err)
	}

	var txnum int64
	if err := binary.Read(buf, binary.BigEndian, &txnum); err != nil {
		return nil, fmt.Errorf("failed to read transaction number: %w", err)
	}

	return NewCommitRecord(txnum), nil
}

func CommitRecordWriteToLog(lm *log.LogMgr, txnum int64) (int, error) {
	record := NewCommitRecord(txnum)
	lsn, _, err := lm.Append(record.ToBytes())
	if err != nil {
		return -1, fmt.Errorf("failed to write commit record to log: %w", err)
	}
	return lsn, nil
}

func NewRollbackRecordFromBytes(data []byte) (*RollbackRecord, error) {
	buf := bytes.NewBuffer(data)

	// Skip past record type
	if err := binary.Read(buf, binary.BigEndian, new(int32)); err != nil {
		return nil, fmt.Errorf("failed to read record type: %w", err)
	}

	var txnum int64
	if err := binary.Read(buf, binary.BigEndian, &txnum); err != nil {
		return nil, fmt.Errorf("failed to read transaction number: %w", err)
	}

	return NewRollbackRecord(txnum), nil
}

func RollbackRecordWriteToLog(lm *log.LogMgr, txnum int64) (int, error) {
	record := NewRollbackRecord(txnum)
	lsn, _, err := lm.Append(record.ToBytes())
	if err != nil {
		return -1, fmt.Errorf("failed to write rollback record to log: %w", err)
	}
	return lsn, nil
}

func NewCheckpointRecordFromBytes(data []byte) (*CheckpointRecord, error) {
	buf := bytes.NewBuffer(data)

	// Skip past record type
	if err := binary.Read(buf, binary.BigEndian, new(int32)); err != nil {
		return nil, fmt.Errorf("failed to read record type: %w", err)
	}

	return NewCheckpointRecord(), nil
}

func CheckpointRecordWriteToLog(lm *log.LogMgr) (int, error) {
	record := NewCheckpointRecord()
	lsn, _, err := lm.Append(record.ToBytes())
	if err != nil {
		return -1, fmt.Errorf("failed to write checkpoint record to log: %w", err)
	}
	return lsn, nil
}

// Type and TxNum getters for each record type
func (r *StartRecord) Op() int32 {
	return START
}

func (r *StartRecord) TxNumber() int64 {
	return r.txnum
}

func (r *StartRecord) Undo(tx txinterface.TxInterface) error {
	return nil
}

func (r *CommitRecord) Op() int32 {
	return COMMIT
}

func (r *CommitRecord) TxNumber() int64 {
	return r.txnum
}

func (r *CommitRecord) Undo(tx txinterface.TxInterface) error {
	return nil
}

func (r *RollbackRecord) Op() int32 {
	return ROLLBACK
}

func (r *RollbackRecord) TxNumber() int64 {
	return r.txnum
}

func (r *RollbackRecord) Undo(tx txinterface.TxInterface) error {
	return nil
}

func (r *CheckpointRecord) Op() int32 {
	return CHECKPOINT
}

func (r *CheckpointRecord) TxNumber() int64 {
	return -1
}

func (r *CheckpointRecord) Undo(tx txinterface.TxInterface) error {
	return nil
}
