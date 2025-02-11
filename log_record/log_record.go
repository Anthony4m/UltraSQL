package log_record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"ultraSQL/kfile"
	"ultraSQL/log"
)

const (
	UNIFIEDUPDATE = 5 // Add this with other log record type constants
)

type UnifiedUpdateRecord struct {
	txnum    int64
	blk      kfile.BlockId
	key      []byte
	oldBytes []byte
	newBytes []byte
}

// FromBytesUnifiedUpdate creates a UnifiedUpdateRecord from raw bytes
func FromBytesUnifiedUpdate(data []byte) (*UnifiedUpdateRecord, error) {
	buf := bytes.NewBuffer(data)

	// Skip past the record type
	if err := binary.Read(buf, binary.BigEndian, new(int32)); err != nil {
		return nil, fmt.Errorf("failed to read record type: %w", err)
	}

	// Read transaction number
	var txnum int64
	if err := binary.Read(buf, binary.BigEndian, &txnum); err != nil {
		return nil, fmt.Errorf("failed to read transaction number: %w", err)
	}

	// Read filename length
	var filenameLen uint32
	if err := binary.Read(buf, binary.BigEndian, &filenameLen); err != nil {
		return nil, fmt.Errorf("failed to read filename length: %w", err)
	}

	// Read filename
	filename := make([]byte, filenameLen)
	if _, err := buf.Read(filename); err != nil {
		return nil, fmt.Errorf("failed to read filename: %w", err)
	}

	// Read block number
	var blkNum int
	if err := binary.Read(buf, binary.BigEndian, &blkNum); err != nil {
		return nil, fmt.Errorf("failed to read block number: %w", err)
	}

	// Read key length
	var keyLen uint32
	if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}

	// Read key
	key := make([]byte, keyLen)
	if _, err := buf.Read(key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	// Read old value length
	var oldValueLen uint32
	if err := binary.Read(buf, binary.BigEndian, &oldValueLen); err != nil {
		return nil, fmt.Errorf("failed to read old value length: %w", err)
	}

	// Read old value
	oldBytes := make([]byte, oldValueLen)
	if _, err := buf.Read(oldBytes); err != nil {
		return nil, fmt.Errorf("failed to read old value: %w", err)
	}

	// Read new value length
	var newValueLen uint32
	if err := binary.Read(buf, binary.BigEndian, &newValueLen); err != nil {
		return nil, fmt.Errorf("failed to read new value length: %w", err)
	}

	// Read new value
	newBytes := make([]byte, newValueLen)
	if _, err := buf.Read(newBytes); err != nil {
		return nil, fmt.Errorf("failed to read new value: %w", err)
	}

	// Create BlockId
	blk := kfile.NewBlockId(string(filename), blkNum)

	return &UnifiedUpdateRecord{
		txnum:    txnum,
		blk:      *blk,
		key:      key,
		oldBytes: oldBytes,
		newBytes: newBytes,
	}, nil
}

func (r *UnifiedUpdateRecord) ToBytes() []byte {
	var buf bytes.Buffer

	// Write record type
	if err := binary.Write(&buf, binary.BigEndian, int32(UNIFIEDUPDATE)); err != nil {
		return nil
	}

	// Write transaction number
	if err := binary.Write(&buf, binary.BigEndian, r.txnum); err != nil {
		return nil
	}

	// Write filename length and filename
	filename := r.blk.FileName()
	filenameBytes := []byte(filename)
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(filenameBytes))); err != nil {
		return nil
	}
	if _, err := buf.Write(filenameBytes); err != nil {
		return nil
	}

	// Write block number
	if err := binary.Write(&buf, binary.BigEndian, r.blk.Number()); err != nil {
		return nil
	}

	// Write key length and key
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(r.key))); err != nil {
		return nil
	}
	if _, err := buf.Write(r.key); err != nil {
		return nil
	}

	// Write old value length and bytes
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(r.oldBytes))); err != nil {
		return nil
	}
	if _, err := buf.Write(r.oldBytes); err != nil {
		return nil
	}

	// Write new value length and bytes
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(r.newBytes))); err != nil {
		return nil
	}
	if _, err := buf.Write(r.newBytes); err != nil {
		return nil
	}

	return buf.Bytes()
}

// WriteToLog writes a unified update record to the log and returns the LSN
func WriteToLog(lm log.LogMgr, txnum int64, blk kfile.BlockId, key []byte, oldBytes []byte, newBytes []byte) int {
	record := &UnifiedUpdateRecord{
		txnum:    txnum,
		blk:      blk,
		key:      key,
		oldBytes: oldBytes,
		newBytes: newBytes,
	}

	// Write directly to log manager
	lsn, _, err := lm.Append(record.ToBytes())
	if err != nil {
		return -1
	}
	return lsn
}
func CreateLogRecord(data []byte) *UnifiedUpdateRecord {
	// Peek at op code
	if len(data) < 4 {
		return nil
	}
	op := int32(binary.BigEndian.Uint32(data[0:4]))
	switch op {
	case log.CHECKPOINT:
		return NewCheckpointRecordFromBytes(data)
	case log.START:
		return NewStartRecordFromBytes(data)
	case log.COMMIT:
		return NewCommitRecordFromBytes(data)
	case log.ROLLBACK:
		return NewRollbackRecordFromBytes(data)
	case UNIFIEDUPDATE:
		rec, _ := FromBytesUnifiedUpdate(data)
		return rec
	default:
		return nil
	}
}
