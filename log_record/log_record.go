package log_record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"ultraSQL/kfile"
	_ "ultraSQL/kfile"
	"ultraSQL/log"
	"ultraSQL/transaction"
)

// Example op code if you're not using separate ones.
const UNIFIEDUPDATE = 100

type UnifiedUpdateRecord struct {
	txNum    int64
	blkFile  string
	blkNum   int
	key      []byte
	oldBytes []byte
	newBytes []byte
}

func WriteUnifiedUpdateLogRecord(
	lm *log.LogMgr,
	txNum int64,
	blk *kfile.BlockId,
	key []byte,
	oldBytes []byte,
	newBytes []byte,
) int {
	// Implementation up to you; typically you’d:
	// 1) Create a record structure (e.g. UnifiedUpdateRecord).
	// 2) Serialize txNum, block info, slotIndex, oldBytes, newBytes.
	// 3) Append to log manager, returning the LSN.
	return 0 // placeholder
}

// Ensure it satisfies the LogRecord transaction_interface
func (rec *UnifiedUpdateRecord) Op() int {
	return UNIFIEDUPDATE
}
func (rec *UnifiedUpdateRecord) TxNumber() int64 {
	return rec.txNum
}

// Undo reverts the page/slot to oldBytes
func (rec *UnifiedUpdateRecord) Undo(tx *transaction.TransactionMgr) {
	// 1) Pin or fetch the buffer for rec.blkFile, rec.blkNum
	// 2) Cast to SlottedPage
	// 3) Overwrite the cell at rec.slotIndex with oldBytes
	fmt.Printf("Undoing unified update: restoring old cell bytes for tx=%d slot=%d\n", rec.txNum, rec.slotIndex)
	// ... actual code ...
}

// Serialize the record to bytes
func (rec *UnifiedUpdateRecord) ToBytes() []byte {
	buf := new(bytes.Buffer)
	// 1. Op code
	_ = binary.Write(buf, binary.BigEndian, int32(UNIFIEDUPDATE))
	// 2. txNum
	_ = binary.Write(buf, binary.BigEndian, rec.txNum)

	// 3. block info
	writeString(buf, rec.blkFile)
	_ = binary.Write(buf, binary.BigEndian, int32(rec.blkNum))
	_ = binary.Write(buf, binary.BigEndian, int32(rec.slotIndex))

	// 4. oldBytes
	writeBytes(buf, rec.oldBytes)
	// 5. newBytes
	writeBytes(buf, rec.newBytes)
	return buf.Bytes()
}

// parse UnifiedUpdateRecord from bytes
func FromBytesUnifiedUpdate(data []byte) (*UnifiedUpdateRecord, error) {
	buf := bytes.NewReader(data)

	var op int32
	if err := binary.Read(buf, binary.BigEndian, &op); err != nil {
		return nil, err
	}
	if op != UNIFIEDUPDATE {
		return nil, fmt.Errorf("not a unified update record")
	}

	var txNum int64
	if err := binary.Read(buf, binary.BigEndian, &txNum); err != nil {
		return nil, err
	}

	blkFile, err := readString(buf)
	if err != nil {
		return nil, err
	}

	var blkNum int32
	if err := binary.Read(buf, binary.BigEndian, &blkNum); err != nil {
		return nil, err
	}
	var slotIndex int32
	if err := binary.Read(buf, binary.BigEndian, &slotIndex); err != nil {
		return nil, err
	}

	oldBytes, err := readBytes(buf)
	if err != nil {
		return nil, err
	}
	newBytes, err := readBytes(buf)
	if err != nil {
		return nil, err
	}

	return &UnifiedUpdateRecord{
		txNum:     txNum,
		blkFile:   blkFile,
		blkNum:    int(blkNum),
		slotIndex: int(slotIndex),
		oldBytes:  oldBytes,
		newBytes:  newBytes,
	}, nil
}

// Helpers for writing/reading strings/bytes:

func writeString(buf *bytes.Buffer, s string) {
	writeBytes(buf, []byte(s))
}
func readString(buf *bytes.Reader) (string, error) {
	b, err := readBytes(buf)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func writeBytes(buf *bytes.Buffer, data []byte) {
	_ = binary.Write(buf, binary.BigEndian, int32(len(data)))
	buf.Write(data)
}
func readBytes(buf *bytes.Reader) ([]byte, error) {
	var length int32
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("negative length")
	}
	b := make([]byte, length)
	n, err := buf.Read(b)
	if err != nil || n != int(length) {
		return nil, fmt.Errorf("failed to read bytes")
	}
	return b, nil
}

func CreateLogRecord(data []byte) log.LogRecord {
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
