package log

//
//import (
//	"awesomeDB/kfile"
//	"awesomeDB/utils"
//	"fmt"
//	"os"
//	"path/filepath"
//	"testing"
//	"time"
//	"unsafe"
//)
//
//func TestLogMgr(t *testing.T) {
//	// Setup
//	tempDir := filepath.Join(os.TempDir(), "simpledb_test_"+time.Now().Format("20060102150405"))
//	blockSize := 400
//	fm, err := kfile.NewFileMgr(tempDir, blockSize)
//	if err != nil {
//		t.Fatalf("Failed to create FileMgr: %v", err)
//	}
//	defer func() {
//		fm.Close()
//		os.RemoveAll(tempDir)
//	}()
//
//	// Test file creation and appending
//	filename := "test.db"
//	_, err = fm.Append(filename)
//	if err != nil {
//		t.Fatalf("Failed to append block: %v", err)
//	}
//	lm, _ := newLogMgr(fm, filename)
//
//	createRecords(t, lm, 1, 35)
//	printLogRecords(t, lm, "The log file now has these records:")
//
//	// Create and append additional records
//	createRecords(t, lm, 36, 70)
//	err = lm.Flush()
//	if err != nil {
//		return
//	}
//	printLogRecords(t, lm, "The log file now has these records:")
//}
//
//func createRecords(t *testing.T, lm *LogMgr, start, end int) {
//	t.Logf("Creating records:")
//	for i := start; i <= end; i++ {
//		record := createLogRecord(fmt.Sprintf("record%d", i), i+100)
//		lsn := lm.Append(record)
//		t.Logf("Record LSN: %d", lsn)
//	}
//}
//
//func printLogRecords(t *testing.T, lm *LogMgr, msg string) {
//	t.Log(msg)
//	iter := lm.Iterator()
//	for iter.HasNext() {
//		rec, err := iter.Next()
//		if err != nil {
//			panic(err)
//		}
//		page := kfile.NewPageFromBytes(rec)
//		s, err := page.GetString(0)
//		if err != nil {
//			panic(err)
//		}
//		npos := utils.MaxLength(len(s))
//		val, _ := page.GetInt(npos)
//		t.Logf("[%s, %d]", s, val)
//	}
//	t.Log()
//}
//
//func createLogRecord(s string, n int) []byte {
//	npos := utils.MaxLength(len(s))
//	record := make([]byte, npos+int(unsafe.Sizeof(0))) // String + Integer
//	page := kfile.NewPageFromBytes(record)
//
//	if err := page.SetString(0, s); err != nil {
//		panic(fmt.Sprintf("Failed to set string: %v", err))
//	}
//	if err := page.SetInt(npos, n); err != nil {
//		panic(fmt.Sprintf("Failed to set int: %v", err))
//	}
//
//	// Log serialized record details
//	fmt.Printf("Serialized record [%s, %d]: npos=%d, recordLen=%d\n", s, n, npos, len(record))
//	return record
//}
