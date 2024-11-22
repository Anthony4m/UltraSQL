package main

import (
	"awesomeDB/kfile"
	"fmt"
	"log"
	"path/filepath"
	"time"
)

func checkError(err error, message string) {
	if err != nil {
		log.Fatalf("%s: %v", message, err)
	}
}

func main() {
	dbDir := filepath.Join(".", "mydb")
	blockSize := 128
	const Filename = "datafile.dat"

	fm, err := kfile.NewFileMgr(dbDir, blockSize)
	checkError(err, "Failed to initialize FileMgr")
	defer func() {
		checkError(fm.Close(), "Failed to close FileMgr")
	}()

	blk, err := fm.Append(Filename)
	checkError(err, "Failed to append block")
	fmt.Printf("Appended Block: %v\n", blk)

	pageManager := kfile.NewPageManager(blockSize)
	newPage := kfile.NewPage(blockSize, Filename)
	pageID := kfile.NewPageId(kfile.BlockId{Filename: Filename, Blknum: blk.Number()})

	err = newPage.SetInt(0, 42)
	checkError(err, "Failed to set int")

	err = newPage.SetString(4, "Hello, Go!")
	checkError(err, "Failed to set string")

	currentTime := time.Now()
	err = newPage.SetDate(15, currentTime)
	checkError(err, "Failed to set date")

	err = newPage.SetBool(50, true)
	checkError(err, "Failed to set bool")

	err = fm.Write(blk, newPage)
	checkError(err, "Failed to write to block")

	readPage := kfile.NewPage(blockSize, Filename)
	pageManager.SetPage(pageID, readPage)

	err = fm.Read(blk, pageManager, pageID)
	checkError(err, "Failed to read from block")

	intVal, err := readPage.GetInt(0)
	checkError(err, "Failed to get int")

	strVal, err := readPage.GetString(4, len("Hello, Go!"))
	checkError(err, "Failed to get string")

	dateVal, err := readPage.GetDate(30)
	checkError(err, "Failed to get date")

	boolVal, err := readPage.GetBool(50)
	checkError(err, "Failed to get bool")

	fmt.Printf("Integer: %d, String: %s, Date: %s, Bool: %v\n",
		intVal, strVal, dateVal, boolVal)

	fmt.Printf("Stats - Blocks Read: %d, Blocks Written: %d\n", fm.BlocksRead(), fm.BlocksWritten())
	stats := fm.ReadLog()
	stats1 := fm.BlockSize()
	stats2 := fm.BlocksRead()
	stats3 := fm.BlocksWritten()
	fmt.Printf("Stats: %v\n", stats)
	fmt.Printf("Block Size: %d\n", stats1)
	fmt.Printf("Blocks Read: %d\n", stats2)
	fmt.Printf("Blocks Written: %d\n", stats3)
	fmt.Printf("Stats4Value: %v\n", readPage.Contents())
}
