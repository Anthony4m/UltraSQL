package main

import (
	"fmt"
	"log"
	"path/filepath"
	"time"
	"ultraSQL/kfile"
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
	newPage := kfile.NewPage(blockSize)

	err = newPage.SetInt(0, 42)
	checkError(err, "Failed to set int")

	err = newPage.SetString(4, "Helloooooooooooooo, Go!")
	checkError(err, "Failed to set string")

	currentTime := time.Date(2023, time.December, 1, 10, 30, 0, 0, time.UTC)
	err = newPage.SetDate(50, currentTime)
	checkError(err, "Failed to set date")

	err = newPage.SetBool(90, true)
	checkError(err, "Failed to set bool")

	err = fm.Write(blk, newPage)
	checkError(err, "Failed to write to block")

	readPage := kfile.NewPage(blockSize)

	err = fm.Read(blk, readPage)
	checkError(err, "Failed to read from block")

	intVal, err := readPage.GetInt(0)
	checkError(err, "Failed to get int")

	strVal, err := readPage.GetString(4)
	checkError(err, "Failed to get string")

	dateVal, err := readPage.GetDate(50)
	checkError(err, "Failed to get date")

	boolVal, err := readPage.GetBool(90)
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
