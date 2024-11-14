package main

import (
	"awesomeDB/kfile"
	"fmt"
	"log"
	"path/filepath"
	"time"
)

func main() {
	// Define the database directory and block size
	dbDir := filepath.Join(".", "mydb")
	blockSize := 128 // 128 bytes per block

	// Initialize FileMgr
	fm, err := kfile.NewFileMgr(dbDir, blockSize)
	if err != nil {
		log.Fatalf("Failed to initialize FileMgr: %v", err)
	}
	defer func() {
		err := fm.Close()
		if err != nil {
			log.Printf("Failed to close FileMgr: %v", err)
		}
	}()

	// Append a new block to "datafile.dat"
	blk, err := fm.Append("datafile.dat")
	if err != nil {
		log.Fatalf("Failed to append block: %v", err)
	}
	fmt.Printf("Appended Block: %v\n", blk)

	// Create a new Page and write data
	page := kfile.NewPage(blockSize)
	err = page.SetInt(0, 42)
	if err != nil {
		log.Fatalf("Failed to set int: %v", err)
	}
	err = page.SetString(4, "Hello, Go!")
	if err != nil {
		log.Fatalf("Failed to set string: %v", err)
	}

	err = page.SetDate(15, time.Now())
	if err != nil {
		log.Fatalf("Failed to set string: %v", err)
	}
	err = page.SetBool(24, true)
	if err != nil {
		log.Fatalf("Failed to set string: %v", err)
	}
	err = page.SetDate(25, time.Now())
	if err != nil {
		log.Fatalf("Failed to set string: %v", err)
	}

	// Write the Page to the block
	err = fm.Write(blk, page)
	if err != nil {
		log.Fatalf("Failed to write to block: %v", err)
	}
	fmt.Printf("Written to Block: %v\n", blk)

	// Read the Page from the block
	readPage := kfile.NewPage(blockSize)
	err = fm.Read(blk, readPage)
	if err != nil {
		log.Fatalf("Failed to read from block: %v", err)
	}

	// Retrieve data from the read Page
	intVal, err := readPage.GetInt(0)
	if err != nil {
		log.Fatalf("Failed to get int: %v", err)
	}
	strVal, err := readPage.GetString(4, 10)
	if err != nil {
		log.Fatalf("Failed to get string: %v", err)
	}
	dateVal, err := readPage.GetDate(15)
	if err != nil {
		log.Fatalf("Failed to get string: %v", err)
	}
	boolVal, err := readPage.GetBool(25)
	if err != nil {
		log.Fatalf("Failed to get string: %v", err)
	}

	stats := fm.ReadLog()
	stats1 := fm.BlockSize()
	stats2 := fm.BlocksRead()
	stats3 := fm.BlocksWritten()

	fmt.Printf("Read from Block: %v\n", blk)
	fmt.Printf("Integer Value: %d\n", intVal)
	fmt.Printf("String Value: %s\n", strVal)
	fmt.Printf("Date Value: %s\n", dateVal)
	fmt.Printf("Bool Value: %s\n", boolVal)
	fmt.Printf("Stats Value: %s\n", stats)
	fmt.Printf("Stats1 Value: %s\n", stats1)
	fmt.Printf("Stats2 Value: %s\n", stats2)
	fmt.Printf("Stats3Value: %s\n", stats3)
	fmt.Printf("Stats4Value: %s\n", readPage.Contents())
}
