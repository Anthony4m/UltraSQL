package buffer

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"ultraSQL/kfile"
	"ultraSQL/log"
)

type Buffer struct {
	fm             *kfile.FileMgr
	lm             *log.LogMgr
	contents       *kfile.Page
	blk            *kfile.BlockId
	pins           int
	txnum          int
	lsn            int
	lastAccessTime uint64
	Dirty          bool
	prev, next     *Buffer
}

const (
	PageSizeThreshold = 8 * 1024
)

func NewBuffer(fm *kfile.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: kfile.NewPage(fm.BlockSize()),
		blk:      nil,
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) GetContents() *kfile.Page {
	return b.contents
}

func (b *Buffer) Block() *kfile.BlockId {
	return b.blk
}

func (b *Buffer) MarkModified(txtnum int, lsn int) {
	b.txnum = txtnum
	if lsn > 0 {
		b.lsn = lsn
	}
	b.Dirty = true
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) modifyingTx() int {
	return b.txnum
}

func (b *Buffer) assignToBlock(block *kfile.BlockId) error {
	if err := b.flush(); err != nil {
		return err
	}
	b.blk = block
	if err := b.fm.Read(b.blk, b.contents); err != nil {
		return err
	}
	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	if b.txnum > 0 && b.blk != nil {
		err := b.lm.FlushLSN(b.lsn)
		if err != nil {
			return err
		}
		if err := b.fm.Write(b.blk, b.contents); err != nil {
			return err
		}
		b.txnum = -1
	}
	return nil
}

func (b *Buffer) isDirty() bool {
	return b.Dirty
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() error {
	if b.pins <= 0 {
		return errors.New("unpin operation failed: blk is not pinned")
	}
	b.pins--
	return nil
}

func (b *Buffer) compressPage(page *kfile.Page) error {
	if len(page.Contents()) <= PageSizeThreshold || page.IsCompressed {
		return nil
	}

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	_, err := writer.Write(page.Contents())
	if err != nil {
		return fmt.Errorf("compression write error: %v", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("compression close error: %v", err)
	}

	page.SetContents(buf.Bytes())
	page.IsCompressed = true
	return nil
}

func (b *Buffer) decompressPage(page *kfile.Page) error {
	if !page.IsCompressed {
		return nil
	}

	reader, err := gzip.NewReader(bytes.NewReader(page.Contents()))
	if err != nil {
		return fmt.Errorf("decompression reader error: %v", err)
	}
	defer reader.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(reader); err != nil {
		return fmt.Errorf("decompression read error: %v", err)
	}

	page.SetContents(buf.Bytes())
	page.IsCompressed = false
	return nil
}
