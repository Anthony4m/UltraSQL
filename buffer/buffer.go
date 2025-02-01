package buffer

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"ultraSQL/kfile"
)

const PageSizeThreshold = 8 * 1024

type Buffer struct {
	fm             *kfile.FileMgr
	contents       *kfile.SlottedPage
	blk            *kfile.BlockId
	pins           int
	txnum          int
	lsn            int
	Dirty          bool
	lastAccessTime uint64
	prev, next     *Buffer
}

// NewBuffer ...
func NewBuffer(fm *kfile.FileMgr) *Buffer {
	return &Buffer{
		fm:       fm,
		contents: kfile.NewSlottedPage(fm.BlockSize()),
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) Contents() *kfile.SlottedPage {
	return b.contents
}

func (b *Buffer) SetContents(sp *kfile.SlottedPage) {
	b.contents = sp
}

func (b *Buffer) Block() *kfile.BlockId {
	return b.blk
}

func (b *Buffer) MarkModified(txnum, lsn int) {
	b.txnum = txnum
	if lsn > 0 {
		b.lsn = lsn
	}
	b.Dirty = true
}

func (b *Buffer) Pinned() bool {
	return b.pins > 0
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() error {
	if b.pins <= 0 {
		return errors.New("buffer is not pinned")
	}
	b.pins--
	return nil
}

func (b *Buffer) Flush() error {
	// only flush if dirty and we have a valid block assigned
	if b.Dirty && b.blk != nil {
		if err := b.fm.Write(b.blk, b.contents); err != nil {
			return fmt.Errorf("flush: write error: %w", err)
		}
		b.Dirty = false
		b.txnum = -1
	}
	return nil
}

func (b *Buffer) assignToBlock(blk *kfile.BlockId) error {
	// flush old contents first
	if err := b.Flush(); err != nil {
		return fmt.Errorf("assignToBlock: flush error: %w", err)
	}
	b.blk = blk
	if err := b.fm.Read(blk, b.contents); err != nil {
		return fmt.Errorf("assignToBlock: read error: %w", err)
	}
	b.pins = 0
	return nil
}

func (b *Buffer) FlushLSN(lsn int) error {
	if lsn >= b.lsn {
		return b.Flush()
	}
	return nil
}

func (b *Buffer) LogFlush(blk *kfile.BlockId) error {
	b.blk = blk
	if err := b.fm.Write(b.blk, b.contents); err != nil {
		return fmt.Errorf("logFlush: write error: %w", err)
	}
	return nil
}

// compressPage / decompressPage could remain the same, or be simplified:
func (b *Buffer) compressPage(page *kfile.Page) error {
	if len(page.Contents()) <= PageSizeThreshold || page.IsCompressed {
		return nil
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(page.Contents()); err != nil {
		return fmt.Errorf("compressPage: write error: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("compressPage: close error: %w", err)
	}

	page.SetContents(buf.Bytes())
	page.IsCompressed = true
	return nil
}

func (b *Buffer) decompressPage(page *kfile.Page) error {
	if !page.IsCompressed {
		return nil
	}
	gz, err := gzip.NewReader(bytes.NewReader(page.Contents()))
	if err != nil {
		return fmt.Errorf("decompressPage: new reader: %w", err)
	}
	defer gz.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, gz); err != nil {
		return fmt.Errorf("decompressPage: read error: %w", err)
	}

	page.SetContents(buf.Bytes())
	page.IsCompressed = false
	return nil
}
func (b *Buffer) ModifyingTxID() int {
	return b.txnum
}
