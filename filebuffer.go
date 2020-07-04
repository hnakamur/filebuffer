package filebuffer

import (
	"errors"
	"os"

	"github.com/willf/bitset"
)

// FileBuffer is a file buffer which can read the file
// partialy in page size units.
//
// FileBuffer assumes the file size never changes.
//
// FileBuffer uses vector I/O (preadv and pwritev)
// on Linux for reading and writing successive pages in the
// file.
//
// FileBuffer implements ReadWriterAt interface.
type FileBuffer struct {
	file       *os.File
	fileSize   int64
	pages      map[int64][]byte
	pageSize   int64
	readPages  *bitset.BitSet
	dirtyPages *bitset.BitSet
}

// New creates a new whole file buffer.
func New(file *os.File, fileSize, pageSize int64) *FileBuffer {
	pageCount := uint((fileSize + pageSize - 1) / pageSize)
	return &FileBuffer{
		file:       file,
		fileSize:   fileSize,
		pages:      make(map[int64][]byte),
		pageSize:   pageSize,
		readPages:  bitset.New(pageCount),
		dirtyPages: bitset.New(pageCount),
	}
}

// ReadAt reads len(p) bytes into p starting at offset off
// in the buffer.
// It returns the number of bytes read (n == len(p)) and
// any error encountered.
//
// ReadAt reads necessary pages using Preread method
// which are not already read.
//
// If you are going to call ReadAt multiple times to get
// data spanning to multiple pages, you may want to call
// Preread for the total range beforehand to reduce file I/O
// system calls.
//
// ReadAt implements io.ReaderAt interface.
func (b *FileBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	length := int64(len(p))
	if err := checkOffsetAndLength(b.fileSize, off, length); err != nil {
		return 0, err
	}

	if err := b.Preread(off, length); err != nil {
		return 0, err
	}

	r := pageRangeForFileRange(b.pageSize, off, length)
	offInPage := off % b.pageSize
	buf := b.getBuf(r.start)
	n = copy(p, buf[offInPage:])
	p = p[n:]
	for page := r.start + 1; page <= r.end; page++ {
		buf = b.getBuf(page)
		n := copy(p, buf)
		p = p[n:]
	}
	return int(length), nil
}

// WriteAt writes len(p) bytes from p to the buffer at offset off.
// It returns the number of bytes written from p (n == len(p))
// and any error encountered that caused the write to stop early.
//
// Pages for the corresponding range will be read first
// if not already read.
//
// The caller must call Flush later to write dirty pages
// to the file.
//
// WriteAt implements io.WriterAt interface.
func (b *FileBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	length := int64(len(p))
	if err := checkOffsetAndLength(b.fileSize, off, length); err != nil {
		return 0, err
	}

	if err := b.Preread(off, length); err != nil {
		return 0, err
	}

	r := pageRangeForFileRange(b.pageSize, off, length)
	offInPage := off % b.pageSize
	buf := b.getBuf(r.start)
	n = copy(buf[offInPage:], p)
	p = p[n:]
	for page := r.start + 1; page <= r.end; page++ {
		buf = b.getBuf(page)
		n = copy(buf, p)
		p = p[n:]
	}
	setDirty(b.dirtyPages, b.pageSize, off, length)
	return len(p), nil
}

// Preread reads a bytes of the specified length starting at
// offset off data from the underlying file.
//
// It reads the file in page size units and skips pages
// which were already read and kept in the buffer.
func (b *FileBuffer) Preread(off, length int64) error {
	for _, r := range pageRangesToRead(b.readPages, b.pageSize, off, length) {
		off := r.start * b.pageSize
		iovs := b.iovsForPageRange(r)
		_, err := preadvFull(b.file, iovs, off)
		if err != nil {
			return err
		}
		for page := r.start; page < r.end; page++ {
			b.readPages.Set(uint(page))
		}
	}
	return nil
}

// Flush writes dirty pages to the file.
func (b *FileBuffer) Flush() error {
	for _, r := range dirtyPageRanges(b.dirtyPages) {
		off := r.start * b.pageSize
		iovs := b.iovsForPageRange(r)
		_, err := pwritevFull(b.file, iovs, off)
		if err != nil {
			return err
		}
	}
	b.dirtyPages.ClearAll()
	return nil
}

func (b *FileBuffer) iovsForPageRange(pr pageRange) [][]byte {
	iovs := make([][]byte, pr.end-pr.start)
	for i := range iovs {
		iovs[i] = b.getBuf(pr.start + int64(i))
	}
	return iovs
}

func (b *FileBuffer) getBuf(page int64) []byte {
	buf := b.pages[page]
	if buf == nil {
		off := b.pageSize * page
		bufSize := b.pageSize
		if off+bufSize > b.fileSize {
			bufSize = b.fileSize - off
		}
		buf = make([]byte, bufSize)
		b.pages[page] = buf
	}
	return buf
}

// setDirty marks pages for the specified range dirty.
//
// When Flush is called afterward, the dirty pages
// are written back to the underlying file.
func setDirty(dirtyPages *bitset.BitSet, pageSize, off, length int64) {
	pr := pageRangeForFileRange(pageSize, off, length)
	for page := pr.start; page <= pr.end; page++ {
		dirtyPages.Set(uint(page))
	}
}

// dirtyPageRanges returns a slice of page ranges for dirty pages.
// NOTE: The end of the returned page range is exclusive.
func dirtyPageRanges(dirtyPages *bitset.BitSet) []pageRange {
	var ranges []pageRange
	var i, count int64
	for ; i < int64(dirtyPages.Len()); i++ {
		if dirtyPages.Test(uint(i)) {
			count++
			continue
		}

		if count > 0 {
			ranges = append(ranges, pageRange{
				start: i - count,
				end:   i,
			})
			count = 0
		}
	}
	if count > 0 {
		ranges = append(ranges, pageRange{
			start: i - count,
			end:   i,
		})
	}
	return ranges
}

// pageRangeForFileRange returns a page range for a file range.
// NOTE: The end of the returned page range is inclusive.
func pageRangeForFileRange(pageSize, off, length int64) pageRange {
	return pageRange{
		start: off / pageSize,
		end:   (off + length - 1) / pageSize,
	}
}

func checkOffsetAndLength(fileSize, off, length int64) error {
	if off < 0 {
		return errors.New("negative offset")
	}
	if off+length > fileSize {
		return errors.New("offset and length out of bounds")
	}
	return nil
}
