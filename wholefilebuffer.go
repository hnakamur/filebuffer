package wholefilebuffer

import (
	"io"

	"github.com/willf/bitset"
)

// WholeFileBuffer is a file buffer whose size is the same as file size.
// It can read the file partialy in page size units.
// You need to call Flush after modifying the buffer with PutAt.
type WholeFileBuffer struct {
	file       ReadWriterAt
	buf        []byte
	pageSize   int64
	readPages  *bitset.BitSet
	dirtyPages *bitset.BitSet
}

// ReadWriterAt is the interface that groups io.ReadAt and io.WriteAt.
type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

type pageRange struct {
	start int64
	end   int64
}

// NewWholeFileBuffer creates a new whole file buffer.
func NewWholeFileBuffer(file ReadWriterAt, fileSize, pageSize int64) *WholeFileBuffer {
	pageCount := uint((fileSize + pageSize - 1) / pageSize)
	return &WholeFileBuffer{
		file:       file,
		buf:        make([]byte, fileSize),
		pageSize:   pageSize,
		readPages:  bitset.New(pageCount),
		dirtyPages: bitset.New(pageCount),
	}
}

func (b *WholeFileBuffer) fileSize() int64 {
	return int64(len(b.buf))
}

// GetAt returns a slice to the partial buffer after reading
// pages from the underlying file for the specified range
// if necessary.
//
// The caller must not modify the content of the returned
// slice directly.  Use PutAt instead.
//
// If you are going to call GetAt multiple times to get
// data spanning to multiple pages, you may want to call
// Get for the total range beforehand to reduce file I/O
// system calls.
func (b *WholeFileBuffer) GetAt(off, length int64) ([]byte, error) {
	if err := b.read(off, length); err != nil {
		return nil, err
	}
	return b.buf[off : off+length], nil
}

// PutAt copies data to the file buffer b and marks the
// corresponding pages dirty.
//
// The caller must call Flush later to write dirty pages
// to the file.
func (b *WholeFileBuffer) PutAt(data []byte, off int64) {
	copy(b.buf[off:], data)
	b.setDirty(off, int64(len(data)))
}

// read reads a bytes of the specified length starting at
// offset off data from the underlying file.
//
// It reads the file in page size units and skips pages
// which were already read and kept in the buffer.
func (b *WholeFileBuffer) read(off, length int64) error {
	for _, pr := range b.pageRangesToRead(off, length) {
		off := pr.start * b.pageSize
		end := pr.end * b.pageSize
		if end > b.fileSize() {
			end = b.fileSize()
		}
		if _, err := b.file.ReadAt(b.buf[off:end], off); err != nil {
			return err
		}
		for page := pr.start; page < pr.end; page++ {
			b.readPages.Set(uint(page))
		}
	}
	return nil
}

// pageRangesToRead returns a slice of page ranges to read.
// NOTE: The end of the returned page range is exclusive.
func (b *WholeFileBuffer) pageRangesToRead(off, length int64) []pageRange {
	var ranges []pageRange
	var count int64
	pr := b.pageRangeForFileRange(off, length)
	page := pr.start
	for ; page <= pr.end; page++ {
		if !b.readPages.Test(uint(page)) {
			count++
			continue
		}

		if count > 0 {
			ranges = append(ranges, pageRange{
				start: page - count,
				end:   page,
			})
			count = 0
		}
	}
	if count > 0 {
		ranges = append(ranges, pageRange{
			start: page - count,
			end:   page,
		})
	}
	return ranges
}

// setDirty marks pages for the specified range dirty.
//
// When Flush is called afterward, the dirty pages
// are written back to the underlying file.
func (b *WholeFileBuffer) setDirty(off, length int64) {
	pr := b.pageRangeForFileRange(off, length)
	for page := pr.start; page <= pr.end; page++ {
		b.dirtyPages.Set(uint(page))
	}
}

// Flush writes dirty pages to the file.
func (b *WholeFileBuffer) Flush() error {
	for _, r := range b.dirtyPageRanges() {
		off := r.start * b.pageSize
		end := r.end * b.pageSize
		if end > b.fileSize() {
			end = b.fileSize()
		}
		if _, err := b.file.WriteAt(b.buf[off:end], off); err != nil {
			return err
		}
	}
	b.dirtyPages.ClearAll()
	return nil
}

// dirtyPageRanges returns a slice of page ranges for dirty pages.
// NOTE: The end of the returned page range is exclusive.
func (b *WholeFileBuffer) dirtyPageRanges() []pageRange {
	if b.dirtyPages == nil {
		return nil
	}

	var ranges []pageRange
	var i, count int64
	for ; i < int64(b.dirtyPages.Len()); i++ {
		if b.dirtyPages.Test(uint(i)) {
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
func (b *WholeFileBuffer) pageRangeForFileRange(off, length int64) pageRange {
	return pageRange{
		start: off / b.pageSize,
		end:   (off + length - 1) / b.pageSize,
	}
}
