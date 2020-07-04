package filebuffer

import (
	"io"

	"github.com/willf/bitset"
)

// WholeFileBuffer is a file buffer whose size is the same as file size.
//
// WholeFileBuffer assumes the file size never changes.
//
// It can read the file partialy in page size units.
//
// WholeFileBuffer is not suitable for a very large file.
// Use PagedFileBuffer instead.
//
// WholeFileBuffer implements ReadWriterAt interface.
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
func (b *WholeFileBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	length := int64(len(p))
	if err := checkOffsetAndLength(b.fileSize(), off, length); err != nil {
		return 0, err
	}

	if err := b.Preread(off, length); err != nil {
		return 0, err
	}
	copy(p, b.buf[off:off+length])
	return len(p), nil
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
func (b *WholeFileBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	length := int64(len(p))
	if err := checkOffsetAndLength(b.fileSize(), off, length); err != nil {
		return 0, err
	}

	if err := b.Preread(off, length); err != nil {
		return 0, err
	}
	copy(b.buf[off:], p)
	setDirty(b.dirtyPages, b.pageSize, off, length)
	return len(p), nil
}

// Preread reads a bytes of the specified length starting at
// offset off data from the underlying file.
//
// It reads the file in page size units and skips pages
// which were already read and kept in the buffer.
func (b *WholeFileBuffer) Preread(off, length int64) error {
	for _, pr := range pageRangesToRead(b.readPages, b.pageSize, off, length) {
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
func pageRangesToRead(readPages *bitset.BitSet, pageSize, off, length int64) []pageRange {
	var ranges []pageRange
	var count int64
	pr := pageRangeForFileRange(pageSize, off, length)
	page := pr.start
	for ; page <= pr.end; page++ {
		if !readPages.Test(uint(page)) {
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

// Flush writes dirty pages to the file.
func (b *WholeFileBuffer) Flush() error {
	for _, r := range dirtyPageRanges(b.dirtyPages) {
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
