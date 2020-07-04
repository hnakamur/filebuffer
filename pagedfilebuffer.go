package filebuffer

import (
	"errors"
	"os"

	"github.com/willf/bitset"
)

// PagedFileBuffer is a file buffer which can read the file
// partialy in page size units.
type PagedFileBuffer struct {
	file       *os.File
	fileSize   int64
	pages      map[int64][]byte
	pageSize   int64
	readPages  *bitset.BitSet
	dirtyPages *bitset.BitSet
}

// NewPagedFileBuffer creates a new whole file buffer.
func NewPagedFileBuffer(file *os.File, fileSize, pageSize int64) *PagedFileBuffer {
	pageCount := uint((fileSize + pageSize - 1) / pageSize)
	return &PagedFileBuffer{
		file:       file,
		fileSize:   fileSize,
		pages:      make(map[int64][]byte),
		pageSize:   pageSize,
		readPages:  bitset.New(pageCount),
		dirtyPages: bitset.New(pageCount),
	}
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
func (b *PagedFileBuffer) GetAt(off, length int64) ([]byte, error) {
	if err := checkOffsetAndLength(b.fileSize, off, length); err != nil {
		return nil, err
	}

	if err := b.read(off, length); err != nil {
		return nil, err
	}

	r := pageRangeForFileRange(b.pageSize, off, length)
	offInPage := off % b.pageSize
	buf := b.getBuf(r.start)
	if r.start == r.end {
		return buf[offInPage : offInPage+length], nil
	}

	data := make([]byte, length)
	copy(data, buf[offInPage:])
	dest := data[len(buf[offInPage:]):]
	for page := r.start + 1; page <= r.end; page++ {
		buf = b.getBuf(page)
		n := copy(dest, buf)
		dest = dest[n:]
	}
	return data, nil
}

// PutAt copies data to the file buffer b and marks the
// corresponding pages dirty.
//
// The caller must call Flush later to write dirty pages
// to the file.
func (b *PagedFileBuffer) PutAt(data []byte, off int64) error {
	length := int64(len(data))
	if err := checkOffsetAndLength(b.fileSize, off, length); err != nil {
		return err
	}

	r := pageRangeForFileRange(b.pageSize, off, length)
	offInPage := off % b.pageSize
	buf := b.getBuf(r.start)
	n := copy(buf[offInPage:], data)
	data = data[n:]
	for page := r.start + 1; page <= r.end; page++ {
		buf := b.getBuf(page)
		n := copy(buf, data)
		data = data[n:]
	}
	setDirty(b.dirtyPages, b.pageSize, off, length)
	return nil
}

// read reads a bytes of the specified length starting at
// offset off data from the underlying file.
//
// It reads the file in page size units and skips pages
// which were already read and kept in the buffer.
func (b *PagedFileBuffer) read(off, length int64) error {
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
func (b *PagedFileBuffer) Flush() error {
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

func (b *PagedFileBuffer) iovsForPageRange(pr pageRange) [][]byte {
	iovs := make([][]byte, pr.end-pr.start)
	for i := range iovs {
		iovs[i] = b.getBuf(pr.start + int64(i))
	}
	return iovs
}

func (b *PagedFileBuffer) getBuf(page int64) []byte {
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

func checkOffsetAndLength(fileSize, off, length int64) error {
	if off < 0 {
		return errors.New("negative offset")
	}
	if off+length > fileSize {
		return errors.New("offset and length out of bounds")
	}
	return nil
}
