package filebuffer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

type mockFile struct {
	buf  []byte
	logs []string
}

func newMockFile(fileSize, pageSize int64) *mockFile {
	buf := make([]byte, fileSize)
	pageCount := (fileSize + pageSize - 1) / pageSize
	for i := int64(0); i < pageCount; i++ {
		off := i * pageSize
		end := (i + 1) * pageSize
		if end > fileSize {
			end = fileSize
		}
		for j := off; j < end; j++ {
			buf[j] = '0' + byte(i)
		}
	}
	return &mockFile{buf: buf}
}

func (f *mockFile) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if off+int64(len(b)) > int64(len(f.buf)) {
		return 0, errors.New("offset and length out of bounds")
	}

	copy(b, f.buf[off:off+int64(len(b))])
	f.logs = append(f.logs, fmt.Sprintf("ReadAt(off=%d,len=%d)", off, len(b)))
	return len(b), nil
}

func (f *mockFile) WriteAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if off+int64(len(b)) > int64(len(f.buf)) {
		return 0, errors.New("offset and length out of bounds")
	}

	copy(f.buf[off:], b)
	f.logs = append(f.logs, fmt.Sprintf("WriteAt(off=%d,len=%d)", off, len(b)))
	return len(b), nil
}

func TestWholeFileBufferMockFile(t *testing.T) {
	const fileSize = 50
	const pageSize = 8

	file := newMockFile(fileSize, pageSize)
	b := NewWholeFileBuffer(file, fileSize, pageSize)

	buf := make([]byte, fileSize)
	data := buf
	_, err := b.ReadAt(data, -1)
	if want := "negative offset"; err == nil || err.Error() != want {
		t.Errorf("unexpected error: got=%v, want=%s", err, want)
	}

	data = buf[:2]
	_, err = b.ReadAt(data, fileSize-1)
	if want := "offset and length out of bounds"; err == nil || err.Error() != want {
		t.Errorf("unexpected error: got=%v, want=%s", err, want)
	}

	data = buf[:2]
	if _, err := b.ReadAt(data, 7); err != nil {
		t.Fatal(err)
	} else if got, want := data, []byte("01"); !bytes.Equal(got, want) {
		t.Errorf("data unmatch for GetAt(data, 2), got=%s, want=%s", string(got), string(want))
	}

	if _, err := b.WriteAt([]byte("aa"), 1); err != nil {
		t.Fatal(err)
	}

	data = buf[:pageSize]
	if _, err := b.ReadAt(data, 4*pageSize); err != nil {
		t.Fatal(err)
	} else if got, want := data, []byte("44444444"); !bytes.Equal(got, want) {
		t.Errorf("data unmatch for GetAt(data, 4*pageSize), got=%s, want=%s", string(got), string(want))
	}

	data = buf[:fileSize-(4*pageSize-1)]
	if _, err := b.ReadAt(data, 4*pageSize-1); err != nil {
		t.Fatal(err)
	} else if got, want := data, []byte("3444444445555555566"); !bytes.Equal(got, want) {
		t.Errorf("data unmatch for GetAt(data, 4*pageSize-1), got=%s, want=%s", string(got), string(want))
	}

	data = buf[:pageSize]
	if _, err := b.ReadAt(data, 4*pageSize); err != nil {
		t.Fatal(err)
	} else if got, want := data, []byte("44444444"); !bytes.Equal(got, want) {
		t.Errorf("data unmatch for GetAt(data, 4*pageSize), got=%s, want=%s", string(got), string(want))
	}

	_, err = b.WriteAt(data, -1)
	if want := "negative offset"; err == nil || err.Error() != want {
		t.Errorf("unexpected error: got=%v, want=%s", err, want)
	}

	if _, err := b.WriteAt([]byte("bbb"), fileSize-3); err != nil {
		t.Fatal(err)
	}

	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}

	if got, want := string(file.buf), "0aa00000111111112222222233333333444444445555555bbb"; got != want {
		t.Errorf("buf unmatch, got=%s, want=%s", got, want)
	}
	if got, want := file.logs, []string{
		"ReadAt(off=0,len=16)",
		"ReadAt(off=32,len=8)",
		"ReadAt(off=24,len=8)",
		"ReadAt(off=40,len=10)",
		"WriteAt(off=0,len=8)",
		"WriteAt(off=40,len=10)",
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("logs unmatch, got=%v, want=%v", got, want)
	}
}

func TestWholeFileBufferReadFile(t *testing.T) {
	file, err := ioutil.TempFile("", "wholefilebuffer-test.dat")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Remove(file.Name())
	})

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	const maxIov = 1024
	const pageSize = 4096
	const fileSize = maxIov*pageSize + 1
	if _, err := io.CopyN(file, rnd, fileSize); err != nil {
		t.Fatal(err)
	}

	wBuf := NewWholeFileBuffer(file, fileSize, pageSize)

	want, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, fileSize)
	if _, err := wBuf.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("WholeFileBuffer inital content unmatch")
	}

	if _, err := rnd.Read(want); err != nil {
		t.Fatal(err)
	}
	if _, err := wBuf.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := wBuf.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err = ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("WholeFileBuffer modified content unmatch")
	}
}
