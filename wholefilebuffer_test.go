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
	name string
	buf  []byte
	logs []string
}

func newMockFile(name string, fileSize, pageSize int64) *mockFile {
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
	return &mockFile{name: name, buf: buf}
}

func (f *mockFile) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &os.PathError{Op: "readat", Path: f.name, Err: errors.New("negative offset")}
	}
	if off+int64(len(b)) > int64(len(f.buf)) {
		return 0, &os.PathError{Op: "readat", Path: f.name, Err: errors.New("offset and length out of bounds")}
	}

	copy(b, f.buf[off:off+int64(len(b))])
	f.logs = append(f.logs, fmt.Sprintf("ReadAt(off=%d,len=%d)", off, len(b)))
	return len(b), nil
}

func (f *mockFile) WriteAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &os.PathError{Op: "writeat", Path: f.name, Err: errors.New("negative offset")}
	}
	if off+int64(len(b)) > int64(len(f.buf)) {
		return 0, &os.PathError{Op: "writeat", Path: f.name, Err: errors.New("offset and length out of bounds")}
	}

	copy(f.buf[off:], b)
	f.logs = append(f.logs, fmt.Sprintf("WriteAt(off=%d,len=%d)", off, len(b)))
	return len(b), nil
}

func TestWholeFileBufferMockFile(t *testing.T) {
	const fileSize = 50
	const pageSize = 8

	file := newMockFile("mockfile", fileSize, pageSize)
	b := NewWholeFileBuffer(file, fileSize, pageSize)

	if data, err := b.GetAt(7, 2); err != nil {
		t.Fatal(err)
	} else if want := []byte("01"); !bytes.Equal(data, want) {
		t.Errorf("data unmatch for GetAt(7, 2), got=%s, want=%s", string(data), string(want))
	}

	b.PutAt([]byte("aa"), 1)

	if data, err := b.GetAt(4*pageSize, pageSize); err != nil {
		t.Fatal(err)
	} else if want := []byte("44444444"); !bytes.Equal(data, want) {
		t.Errorf("data unmatch for GetAt(4*pageSize, pageSize), got=%s, want=%s", string(data), string(want))
	}

	if data, err := b.GetAt(4*pageSize-1, fileSize-(4*pageSize-1)); err != nil {
		t.Fatal(err)
	} else if want := []byte("3444444445555555566"); !bytes.Equal(data, want) {
		t.Errorf("data unmatch for GetAt(4*pageSize-1, fileSize-(4*pageSize-1)), got=%s, want=%s", string(data), string(want))
	}

	if data, err := b.GetAt(4*pageSize, pageSize); err != nil {
		t.Fatal(err)
	} else if want := []byte("44444444"); !bytes.Equal(data, want) {
		t.Errorf("data unmatch for GetAt(4*pageSize, pageSize), got=%s, want=%s", string(data), string(want))
	}

	b.PutAt([]byte("bbb"), fileSize-3)

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

	want := make([]byte, fileSize)
	if _, err := rnd.Read(want); err != nil {
		t.Fatal(err)
	}

	if _, err := wBuf.GetAt(0, fileSize); err != nil {
		t.Fatal(err)
	}
	if err := wBuf.PutAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := wBuf.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, want) {
		t.Errorf("WholeFileBuffer content unmatch")
	}
}
