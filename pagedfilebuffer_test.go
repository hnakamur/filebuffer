package filebuffer

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestPagedFileBuffer(t *testing.T) {
	const fileSize = 50
	const pageSize = 8

	file, err := ioutil.TempFile("", "pagedfilebuffer-test.dat")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	buf := make([]byte, fileSize)
	pageCount := int64(fileSize+pageSize-1) / pageSize
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
	if _, err := file.WriteAt(buf, 0); err != nil {
		t.Fatal(err)
	}

	b := NewPagedFileBuffer(file, fileSize, pageSize)

	data := buf
	_, err = b.ReadAt(data, -1)
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

	gotBytes, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	if got, want := string(gotBytes), "0aa00000111111112222222233333333444444445555555bbb"; got != want {
		t.Errorf("buf unmatch, got=%s, want=%s", got, want)
	}
}

func TestPagedFileBufferOverMaxIov(t *testing.T) {
	file, err := ioutil.TempFile("", "pagedfilebuffer-test-over-max-iov.dat")
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

	pBuf := NewPagedFileBuffer(file, fileSize, pageSize)

	want, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, fileSize)
	if _, err := pBuf.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("WholeFileBuffer inital content unmatch")
	}

	if _, err := rnd.Read(want); err != nil {
		t.Fatal(err)
	}
	if _, err := pBuf.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := pBuf.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err = ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("PagedFileBuffer modified content unmatch")
	}
}
