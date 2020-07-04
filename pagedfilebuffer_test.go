package filebuffer

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestPagedFileBuffer(t *testing.T) {
	const fileSize = 50
	const pageSize = 8

	file := testSetupFile(t, fileSize, pageSize)
	b := NewPagedFileBuffer(file, fileSize, pageSize)

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

	data, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	if got, want := string(data), "0aa00000111111112222222233333333444444445555555bbb"; got != want {
		t.Errorf("buf unmatch, got=%s, want=%s", got, want)
	}
}

func testSetupFile(t *testing.T, fileSize, pageSize int64) *os.File {
	file, err := ioutil.TempFile("", "filebuffer-test.dat")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

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
	if _, err := file.WriteAt(buf, 0); err != nil {
		t.Fatal(err)
	}

	return file
}
