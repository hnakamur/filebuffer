package filebuffer

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestIovsAdjust(t *testing.T) {
	const fileSize = 30000
	data := make([]byte, fileSize)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	if _, err := rnd.Read(data); err != nil {
		t.Fatal(err)
	}

	file, err := ioutil.TempFile("", "filebuffer-iovs-adjust-test.dat")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Remove(file.Name())
	})

	const partialSize = 20000
	if _, err := file.WriteAt(data[:partialSize], 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	const pageSize = 4096
	pageCount := (fileSize + pageSize - 1) / pageSize
	iovs := make([][]byte, pageCount)
	total := 0
	for i := range iovs {
		bufSize := pageSize
		if total+bufSize > fileSize {
			bufSize = fileSize - total
		}
		iovs[i] = make([]byte, bufSize)

		total += bufSize
	}
	if got, want := iovsTotalLen(iovs), fileSize; got != want {
		t.Errorf("unmatched iovs total length, got=%d, want=%d", got, want)
	}

	file2, err := os.Open(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	n, err := preadvFull(file2, iovs, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, partialSize; got != want {
		t.Errorf("unmatched length of bytes for first read, got=%d, want=%d", got, want)
	}

	iovs2 := iovsAdjust(iovs, n)
	if got, want := iovsTotalLen(iovs2), fileSize-partialSize; got != want {
		t.Errorf("unmatched iovs2 total length, got=%d, want=%d", got, want)
	}

	if _, err := file.WriteAt(data[partialSize:], partialSize); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	n2, err := preadvFull(file2, iovs2, partialSize)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n2, fileSize-partialSize; got != want {
		t.Errorf("unmatched length of bytes for first read, got=%d, want=%d", got, want)
	}
}
