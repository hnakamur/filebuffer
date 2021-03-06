package filebuffer

import (
	"os"
)

func pwritevFull(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	total := iovsTotalLen(iovs)
	if total == 0 {
		return 0, nil
	}
	done := 0
	for {
		n, err = pwritev(file, iovs, offset)
		done += n
		if err != nil || done >= total {
			break
		}
		iovs = iovsAdjust(iovs, n)
		offset += int64(n)
	}
	return done, nil
}

func preadvFull(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	total := iovsTotalLen(iovs)
	if total == 0 {
		return 0, nil
	}
	done := 0
	for {
		n, err = preadv(file, iovs, offset)
		done += n
		if err != nil || n == 0 || done >= total {
			break
		}
		iovs = iovsAdjust(iovs, n)
		offset += int64(n)
	}
	return done, nil
}

func iovsTotalLen(iovs [][]byte) int {
	var total int
	for _, iov := range iovs {
		total += len(iov)
	}
	return total
}

func iovsAdjust(iovs [][]byte, n int) [][]byte {
	for len(iovs) > 0 && n >= len(iovs[0]) {
		n -= len(iovs[0])
		iovs = iovs[1:]
	}
	if len(iovs) == 0 {
		return nil
	}
	iovs2 := make([][]byte, len(iovs))
	copy(iovs2, iovs)
	iovs2[0] = iovs2[0][n:]
	return iovs2
}
