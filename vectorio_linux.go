package filebuffer

import (
	"log"
	"os"

	"github.com/tklauser/go-sysconf"
	"golang.org/x/sys/unix"
)

func pwritev(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	max, err := sysconf.Sysconf(sysconf.SC_UIO_MAXIOV)
	if err != nil {
		return 0, err
	}

	for len(iovs) > 0 {
		count := int64(len(iovs))
		if count > max {
			count = max
		}
		n0, err := unix.Pwritev(int(file.Fd()), iovs[:count], offset)
		n += n0
		log.Printf("after unix.Pwritev, n=%d, n0=%d, err=%v", n, n0, err)
		if err != nil {
			return n, err
		}

		iovs = iovs[count:]
		offset += int64(n0)
	}
	return n, nil
}

func preadv(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	max, err := sysconf.Sysconf(sysconf.SC_UIO_MAXIOV)
	if err != nil {
		return 0, err
	}

	for len(iovs) > 0 {
		count := int64(len(iovs))
		if count > max {
			count = max
		}
		n0, err := unix.Preadv(int(file.Fd()), iovs[:count], offset)
		n += n0
		log.Printf("after unix.Preadv, n=%d, n0=%d, err=%v", n, n0, err)
		if err != nil || n0 == 0 {
			return n, err
		}

		iovs = iovs[count:]
		offset += int64(n0)
	}
	return n, nil
}
