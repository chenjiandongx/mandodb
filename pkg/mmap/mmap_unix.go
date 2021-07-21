// +build !windows,!plan9

package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

func syscallMmap(f *os.File, length int) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ, unix.MAP_SHARED)
}

func syscallMunmap(b []byte) (err error) {
	return unix.Munmap(b)
}
