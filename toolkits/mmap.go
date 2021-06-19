package toolkits

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type MmapFile struct {
	f *os.File
	b []byte
}

func OpenMmapFile(path string) (mf *MmapFile, retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("try lock file")
	}

	defer func() {
		if retErr != nil {
			f.Close()
		}
	}()

	var size int
	info, err := f.Stat()
	if err != nil {
		return nil, errors.New("stat")
	}
	size = int(info.Size())

	b, err := syscallMmap(f, size)
	if err != nil {
		return nil, errors.New("mmap")
	}

	return &MmapFile{f: f, b: b}, nil
}

func (f *MmapFile) Close() error {
	err0 := syscallMunmap(f.b)
	err1 := f.f.Close()

	if err0 != nil {
		return err0
	}
	return err1
}

func (f *MmapFile) File() *os.File {
	return f.f
}

func (f *MmapFile) Bytes() []byte {
	return f.b
}

func syscallMmap(f *os.File, length int) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ, unix.MAP_SHARED)
}

func syscallMunmap(b []byte) (err error) {
	return unix.Munmap(b)
}
