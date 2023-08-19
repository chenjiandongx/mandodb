package mmap

import (
	"os"
	"syscall"
	"unsafe"
)

const maxMapSize = 0x7FFFFFFF // 2GB

func syscallMmap(f *os.File, size int) ([]byte, error) {
	low, high := uint32(size), uint32(size>>32)
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, high, low, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	return (*[maxMapSize]byte)(unsafe.Pointer(addr))[:size], nil
}

func syscallMunmap(b []byte) error {
	if err := syscall.UnmapViewOfFile((uintptr)(unsafe.Pointer(&b[0]))); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
