//go:build darwin

package database

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func mmapFile(fd uintptr, offset int64, length int, prot, flags int) ([]byte, error) {
	return syscall.Mmap(int(fd), offset, length, prot, flags)
}

func unmapFile(data []byte) error {
	return syscall.Munmap(data)
}

func fallocateFile(fd uintptr, offset int64, length int64) error {
	// use mmap let file map to memory
	_, err := unix.Mmap(int(fd), 0, int(offset+length), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	return err
}

func pwriteFile(fd uintptr, data []byte, offset int64) (int, error) {
	return syscall.Pwrite(int(fd), data, offset)
}
