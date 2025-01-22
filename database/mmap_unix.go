//go:build !windows

package database

import "syscall"

func mmapFile(fd uintptr, offset int64, length int, prot, flags int) ([]byte, error) {
	return syscall.Mmap(int(fd), offset, length, prot, flags)
}

func unmapFile(data []byte) error {
	return syscall.Munmap(data)
}

func fallocateFile(fd uintptr, offset int64, length int64) error {
	return syscall.Fallocate(int(fd), 0, offset, length)
}

func pwriteFile(fd uintptr, data []byte, offset int64) (int, error) {
	return syscall.Pwrite(int(fd), data, offset)
}
