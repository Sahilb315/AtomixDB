//go:build windows

package database

import (
	"syscall"
	"unsafe"
)

func mmapFile(fd uintptr, offset int64, length int, prot, flags int) ([]byte, error) {
	h, err := syscall.CreateFileMapping(syscall.Handle(fd), nil, uint32(syscall.PAGE_READWRITE),
		uint32(offset>>32), uint32(offset&0xffffffff), nil)
	if err != nil {
		return nil, err
	}
	defer syscall.CloseHandle(h)

	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE,
		uint32(offset>>32), uint32(offset&0xffffffff), uintptr(length))
	if err != nil {
		return nil, err
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), length)
	return data, nil
}

func unmapFile(data []byte) error {
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))
}

func fallocateFile(fd uintptr, offset int64, length int64) error {
	size := offset + length
	lowOffset := int32(size & 0xFFFFFFFF)
	highOffset := int32(size >> 32)

	_, err := syscall.SetFilePointer(syscall.Handle(fd), lowOffset, &highOffset, syscall.FILE_BEGIN)
	if err != nil {
		return err
	}

	return syscall.SetEndOfFile(syscall.Handle(fd))
}

func pwriteFile(fd uintptr, data []byte, offset int64) (int, error) {
	var bytesWritten uint32
	var overlapped syscall.Overlapped
	overlapped.Offset = uint32(offset)
	overlapped.OffsetHigh = uint32(offset >> 32)
	err := syscall.WriteFile(syscall.Handle(fd), data, &bytesWritten, &overlapped)
	return int(bytesWritten), err
}
