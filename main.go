package main

import (
	"atomixDB/src"
	"fmt"
	"math/rand"
	"os"
)

func main() {
	src.BtreeImpl()
}

// Issues -
// 1. Truncates file before updating it
// 2. Writing data to file may not be atomic
// 3. The data is probably still in the operating system’s page cache after the write syscall returns
func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	return err
}

// Issue - Doesnt control when the data is persisted to the disk & the metadata might
// be persisted to the disk before data causing the file could be corrupted when the system crashes
func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	fp, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		/// Remove the tmp file if the operation failed
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func SaveData3(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	fp, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		/// Remove the tmp file if the operation failed
		os.Remove(tmp)
		return err
	}
	err = fp.Sync()
	if err != nil {
		/// Remove the tmp file if the operation failed
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
