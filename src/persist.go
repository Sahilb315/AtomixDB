package src

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

const DB_SIG = "AtmoixDB"

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree
	mmap struct {
		file   int      // file size, can be larger than DB size
		total  int      // mmap size, can be larger than file size
		chunks [][]byte // multiple mmaps, can be non-continous
	}
	page struct {
		flushed uint64   // DB size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	// create the inital mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// btree callbacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	// process completed
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV Open: %w", err)
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}
	_ = db.fp.Close()
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Delete(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *KV) error {
	npages := int(db.page.flushed) + len(db.page.temp)
	// extends mmap & file if needed
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	for i, tempPage := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		// copy the data from temp pages into flushed pages
		copy(db.pageGet(ptr).data, tempPage)
	}
	return nil
}

func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used |
// | 8B | 	   8B 	  | 	 8B	  |

func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created
		db.page.flushed = 1 // reserved for the first page
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[8:])
	pageUsed := binary.LittleEndian.Uint64(data[16:])

	if !bytes.Equal([]byte(DB_SIG), data[:8]) {
		return errors.New("bad signature")
	}

	isBad := !(pageUsed >= 1 && pageUsed <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	isBad = isBad || !(0 <= root && root < pageUsed)

	if isBad {
		return errors.New("bad master page")
	}

	db.tree.root = root
	db.page.flushed = pageUsed
	return nil
}

func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[8:16], db.tree.root)
	binary.LittleEndian.PutUint64(data[16:24], db.page.flushed)
	// Pwrite ensures that updating the page is atomic
	_, err := syscall.Pwrite(int(db.fp.Fd()), data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size.")
	}
	// 16,384
	mmapSize := 64 << 20
	for mmapSize < int(fi.Size()) {
		// mmapSize can be larger than the file
		mmapSize *= 2
	}
	// maps the file data into the process's virtual address space
	chunk, err := syscall.Mmap(int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}

	return int(fi.Size()), chunk, nil
}

func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
		return nil
	}

	chunk, err := syscall.Mmap(int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages > npages {
		return nil
	}

	for filePages < npages {
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	fileSize := filePages * BTREE_PAGE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		// Fallback to truncate
		err = db.fp.Truncate(int64(fileSize))
		if err != nil {
			return fmt.Errorf("fallocate: %w", err)
		}
	}
	db.mmap.file = fileSize
	return nil
}

// callbacks for BTree
func (db *KV) pageGet(ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := (ptr - start) * BTREE_PAGE_SIZE
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr")
}

func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.data)
	return ptr
}

func (db *KV) pageDel(pagePtr uint64) {
	if pagePtr >= db.page.flushed {
		return
	}
	if db.page.temp == nil {
		db.page.temp = make([][]byte, 0)
	}

	start := uint64(0)
	found := false
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if pagePtr < end {
			offset := (pagePtr - start) * BTREE_PAGE_SIZE
			db.page.temp = append(db.page.temp, chunk[offset:offset+BTREE_PAGE_SIZE])
			found = true
			break
		}
		start = end
	}

	if !found {
		return
	}
}
