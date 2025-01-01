package database

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
	free *FreeList
	mmap struct {
		file   int      // file size, can be larger than DB size
		total  int      // mmap size, can be larger than file size
		chunks [][]byte // multiple mmaps, can be non-continous
	}
	page struct {
		flushed uint64 // DB size in number of pages
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte // Temporary pages are kept in a map keyed by their assigned page numbers. And removed page numbers are also there
	}
}

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used | free_list |
// |  8B | 	   8B 	  | 	 8B	  |		8B	  |

func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0o644)
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

	// init freelist
	db.free = &FreeList{
		head: 0,
		use:  db.pageUse,
		new:  db.pageAppend,
		get:  db.pageGet,
	}
	db.page.updates = make(map[uint64][]byte)

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
		if err != nil {
			fmt.Println("Error while closing DB")
		}
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

func (db *KV) Delete(req *DeleteReq) (bool, error) {
	val, _ := db.Get(req.Key)
	deleted := db.tree.Delete(req.Key)
	if deleted {
		req.Old = val
	}
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
	freed := []uint64{}

	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)
	npages := int(db.page.flushed) + db.page.nappend
	// extends mmap & file if needed
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr).data, page)
		}
	}
	// for _, v := range db.page.updates {
	// 	fmt.Println(string(v))
	// }
	return nil
}

func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(db.page.nappend)
	db.page.updates = map[uint64][]byte{}

	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created
		db.page.flushed = 1 // reserved for the first page
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[8:])
	pagesUsed := binary.LittleEndian.Uint64(data[16:])
	freeListPtr := binary.LittleEndian.Uint64(data[24:])

	if !bytes.Equal([]byte(DB_SIG), data[:8]) {
		return errors.New("bad signature")
	}

	isBad := !(pagesUsed >= 1 && pagesUsed <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	isBad = isBad || !(0 <= root && root < pagesUsed)

	if isBad {
		return errors.New("bad master page")
	}

	db.tree.root = root
	db.page.flushed = pagesUsed
	db.free.head = freeListPtr
	return nil
}

func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:8], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[8:16], db.tree.root)
	binary.LittleEndian.PutUint64(data[16:24], db.page.flushed)
	binary.LittleEndian.PutUint64(data[24:32], db.free.head)
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
		return 0, nil, errors.New("file size is not a multiple of page size")
	}

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

// callbacks for BTree & Freelist, dereference a pointer
func (db *KV) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		return BNode{page}
	}
	return pageGetMapped(db, ptr)
}

// callback for BTree, allocate a new page
func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		// append a new page
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

func pageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr")
}

// callback for Freelist, allocate new page
func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}
