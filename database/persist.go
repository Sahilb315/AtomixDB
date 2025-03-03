package database

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

const DB_SIG = "AtomixDB"

const (
	PROT_READ  = 0x1
	PROT_WRITE = 0x2
	MAP_SHARED = 0x1
)

type KV struct {
	Path string
	// internals
	fp *os.File

	tree struct {
		root uint64
	}
	free FreeListData

	mmap struct {
		file   int      // file size, can be larger than DB size
		total  int      // mmap size, can be larger than file size
		chunks [][]byte // multiple mmaps, can be non-continous
	}
	page struct {
		flushed uint64 // DB size in number of pages
	}

	mu     sync.Mutex
	writer sync.Mutex

	version uint64
	readers ReaderList // heap, for tranking the minimum reader version
}

// implements heap.Interface
type ReaderList []*KVReader

func (rl ReaderList) Len() int {
	return len(rl)
}

func (rl ReaderList) Less(i int, j int) bool {
	if rl[i] == nil || rl[j] == nil {
		return false
	}
	return rl[i].index < rl[j].index
}

func (rl ReaderList) Swap(i, j int) {
	rl[i], rl[j] = rl[j], rl[i]
}

func (rl *ReaderList) Push(item interface{}) {
	*rl = append(*rl, item.(*KVReader))
}

func (rl *ReaderList) Pop() interface{} {
	old := *rl
	n := len(old)
	x := old[n-1]
	*rl = old[0 : n-1]
	return x
}

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used | free_list | version |
// |  8B | 	   8B 	  | 	 8B	  |		8B	  |   8B    |

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

	db.free = FreeListData{
		head: 0,
	}
	err = masterLoad(db)
	if err != nil {
		goto fail
	}
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV Open: %w", err)
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := unmapFile(chunk)
		if err != nil {
			fmt.Println("Error while closing DB")
		}
	}
	_ = db.fp.Close()
}

func (db *KVTX) Get(key []byte) ([]byte, bool, error) {
	return db.Tree.Get(key)
}

func (db *KVTX) Set(key, val []byte) error {
	db.Tree.Insert(key, val)
	return flushPages(db)
}

func (db *KVTX) Delete(req *DeleteReq) (bool, error) {
	val, _, err := db.Get(req.Key)
	if err != nil {
		return false, err
	} else if len(val) == 0 {
		return false, errors.New("record not found")
	}
	deleted := db.Tree.Delete(req.Key)
	if deleted {
		req.Old = val
	}
	return deleted, flushPages(db)
}

// persist the newly allocated pages after updates
func flushPages(db *KVTX) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *KVTX) error {
	freed := []uint64{}

	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Add(freed)
	npages := int(db.page.nappend) + int(db.kv.page.flushed)

	// extends mmap & file if needed
	if err := extendFile(db.kv, npages); err != nil {
		return err
	}
	if err := extendMmap(db.kv, npages); err != nil {
		return err
	}

	for ptr, page := range db.page.updates {
		if page != nil {
			copy(db.pageGetMapped(ptr).data, page)
		}
	}
	return nil
}

func syncPages(db *KVTX) error {
	if err := db.kv.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.kv.page.flushed += uint64(db.page.nappend)
	db.page.updates = map[uint64][]byte{}

	if err := masterStore(db.kv); err != nil {
		return err
	}
	if err := db.kv.fp.Sync(); err != nil {
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
	isBad := 1 > pagesUsed || pagesUsed > uint64(db.mmap.file/BTREE_PAGE_SIZE)
	isBad = isBad || (root >= pagesUsed)

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
	_, err := pwriteFile(db.fp.Fd(), data[:], 0)
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
	chunk, err := mmapFile(fp.Fd(), 0, mmapSize, PROT_READ|PROT_WRITE, MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}

	return int(fi.Size()), chunk, nil
}

func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
		return nil
	}

	chunk, err := mmapFile(db.fp.Fd(), int64(db.mmap.total), db.mmap.total, PROT_READ|PROT_WRITE, MAP_SHARED)
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
	err := fallocateFile(db.fp.Fd(), 0, 0)
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
func (db *KVTX) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		return BNode{page}
	}
	return db.pageGetMapped(ptr)
}

// callback for BTree, allocate a new page
func (db *KVTX) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := db.free.Pop()
	if ptr == 0 {
		ptr = db.free.new(node)
	}
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KVTX) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

func (db *KVReader) pageGetMapped(ptr uint64) BNode {
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
func (db *KVTX) pageAppend(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := uint64(db.page.nappend) + db.kv.page.flushed
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KVTX) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}
