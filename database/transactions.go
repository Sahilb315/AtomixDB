package database

import (
	"container/heap"
	"fmt"
)

// DB transaction
type DBTX struct {
	kv KVTX
	db *DB
}

type KVReader struct {
	// snapshot
	version uint64
	Tree    BTree
	mmap    struct {
		chunks [][]byte // copied from sttruct KV, read-only
	}
	index int
}

// KV Transaction
type KVTX struct {
	KVReader
	kv   *KV
	free FreeList
	page struct {
		nappend int // no of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte
	}
}

// initialising the reader from the kv
func (kv *KV) BeginRead(tx *KVReader) {
	kv.mu.Lock()
	tx.mmap.chunks = kv.mmap.chunks
	tx.Tree.root = kv.tree.root
	tx.Tree.get = tx.pageGetMapped
	tx.version = kv.version
	heap.Push(&kv.readers, tx)
	kv.mu.Unlock()
}

func (kv *KV) EndRead(tx *KVReader) {
	kv.mu.Lock()
	heap.Remove(&kv.readers, tx.index)
	kv.mu.Unlock()
}

func (tx *KVReader) Seek(key []byte, cmp int) *BIter {
	return tx.Tree.Seek(key, cmp)
}

func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	db.kv.Begin(&tx.kv)
}

func (db *DB) Commit(tx *DBTX) error {
	return db.kv.Commit(&tx.kv)
}

func (db *DB) Abort(tx *DBTX) {
	db.kv.Abort(&tx.kv)
}

func (tx *DBTX) TableNew(tdef *TableDef) error {
	return tx.db.TableNew(tdef, &tx.kv)
}

func (tx *DBTX) Set(table string, rec Record, mode int) (bool, error) {
	return tx.db.Set(table, rec, mode, &tx.kv)
}

func (tx *DBTX) Delete(table string, rec Record) (bool, error) {
	return tx.db.Delete(table, rec, &tx.kv)
}

func (tx *DBTX) Scan(table string, req *Scanner) error {
	return tx.db.Scan(table, req, &tx.kv.Tree)
}

func (kv *KV) Begin(tx *KVTX) {
	tx.kv = kv
	tx.page.updates = map[uint64][]byte{}
	tx.mmap.chunks = kv.mmap.chunks

	kv.writer.Lock()
	tx.version = kv.version
	// btree
	tx.Tree.root = kv.tree.root
	tx.Tree.get = tx.pageGet
	tx.Tree.new = tx.pageNew
	tx.Tree.del = tx.pageDel

	// freelist
	tx.free.FreeListData = kv.free
	tx.free.version = kv.version
	tx.free.get = tx.pageGet
	tx.free.new = tx.pageAppend
	tx.free.use = tx.pageUse

	tx.free.minReader = kv.version
	kv.mu.Lock()

	if len(kv.readers) > 0 {
		tx.free.minReader = kv.readers[0].version
	}
	kv.mu.Unlock()
}

// end a transaction: commit updates
func (kv *KV) Commit(tx *KVTX) error {
	defer kv.writer.Unlock()
	if kv.tree.root == tx.Tree.root {
		return nil // no updates
	}

	// phase 1: persist the page data to disk
	if err := writePages(tx); err != nil {
		rollbackTX(tx)
		return err
	}

	// the page data must reach disk before master page.
	// the `fsync` serves as a barrier here
	if err := kv.fp.Sync(); err != nil {
		rollbackTX(tx)
		return fmt.Errorf("fsync: %w", err)
	}

	// transaction is visible
	kv.page.flushed += uint64(tx.page.nappend)
	kv.free = tx.free.FreeListData
	kv.mu.Lock()
	kv.tree.root = tx.Tree.root
	kv.version++
	kv.mu.Unlock()

	// phase 2: update the master page to point to new tree
	if err := masterStore(kv); err != nil {
		return err
	}

	if err := kv.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// end a transaction: rollback
func (kv *KV) Abort(tx *KVTX) {
	kv.writer.Unlock()
}

func (tx *KVTX) Seek(key []byte, cmp int) *BIter {
	return tx.Tree.Seek(key, cmp)
}

func (tx *KVTX) Update(req *InsertReq) bool {
	tx.Tree.InsertEx(req)
	return req.Added
}

func (tx *KVTX) Del(req *DeleteReq) bool {
	return tx.Tree.DeleteEx(req)
}

// rollbackTX the tree & other in-memmory data structures
func rollbackTX(tx *KVTX) {
	tx.kv.tree.root = tx.Tree.root
	tx.kv.free = tx.free.FreeListData
	tx.page.nappend = 0
	tx.page.updates = make(map[uint64][]byte)
}
