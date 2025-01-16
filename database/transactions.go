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

func (db *DB) ConcurrentRead(tableName string, record Record) chan ReadResult {
	resultChan := make(chan ReadResult, 1)

	go func(resultChan chan ReadResult) {
		defer close(resultChan)

		var kvReader KVReader
		defer db.KV.EndRead(&kvReader)
		db.KV.BeginRead(&kvReader)

		// found, err := db.GetKVReader(tableName, &record, kvReader)
		found, err := db.Get(tableName, &record, &kvReader)

		resultChan <- ReadResult{
			Record: record,
			Found:  found,
			Error:  err,
		}
	}(resultChan)

	return resultChan
}
func (db *DB) GetKVReader(table string, rec *Record, kv KVReader) (bool, error) {
	tdef := GetTableDef(db, table, &kv.Tree)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec, &kv.Tree)
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
	db.KV.Begin(&tx.kv)
}

func (db *DB) Commit(tx *DBTX) error {
	return db.KV.Commit(&tx.kv)
}

func (db *DB) Abort(tx *DBTX) {
	db.KV.Abort(&tx.kv)
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
	kv := tx.kv
	kv.tree.root = tx.Tree.root
	kv.free.head = tx.free.head
}
