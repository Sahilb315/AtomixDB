package database

import "fmt"

// DB transaction
type DBTX struct {
	kv KVTX
	db *DB
}

// KV Transaction
type KVTX struct {
	kv *KV
	// for the rollback
	tree struct {
		root uint64
	}
	free struct {
		head uint64
	}
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
	return tx.db.TableNew(tdef)
}

func (tx *DBTX) Get(table string, rec *Record) (bool, error) {
	return tx.db.Get(table, rec)
}

func (tx *DBTX) Set(table string, rec Record, mode int) (bool, error) {
	return tx.db.Set(table, rec, mode)
}

func (tx *DBTX) Delete(table string, rec Record) (bool, error) {
	return tx.db.Delete(table, rec)
}

func (tx *DBTX) Scan(table string, req *Scanner) error {
	return tx.db.Scan(table, req)
}

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	tx.kv = kv
	tx.tree.root = kv.tree.root
	tx.free.head = kv.free.head
}

// end a transaction: commit updates
func (kv *KV) Commit(tx *KVTX) error {
	if kv.tree.root == tx.tree.root {
		return nil // no updates
	}

	// phase 1: persist the page data to disk
	if err := writePages(kv); err != nil {
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
	kv.page.flushed = uint64(kv.page.nappend)
	kv.page.nfree = 0
	kv.page.nappend = 0
	kv.page.updates = map[uint64][]byte{}

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
	rollbackTX(tx)
}

// KV Operations
func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	return tx.kv.tree.Get(key)
}

func (tx *KVTX) Seek(key []byte, cmp int) *BIter {
	return tx.kv.tree.Seek(key, cmp)
}

func (tx *KVTX) Update(req *InsertReq) bool {
	tx.kv.tree.InsertEx(req)
	return req.Added
}

func (tx *KVTX) Del(req *DeleteReq) bool {
	return tx.kv.tree.DeleteEx(req)
}

// rollbackTX the tree & other in-memmory data structures
func rollbackTX(tx *KVTX) {
	kv := tx.kv
	kv.tree.root = tx.tree.root
	kv.free.head = tx.free.head
	kv.page.nfree = 0
	kv.page.nappend = 0
	kv.page.updates = map[uint64][]byte{}
}
