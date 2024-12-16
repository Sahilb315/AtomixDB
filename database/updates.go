package database

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new key
)

const TABLE_PREFIX_MIN = 1

type InsertReq struct {
	tree *BTree
	// out
	Added bool // added a new key
	// in
	Key   []byte
	Value []byte
	Mode  int
}

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}
	// allocate a new prefix
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	if err != nil {
		return err
	}
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	_, err = dbUpdate(db, TDEF_META, *meta, 0)
	if err != nil {
		return err
	}

	val, err := json.Marshal(tdef)
	if err != nil {
		return err
	}
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}

func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	return db.kv.Delete(key)
}

func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	vals := encodeValues(nil, values[tdef.PKeys:])
	return db.kv.SetWithMode(key, vals, mode)
}

func (tree *BTree) InsertEx(req *InsertReq) {
	if tree == nil || req == nil {
		return
	}

	// Check if the key already exists
	_, exists := tree.Get(req.Key)

	switch req.Mode {
	case MODE_UPSERT:
		// insert or replace the key
		tree.Insert(req.Key, req.Value)
		req.Added = !exists

	case MODE_UPDATE_ONLY:
		// Only update if the key exists
		if exists {
			tree.Insert(req.Key, req.Value)
			req.Added = false
		}

	case MODE_INSERT_ONLY:
		// Only insert if the key does not exist
		if !exists {
			tree.Insert(req.Key, req.Value)
			req.Added = true
		}
	}
}

func (db *KV) SetWithMode(key []byte, val []byte, mode int) (bool, error) {
	switch mode {
	case MODE_UPDATE_ONLY:
		_, exists := db.Get(key)
		if exists {
			err := db.Set(key, val)
			return true, err
		}
		return false, errors.New("key does not exist")

	case MODE_UPSERT:
		err := db.Set(key, val)
		return true, err

	case MODE_INSERT_ONLY:
		_, exists := db.Get(key)
		if !exists {
			err := db.Set(key, val)
			return true, err
		}
		return false, errors.New("key already exists")

	default:
		return false, errors.New("invalid update mode")
	}
}

func tableDefCheck(tdef *TableDef) error {
	if tdef.Name == "" {
		return errors.New("table name cannot be empty")
	}
	// if !isValidTableName(tdef.Name) {
	// 	return errors.New("invalid table name")
	// }
	if len(tdef.Cols) == 0 {
		return errors.New("table must have at least one column")
	}
	// Track column names to check for duplicates
	columnNames := make(map[string]bool)

	for i, col := range tdef.Cols {
		if col == "" {
			return errors.New("column name cannot be empty")
		}
		if columnNames[col] {
			return fmt.Errorf("duplicate column name: %s", col)
		}
		columnNames[col] = true

		if tdef.Types[i] != TYPE_BYTES && tdef.Types[i] != TYPE_INT64 {
			return fmt.Errorf("invalid data type for column %s", col)
		}
	}

	if tdef.PKeys > 1 {
		return errors.New("only one primary key is allowed")
	}
	return nil
}

func isValidTableName(name string) bool {
	return regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(name)
}
