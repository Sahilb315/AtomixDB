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
	Added   bool   // added a new key
	Updated bool   // added a new key or an old key was changed
	Old     []byte // the key before the update
	// in
	Key   []byte
	Value []byte
	Mode  int
}

type DeleteReq struct {
	tree *BTree
	// in
	Key []byte
	// out
	Old []byte
}

func (db *DB) TableNew(tdef *TableDef, kvtx *KVTX) error {
	if err := tableDefCheck(tdef); err != nil {
		return fmt.Errorf("invalid table definition: %w", err)
	}
	table := (&Record{}).AddStr("name", []byte(tdef.Name))

	// Table existence check
	ok, err := dbGet(db, TDEF_TABLE, table, &kvtx.Tree)
	if err != nil {
		return fmt.Errorf("error checking table existence: %w", err)
	}
	if ok {
		return fmt.Errorf("%w: %s", ErrTableAlreadyExists, tdef.Name)
	}
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta, &kvtx.Tree)
	if err != nil {
		return fmt.Errorf("error reading meta: %w", err)
	}

	if ok {
		if len(meta.Get("val").Str) < 4 {
			return fmt.Errorf("corrupted meta value: invalid length")
		}
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		if TABLE_PREFIX_MIN > tdef.Prefix {
			return errors.New("table prefix less than the min TABLE_PREFIX")
		}
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	if len(tdef.Indexes) > 0 {
		tdef.IndexPrefix = make([]uint32, len(tdef.Indexes))
		for i := range tdef.Indexes {
			prefix := tdef.Prefix + 1 + uint32(i)
			if prefix < tdef.Prefix { // Check for overflow
				return fmt.Errorf("index prefix overflow")
			}
			tdef.IndexPrefix[i] = prefix
		}
	}

	ntree := 1 + uint32(len(tdef.IndexPrefix))
	nextPrefix := tdef.Prefix + ntree
	if nextPrefix < tdef.Prefix {
		return fmt.Errorf("prefix overflow")
	}
	// Update meta
	binary.LittleEndian.PutUint32(meta.Get("val").Str, nextPrefix)

	added, err := dbUpdate(db, TDEF_META, *meta, MODE_UPSERT, kvtx)
	if err != nil {
		return fmt.Errorf("failed to update meta: %w", err)
	}
	if !added {
		return fmt.Errorf("failed to add meta entry")
	}

	// Marshal and store table definition
	val, err := json.Marshal(tdef)
	if err != nil {
		return fmt.Errorf("failed to marshal table definition: %w", err)
	}
	table.AddStr("def", val)
	added, err = dbUpdate(db, TDEF_TABLE, *table, MODE_UPSERT, kvtx)
	if err != nil {
		return fmt.Errorf("failed to update table definition: %w", err)
	}
	if !added {
		return fmt.Errorf("failed to add table definition")
	}
	return nil
}

func (db *DB) Set(table string, rec Record, mode int, kvtx *KVTX) (bool, error) {
	tdef := GetTableDef(db, table, &kvtx.Tree)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode, kvtx)
}

func (db *DB) Get(table string, rec *Record, kvReader *KVReader) (bool, error) {
	tdef := GetTableDef(db, table, &kvReader.Tree)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec, &kvReader.Tree)
}

func (db *DB) GetRange(table string, start, end *Record, kvReader *KVReader) ([]*Record, error) {
	tdef := GetTableDef(db, table, &kvReader.Tree)
	if tdef == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	var results []*Record
	maxResults := 500 // Safety limit

	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: *start,
		Key2: *end,
	}

	if err := dbScan(db, tdef, &sc, &kvReader.Tree); err != nil {
		return nil, err
	}

	count := 0
	for sc.Valid() && count < maxResults {
		rec := &Record{
			Cols: make([]string, len(tdef.Cols)),
			Vals: make([]Value, len(tdef.Cols)),
		}
		copy(rec.Cols, tdef.Cols)

		sc.Deref(rec, &kvReader.Tree)
		results = append(results, rec)

		sc.Next()
		count++
	}

	if count >= maxResults {
		return results, fmt.Errorf("reached maximum result limit")
	}

	return results, nil
}

func (db *DB) Insert(table string, rec Record, kvtx *KVTX) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY, kvtx)
}

func (db *DB) Update(table string, rec Record, kvtx *KVTX) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY, kvtx)
}

func (db *DB) Upsert(table string, rec Record, kvtx *KVTX) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT, kvtx)
}

func (db *DB) Delete(table string, rec Record, kvtx *KVTX) (bool, error) {
	tdef := GetTableDef(db, table, &kvtx.Tree)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec, kvtx)
}

func dbDelete(db *DB, tdef *TableDef, rec Record, kvtx *KVTX) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	req := DeleteReq{Key: key}
	deleted, error := kvtx.Delete(&req)
	if error != nil || !deleted || len(tdef.Indexes) == 0 {
		return deleted, error
	}
	for i := tdef.PKeys; i <= len(tdef.Cols[tdef.PKeys:]); i++ {
		values[i] = Value{Type: tdef.Types[i]}
	}
	if deleted {
		decodeValues(req.Old, values[tdef.PKeys:])
		indexOp(db, tdef, Record{tdef.Cols, values}, INDEX_DEL, kvtx)
	}
	return deleted, nil
}

func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int, kvtx *KVTX) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	isTableValid := validateTableTypes(tdef, rec)
	if !isTableValid {
		return false, errors.New("invalid type")
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	vals := encodeValues(nil, values[tdef.PKeys:])
	req := InsertReq{Key: key, Value: vals, Mode: mode}
	added, err := kvtx.SetWithMode(&req)
	// if err or no changes made return
	if err != nil || len(tdef.Indexes) == 0 {
		return added, err
	}

	if req.Updated && !req.Added {
		//  delete the old index entries
		decodeValues(req.Old, values[tdef.PKeys:]) // get the old row
		indexOp(db, tdef, Record{tdef.Cols, values}, INDEX_DEL, kvtx)
	}
	if req.Updated || req.Added {
		indexOp(db, tdef, rec, INDEX_ADD, kvtx)
	}
	return added, nil
}

func (tree *BTree) DeleteEx(req *DeleteReq) bool {
	if tree == nil || req == nil {
		return false
	}

	// Check if the key already exists
	key, exists, err := tree.Get(req.Key)
	if err != nil || !exists {
		return false
	}
	isDeleted := tree.Delete(key)
	if isDeleted {
		req.Old = key
	}
	return isDeleted
}

func (tree *BTree) InsertEx(req *InsertReq) {
	if tree == nil || req == nil {
		return
	}

	// Check if the key already exists
	_, exists, err := tree.Get(req.Key)
	if err != nil {
		return
	}

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

func (db *KVTX) SetWithMode(req *InsertReq) (bool, error) {
	switch req.Mode {
	case MODE_UPDATE_ONLY:
		old, exists, err := db.Get(req.Key)
		if err != nil {
			return false, err
		}
		if exists {
			err := db.Set(req.Key, req.Value)
			req.Updated = true
			req.Old = old
			return true, err
		}
		return false, errors.New("record not found")

	case MODE_UPSERT:
		old, exists, _ := db.Get(req.Key)
		if exists {
			req.Old = old
		}
		err := db.Set(req.Key, req.Value)
		req.Updated = true
		return true, err

	case MODE_INSERT_ONLY:
		_, exists, err := db.Get(req.Key)
		if err != nil {
			return false, err
		}
		if !exists {
			err := db.Set(req.Key, req.Value)
			req.Added = true
			return true, err
		}
		return false, errors.New("record already exists")

	default:
		return false, errors.New("invalid update mode")
	}
}

func tableDefCheck(tdef *TableDef) error {
	if tdef.Name == "" {
		return errors.New("table name cannot be empty")
	}
	if len(tdef.Cols) == 0 {
		return errors.New("table must have at least one column")
	}
	if len(tdef.Cols) != len(tdef.Types) {
		return errors.New("length of columns & types do not match")
	}
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
	for i, index := range tdef.Indexes {
		index, err := checkIndexKeys(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = index
	}
	return nil
}

func isValidTableName(name string) bool {
	return regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(name)
}

func validateTableTypes(tdef *TableDef, rec Record) bool {
	for i := range rec.Cols {
		if rec.Vals[i].Type != tdef.Types[i] {
			return false
		}
	}
	return true
}
