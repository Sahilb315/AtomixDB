package database

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	TYPE_ERROR = 0
	TYPE_INT64 = 1
	TYPE_BYTES = 2
)

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

type DB struct {
	Path   string
	kv     KV
	tables map[string]*TableDef // cached table definition
}

type TableDef struct {
	Name    string
	Types   []uint32 // column types
	Cols    []string // column names
	PKeys   int      // the first `PKeys` columns are the pimary key
	Indexes [][]string
	// auto-assigned B-tree key prefixes for different tables/indexes
	Prefix      uint32
	IndexPrefix []uint32
}

// internal table: metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

func (rec *Record) AddStr(key string, val []byte) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: 2, Str: val})
	return rec
}
func (rec *Record) AddInt64(key string, val int64) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: 1, I64: val})
	return rec
}
func (rec *Record) Get(key string) *Value {
	for i, col := range rec.Cols {
		if key == col {
			return &rec.Vals[i]
		}
	}
	return nil
}

func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	// get the tdef from the `BTree` using the PKey - `name`
	ok, err := dbGet(db, TDEF_TABLE, rec)
	if err != nil {
		return nil
	}
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	if err != nil {
		return nil
	}
	return tdef
}

// get row by primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	sc := Scanner{
		Cmp1: CMP_GE,
		Key1: *rec,
		Cmp2: CMP_LE,
		Key2: *rec,
	}
	if err := dbScan(db, tdef, &sc); err != nil {
		return false, err
	}
	if sc.Valid() {
		sc.Deref(rec)
		return true, nil
	} else {
		return false, nil
	}
}

func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("invalid type")
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) {
	index := 0
	for index < len(in) {
		var v Value
		if index+8 < len(in) {
			v.Type = TYPE_INT64
			u := binary.BigEndian.Uint64(in[index : index+8])
			v.I64 = int64(u - (1 << 63))
			index += 8
			out = append(out, v)
		}
		nullIdx := bytes.IndexByte(in[index:], 0)
		if nullIdx != -1 {
			v.Type = TYPE_BYTES
			v.Str = unescapeString(in[index : index+nullIdx])
			out = append(out, v)
			index += nullIdx + 1
		}
	}
}

// Strings are encoded as nul terminated strings,
// escape the nul byte so that strings contain no nul byte.
func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})

	if zeros+ones == 0 {
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	for _, ch := range in {
		if ch <= 1 { // if null character found
			out[pos+0] = 0x01 // replace null character by escaping character
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

func unescapeString(in []byte) []byte {
	if len(in) == 0 {
		return in
	}

	escapeCount := 0
	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 && i+1 < len(in) {
			escapeCount++
			i++
		}
	}

	if escapeCount == 0 {
		return in
	}

	out := make([]byte, len(in)-escapeCount)
	pos := 0

	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 && i+1 < len(in) {
			out[pos] = in[i+1] - 1
			pos++
			i++
		} else {
			out[pos] = in[i]
			pos++
		}
	}

	return out
}

func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	orderedValues := make([]Value, len(tdef.Cols))

	if n == tdef.PKeys {
		for i := 0; i < tdef.PKeys; i++ {
			if !contains(rec.Cols, tdef.Cols[i]) {
				return nil, fmt.Errorf("missing primary key column: %s", tdef.Cols[i])
			}
			index := indexOf(rec.Cols, tdef.Cols[i])
			orderedValues[i] = rec.Vals[index]
		}
	}

	if n == len(tdef.Cols) {
		for i, col := range tdef.Cols {
			if !contains(rec.Cols, col) {
				return nil, fmt.Errorf("missing column: %s", col)
			}
			index := indexOf(rec.Cols, col)
			orderedValues[i] = rec.Vals[index]
		}
	}
	return orderedValues, nil
}

// Helper functions
func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}
func indexOf(slice []string, item string) int {
	for i, v := range slice {
		if v == item {
			return i
		}
	}
	return -1 // Return -1 if not found
}
