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

// table row
type Record struct {
	Cols []string
	Vals []Value
}

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

type DB struct {
	Path   string
	kv     KV
	pool   *WorkerPool
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
	Name:        "@meta",
	Types:       []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:        []string{"key", "val"},
	PKeys:       1,
	Indexes:     make([][]string, 0),
	Prefix:      1,
	IndexPrefix: make([]uint32, 0),
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Name:        "@table",
	Types:       []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:        []string{"name", "def"},
	PKeys:       1,
	Indexes:     make([][]string, 0),
	Prefix:      2,
	IndexPrefix: make([]uint32, 0),
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

func GetTableDef(db *DB, name string, tree *BTree) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name, tree)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string, tree *BTree) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	// get the tdef from the `BTree` using the PKey - `name`
	ok, err := dbGet(db, TDEF_TABLE, rec, tree)
	if err != nil {
		return nil
	}
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	// Verify Once
	if rec.Get("def").Str != nil {
		err = json.Unmarshal(rec.Get("def").Str, tdef)
	}
	if err != nil {
		fmt.Println("Err while Unmarshal: ", err.Error())
		return nil
	}
	return tdef
}

// get row by primary key
func dbGet(db *DB, tdef *TableDef, rec *Record, tree *BTree) (bool, error) {
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: *rec,
		Key2: *rec,
	}
	if err := dbScan(db, tdef, &sc, tree); err != nil {
		return false, err
	}
	if sc.Valid() {
		sc.Deref(rec, tree)
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

func dbGetRange(db *DB, tdef *TableDef, start *Record, end *Record, tree *BTree) ([]*Record, error) {
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: *start,
		Key2: *end,
	}

	if err := dbScan(db, tdef, &sc, tree); err != nil {
		return nil, err
	}

	var results []*Record
	for sc.Valid() {
		rec := &Record{
			Cols: make([]string, len(tdef.Cols)),
			Vals: make([]Value, len(tdef.Cols)),
		}
		copy(rec.Cols, tdef.Cols)
		sc.Deref(rec, tree)
		results = append(results, rec)
		sc.Next()
	}

	return results, nil
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
			if v.Str == nil {
				out = append(out, 0)
				continue
			}
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0)
		default:
			panic("invalid type while encodeValues")
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) {
	remaining := in
	for i, v := range out {
		switch v.Type {
		case TYPE_INT64:
			if len(remaining) < 8 {
				return
			}
			u := binary.BigEndian.Uint64(remaining[:8])
			val := int64(u - (1 << 63))
			out[i] = Value{Type: TYPE_INT64, I64: val}
			remaining = remaining[8:]
		case TYPE_BYTES:
			end := 0
			for end < len(remaining) && remaining[end] != 0 {
				end++
			}
			if end >= len(remaining) {
				return
			}
			unEscStr := unEscapeString(remaining[:end])
			out[i] = Value{Type: TYPE_BYTES, Str: unEscStr}
			remaining = remaining[end+1:]
		default:
			panic("invalid type while decodeValues")
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
	if len(in) > 0 && in[0] >= 0xfe {
		out[0] = 0xfe
		out[1] = in[0]
		pos += 2
		in = in[1:]
	}
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

func unEscapeString(in []byte) []byte {
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

	if escapeCount == 0 && (len(in) == 0 || in[0] != 0xfe) {
		return in
	}
	outLen := len(in) - escapeCount
	if in[0] == 0xfe {
		outLen--
	}
	out := make([]byte, outLen)
	pos := 0
	i := 0
	if in[0] == 0xfe {
		if len(in) < 2 {
			return in
		}
		out[pos] = in[1]
		pos++
		i += 2
	}

	for i < len(in) {
		if in[i] == 0x01 && i+1 < len(in) {
			out[pos] = in[i+1] - 1
			pos++
			i += 2
		} else {
			out[pos] = in[i]
			pos++
			i++
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
	return -1
}
