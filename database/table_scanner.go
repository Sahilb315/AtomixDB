package database

import (
	"bytes"
	"fmt"
)

type TableScanner struct {
	db       *DB
	tdef     *TableDef
	kvReader *KVReader
	iter     *BIter
	prefix   []byte
}

func (db *DB) QueryWithFilter(table string, tdef *TableDef, filterRec *Record) ([]*Record, error) {
	results, err := fullTableScan(db, table, tdef)
	if err != nil {
		return nil, err
	}

	idx := ColIndex(tdef, filterRec.Cols[0])
	if idx == -1 {
		return nil, fmt.Errorf("column %s not found", filterRec.Cols[0])
	}
	var matchingRecords []*Record
	for _, record := range results {
		for _, filterVal := range filterRec.Vals {
			if compareValues(record.Vals[idx], filterVal) {
				matchingRecords = append(matchingRecords, record)
				break
			}
		}
	}

	if len(matchingRecords) == 0 {
		return nil, fmt.Errorf("no matching records found")
	}

	return matchingRecords, nil
}

func NewTableScanner(db *DB, table string, kvReader *KVReader, tdef *TableDef) (*TableScanner, error) {
	if tdef == nil {
		return nil, fmt.Errorf("table definition not found")
	}

	return &TableScanner{
		db:       db,
		tdef:     tdef,
		kvReader: kvReader,
		prefix:   encodeKey(nil, tdef.Prefix, nil),
	}, nil
}

func fullTableScan(db *DB, table string, tdef *TableDef) ([]*Record, error) {
	var kvReader KVReader
	db.kv.BeginRead(&kvReader)
	defer db.kv.EndRead(&kvReader)

	scanner, err := NewTableScanner(db, table, &kvReader, tdef)
	if err != nil {
		return nil, fmt.Errorf("scanner creation failed: %v", err)
	}

	var results []*Record
	scanner.Start()

	for {
		rec, ok, recOK := scanner.Next()
		if !ok {
			if recOK {
				results = append(results, rec)
			}
			break
		}
		results = append(results, rec)
	}

	return results, nil
}

func (ts *TableScanner) Start() {
	if ts.kvReader == nil {
		fmt.Println("KVReader is nil")
		return
	}
	ts.iter = ts.kvReader.Tree.Seek(ts.prefix, CMP_GE)
}

func (ts *TableScanner) Next() (*Record, bool, bool) {
	if ts.iter == nil || !ts.iter.Valid() {
		return nil, false, false
	}

	key, val := ts.iter.Deref()

	if !bytes.HasPrefix(key, ts.prefix) {
		return nil, false, false
	}

	rec := &Record{
		Cols: make([]string, len(ts.tdef.Cols)),
		Vals: make([]Value, len(ts.tdef.Cols)),
	}
	for i := range rec.Cols {
		rec.Vals[i].Type = ts.tdef.Types[i]
	}
	copy(rec.Cols, ts.tdef.Cols)
	decodeValues(key[4:], rec.Vals[:ts.tdef.PKeys])
	decodeValues(val, rec.Vals[ts.tdef.PKeys:])

	ts.iter.Next()

	nextKey, _ := ts.iter.Deref()
	if bytes.Equal(key, nextKey) {
		ts.iter = &BIter{}
		return rec, false, true
	}

	return rec, true, true
}

func (ts *TableScanner) Current() (*Record, error) {
	key, val := ts.iter.Deref()
	rec := &Record{
		Cols: make([]string, len(ts.tdef.Cols)),
		Vals: make([]Value, len(ts.tdef.Cols)),
	}
	for i := range rec.Cols {
		rec.Vals[i].Type = ts.tdef.Types[i]
	}
	decodeValues(key[4:], rec.Vals[:ts.tdef.PKeys])
	decodeValues(val, rec.Vals[ts.tdef.PKeys:])
	return rec, nil
}

func compareValues(v1, v2 Value) bool {
	if v1.Type != v2.Type {
		return false
	}

	switch v1.Type {
	case TYPE_INT64:
		return v1.I64 == v2.I64
	case TYPE_BYTES:
		return bytes.Equal(v1.Str, v2.Str)
	default:
		return false
	}
}
