package database

import (
	"fmt"
)

const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

func indexOp(db *DB, tdef *TableDef, rec Record, op int) {
	key := make([]byte, 0, 256)
	irec := make([]Value, len(rec.Cols))

	for i, index := range tdef.Indexes {
		// indexed key
		for j, c := range index {
			irec[j] = *rec.Get(c)
		}
		key = encodeKey(key[:0], tdef.IndexPrefix[i], irec[:len(index)])
		done, err := false, error(nil)
		switch op {
		case INDEX_ADD:
			done, err = db.kv.SetWithMode(&InsertReq{Key: key})
		case INDEX_DEL:
			done, err = db.kv.Delete(&DeleteReq{Key: key})
		default:
			panic("invalid index op")
		}
		if err != nil {
			//! TODO
		}
		assert(done)
	}
}

func checkIndexKeys(tdef *TableDef, index []string) ([]string, error) {
	icols := map[string]bool{}

	for _, c := range index {
		if !isValidCol(tdef, c) {
			return nil, fmt.Errorf("invalid index column: %s", c)
		}
		icols[c] = true
	}

	for _, c := range tdef.Cols[:tdef.PKeys] {
		if !icols[c] {
			// append the pk cols which are not existing in the index
			index = append(index, c)
		}
	}
	assert(len(index) < len(tdef.Cols))
	return index, nil
}

func isValidCol(tdef *TableDef, col string) bool {
	for _, c := range tdef.Cols {
		if c == col {
			return true
		}
	}
	return false
}

func colIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}
