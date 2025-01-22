package database

import (
	"errors"
	"fmt"
)

const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

func indexOp(_ *DB, tdef *TableDef, rec Record, op int, kvtx *KVTX) {
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
			done, err = kvtx.SetWithMode(&InsertReq{Key: key})
		case INDEX_DEL:
			done, err = kvtx.Delete(&DeleteReq{Key: key})
		default:
			panic("invalid index op")
		}
		if err != nil {
			//! TODO
		}
		assert(done)
	}
}

func encodeKeyPartial(
	out []byte,
	prefix uint32,
	values []Value,
	tdef *TableDef,
	keys []string,
	cmp int,
) []byte {
	out = encodeKey(out, prefix, values)
	max := cmp == CMP_GT || cmp == CMP_LE

loop:
	for i := len(values); max && i < len(keys); i++ {
		switch tdef.Types[ColIndex(tdef, keys[i])] {
		case TYPE_BYTES:
			out = append(out, 0xff)
			//	Any byte string with a prefix of [X, 0xFF] will be greater than all byte strings with prefix [X]
			break loop
		case TYPE_INT64:
			out = append(out, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		default:
			panic("type mismatch encodeKeyPartial")
		}
	}
	return out
}

func findIndex(tdef *TableDef, keys []string) (int, error) {
	pk := tdef.Cols[:tdef.PKeys]

	if isPrefix(pk, keys) {
		// use primary key
		return -1, nil
	}

	// find suitable index
	winner := -2
	for i, index := range tdef.Indexes {
		if !isPrefix(index, keys) {
			continue
		}
		if winner == -2 || len(index) < len(tdef.Indexes[winner]) {
			winner = i
		}
	}
	if winner == -2 {
		return -2, fmt.Errorf("no index found")
	}
	return winner, nil
}

func isPrefix(long []string, short []string) bool {
	if len(long) < len(short) {
		return false
	}
	for i, c := range short {
		if long[i] != c {
			return false
		}
	}
	return true
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
	if len(index) >= len(tdef.Cols) {
		return nil, errors.New("index len should be shorter than columns")
	}
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

func ColIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}
