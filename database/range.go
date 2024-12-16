package database

import (
	"bytes"
	"fmt"
)

const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)

// the iterator for range queries
type Scanner struct {
	// the range, from Key1 to Key2
	Cmp1 int // CMP_??
	Cmp2 int
	Key1 Record
	Key2 Record
	// internal
	tdef   *TableDef
	iter   *BIter // underlying BTree iterator
	keyEnd []byte // the encoded Key2
}

func (db *DB) Scan(table string, req *Scanner) error {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return dbScan(db, tdef, req)
}

func dbScan(db *DB, tdef *TableDef, req *Scanner) error {
	// sanity checks
	switch {
	case req.Cmp1 > 0 && req.Cmp2 < 0:
	case req.Cmp2 > 0 && req.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}

	values1, err := checkRecord(tdef, req.Key1, tdef.PKeys)
	if err != nil {
		return err
	}
	values2, err := checkRecord(tdef, req.Key2, tdef.PKeys)
	if err != nil {
		return err
	}
	req.tdef = tdef

	// seek to the start key
	keyStart := encodeKey(nil, tdef.Prefix, values1[:tdef.PKeys])
	req.keyEnd = encodeKey(nil, tdef.Prefix, values2[:tdef.PKeys])
	req.iter = db.kv.tree.Seek(keyStart, req.Cmp1)
	return nil
}

// within the range or not
func (sc *Scanner) Valid() bool {
	if sc.iter == nil || !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()

	return cmpOK(key, sc.Cmp2, sc.keyEnd)
}

// move the underlying B-tree iterator
func (sc *Scanner) Next() {
	if !sc.Valid() {
		return
	}
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

// fetch the current row
func (sc *Scanner) Deref(rec *Record) {
	if !sc.Valid() {
		return
	}
	tdef := sc.tdef
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return
	}
	encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
}

// B-Tree Iterator
type BIter struct {
	tree *BTree
	path []BNode  // from root to leaf
	pos  []uint16 // indexes into nodes
}

// get current KV pair
func (iter *BIter) Deref() (key []byte, val []byte) {
	currentNode := iter.path[len(iter.path)-1]
	idx := iter.pos[len(iter.pos)-1]
	key = currentNode.getKey(idx)
	val = currentNode.getVal(idx)
	return
}

// precondition of the Deref()
func (iter *BIter) Valid() bool {
	lastNode := iter.path[len(iter.path)-1]
	return len(iter.path) > 0 && lastNode.data != nil && iter.pos[len(iter.pos)-1] < lastNode.nKeys()
}

// moving backward and forward
func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path)-1)
}

func (iter *BIter) Next() {
	iterNext(iter, 0)
}

func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	if cmp != CMP_LE && iter.Valid() {
		cur, _ := iter.Deref()
		if !cmpOK(cur, cmp, key) {
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}
	return iter
}

func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		if node.bNodeType() == BNODE_INODE {
			ptr = node.getPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

// compares current key & ref key & checks if cmp is valid
func cmpOK(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)
	switch cmp {
	case CMP_GE:
		return r >= 0
	case CMP_GT:
		return r > 0
	case CMP_LT:
		return r < 0
	case CMP_LE:
		return r <= 0
	default:
		panic("what?")
	}
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- // move within this node
	} else if level > 0 { // make sure the level is not less than the `root`
		iterPrev(iter, level-1)
	} else {
		return
	}
	if level+1 < len(iter.pos) {
		// update the kid prevNode
		prevNode := iter.path[level]
		kid := iter.tree.get(prevNode.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nKeys() - 1
	}
}

func iterNext(iter *BIter, level int) {
	currentNode := iter.path[level]
	if iter.pos[level] < uint16(currentNode.nKeys())-1 {
		iter.pos[level]++ // move within this node
	} else if level < len(iter.path)-1 {
		iterNext(iter, level+1)
	} else {
		return
	}
	if level+1 < len(iter.pos) {
		// update the kid nextNode
		nextNode := iter.path[level]
		kid := iter.tree.get(nextNode.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}
