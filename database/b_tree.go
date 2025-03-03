package database

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type BNode struct {
	data []byte // Dumpable to disk
}

/// BNode Structure
// Pointers - A list of pointers to the child nodes. (Used by internal nodes).
// Offsets -  A list of offsets pointing to each key-value pair.
// +--------+-------+-----------+---------+--------------------+
// | type   | nkeys | pointers  | offsets  | key-values |
// | 2B     | 2B    | nkeys*8B  | nkeys*2B | ...        |
// |<-HEADER (4B) ->|

// Format of KV pair
// | klen | vlen | key | val |
// | 2B   | 2B   | ... | ... |

type BTree struct {
	// a pointer (a non-zero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference the page number (pointer)
	new func(BNode) uint64 // create a new page
	del func(uint64)       // de-allocate the page
}

func (tree *BTree) Insert(key, val []byte) error {
	if len(key) == 0 || len(key) > BTREE_MAX_KEY_SIZE {
		return errors.New("key size not valid")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		return errors.New("val size exceeds the max size")
	}

	if tree.root == 0 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return nil
	}
	node := tree.get(tree.root)
	tree.del(tree.root)
	// Inserts the KV pair & returns the node
	node = treeInsert(tree, node, key, val)
	// If the updated node is big we split it
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_INODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}
	return nil
}

func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) != 0)
	assert(len(key) <= BTREE_MAX_KEY_SIZE)
	if tree.root == 0 {
		return false
	}
	// Gives the new updated node after deleting the key
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false
	}
	tree.del(tree.root)
	if updated.bNodeType() == BNODE_INODE && updated.nKeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 || len(key) > BTREE_MAX_KEY_SIZE {
		return nil, false, errors.New("key size is not valid")
	}

	if tree.root == 0 {
		return nil, false, nil
	}
	node := tree.get(tree.root)
	for {
		switch node.bNodeType() {
		case BNODE_LEAF:
			idx := nodeLookupLE(node, key)
			if bytes.Equal(node.getKey(idx), key) {
				return node.getVal(idx), true, nil
			}
			return nil, false, nil
		case BNODE_INODE:
			idx := nodeLookupLE(node, key)
			node = tree.get(node.getPtr(idx))
		default:
			panic("bad node type")
		}
	}
}

const HEADER = 4

const (
	BTREE_PAGE_SIZE = 4096
	// Adding constraint to KV so a single pair can fit on a single page
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	// 8 - Pointers | 2 - Offsets | 4 - klen(2) & vlen(2)
	nodeMax := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assertWithSrc(nodeMax <= BTREE_PAGE_SIZE, "Node Max is greater than tree size")
}

const (
	BNODE_INODE = 1 // internal nodes without values
	BNODE_LEAF  = 2 // leaf node with values
)

func (node BNode) bNodeType() uint16 {
	// Get the type of the node(1 or 2) from first 2 bytes
	return binary.LittleEndian.Uint16(node.data)
}

// Get the number of keys
func (node BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	// For example, if:
	// btype = 1 (BNODE_INODE)
	// nkeys = 3

	// First 2 bytes for type (0-1)
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	// Second 2 bytes for nkeys (2-3)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
	// |   01   |   00   |   03   |   00   |
}

func (node BNode) getPtr(idx uint16) uint64 {
	// <---HEADER---><-ptr 1-> <-ptr 2-> <-ptr 3->
	// [01 00 00 00 | 64 bits | 64 bits | 64 bits]
	assertWithSrc(idx < node.nKeys(), "Failed in getPtr")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(ptr uint64, idx uint16) {
	assertWithSrc(idx < node.nKeys(), "Failed in setPtr")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], ptr)
}

func offsetPos(node BNode, idx uint16) uint16 {
	// Reason for (idx - 1) ->  The offset for the first key-value pair is always 0, so it's not stored.
	return HEADER + 8*node.nKeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := offsetPos(node, idx)
	return binary.LittleEndian.Uint16(node.data[pos:])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	pos := offsetPos(node, idx)
	binary.LittleEndian.PutUint16(node.data[pos:], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	assertWithSrc(idx <= node.nKeys(), "Failed in kvPos")
	return HEADER + 8*node.nKeys() + 2*node.nKeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assertWithSrc(idx < node.nKeys(), "Failed in getKey")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assertWithSrc(idx < node.nKeys(), "Failed in getVal")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	// Skip the klen & the vlen by adding 4, then skip the key by adding the klen
	return node.data[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nKeys())
}

/// B-Tree Insertion

// Returns the index of the key where it should be located
func nodeLookupLE(node BNode, key []byte) uint16 {
	keysLen := node.nKeys()
	found := uint16(0)
	/*	The first key is a copy from the parent node
		Eg.
			 	 [30, 50]
			    /   |    \
		[10, 20] [30*, 40] [50*, 60, 70]
		30* & 50* are the copies from the parent node
	*/
	for i := uint16(1); i < keysLen; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp > 0 {
			break
		}
	}
	return found
}

// node - Its the node where the insertion is taking place
func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	// Creating node with double size for copying all vals/ptrs from existing node & inserting the new key/val
	newNode := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	idx := nodeLookupLE(node, key)
	switch node.bNodeType() {
	case BNODE_LEAF:
		// If already exists update the key
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newNode, node, idx, key, val)
		} else {
			leafInsert(newNode, node, idx+1, key, val)
		}
	case BNODE_INODE:
		nodeInsert(tree, newNode, node, idx, key, val)
	default:
		panic("bad node!!")
	}
	return newNode
}

func nodeInsert(tree *BTree, new, node BNode, idx uint16, key, val []byte) {
	kptr := node.getPtr(idx)
	// Leaf node by the kptr(child ptr)
	knode := tree.get(kptr)
	tree.del(kptr)
	knode = treeInsert(tree, knode, key, val)
	nsplit, splitted := nodeSplit3(knode)
	nodeReplaceKidN(tree, new, node, idx, splitted[:nsplit]...)
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		return 2, [3]BNode{left, right}
	}
	leftLeft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftLeft, middle, left)
	assertWithSrc(leftLeft.nbytes() <= BTREE_PAGE_SIZE, "Failed in nodeSplit3")
	return 3, [3]BNode{left, middle, right}
}

func nodeSplit2(left, right, old BNode) {
	midIndex := old.nKeys() / 2
	nodeAppendRange(left, old, 0, 0, midIndex)
	nodeAppendRange(right, old, 0, midIndex, old.nKeys()-1)
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_INODE, old.nKeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nKeys()-(idx+1))
}

func leafInsert(new BNode, old BNode, idx uint16, key, val []byte) {
	new.setHeader(BNODE_LEAF, old.nKeys()+1)
	// Copy all the values occurring before the insertion index
	nodeAppendRange(new, old, 0, 0, idx)
	// Insert the KV (ptr 0 bcoz leaf node)
	nodeAppendKV(new, idx, 0, key, val)
	// Copy the remaining values from the same index where we left
	nodeAppendRange(new, old, idx+1, idx, old.nKeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nKeys())
	// Copy all the values occurring before the insertion index
	nodeAppendRange(new, old, 0, 0, idx)
	// Update the KV (ptr 0 bcoz leaf node)
	nodeAppendKV(new, idx, 0, key, val)
	// Copy the remaining values from the next index where we left
	nodeAppendRange(new, old, idx+1, idx+1, old.nKeys()-idx-1)
}

func nodeAppendRange(new BNode, old BNode, dst, src, num uint16) {
	assertWithSrc(src+num <= old.nKeys(), "Failed in nodeAppendRange src+num <= old.nkeys()")
	assertWithSrc(dst+num <= new.nKeys(), "Failed in nodeAppendRange dst+num <= old.nkeys()")
	if num == 0 {
		return
	}
	// pointers
	for i := uint16(0); i < num; i++ {
		new.setPtr(old.getPtr(src+i), dst+i)
	}

	// offset
	dstBegin := new.getOffset(dst)
	srcBegin := old.getOffset(src)
	for i := uint16(1); i <= num; i++ {
		offset := dstBegin + old.getOffset(src+i) - srcBegin
		new.setOffset(dst+i, offset)
	}

	begin := old.kvPos(src)
	end := old.kvPos(src + num)

	copy(new.data[new.kvPos(dst):], old.data[begin:end])
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key, val []byte) {
	new.setPtr(ptr, idx)
	pos := new.kvPos(idx)

	keyLen := uint16(len(key))
	binary.LittleEndian.PutUint16(new.data[pos+0:], keyLen)
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))

	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+keyLen:], val)
	// Set the offset for the next KV pair. Offset - Get the previous offset & add the current KV len
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// B-Tree Deletion

func leafDelete(new, old BNode, idx uint16) {
	new.setHeader(old.bNodeType(), old.nKeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nKeys()-(idx+1))
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// find the key idx
	idx := nodeLookupLE(node, key)

	switch node.bNodeType() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new
	case BNODE_INODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node!!")
	}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{}
	}
	tree.del(kptr)

	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

func nodeMerge(new, left, right BNode) {
	new.setHeader(left.bNodeType(), left.nKeys()+right.nKeys())
	nodeAppendRange(new, left, 0, 0, left.nKeys())
	nodeAppendRange(new, right, left.nKeys(), 0, right.nKeys())
}

func nodeReplace2Kid(new, node BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(node.bNodeType(), node.nKeys()-1)
	nodeAppendRange(new, node, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, node, idx+1, idx+2, node.nKeys()-(idx+2))
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nKeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}

	}
	return 0, BNode{}
}

func assert(condition bool) {
	if !condition {
		panic("assertion failed")
	}
}

func assertWithSrc(condition bool, src string) {
	if !condition {
		panic(src)
	}
}
