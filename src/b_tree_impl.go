package src

import (
	"fmt"
	"unsafe"
)

type DB struct {
	tree  BTree
	pages map[uint64]BNode
	// ref to verify the correctness of B-tree
	ref map[string]string
}

func newDB() *DB {
	pages := map[uint64]BNode{}
	return &DB{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok)
				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() <= BTREE_PAGE_SIZE)
				// unsafe.Pointer - Can hold any memory address & can bypass go's type safety (for low level manipulations)
				ptr := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[ptr].data == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				if ok {
					delete(pages, ptr)
				}
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (db *DB) add(key string, val string) {
	db.tree.Insert([]byte(key), []byte(val))
	db.ref[key] = val
}

func (db *DB) del(key string) bool {
	delete(db.ref, key)
	return db.tree.Delete([]byte(key))
}

func BtreeImpl() {
	db := newDB()
	db.add("Name", "Aniket")
	db.add("Company", "Amazon")
	if val, ok := db.tree.Get([]byte("Name")); ok {
		fmt.Println("Name is: ", string(val))
	}
	if val, ok := db.tree.Get([]byte("Company")); ok {
		fmt.Println("Company is: ", string(val))
	}
	db.add("Name", "Joseph")
	if val, ok := db.tree.Get([]byte("Name")); ok {
		fmt.Println("Updated Name is: ", string(val))
	}
	db.del("Name")
	if val, ok := db.tree.Get([]byte("Name")); ok {
		fmt.Println("Name is: ", string(val))
	} else {
		fmt.Println("Key not found")
	}
}
