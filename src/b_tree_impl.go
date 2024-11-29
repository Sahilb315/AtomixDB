package src

import (
	"bufio"
	"fmt"
	"os"
	"strings"
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

func (db *DB) get(key string) ([]byte, bool) {
	return db.tree.Get([]byte(key))
}

func BtreeImpl() {
	scanner := bufio.NewReader(os.Stdin)
	db := newDB()

	fmt.Println("Welcome to AtomixDB")
	fmt.Println("Available Commands:")
	fmt.Println("  add   - Add a key-value pair")
	fmt.Println("  del   - Delete a key")
	fmt.Println("  get   - Retrieve the value for a key")
	fmt.Println("  exit  - Exit the program")
	fmt.Println()

	for {
		fmt.Print("> ") // Display prompt
		line, _, err := scanner.ReadLine()
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		command := strings.TrimSpace(string(line))
		switch command {
		case "add":
			fmt.Print("Enter key: ")
			key, _ := scanner.ReadString('\n')
			key = strings.TrimSpace(key)

			fmt.Print("Enter value: ")
			val, _ := scanner.ReadString('\n')
			val = strings.TrimSpace(val)

			db.add(key, val)

		case "del":
			fmt.Print("Enter key: ")
			key, _ := scanner.ReadString('\n')
			key = strings.TrimSpace(key)

			db.del(key)

		case "get":
			fmt.Print("Enter key: ")
			key, _ := scanner.ReadString('\n')
			key = strings.TrimSpace(key)

			val, found := db.get(key)
			if found {
				fmt.Println("Value:", string(val))
			} else {
				fmt.Println("Key not found")
			}

		case "exit":
			fmt.Println("Exiting...")
			os.Exit(0)

		default:
			fmt.Println("Unknown command:", command)
		}
	}
}
