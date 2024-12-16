package database

import (
	"encoding/binary"
)

type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode  // de-reference a pointer
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a page
}

// Free List Node Format
// | type | size | total | next |  pointers |
// |  2B  |  2B  |   8B  |  8B  | size * 8B |

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

func flnSize(node BNode) int {
	return int(node.nKeys())
}

func flnNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node.data[4+8:])
}

func flnPtr(node BNode, idx int) uint64 {
	return binary.LittleEndian.Uint64(node.data[FREE_LIST_HEADER+idx*8:])
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	binary.LittleEndian.PutUint64(node.data[FREE_LIST_HEADER+idx*8:], ptr)
}
func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.data[2:], size)
	binary.LittleEndian.PutUint64(node.data[4+8:], next)
}
func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.data[4:], total)
}

func (fl *FreeList) Get(pgnum int) uint64 {
	node := fl.get(fl.head)
	for flnSize(node) < pgnum {
		pgnum -= flnSize(node)
		next := flnNext(node)
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-pgnum-1)
}

// calculates the number of page `pointers` across all nodes
func (fl *FreeList) Total() int {
	if fl == nil || fl.head == 0 {
		return 0 // Handle nil case or empty list
	}

	total := 0
	nodeId := fl.head

	for nodeId > 0 {
		node := fl.get(nodeId)
		total += flnSize(node)
		nodeId = flnNext(node)
	}
	return total
}

// remove the `popn` pointers & add new `freed` pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	if popn == 0 || len(freed) == 0 {
		return // no change to be done
	}

	total := fl.Total()
	reuse := []uint64{} // construct the new list

	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // reuse the node itself

		if popn >= flnSize(node) {
			popn -= flnSize(node)
		} else {
			remain := flnSize(node) - popn
			popn = 0
			// collect resuable pointers
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		total -= flnSize(node)
		fl.head = flnNext(node)
	}
	assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0)
	flPush(fl, freed, reuse)
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}
		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}
		freed = freed[size:]
		if len(reuse) > 0 {
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			fl.head = fl.new(new)
		}
	}
}
