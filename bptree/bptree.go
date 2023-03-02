package bptree

import (
	"fmt"

	"golang.org/x/exp/slices"
)

const Order = 5

type Value int

func newLeafNode() *node {
	return &node{
		isLeaf: true,
		keys:   make([]int, 0, Order-1),
		values: make([]Value, 0, Order-1),
	}
}

func newInternalNode() *node {
	return &node{
		isLeaf:   false,
		keys:     make([]int, 0, Order-1),
		children: make([]*node, 0, Order),
	}
}

type node struct {
	isLeaf   bool
	keys     []int   // cap = Order - 1
	values   []Value // cap = Order - 1
	children []*node // cap = Order
	next     *node   // leaf only
}

func (n *node) print(ident string) {
	if !n.isLeaf {
		fmt.Printf("%s --------\n", ident)
		n.children[0].print(ident + "\t")
		for idx, key := range n.keys {
			fmt.Printf("%s - %d\n", ident, key)
			n.children[idx+1].print(ident + "\t")
		}
	} else {
		for idx, key := range n.keys {
			fmt.Printf("%s * %d => %d\n", ident, key, n.values[idx])
		}
	}
}

func (n *node) isFull() bool {
	return len(n.keys) == cap(n.keys)
}

func (n *node) isUnderflow() bool {
	return len(n.keys) < cap(n.keys)/2
}

func (n *node) canBorrow() bool {
	return len(n.keys) > cap(n.keys)/2
}

func (n *node) halfCnt() int {
	return cap(n.keys) / 2
}

func (n *node) find(key int) *Value {
	offset, found := slices.BinarySearch(n.keys, key)
	if n.isLeaf {
		if found {
			return &n.values[offset]
		} else {
			return nil
		}
	} else {
		if found {
			offset++
		}
		return n.children[offset].find(key)
	}
}

func (n *node) insert(key int, value Value) (splitKey int, right *node) {
	offset, found := slices.BinarySearch(n.keys, key)
	if n.isLeaf {
		if found {
			n.values[offset] = value
			return
		}

		if n.isFull() {
			// split
			halfCnt := n.halfCnt()
			right = newLeafNode()
			right.keys = append(right.keys, n.keys[halfCnt:]...)
			right.values = append(right.values, n.values[halfCnt:]...)
			n.keys = n.keys[:halfCnt]
			n.values = n.values[:halfCnt]
			right.next = n.next
			n.next = right

			if key < right.keys[0] {
				n.insert(key, value)
			} else {
				right.insert(key, value)
			}
			splitKey = right.keys[0]
			return
		}

		// n is not full
		n.keys = slices.Insert(n.keys, offset, key)
		n.values = slices.Insert(n.values, offset, value)

	} else {
		child := n.children[offset]

		childSplitKey, childRight := child.insert(key, value)
		if childRight == nil {
			return
		}

		// fmt.Printf("child isleaf: %v, child left: %d, child right: %d\n", child.isLeaf, len(child.keys), len(childRight.keys))

		if n.isFull() {
			// split
			offset, _ := slices.BinarySearch(n.keys, childSplitKey)
			halfCnt := n.halfCnt()
			right = newInternalNode()

			if offset < halfCnt {
				// 插入在左边
				right.keys = append(right.keys, n.keys[halfCnt-1:]...)
				right.children = append(right.children, n.children[halfCnt-1:]...)
				n.keys = n.keys[:halfCnt-1]
				n.children = n.children[:halfCnt]

				n.keys = slices.Insert(n.keys, offset, childSplitKey)
				n.children = slices.Insert(n.children, offset+1, childRight)
			} else {
				// 插入在右边
				right.keys = append(right.keys, n.keys[halfCnt:]...)
				right.children = append(right.children, n.children[halfCnt:]...)
				n.keys = n.keys[:halfCnt]
				n.children = n.children[:halfCnt+1]

				offset, _ := slices.BinarySearch(right.keys, childSplitKey)
				right.keys = slices.Insert(right.keys, offset, childSplitKey)
				right.children = slices.Insert(right.children, offset+1, childRight)

			}

			splitKey = right.keys[0]
			right.keys = slices.Delete(right.keys, 0, 1)
			right.children = slices.Delete(right.children, 0, 1)
			return
		}

		// n is not full
		offset, _ := slices.BinarySearch(n.keys, childSplitKey)
		n.keys = slices.Insert(n.keys, offset, childSplitKey)
		n.children = slices.Insert(n.children, offset+1, childRight)
	}
	return
}

func (n *node) delete(key int) {
	offset, found := slices.BinarySearch(n.keys, key)

	if n.isLeaf {
		if found {
			n.keys = slices.Delete(n.keys, offset, offset+1)
			n.values = slices.Delete(n.values, offset, offset+1)
		}
	} else {
		if found {
			offset += 1
		}
		child := n.children[offset]
		child.delete(key)

		if child.isUnderflow() {
			if offset > 0 && n.children[offset-1].canBorrow() {
				n.borrowFromLeft(offset)
			} else if offset+1 < len(n.children) && n.children[offset+1].canBorrow() {
				n.borrowFromRight(offset)
			} else if offset > 0 {
				n.mergeLeft(offset)
			} else if offset+1 < len(n.children) {
				n.mergeLeft(offset + 1)
			}
		}
	}
}

func (n *node) borrowFromLeft(i int) {
	left := n.children[i-1]
	cur := n.children[i]

	if cur.isLeaf {
		cur.keys = slices.Insert(cur.keys, 0, left.keys[len(left.keys)-1])
		cur.values = slices.Insert(cur.values, 0, left.values[len(left.values)-1])
		left.keys = left.keys[:len(left.keys)-1]
		left.values = left.values[:len(left.values)-1]
		n.keys[i-1] = cur.keys[0]
	} else {
		cur.keys = slices.Insert(cur.keys, 0, n.keys[i-1])
		cur.children = slices.Insert(cur.children, 0, left.children[len(left.children)-1])
		n.keys[i-1] = left.keys[len(left.keys)-1]
		left.keys = left.keys[:len(left.keys)-1]
		left.children = left.children[:len(left.children)-1]
	}
}

func (n *node) borrowFromRight(i int) {
	cur := n.children[i]
	right := n.children[i+1]

	if cur.isLeaf {
		cur.keys = append(cur.keys, right.keys[0])
		cur.values = append(cur.values, right.values[0])
		right.keys = slices.Delete(right.keys, 0, 1)
		right.values = slices.Delete(right.values, 0, 1)
		n.keys[i] = right.keys[0]
	} else {
		cur.keys = append(cur.keys, n.keys[i])
		cur.children = append(cur.children, right.children[0])
		n.keys[i] = right.keys[0]
		right.keys = slices.Delete(right.keys, 0, 1)
		right.children = slices.Delete(right.children, 0, 1)
	}
}

func (n *node) mergeLeft(i int) {
	splitKey := n.keys[i-1]
	left := n.children[i-1]
	cur := n.children[i]

	if cur.isLeaf {
		cur.keys = slices.Insert(cur.keys, 0, left.keys...)
		cur.values = slices.Insert(cur.values, 0, left.values...)
	} else {
		cur.keys = slices.Insert(cur.keys, 0, splitKey)
		cur.keys = slices.Insert(cur.keys, 0, left.keys...)
		cur.children = slices.Insert(cur.children, 0, left.children...)
	}
	n.keys = slices.Delete(n.keys, i-1, i)
	n.children = slices.Delete(n.children, i-1, i)
}

func (n *node) first() *Entry {
	if n.isLeaf {
		return NewEntry(
			n.keys[0],
			&n.values[0],
		)
	} else {
		return n.children[0].first()
	}
}

func (n *node) last() *Entry {
	if n.isLeaf {
		return NewEntry(
			n.keys[len(n.keys)-1],
			&n.values[len(n.values)-1],
		)
	} else {
		return n.children[len(n.children)-1].last()
	}
}

func NewEntry(key int, val *Value) *Entry {
	return &Entry{
		Key: key,
		Val: val,
	}
}

type Entry struct {
	Key int
	Val *Value
}

type Iterator struct {
	n      *node
	offset int
}

func (i *Iterator) Next() *Entry {
	if i.n == nil {
		return nil
	}
	ret := NewEntry(
		i.n.keys[i.offset],
		&i.n.values[i.offset],
	)
	i.offset++
	if i.offset == len(i.n.keys) {
		i.n = i.n.next
		i.offset = 0
	}
	return ret
}

func NewBpTree() *BpTree {
	return &BpTree{}
}

type BpTree struct {
	root *node
}

func (t *BpTree) Print() {
	if t.root != nil {
		t.root.print("")
		fmt.Println("=========================")
	}
}

func (t *BpTree) Insert(key int, value Value) {
	if t.root == nil {
		t.root = newLeafNode()
	}
	splitKey, right := t.root.insert(key, value)
	if right != nil {
		root := newInternalNode()
		root.keys = append(root.keys, splitKey)
		root.children = append(root.children, t.root, right)
		t.root = root
	}
}

func (t *BpTree) Delete(key int) {
	if t.root != nil {
		t.root.delete(key)
		if !t.root.isLeaf && len(t.root.children) == 1 {
			t.root = t.root.children[0]
		}
	}
}

func (t *BpTree) Find(key int) *Value {
	if t.root != nil {
		return t.root.find(key)
	}
	return nil
}

func (t *BpTree) Clear() {
	t.root = nil
}

func (t *BpTree) Append(other *BpTree) {
	iter := other.Iterator()
	entry := iter.Next()
	for entry != nil {
		t.Insert(entry.Key, *entry.Val)
		entry = iter.Next()
	}
	other.Clear()
}

func (t *BpTree) Contains(key int) bool {
	return t.Find(key) != nil
}

func (t *BpTree) First() *Entry {
	if t.root == nil {
		return nil
	}
	return t.root.first()
}

func (t *BpTree) Last() *Entry {
	if t.root == nil {
		return nil
	}
	return t.root.last()
}

func (t *BpTree) firstLeaf() *node {
	if t.root == nil {
		return nil
	}
	n := t.root
	for !n.isLeaf {
		n = n.children[0]
	}
	return n
}

func (t *BpTree) Iterator() Iterator {
	leaf := t.firstLeaf()
	return Iterator{
		n:      leaf,
		offset: 0,
	}
}

func (t *BpTree) Len() int {
	leaf := t.firstLeaf()
	ret := 0

	for leaf != nil {
		ret += len(leaf.keys)
		leaf = leaf.next
	}
	return ret
}
