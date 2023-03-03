package bptree_disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/exp/slices"
)

const PageSize = 4096
const InternalOrder = (PageSize + 5) / 16

type Key int64
type Value []byte
type PageNum int64

var pageCache = sync.Pool{
	New: func() any {
		return make([]byte, PageSize)
	},
}

type meta struct {
	root         PageNum
	valueMaxLen  uint16
	freePageNums []PageNum
}

func (m *meta) write(writer io.Writer) error {
	page := pageCache.Get().([]byte)
	defer pageCache.Put(page)

	offset := 0

	binary.BigEndian.PutUint64(page[offset:], uint64(m.root))
	offset += 8

	binary.BigEndian.PutUint16(page[offset:], m.valueMaxLen)
	offset += 2

	binary.BigEndian.PutUint16(page[offset:], uint16(len(m.freePageNums)))
	offset += 2

	for _, n := range m.freePageNums {
		binary.BigEndian.PutUint64(page[offset:], uint64(n))
		offset += 8
	}
	_, err := writer.Write(page[:PageSize])
	return err
}

func (m *meta) leafOrder() int {
	return int((PageSize-11)/(m.valueMaxLen+10)) + 1
}

func (m *meta) getFreePageNum() PageNum {
	if len(m.freePageNums) > 0 {
		ret := m.freePageNums[0]
		m.freePageNums = m.freePageNums[1:]
		return ret
	}
	return 0
}

func (m *meta) addFreePageNum(pageNum PageNum) {
	m.freePageNums = append(m.freePageNums, pageNum)
}

func loadMeta(reader io.Reader) (*meta, error) {
	page := pageCache.Get().([]byte)
	defer pageCache.Put(page)
	size, err := reader.Read(page)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	if size != PageSize {
		return nil, fmt.Errorf("partial page, got %d bytes", size)
	}

	offset := 0

	root := binary.BigEndian.Uint64(page[offset:])
	offset += 8

	valueMaxLen := binary.BigEndian.Uint16(page[offset:])
	offset += 2

	freePageLen := binary.BigEndian.Uint16(page[offset:])
	offset += 2

	freePageNums := make([]PageNum, freePageLen)
	for i := range freePageNums {
		freePageNums[i] = PageNum(binary.BigEndian.Uint64(page[offset:]))
		offset += 8
	}

	return &meta{
		root:         PageNum(root),
		valueMaxLen:  valueMaxLen,
		freePageNums: freePageNums,
	}, nil
}

func NewPager(filename string, valueMaxLen uint16) error {
	f, err := os.Create(filename)
	defer f.Close()
	if err != nil {
		return err
	}
	m := meta{
		valueMaxLen:  valueMaxLen,
		freePageNums: nil,
	}
	return m.write(f)
}

func LoadPager(filename string) (*Pager, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	fileLen, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}
	if fileLen%PageSize != 0 {
		return nil, fmt.Errorf("invalid format")
	}
	if fileLen == 0 {
		return nil, fmt.Errorf("empty file")
	}
	f.Seek(0, io.SeekStart)
	meta, err := loadMeta(f)
	if err != nil {
		return nil, fmt.Errorf("laodMeta: %w", err)
	}
	return &Pager{
		f:       f,
		meta:    meta,
		numPage: fileLen/PageSize - 1,
		pageMap: make(map[PageNum]*node),
	}, nil
}

type Pager struct {
	f       *os.File
	meta    *meta
	numPage int64
	pageMap map[PageNum]*node
}

func (p *Pager) getPage(pageNum PageNum) (*node, error) {
	if pageNum > PageNum(p.numPage) || pageNum <= 0 {
		panic(fmt.Sprintf("current numPage: %d, input pageNum: %d", p.numPage, pageNum))
	}
	if n, ok := p.pageMap[pageNum]; !ok {
		page := pageCache.Get().([]byte)
		defer pageCache.Put(page)
		size, err := p.f.ReadAt(page, int64(pageNum)*int64(PageSize))
		if err != nil {
			return nil, err
		}
		if size != PageSize {
			return nil, fmt.Errorf("partial page, got %d bytes", size)
		}
		node, err := newNodeFromPage(p, page, pageNum)
		if err != nil {
			return nil, err
		}
		p.pageMap[pageNum] = node
		return node, nil
	} else {
		return n, nil
	}
}

func (p *Pager) newInternalNode() *node {
	pageNum := p.getFreePageNum()

	ret := &node{
		pager:    p,
		isLeaf:   false,
		keys:     make([]Key, 0, InternalOrder-1),
		children: make([]PageNum, 0, InternalOrder),
		pageNum:  pageNum,
	}
	p.pageMap[pageNum] = ret
	return ret
}

func (p *Pager) newLeafNode() *node {
	pageNum := p.getFreePageNum()

	leafOrder := p.meta.leafOrder() - 1

	ret := &node{
		pager:   p,
		isLeaf:  true,
		keys:    make([]Key, 0, leafOrder),
		values:  make([]Value, 0, leafOrder),
		next:    0,
		pageNum: pageNum,
	}
	p.pageMap[pageNum] = ret
	return ret
}

func (p *Pager) getFreePageNum() PageNum {
	pageNum := p.meta.getFreePageNum()
	if pageNum == 0 {
		p.numPage++
		pageNum = PageNum(p.numPage)
	}
	return pageNum
}

func (p *Pager) freePage(pageNum PageNum) error {
	delete(p.pageMap, pageNum)
	p.meta.addFreePageNum(pageNum)
	return p.meta.write(p.f)
}

func (p *Pager) flush(pageNum PageNum) error {
	if n, ok := p.pageMap[pageNum]; ok {
		page := pageCache.Get().([]byte)
		defer pageCache.Put(page)
		n.toBytes(page, p.meta.leafOrder())
		_, err := p.f.WriteAt(page, int64(pageNum)*PageSize)
		return err
	}
	return nil
}

func (p *Pager) FlushAll() error {
	_, err := p.f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = p.meta.write(p.f)
	if err != nil {
		return err
	}
	for pageNum := range p.pageMap {
		if err := p.flush(pageNum); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pager) NumPage() int64 {
	return p.numPage
}

func (p *Pager) FreePageList() []PageNum {
	return p.meta.freePageNums
}

type node struct {
	pager    *Pager
	isLeaf   bool
	keys     []Key     // cap = Order - 1
	values   []Value   // cap = Order - 1
	children []PageNum // cap = Order
	next     PageNum   // leaf only
	pageNum  PageNum
}

// internal node
// byte[0] = 0       offset:0
// key的数量：2个byte  offset:1
// (Order-1) 个 key => 8*(Order-1)个byte offset:3
// Order 个 child => 8*Order个byte offset: 2043 ~
// 3 + 8(o-1) + 8o = 4096 => 16o - 5 = 4096 => Order = 256  固定的

// leaf node
// byte[0] = 1
// nextPtr：8个byte
// key的数量：2个byte
// (Order-1) 个 key => 8*(Order-1)个byte
// value的最大长度为L个Byte，用于记录value长度的2个byte，每个Value使用L+2个byte
// 1 + 8 + 2 + 8(o-1) + (L+2)(o-1) = 4096 => (L+10)(o-1) = 4085 => o = 4085/(L+10) + 1

func newNodeFromPage(pager *Pager, page []byte, pageNum PageNum) (*node, error) {
	leafOrder := pager.meta.leafOrder()
	if page[0] == 0 {
		ret := &node{
			pager:    pager,
			isLeaf:   false,
			keys:     make([]Key, 0, InternalOrder-1),
			children: make([]PageNum, 0, InternalOrder),
			pageNum:  pageNum,
		}

		keyLen := binary.BigEndian.Uint16(page[1:])
		if keyLen > InternalOrder-1 {
			return nil, fmt.Errorf("invalid format")
		}
		offset := 3
		for i := 0; i < int(keyLen); i++ {
			key := binary.BigEndian.Uint64(page[offset:])
			offset += 8
			ret.keys = append(ret.keys, Key(key))
		}
		offset = 3 + InternalOrder*8
		for i := 0; i < int(keyLen+1); i++ {
			pageNum := binary.BigEndian.Uint64(page[offset:])
			offset += 8
			ret.children = append(ret.children, PageNum(pageNum))
		}
		return ret, nil
	} else {
		ret := &node{
			pager:   pager,
			isLeaf:  true,
			keys:    make([]Key, 0, leafOrder-1),
			values:  make([]Value, 0, leafOrder-1),
			next:    0,
			pageNum: pageNum,
		}

		ret.next = PageNum(binary.BigEndian.Uint64(page[1:]))
		keyLen := binary.BigEndian.Uint16(page[9:])
		if keyLen > uint16(leafOrder)-1 {
			return nil, fmt.Errorf("invalid format")
		}
		offset := 11
		for i := 0; i < int(keyLen); i++ {
			key := binary.BigEndian.Uint64(page[offset:])
			offset += 8
			ret.keys = append(ret.keys, Key(key))
		}
		offset = 11 + 8*(leafOrder-1)
		for i := 0; i < int(keyLen); i++ {
			valLen := binary.BigEndian.Uint16(page[offset:])
			offset += 2
			val := make([]byte, valLen)
			copy(val, page[offset:])
			offset += int(valLen)
			ret.values = append(ret.values, Value(val))
		}
		return ret, nil
	}
}

func (n *node) toBytes(b []byte, leafOrder int) {
	if !n.isLeaf {
		b[0] = 0
		binary.BigEndian.PutUint16(b[1:], uint16(len(n.keys)))
		offset := 3
		for _, key := range n.keys {
			binary.BigEndian.PutUint64(b[offset:], uint64(key))
			offset += 8
		}
		offset = 3 + InternalOrder*8
		for _, child := range n.children {
			binary.BigEndian.PutUint64(b[offset:], uint64(child))
			offset += 8
		}
	} else {
		b[0] = 1
		binary.BigEndian.PutUint64(b[1:], uint64(n.next))
		binary.BigEndian.PutUint16(b[9:], uint16(len(n.keys)))
		offset := 11
		for _, key := range n.keys {
			binary.BigEndian.PutUint64(b[offset:], uint64(key))
			offset += 8
		}
		offset = 11 + 8*(leafOrder-1)
		for _, val := range n.values {
			binary.BigEndian.PutUint16(b[offset:], uint16(len(val)))
			offset += 2
			copy(b[offset:], val)
			offset += len(val)
		}

	}
}

func (n *node) getChild(offset int) *node {
	ret, err := n.pager.getPage(n.children[offset])
	if err != nil {
		panic("getPage err")
	}
	return ret
}

func (n *node) print(ident string) {
	if !n.isLeaf {
		fmt.Printf("%s --------\n", ident)
		n.getChild(0).print(ident + "\t")
		for idx, key := range n.keys {
			fmt.Printf("%s - %d\n", ident, key)
			n.getChild(idx + 1).print(ident + "\t")
		}
	} else {
		for _, key := range n.keys {
			fmt.Printf("%s * %d => [value]\n", ident, key)
		}
	}
}

func (n *node) isFull() bool {
	fmt.Printf("len: %d, cap: %d\n", len(n.keys), cap(n.keys))
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

func (n *node) find(key Key) *Value {
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
		return n.getChild(offset).find(key)
	}
}

func (n *node) insert(key Key, value Value) (splitKey Key, right *node) {
	offset, found := slices.BinarySearch(n.keys, key)
	if n.isLeaf {
		if found {
			n.values[offset] = value
			return
		}

		if n.isFull() {
			// split
			halfCnt := n.halfCnt()
			right = n.pager.newLeafNode()

			right.keys = append(right.keys, n.keys[halfCnt:]...)
			right.values = append(right.values, n.values[halfCnt:]...)
			n.keys = n.keys[:halfCnt]
			n.values = n.values[:halfCnt]
			right.next = n.next
			n.next = right.pageNum

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
		child := n.getChild(offset)

		childSplitKey, childRight := child.insert(key, value)
		if childRight == nil {
			return
		}

		// fmt.Printf("child isleaf: %v, child left: %d, child right: %d\n", child.isLeaf, len(child.keys), len(childRight.keys))

		if n.isFull() {
			// split
			offset, _ := slices.BinarySearch(n.keys, childSplitKey)
			halfCnt := n.halfCnt()
			right = n.pager.newInternalNode()

			if offset < halfCnt {
				// 插入在左边
				right.keys = append(right.keys, n.keys[halfCnt-1:]...)
				right.children = append(right.children, n.children[halfCnt-1:]...)
				n.keys = n.keys[:halfCnt-1]
				n.children = n.children[:halfCnt]

				n.keys = slices.Insert(n.keys, offset, childSplitKey)
				n.children = slices.Insert(n.children, offset+1, childRight.pageNum)
			} else {
				// 插入在右边
				right.keys = append(right.keys, n.keys[halfCnt:]...)
				right.children = append(right.children, n.children[halfCnt:]...)
				n.keys = n.keys[:halfCnt]
				n.children = n.children[:halfCnt+1]

				offset, _ := slices.BinarySearch(right.keys, childSplitKey)
				right.keys = slices.Insert(right.keys, offset, childSplitKey)
				right.children = slices.Insert(right.children, offset+1, childRight.pageNum)
			}

			splitKey = right.keys[0]
			right.keys = slices.Delete(right.keys, 0, 1)
			right.children = slices.Delete(right.children, 0, 1)
			return
		}

		// n is not full
		offset, _ := slices.BinarySearch(n.keys, childSplitKey)
		n.keys = slices.Insert(n.keys, offset, childSplitKey)
		n.children = slices.Insert(n.children, offset+1, childRight.pageNum)
	}
	return
}

func (n *node) delete(key Key) {
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
		child := n.getChild(offset)
		child.delete(key)

		if child.isUnderflow() {
			if offset > 0 && n.getChild(offset-1).canBorrow() {
				n.borrowFromLeft(offset)
			} else if offset+1 < len(n.children) && n.getChild(offset+1).canBorrow() {
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
	left := n.getChild(i - 1)
	cur := n.getChild(i)

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
	cur := n.getChild(i)
	right := n.getChild(i + 1)

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
	left := n.getChild(i - 1)
	cur := n.getChild(i)

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
	n.pager.freePage(left.pageNum)
}

func (n *node) first() *Entry {
	if n.isLeaf {
		return NewEntry(
			n.keys[0],
			&n.values[0],
		)
	} else {
		return n.getChild(0).first()
	}
}

func (n *node) last() *Entry {
	if n.isLeaf {
		return NewEntry(
			n.keys[len(n.keys)-1],
			&n.values[len(n.values)-1],
		)
	} else {
		return n.getChild(len(n.children) - 1).last()
	}
}

func NewEntry(key Key, val *Value) *Entry {
	return &Entry{
		Key: key,
		Val: val,
	}
}

type Entry struct {
	Key Key
	Val *Value
}

type Iterator struct {
	n      *node
	offset int
	pager  *Pager
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
		if i.n.next == 0 {
			i.n = nil
		} else {
			var err error
			i.n, err = i.pager.getPage(i.n.next)
			if err != nil {
				panic("getPage error")
			}
			i.offset = 0
		}
	}
	return ret
}

func NewBpTree(pager *Pager) *BpTree {
	ret := &BpTree{
		pager: pager,
	}
	if pager.meta.root != 0 {
		var err error
		ret.root, err = pager.getPage(pager.meta.root)
		if err != nil {
			panic("get root page failed")
		}
	}
	return ret
}

type BpTree struct {
	root  *node
	pager *Pager
}

func (t *BpTree) Print() {
	if t.root != nil {
		t.root.print("")
		fmt.Println("=========================")
	}
}

func (t *BpTree) Insert(key Key, value Value) {
	if t.root == nil {
		t.root = t.pager.newLeafNode()
		t.pager.meta.root = t.root.pageNum
	}
	splitKey, right := t.root.insert(key, value)
	if right != nil {
		root := t.pager.newInternalNode()
		root.keys = append(root.keys, splitKey)
		root.children = append(root.children, t.root.pageNum, right.pageNum)
		t.root = root
		t.pager.meta.root = root.pageNum
	}
}

func (t *BpTree) Delete(key Key) {
	if t.root != nil {
		t.root.delete(key)
		if !t.root.isLeaf && len(t.root.children) == 1 {
			t.root = t.root.getChild(0)
		}
	}
}

func (t *BpTree) Find(key Key) *Value {
	if t.root != nil {
		return t.root.find(key)
	}
	return nil
}

// func (t *BpTree) Clear() {
// 	t.root = nil
// }

// func (t *BpTree) Append(other *BpTree) {
// 	iter := other.Iterator()
// 	entry := iter.Next()
// 	for entry != nil {
// 		t.Insert(entry.Key, *entry.Val)
// 		entry = iter.Next()
// 	}
// 	other.Clear()
// }

func (t *BpTree) Contains(key Key) bool {
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
		n = n.getChild(0)
	}
	return n
}

func (t *BpTree) Iterator() Iterator {
	leaf := t.firstLeaf()
	return Iterator{
		n:      leaf,
		offset: 0,
		pager:  t.pager,
	}
}

func (t *BpTree) Len() int {
	leaf := t.firstLeaf()
	ret := 0

	for leaf != nil {
		ret += len(leaf.keys)

		if leaf.next == 0 {
			leaf = nil
		} else {
			var err error
			leaf, err = t.pager.getPage(leaf.next)
			if err != nil {
				panic(err)
			}
		}
	}
	return ret
}

func (t *BpTree) LeafOrder() int {
	return t.pager.meta.leafOrder()
}
