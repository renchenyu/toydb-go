package bptree_test

import (
	"testing"
	"toydb-go/bptree"

	"github.com/stretchr/testify/assert"
)

func TestBpTreeAppend(t *testing.T) {
	tree1 := bptree.NewBpTree()
	for i := 0; i < 10; i++ {
		tree1.Insert(i, bptree.Value(i))
	}
	tree2 := bptree.NewBpTree()
	for i := 100; i < 110; i++ {
		tree2.Insert(i, bptree.Value(i))
	}
	assert.Equal(t, 10, tree1.Len())
	assert.Equal(t, 10, tree2.Len())

	tree1.Append(tree2)

	assert.Equal(t, 20, tree1.Len())
	assert.Equal(t, 0, tree2.Len())

	tree1.Print()
	tree2.Print()
}

func TestBpTree(t *testing.T) {
	tree := bptree.NewBpTree()
	assert.Nil(t, tree.Find(1))
	assert.Nil(t, tree.First())
	assert.Nil(t, tree.Last())

	keys := []int{1, 3, 5, 7, 9, 11, 4, 13, 15, 19, 27, 54, 23, 120, 43}

	for _, key := range keys {
		tree.Insert(key, bptree.Value(key))
	}

	assert.True(t, tree.Contains(1))
	assert.False(t, tree.Contains(999))

	assert.Equal(t, 1, tree.First().Key)
	assert.Equal(t, 120, tree.Last().Key)

	// tree.Print()

	// borrow from left
	tree.Delete(5)
	// tree.Print()

	// borrow from right
	tree.Delete(23)
	// tree.Print()

	// merge with right
	tree.Delete(1)
	// tree.Print()

	// merge with left
	tree.Delete(13)
	// tree.Print()

}

func TestBpTree2(t *testing.T) {
	tree := bptree.NewBpTree()
	for i := 0; i < 50; i++ {
		tree.Insert(i, bptree.Value(i))
	}
	for i := 100; i > 49; i-- {
		tree.Insert(i, bptree.Value(i))
	}
	tree.Insert(1, 1)
	assert.Equal(t, bptree.Value(99), *tree.Find(99))
	assert.Equal(t, bptree.Value(100), *tree.Find(100))
	assert.Equal(t, bptree.Value(98), *tree.Find(98))
	assert.Nil(t, tree.Find(99999))

	// tree.Print()

	// internal: borrow from Left
	tree.Delete(59)
	// tree.Print()

	// internal: borrow from right
	tree.Delete(42)
	// tree.Print()

	tree.Clear()
}
