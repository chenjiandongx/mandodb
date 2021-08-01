package sortedlist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAVLTree(t *testing.T) {
	tree := NewTree()
	tree.Add(5, "e")
	tree.Add(6, "f")
	tree.Add(7, "g")
	tree.Add(3, "c")
	tree.Add(4, "d")
	tree.Add(1, "x")
	tree.Add(2, "b")
	tree.Add(1, "a") //overwrite

	digs := []string{"a", "b", "c", "d", "e", "f", "g"}
	iter := tree.All()
	idx := 0
	for iter.Next() {
		assert.Equal(t, digs[idx], iter.Value().(string))
		idx += 1
	}

	iter = tree.Range(2, 3)
	digs = []string{"b", "c"}
	idx = 0
	for iter.Next() {
		assert.Equal(t, digs[idx], iter.Value().(string))
		idx += 1
	}

	tree.Remove(3)
	digs = []string{"a", "b", "d", "e", "f", "g"}
	iter = tree.All()
	idx = 0
	for iter.Next() {
		assert.Equal(t, digs[idx], iter.Value().(string))
		idx += 1
	}
}
