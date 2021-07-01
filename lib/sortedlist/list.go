package sortedlist

import "math"

type tree struct {
	root *node
}

func NewTree() List {
	return &tree{}
}

// List 实现了排序链表的数据结构
type List interface {
	Remove(key int64) (interface{}, bool)
	Add(key int64, data interface{})
	Range(lower, upper int64) []interface{}
	All() []interface{}
}

type node struct {
	key   int64
	data  interface{}
	left  *node
	right *node
	h     int
}

func (n *node) height() int {
	if n == nil {
		return 0
	}
	return n.h
}

func (n *node) balance() int {
	return n.right.height() - n.left.height()
}

func (n *node) insert(key int64, data interface{}) *node {
	if n == nil {
		return &node{key: key, data: data, h: 1}
	}

	if key == n.key {
		n.data = data
		return n
	}

	if key < n.key {
		n.left = n.left.insert(key, data)
	} else {
		n.right = n.right.insert(key, data)
	}

	n.h = max(n.left.height(), n.right.height()) + 1

	bf := n.balance()

	if bf < -1 {
		if n.left.balance() >= 0 {
			n.left = n.left.rotateLeft()
		}
		n = n.rotateRight()
	} else if bf > 1 {
		if n.right.balance() <= 0 {
			n.right = n.right.rotateRight()
		}
		n = n.rotateLeft()
	}

	return n
}

func (n *node) rotateLeft() *node {
	r := n.right
	n.right = r.left
	r.left = n

	n.h = max(n.left.height(), n.right.height()) + 1
	r.h = max(r.left.height(), r.right.height()) + 1

	return r
}

func (n *node) rotateRight() *node {
	l := n.left
	n.left = l.right
	l.right = n

	n.h = max(n.left.height(), n.right.height()) + 1
	l.h = max(l.left.height(), l.right.height()) + 1

	return l
}

func (n *node) Remove(key int64) (interface{}, bool) {
	if n == nil {
		return nil, false
	}

	if key == n.key {
		prev := n.data
		n.data = nil
		return prev, true
	}

	if key < n.key {
		return n.left.Remove(key)
	}

	return n.right.Remove(key)
}

func (t *tree) Add(key int64, data interface{}) {
	t.root = t.root.insert(key, data)
}

func (t *tree) Remove(key int64) (value interface{}, ok bool) {
	old, ok := t.root.Remove(key)
	if !ok {
		t.Add(key, nil)
		return nil, false
	}

	return old, true
}

func (t *tree) All() []interface{} {
	return t.Range(0, math.MaxInt64)
}

func (t *tree) Range(lower, upper int64) []interface{} {
	if t.root == nil {
		return nil
	}

	results := make([]interface{}, 0)

	nodeInRange := func(n *node) {
		results = append(results, n.data)
	}
	findNodes(t.root, lower, upper, nodeInRange)
	return results
}

func isNodeInRange(n *node, lower, upper int64) bool {
	if n == nil {
		return false
	}

	if n.key == lower || n.key == upper {
		return true
	}

	return (n.key < upper) && (lower < n.key)
}

func findNodes(node *node, lower, upper int64, fn func(*node)) {
	if node == nil {
		return
	}

	if lower < node.key {
		findNodes(node.left, lower, upper, fn)
	}

	if isNodeInRange(node, lower, upper) {
		fn(node)
	}

	if node.key < upper {
		findNodes(node.right, lower, upper, fn)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
