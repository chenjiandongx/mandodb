package sortedlist

import "math"

// List 实现了排序链表的数据结构
type List interface {
	// Remove 移除节点
	Remove(key int64) bool

	// Add 新增节点
	Add(key int64, data interface{})

	// Range 过滤范围内的 key 并返回 Iter 对象
	Range(lower, upper int64) Iter

	// All 迭代所有对象
	All() Iter
}

// Iter 迭代器对象
type Iter interface {
	Next() bool
	Value() interface{}
}

type iter struct {
	cursor int
	data   []interface{}
}

// Next 推进迭代器
func (it *iter) Next() bool {
	it.cursor++
	if len(it.data) > it.cursor {
		return true
	}

	return false
}

// Value 返回迭代器当前 value
func (it *iter) Value() interface{} {
	return it.data[it.cursor]
}

type avlNode struct {
	h     int
	key   int64
	value interface{}
	left  *avlNode
	right *avlNode
}

type aVLTree struct {
	tree *avlNode
}

// NewTree 生成 AVL 树
func NewTree() List {
	return &aVLTree{&avlNode{h: -2}}
}

func (a *aVLTree) Add(k int64, v interface{}) {
	a.tree = insert(k, v, a.tree)
}

func (a *aVLTree) Remove(k int64) bool {
	if a.tree.search(k) {
		a.tree.delete(k)
		return true
	}
	return false
}

func (a *aVLTree) All() Iter {
	return a.tree.values(0, math.MaxInt64)
}

func (a *aVLTree) Range(lower, upper int64) Iter {
	return a.tree.values(lower, upper)
}

func (a *aVLTree) getMaxValue() int64 {
	return a.tree.maxNode().key
}

func (a *aVLTree) getMinValue() int64 {
	return a.tree.minNode().key
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func insert(k int64, v interface{}, t *avlNode) *avlNode {
	if t == nil {
		return &avlNode{key: k, value: v}
	}

	if t.h == -2 {
		t.key = k
		t.value = v
		t.h = 0
		return t
	}

	cmp := k - t.key
	if cmp > 0 {
		// 将节点插入到右子树中
		t.right = insert(k, v, t.right)
	} else if cmp < 0 {
		// 将节点插入到左子树中
		t.left = insert(k, v, t.left)
	} else if cmp == 0 {
		t.value = v
	}
	// 维持树平衡
	t = t.keepBalance(k)
	t.h = max(t.left.height(), t.right.height()) + 1
	return t
}

func (t *avlNode) search(k int64) bool {
	if t == nil {
		return false
	}
	cmp := k - t.key
	if cmp > 0 {
		// 如果 v 大于当前节点值，继续从右子树中寻找
		return t.right.search(k)
	} else if cmp < 0 {
		// 如果 v 小于当前节点值，继续从左子树中寻找
		return t.left.search(k)
	} else {
		// 相等则表示找到
		return true
	}
}

func (t *avlNode) delete(k int64) *avlNode {
	if t == nil {
		return t
	}
	cmp := k - t.key
	if cmp > 0 {
		// 如果 v 大于当前节点值，继续从右子树中删除
		t.right = t.right.delete(k)
	} else if cmp < 0 {
		// 如果 v 小于当前节点值，继续从左子树中删除
		t.left = t.left.delete(k)
	} else {
		// 找到 v
		if t.left != nil && t.right != nil {
			// 如果该节点既有左子树又有右子树
			// 使用右子树中的最小节点取代删除节点，然后删除右子树中的最小节点

			minnode := t.right.minNode()
			t.key = minnode.key
			t.value = minnode.value
			t.right = t.right.delete(t.key)
		} else if t.left != nil {
			// 如果只有左子树，则直接删除节点
			t = t.left
		} else {
			// 只有右子树或空树
			t = t.right
		}
	}

	if t != nil {
		t.h = max(t.left.height(), t.right.height()) + 1
		t = t.keepBalance(k)
	}
	return t
}

func (t *avlNode) minNode() *avlNode {
	if t == nil {
		return nil
	}
	// 整棵树的最左边节点就是值最小的节点
	if t.left == nil {
		return t
	} else {
		return t.left.minNode()
	}
}

func (t *avlNode) maxNode() *avlNode {
	if t == nil {
		return nil
	}
	// 整棵树的最右边节点就是值最大的节点
	if t.right == nil {
		return t
	} else {
		return t.right.maxNode()
	}
}

/*
左左情况：右旋
		*
	   *
	  *
*/
func (t *avlNode) llRotate() *avlNode {
	node := t.left
	t.left = node.right
	node.right = t

	node.h = max(node.left.height(), node.right.height()) + 1
	t.h = max(t.left.height(), t.right.height()) + 1
	return node
}

/*
右右情况：左旋
		*
	     *
	      *
*/
func (t *avlNode) rrRotate() *avlNode {
	node := t.right
	t.right = node.left
	node.left = t

	node.h = max(node.left.height(), node.right.height()) + 1
	t.h = max(t.left.height(), t.right.height()) + 1
	return node
}

/*
左右情况：先左旋 后右旋
		*
	   *
	    *
*/
func (t *avlNode) lrRotate() *avlNode {
	t.left = t.left.rrRotate()
	return t.llRotate()
}

/*
右左情况：先右旋 后左旋
		*
	     *
        *
*/
func (t *avlNode) rlRotate() *avlNode {
	t.right = t.right.llRotate()
	return t.rrRotate()
}

func (t *avlNode) keepBalance(k int64) *avlNode {
	// 左子树失衡
	if t.left.height()-t.right.height() == 2 {
		if k-t.left.key < 0 {
			// 当插入的节点在失衡节点的左子树的左子树中，直接右旋
			t = t.llRotate()
		} else {
			// 当插入的节点在失衡节点的左子树的右子树中，先左旋后右旋
			t = t.lrRotate()
		}
	} else if t.right.height()-t.left.height() == 2 {
		if t.right.right.height() > t.right.left.height() {
			// 当插入的节点在失衡节点的右子树的右子树中，直接左旋
			t = t.rrRotate()
		} else {
			// 当插入的节点在失衡节点的右子树的左子树中，先右旋后左旋
			t = t.rlRotate()
		}
	}
	// 调整树高度
	t.h = max(t.left.height(), t.right.height()) + 1
	return t
}

func (t *avlNode) height() int {
	if t != nil {
		return t.h
	}
	return -1
}

// appendValue 中序遍历按顺序获取所有值
func appendValue(values []interface{}, lower, upper int64, t *avlNode) []interface{} {
	if t != nil {
		values = appendValue(values, lower, upper, t.left)
		if t.key >= lower && t.key <= upper {
			values = append(values, t.value)
		}
		values = appendValue(values, lower, upper, t.right)
	}
	return values
}

func (t *avlNode) values(lower, upper int64) Iter {
	it := &iter{data: []interface{}{nil}}
	it.data = appendValue(it.data, lower, upper, t)

	return it
}
