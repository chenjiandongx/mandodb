package skiplist

import (
	"math/rand"
)

const (
	defaultMaxLevel    int64 = 12
	defaultBranchCount int64 = 4
)

var (
	defaultRand = rand.New(rand.NewSource(0xdeadbeef))
)

type Element struct {
	next  []*Element
	key   CompareKey
	value interface{}
}

func (e *Element) Next() *Element {
	return e.next[0]
}

type Options struct {
	MaxLevel    int64
	BranchCount int64
	Rand        *rand.Rand
}

func (opt *Options) SetDefault() {
	if opt.BranchCount == 0 {
		opt.BranchCount = defaultBranchCount
	}

	if opt.MaxLevel == 0 {
		opt.MaxLevel = defaultMaxLevel
	}

	if opt.Rand == nil {
		opt.Rand = defaultRand
	}
}

type List struct {
	opts   *Options
	head   *Element
	tail   *Element
	length int

	elementsCache []*Element
}

func NewList(opt *Options) *List {
	if opt == nil {
		opt = &Options{}
	}
	opt.SetDefault()

	return &List{
		opts:          opt,
		head:          &Element{next: make([]*Element, opt.MaxLevel)},
		elementsCache: make([]*Element, opt.MaxLevel),
	}
}

func (sl *List) Head() *Element {
	return sl.head.next[0]
}

func (sl *List) Tail() *Element {
	return sl.tail
}

type Iterator interface {
	Next() bool
	Key() CompareKey
	Value() interface{}
}

type slIterator struct {
	start CompareKey
	limit CompareKey
	ele   *Element
}

func (it *slIterator) Next() bool {
	if it.ele == nil || it.ele.Next() == nil || Compare(it.ele.Next().key, it.limit) > 0 {
		return false
	}

	it.ele = it.ele.Next()
	return true
}

func (it *slIterator) Key() CompareKey {
	return it.ele.key
}

func (it *slIterator) Value() interface{} {
	return it.ele.value
}

func (sl *List) Iter(start, end CompareKey) Iterator {
	return &slIterator{
		ele:   &Element{next: []*Element{sl.findElement(start, true)}},
		start: start,
		limit: end,
	}
}

func (sl *List) Get(key CompareKey) *Element {
	return sl.findElement(key, false)
}

func (sl *List) Add(key CompareKey, value interface{}) *Element {
	var element *Element
	prevs := sl.getPrevElements(key)

	if element = prevs[0].next[0]; element != nil && Compare(element.key, key) == 0 {
		element.value = value
		return element
	}

	element = &Element{next: make([]*Element, sl.randLevel()), key: key, value: value}

	for i := range element.next {
		element.next[i] = prevs[i].next[i]
		prevs[i].next[i] = element
	}

	if element.next[0] == nil {
		sl.tail = element
	}

	sl.length++
	return element
}

func (sl *List) Remove(key CompareKey) *Element {
	prevs := sl.getPrevElements(key)

	if element := prevs[0].next[0]; element != nil && Compare(element.key, key) == 0 {
		for k, v := range element.next {
			prevs[k].next[k] = v
		}

		sl.length--
		return element
	}

	return nil
}

func (sl *List) Len() int {
	return sl.length
}

func (sl *List) Exist(key CompareKey) bool {
	if v := sl.Get(key); v != nil {
		return true
	}

	return false
}

func (sl *List) findElement(key CompareKey, isPrev bool) *Element {
	prev := sl.head
	var next *Element

	for i := sl.opts.MaxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && Compare(key, next.key) > 0 {
			prev = next
			next = next.next[i]
		}

		if next != nil && Compare(key, next.key) == 0 {
			return next
		}
	}

	if isPrev {
		return prev
	}

	return nil
}

func (sl *List) getPrevElements(key CompareKey) []*Element {
	prev := sl.head
	cache := sl.elementsCache
	var next *Element
	for i := sl.opts.MaxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && Compare(key, next.key) > 0 {
			prev = next
			next = next.next[i]
		}

		cache[i] = prev
	}

	return cache
}

func (sl *List) randLevel() int {
	l := 1
	for l < int(sl.opts.MaxLevel) && sl.opts.Rand.Int63()%sl.opts.BranchCount == 0 {
		l++
	}

	return l
}

type CompareKey struct {
	Start int64
	End   int64
}

func NewSingleKey(a int64) CompareKey {
	return CompareKey{Start: a, End: a}
}

func Compare(a, b CompareKey) int {
	if a.Start == b.Start && a.End == b.End {
		return 0
	}

	if a.End <= b.Start {
		return -1
	}

	return 1
}
