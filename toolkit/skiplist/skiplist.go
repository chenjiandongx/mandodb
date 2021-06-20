package main

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
	key   int64
	value float64
}

func (e *Element) Next() *Element {
	return e.next[0]
}

func (e *Element) Key() int64 {
	return e.key
}

func (e *Element) Value() float64 {
	return e.value
}

type Options struct {
	MaxLevel    int64
	BranchCount int64
	Rand        *rand.Rand
}

type SkipList struct {
	opts   *Options
	head   *Element
	tail   *Element
	length int

	elementsCache []*Element
}

func NewSkipList() *SkipList {
	opt := &Options{
		MaxLevel:    defaultMaxLevel,
		BranchCount: defaultBranchCount,
		Rand:        defaultRand,
	}

	return &SkipList{
		opts:          opt,
		head:          &Element{next: make([]*Element, opt.MaxLevel)},
		elementsCache: make([]*Element, opt.MaxLevel),
	}
}

func (sl *SkipList) Head() *Element {
	return sl.head.next[0]
}

func (sl *SkipList) Tail() *Element {
	return sl.tail
}

type Iterator interface {
	Next() bool
	Key() int64
	Value() float64
}

type slIterator struct {
	limit int64
	ele   *Element
}

func (it *slIterator) Next() bool {
	if it.ele == nil || it.ele.Next() == nil || it.ele.Next().key > it.limit {
		return false
	}

	it.ele = it.ele.Next()
	return true
}

func (it *slIterator) Key() int64 {
	return it.ele.Key()
}

func (it *slIterator) Value() float64 {
	return it.ele.Value()
}

func (sl *SkipList) Iter(start, limit int64) Iterator {
	return &slIterator{ele: &Element{next: []*Element{sl.findElement(start, true)}}, limit: limit}
}

func (sl *SkipList) Get(key int64) *Element {
	return sl.findElement(key, false)
}

func (sl *SkipList) Add(key int64, value float64) *Element {
	var element *Element
	prevs := sl.getPrevElements(key)

	if element = prevs[0].next[0]; element != nil && element.key == key {
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

func (sl *SkipList) Remove(key int64) *Element {
	prevs := sl.getPrevElements(key)

	if element := prevs[0].next[0]; element != nil && element.key == key {
		for k, v := range element.next {
			prevs[k].next[k] = v
		}

		sl.length--
		return element
	}

	return nil
}

func (sl *SkipList) Len() int {
	return sl.length
}

func (sl *SkipList) Exist(key int64) bool {
	if v := sl.Get(key); v != nil {
		return true
	}

	return false
}

func (sl *SkipList) findElement(key int64, isPrev bool) *Element {
	prev := sl.head
	var next *Element

	for i := sl.opts.MaxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && key > next.key {
			prev = next
			next = next.next[i]
		}

		if next != nil && key == next.key {
			return next
		}
	}

	if isPrev {
		return prev
	}

	return nil
}

func (sl *SkipList) getPrevElements(key int64) []*Element {
	prev := sl.head
	cache := sl.elementsCache
	var next *Element
	for i := sl.opts.MaxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && key > next.key {
			prev = next
			next = next.next[i]
		}

		cache[i] = prev
	}

	return cache
}

func (sl *SkipList) randLevel() int {
	l := 1
	for l < int(sl.opts.MaxLevel) && sl.opts.Rand.Int63()%sl.opts.BranchCount == 0 {
		l++
	}

	return l
}
