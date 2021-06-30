package storage

import (
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// Memory Index

type memorySidSet struct {
	set map[string]struct{}
	mut sync.Mutex
}

func newMemorySidSet() *memorySidSet {
	return &memorySidSet{set: make(map[string]struct{})}
}

func (mss *memorySidSet) Add(a string) {
	mss.mut.Lock()
	defer mss.mut.Unlock()

	mss.set[a] = struct{}{}
}

func (mss *memorySidSet) Size() int {
	mss.mut.Lock()
	defer mss.mut.Unlock()

	return len(mss.set)
}

func (mss *memorySidSet) Copy() *memorySidSet {
	mss.mut.Lock()
	defer mss.mut.Unlock()

	newset := newMemorySidSet()
	for k := range mss.set {
		newset.set[k] = struct{}{}
	}

	return newset
}

func (mss *memorySidSet) Intersection(other *memorySidSet) {
	mss.mut.Lock()
	defer mss.mut.Unlock()

	for k := range mss.set {
		_, ok := other.set[k]
		if !ok {
			delete(mss.set, k)
		}
	}
}

func (mss *memorySidSet) Union(other *memorySidSet) {
	mss.mut.Lock()
	defer mss.mut.Unlock()

	for k := range other.set {
		mss.set[k] = struct{}{}
	}
}

func (mss *memorySidSet) List() []string {
	mss.mut.Lock()
	defer mss.mut.Unlock()

	keys := make([]string, 0, len(mss.set))
	for k := range mss.set {
		keys = append(keys, k)
	}

	return keys
}

type memoryIndexMap struct {
	idx map[string]*memorySidSet
	mut sync.Mutex
}

func newMemoryIndexMap() *memoryIndexMap {
	return &memoryIndexMap{idx: make(map[string]*memorySidSet)}
}

func (mim *memoryIndexMap) Range(f func(k string, v *memorySidSet)) {
	mim.mut.Lock()
	defer mim.mut.Unlock()

	for k, sids := range mim.idx {
		f(k, sids)
	}
}

func (mim *memoryIndexMap) UpdateIndex(sid string, labels LabelSet) {
	mim.mut.Lock()
	defer mim.mut.Unlock()

	for _, label := range labels {
		key := label.MarshalName()
		if _, ok := mim.idx[key]; !ok {
			mim.idx[key] = newMemorySidSet()
		}
		mim.idx[key].Add(sid)
	}
}

func (mim *memoryIndexMap) MatchSids(lvs *labelValueSet, labels LabelSet) []string {
	mim.mut.Lock()
	defer mim.mut.Unlock()

	sids := newMemorySidSet()
	var got bool
	for i := len(labels) - 1; i >= 0; i-- {
		temp := newMemorySidSet()
		vs := lvs.Match(labels[i].Name, labels[i].Value)
		for _, v := range vs {
			midx := mim.idx[joinSeparator(labels[i].Name, v)]
			if midx == nil || midx.Size() <= 0 {
				continue
			}

			temp.Union(midx.Copy())
		}

		if temp == nil || temp.Size() <= 0 {
			return nil
		}

		if !got {
			sids = temp
			got = true
			continue
		}

		sids.Intersection(temp.Copy())
	}

	return sids.List()
}

// Disk Index

type diskSidSet struct {
	set *roaring.Bitmap
	mut sync.Mutex
}

func newDiskSidSet() *diskSidSet {
	return &diskSidSet{set: roaring.New()}
}

func (dss *diskSidSet) Add(a uint32) {
	dss.mut.Lock()
	defer dss.mut.Unlock()

	dss.set.Add(a)
}

type diskIndexMap struct {
	label2sids   map[string]*diskSidSet
	labelOrdered map[int]string

	mut sync.Mutex
}

func newDiskIndexMap(swl []seriesWithLabel) *diskIndexMap {
	dim := &diskIndexMap{
		label2sids:   make(map[string]*diskSidSet),
		labelOrdered: make(map[int]string),
	}

	for i := range swl {
		row := swl[i]
		dim.label2sids[row.Name] = newDiskSidSet()
		for _, sid := range swl[i].Sids {
			dim.label2sids[row.Name].Add(sid)
		}
		dim.labelOrdered[i] = row.Name
	}

	return dim
}

func (dim *diskIndexMap) MatchLabels(lids ...uint32) []Label {
	ret := make([]Label, 0, len(lids))
	for _, lid := range lids {
		labelPair := dim.labelOrdered[int(lid)]
		kv := strings.SplitN(labelPair, separator, 2)
		if len(kv) != 2 {
			continue
		}

		if kv[0] == metricName {
			continue
		}

		ret = append(ret, Label{
			Name:  kv[0],
			Value: kv[1],
		})
	}

	return ret
}

func (dim *diskIndexMap) MatchSids(lvs *labelValueSet, labels LabelSet) []uint32 {
	dim.mut.Lock()
	defer dim.mut.Unlock()

	lst := make([]*roaring.Bitmap, 0)
	for i := len(labels) - 1; i >= 0; i-- {
		temp := make([]*roaring.Bitmap, 0)
		vs := lvs.Match(labels[i].Name, labels[i].Value)

		for _, v := range vs {
			didx := dim.label2sids[joinSeparator(labels[i].Name, v)]
			if didx == nil || didx.set.IsEmpty() {
				continue
			}

			temp = append(temp, didx.set)
		}

		union := roaring.ParOr(2, temp...)
		lst = append(lst, union)
	}

	return roaring.ParAnd(2, lst...).ToArray()
}
