package storage

import (
	"strings"
	"sync"
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

func (mim *memoryIndexMap) MatchSids(labels LabelSet) []string {
	mim.mut.Lock()
	defer mim.mut.Unlock()

	sids := newMemorySidSet()
	for i := len(labels) - 1; i >= 0; i-- {
		midx := mim.idx[labels[i].MarshalName()]

		if labels[i].Name == metricName {
			// 匹配不到 metricName 则表明该 metric 不存在 直接返回
			if midx == nil {
				return nil
			}

			sids = midx.Copy()
			if sids.Size() <= 0 {
				return nil
			}
			continue
		}

		if midx != nil {
			sids.Intersection(midx.Copy())
		}
	}

	return sids.List()
}

// Disk Index

type diskSidSet struct {
	set map[uint32]struct{}
	mut sync.Mutex
}

func newDiskSidSet() *diskSidSet {
	return &diskSidSet{set: make(map[uint32]struct{})}
}

func (dss *diskSidSet) Add(a uint32) {
	dss.mut.Lock()
	defer dss.mut.Unlock()

	dss.set[a] = struct{}{}
}

func (dss *diskSidSet) Size() int {
	dss.mut.Lock()
	defer dss.mut.Unlock()

	return len(dss.set)
}

func (dss *diskSidSet) Intersection(other *diskSidSet) {
	dss.mut.Lock()
	defer dss.mut.Unlock()

	for k := range dss.set {
		_, ok := other.set[k]
		if !ok {
			delete(dss.set, k)
		}
	}
}

func (dss *diskSidSet) List() []uint32 {
	dss.mut.Lock()
	defer dss.mut.Unlock()

	keys := make([]uint32, 0, len(dss.set))
	for k := range dss.set {
		keys = append(keys, k)
	}

	return keys
}

type diskIndexMap struct {
	label2sids   map[string]*diskSidSet
	labelOrdered map[int]string

	sid2Labels map[string]*diskSidSet
	mut        sync.Mutex
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

func (dim *diskIndexMap) MatchSids(labels LabelSet) []uint32 {
	dim.mut.Lock()
	defer dim.mut.Unlock()

	sids := newDiskSidSet()
	for i := len(labels) - 1; i >= 0; i-- {
		midx := dim.label2sids[labels[i].MarshalName()]

		if labels[i].Name == metricName {
			// 匹配不到 metricName 则表明该 metric 不存在 直接返回
			if midx == nil {
				return nil
			}

			// 从磁盘 chuck 是不可变的 所以 map 不会有更新操作 无需 copy
			sids = midx
			if sids.Size() <= 0 {
				return nil
			}
			continue
		}

		if midx != nil {
			sids.Intersection(midx)
		}
	}

	return sids.List()
}
