package storage

import (
	"strconv"
	"sync"
)

type sidSet struct {
	mut sync.Mutex
	set map[string]struct{}
}

func newSidSet() *sidSet {
	return &sidSet{set: make(map[string]struct{})}
}

func (s *sidSet) Add(sid string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.set[sid] = struct{}{}
}

func (s *sidSet) Size() int {
	s.mut.Lock()
	defer s.mut.Unlock()

	return len(s.set)
}

func (s *sidSet) Copy() *sidSet {
	s.mut.Lock()
	defer s.mut.Unlock()

	newset := newSidSet()
	for k := range s.set {
		newset.set[k] = struct{}{}
	}

	return newset
}

func (s *sidSet) Intersection(other *sidSet) {
	s.mut.Lock()
	defer s.mut.Unlock()

	for k := range s.set {
		_, ok := other.set[k]
		if !ok {
			delete(s.set, k)
		}
	}
}

func (s *sidSet) List() []string {
	s.mut.Lock()
	defer s.mut.Unlock()

	keys := make([]string, 0, len(s.set))
	for k := range s.set {
		keys = append(keys, k)
	}

	return keys
}

type indexMap struct {
	idx map[string]*sidSet
	mut sync.Mutex
}

func newIndexMap() *indexMap {
	return &indexMap{idx: make(map[string]*sidSet)}
}

func (im *indexMap) Range(f func(k string, v *sidSet)) {
	im.mut.Lock()
	defer im.mut.Unlock()

	for k, sids := range im.idx {
		f(k, sids)
	}
}

func buildIndexMapForDisk(m map[string][]uint32) *indexMap {
	idxmap := &indexMap{idx: map[string]*sidSet{}}

	for k, sids := range m {
		idxmap.idx[k] = newSidSet()
		for _, sid := range sids {
			idxmap.idx[k].Add(strconv.Itoa(int(sid)))
		}
	}

	return idxmap
}

func (im *indexMap) UpdateIndex(sid string, labels LabelSet) {
	im.mut.Lock()
	defer im.mut.Unlock()

	for _, label := range labels {
		key := joinSeparator(label.Name, label.Value)
		if _, ok := im.idx[key]; !ok {
			im.idx[key] = newSidSet()
		}
		im.idx[key].Add(sid)
	}
}

func (im *indexMap) MatchSidsString(labels LabelSet) []string {
	return im.matchSids(labels)
}

func (im *indexMap) MatchSidsInt(labels LabelSet) []int {
	sids := im.matchSids(labels)
	ret := make([]int, 0, len(sids))
	for _, sid := range im.matchSids(labels) {
		i, _ := strconv.Atoi(sid)
		ret = append(ret, i)
	}

	return ret
}

func (im *indexMap) matchSids(labels LabelSet) []string {
	im.mut.Lock()
	defer im.mut.Unlock()

	sids := newSidSet()
	for i := len(labels) - 1; i >= 0; i-- {
		key := joinSeparator(labels[i].Name, labels[i].Value)
		midx := im.idx[key]

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
