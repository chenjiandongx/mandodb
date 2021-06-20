package storage

import (
	"os"
	"sort"
)

type SegmentType string

const (
	DiskSegmentType   SegmentType = "DISK"
	MemorySegmentType             = "MEMORY"
)

type Segment interface {
	InsertRows(row []*Row)
	QueryRange(labels LabelSet, start, end int64) ([]MetricRet, error)
	MinTs() int64
	MaxTs() int64
	Frozen() bool
	Marshal() ([]byte, []byte, error)
	Type() SegmentType
	Close() error
}

type SegmentList struct {
	head Segment
	lst  []Segment
}

func newSegmentList() *SegmentList {
	return &SegmentList{head: newMemorySegment()}
}

func (sl *SegmentList) Less(i, j int) bool {
	return sl.lst[i].MaxTs() < sl.lst[j].MinTs()
}

func (sl *SegmentList) Len() int {
	return len(sl.lst)
}

func (sl *SegmentList) Swap(i, j int) {
	sl.lst[i], sl.lst[j] = sl.lst[j], sl.lst[i]
}

func (sl *SegmentList) Get(start, end int64) []Segment {
	segs := make([]Segment, 0)

	for _, seg := range sl.lst {
		if sl.Choose(seg, start, end) {
			segs = append(segs, seg)
		}
	}

	// 头部永远是最新的 所以放最后
	if sl.Choose(sl.head, start, end) {
		segs = append(segs, sl.head)
	}

	return segs
}

func (sl *SegmentList) Choose(seg Segment, start, end int64) bool {
	if seg.MinTs() < start && seg.MaxTs() > start {
		return true
	}

	if seg.MinTs() > start && seg.MaxTs() < end {
		return true
	}

	if seg.MinTs() < end && seg.MaxTs() > end {
		return true
	}

	return false
}

func (sl *SegmentList) Add(segment Segment) {
	sl.lst = append(sl.lst, segment)
	sort.Sort(sl)
}

func (sl *SegmentList) Remove(segment Segment) {

}

const metricName = "__name__"

func isFileExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
