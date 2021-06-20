package storage

import (
	"os"

	"github.com/chenjiandongx/mandodb/toolkit/skiplist"
)

type SegmentType string

const (
	DiskSegmentType   SegmentType = "DISK"
	MemorySegmentType             = "MEMORY"
)

type Segment interface {
	InsertRow(row *Row)
	QueryRange(labels LabelSet, start, end int64) []MetricRet
	MinTs() int64
	MaxTs() int64
	Frozen() bool
	Marshal() ([]byte, []byte, error)
	Unmarshal([]byte, *Metadata) error
	Type() SegmentType
}

type SegmentList struct {
	head Segment
	lst  *skiplist.List
}

func newSegmentList() *SegmentList {
	return &SegmentList{
		head: newMemorySegment(),
		lst:  skiplist.NewList(nil),
	}
}

func (sl *SegmentList) Get(start, end int64) []Segment {
	segs := make([]Segment, 0)

	startKey := skiplist.NewSingleKey(start)
	endKey := skiplist.NewSingleKey(end)

	iter := sl.lst.Iter(startKey, endKey)
	for iter.Next() {
		seg, ok := iter.Value().(Segment)
		if !ok || skiplist.Compare(iter.Key(), startKey) < 0 {
			continue
		}

		segs = append(segs, seg)
	}

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
	comparekey := skiplist.CompareKey{
		Start: segment.MinTs(),
		End:   segment.MaxTs(),
	}

	sl.lst.Add(comparekey, segment)
}

func (sl *SegmentList) Remove(segment Segment) {
	comparekey := skiplist.CompareKey{
		Start: segment.MinTs(),
		End:   segment.MaxTs(),
	}

	sl.lst.Remove(comparekey)
}

const metricName = "__name__"

func isFileExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
