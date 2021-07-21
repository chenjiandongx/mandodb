package mandodb

import (
	"os"

	"github.com/chenjiandongx/mandodb/pkg/sortedlist"
)

type SegmentType string

const (
	DiskSegmentType   SegmentType = "DISK"
	MemorySegmentType             = "MEMORY"
)

type Segment interface {
	InsertRows(row []*Row)
	QueryRange(lms LabelMatcherSet, start, end int64) ([]MetricRet, error)
	QuerySeries(lms LabelMatcherSet) ([]LabelSet, error)
	QueryLabelValues(label string) []string
	MinTs() int64
	MaxTs() int64
	Frozen() bool
	Close() error
	Type() SegmentType
	Load() Segment
}

type Desc struct {
	SeriesCount     int64 `json:"seriesCount"`
	DataPointsCount int64 `json:"dataPointsCount"`
	MaxTs           int64 `json:"maxTs"`
	MinTs           int64 `json:"minTs"`
}

type segmentList struct {
	head Segment
	lst  sortedlist.List
}

func newSegmentList() *segmentList {
	return &segmentList{head: newMemorySegment(), lst: sortedlist.NewTree()}
}

func (sl *segmentList) Get(start, end int64) []Segment {
	segs := make([]Segment, 0)

	iter := sl.lst.All()

	for iter.Next() {
		seg := iter.Value().(Segment)
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

func (sl *segmentList) Choose(seg Segment, start, end int64) bool {
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

func (sl *segmentList) Add(segment Segment) {
	sl.lst.Add(segment.MinTs(), segment)
}

func (sl *segmentList) Remove(segment Segment) {
	sl.lst.Remove(segment.MinTs())
}

const metricName = "__name__"

func isFileExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
