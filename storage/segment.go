package storage

import (
	"os"

	"github.com/chenjiandongx/mandodb/lib/sortedlist"
)

type SegmentType string

const (
	DiskSegmentType   SegmentType = "DISK"
	MemorySegmentType             = "MEMORY"
)

type Segment interface {
	InsertRows(row []*Row)
	QueryRange(labels LabelSet, start, end int64) ([]MetricRet, error)
	QuerySeries(labels LabelSet) ([]LabelSet, error)
	QueryLabelValues(label string) []string
	MinTs() int64
	MaxTs() int64
	Frozen() bool
	Marshal() ([]byte, []byte, error)
	Type() SegmentType
	Close() error
	Load() Segment
}

type Desc struct {
	SeriesCount     int64 `json:"seriesCount"`
	DataPointsCount int64 `json:"dataPointsCount"`
	MaxTs           int64 `json:"maxTs"`
	MinTs           int64 `json:"minTs"`
}

type SegmentList struct {
	head Segment
	lst  sortedlist.List
}

func newSegmentList() *SegmentList {
	return &SegmentList{head: newMemorySegment(), lst: sortedlist.NewTree()}
}

func (sl *SegmentList) Get(start, end int64) []Segment {
	segs := make([]Segment, 0)

	rows := sl.lst.All()
	for i := 0; i < len(rows); i++ {
		seg := rows[i].(Segment)
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
	sl.lst.Add(segment.MaxTs(), segment)
}

func (sl *SegmentList) Remove(segment Segment) {
	sl.lst.Remove(segment.MinTs())
}

const metricName = "__name__"

func isFileExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
