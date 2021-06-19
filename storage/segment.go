package storage

import (
	"container/list"
	"os"
)

// TODO: segment duration
type Segment interface {
	InsertRow(row *Row)
	QueryRange(metric string, labels LabelSet, start, end int64)
	MinTs() int64
	MaxTs() int64
	Frozen() bool
}

type SegmentList struct {
	head Segment
	lst  *list.List
}

func (sl *SegmentList) Get(start, end int64) []Segment {
	return nil
}

func (sl *SegmentList) Add(segment Segment) {
	sl.lst.PushFront(segment)
}

func (sl *SegmentList) Remove(segment Segment) {

}

func (sl *SegmentList) Head() Segment {
	return sl.head
}

const metricName = "__name__"

func isFileExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
