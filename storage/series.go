package storage

import (
	"github.com/dgryski/go-tsz"
)

type tszStore struct {
	block *tsz.Series
	maxTs int64
	count int
}

func (store *tszStore) Append(point *DataPoint) *DataPoint {
	if store.maxTs >= point.Ts || point.Ts <= 0 {
		return point
	}

	// 懒加载的方式初始化
	if store.count <= 0 {
		store.block = tsz.New(uint32(point.Ts))
	}

	store.block.Push(uint32(point.Ts), point.Value)
	store.count++

	return nil
}

func (store *tszStore) Get(start, end int64) []DataPoint {
	points := make([]DataPoint, 0)

	it := store.block.Iter()
	for it.Next() {
		ts, val := it.Values()
		if ts > uint32(end) {
			break
		}

		if ts >= uint32(start) {
			points = append(points, DataPoint{Ts: int64(ts), Value: val})
		}
	}

	return points
}

func (store *tszStore) Count() int {
	return store.count
}

func (store *tszStore) Bytes() []byte {
	return store.block.Bytes()
}

type Series struct {
	labels LabelSet
	store  *tszStore
}

func newSeries(row *Row) *Series {
	return &Series{labels: row.Labels, store: &tszStore{}}
}
