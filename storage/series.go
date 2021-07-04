package storage

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/dgryski/go-tsz"

	"github.com/chenjiandongx/mandodb/lib/sortedlist"
)

type tszStore struct {
	block *tsz.Series
	lock  sync.Mutex
	maxTs int64
	count int64
}

func (store *tszStore) Append(point *DataPoint) *DataPoint {
	store.lock.Lock()
	defer store.lock.Unlock()

	if store.maxTs >= point.Ts {
		return point
	}
	store.maxTs = point.Ts

	// 懒加载的方式初始化
	if store.count <= 0 {
		store.block = tsz.New(uint32(point.Ts))
	}

	store.block.Push(uint32(point.Ts), point.Value)
	store.maxTs = point.Ts

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

func (store *tszStore) All() []DataPoint {
	return store.Get(math.MinInt64, math.MaxInt64)
}

func (store *tszStore) Count() int {
	return int(atomic.LoadInt64(&store.count))
}

func (store *tszStore) Bytes() []byte {
	return store.block.Bytes()
}

func (store *tszStore) MergeOutdatedList(lst sortedlist.List) *tszStore {
	if lst == nil {
		return store
	}

	news := &tszStore{}

	l1 := store.block.Iter()
	l2 := lst.All()
	for l1.Next() && l2.Next() {
		t1, v1 := l1.Values()

		dp := l2.Value().(DataPoint)
		t2, v2 := dp.Ts, dp.Value

		if int64(t1) <= t2 {
			news.Append(&DataPoint{Ts: int64(t1), Value: v1})
			l1.Next()
		} else {
			news.Append(&DataPoint{Ts: t2, Value: v2})
			l2.Next()
		}
	}

	if !l2.End() {
		for {
			dp := l2.Value().(DataPoint)
			t2, v2 := dp.Ts, dp.Value
			news.Append(&DataPoint{Ts: t2, Value: v2})

			if !l2.Next() {
				break
			}
		}
	} else {
		for {
			t1, v1 := l1.Values()
			news.Append(&DataPoint{Ts: int64(t1), Value: v1})

			if !l1.Next() {
				break
			}
		}
	}

	return news
}

type memorySeries struct {
	labels LabelSet
	*tszStore
}

func newSeries(row *Row) *memorySeries {
	return &memorySeries{labels: row.Labels, tszStore: &tszStore{}}
}
