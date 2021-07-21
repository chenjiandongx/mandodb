package mandodb

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/dgryski/go-tsz"

	"github.com/chenjiandongx/mandodb/pkg/sortedlist"
)

type tszStore struct {
	block *tsz.Series
	lock  sync.Mutex
	maxTs int64
	count int64
}

func (store *tszStore) Append(point *Point) *Point {
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

func (store *tszStore) Get(start, end int64) []Point {
	points := make([]Point, 0)

	it := store.block.Iter()
	for it.Next() {
		ts, val := it.Values()
		if ts > uint32(end) {
			break
		}

		if ts >= uint32(start) {
			points = append(points, Point{Ts: int64(ts), Value: val})
		}
	}

	return points
}

func (store *tszStore) All() []Point {
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
	tmp := store.All()
	it := lst.All()
	for it.Next() {
		dp := it.Value().(Point)
		tmp = append(tmp, Point{Ts: dp.Ts, Value: dp.Value})
	}

	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].Ts < tmp[j].Ts
	})

	for i := 0; i < len(tmp); i++ {
		news.Append(&tmp[i])
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
