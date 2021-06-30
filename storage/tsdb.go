package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/chenjiandongx/logger"
	"github.com/chenjiandongx/mandodb/lib/mmap"
)

// TODO: list
// * 归档数据使用 snappy/zstd 压缩
// * 磁盘文件合并 参考 leveldb
// * WAL 做灾备

type tsdbOptions struct {
	metaSerializer  MetaSerializer
	bytesCompressor BytesCompressor
}

var globalOpts = &tsdbOptions{
	metaSerializer:  &binaryMetaSerializer{},
	bytesCompressor: &zstdBytesCompressor{},
}

type Option func(c *tsdbOptions)

func WithMetaSerializerType(t MetaSerializerType) Option {
	return func(c *tsdbOptions) {
		switch t {
		default: // binary
			c.metaSerializer = &binaryMetaSerializer{}
		}
	}
}

func WithMetaBytesCompressorType(t BytesCompressorType) Option {
	return func(c *tsdbOptions) {
		switch t {
		case NoopBytesCompressor:
			c.bytesCompressor = &noopBytesCompressor{}
		case SnappyBytesCompressor:
			c.bytesCompressor = &snappyBytesCompressor{}
		default: // zstd
			c.bytesCompressor = &zstdBytesCompressor{}
		}
	}
}

const (
	separator         = "/-/"
	defaultQSize      = 128
	defaultWriteBatch = 256
)

type DataPoint struct {
	Ts    int64
	Value float64
}

func (dp DataPoint) ToInterface() [2]interface{} {
	return [2]interface{}{dp.Ts, fmt.Sprintf("%f", dp.Value)}
}

func joinSeparator(a, b interface{}) string {
	return fmt.Sprintf("%v%s%v", a, separator, b)
}

func filePrefix(a, b int64) string {
	return fmt.Sprintf("seg-%d-%d.", a, b)
}

type Row struct {
	Metric    string
	Labels    LabelSet
	DataPoint DataPoint
}

func (r Row) M() uint64 {
	return xxhash.Sum64([]byte(r.Metric))
}

func (r Row) ID() string {
	return joinSeparator(r.M(), r.Labels.Hash())
}

type MetricRet struct {
	Labels     []Label
	DataPoints []DataPoint
}

type TSDB struct {
	segs *SegmentList
	mut  sync.Mutex
	srv  *server

	ctx    context.Context
	cancel context.CancelFunc

	q  chan []*Row
	wg sync.WaitGroup
}

func (tsdb *TSDB) InsertRows(rows []*Row) error {
	select {
	case tsdb.q <- rows:
	}

	return nil
}

func (tsdb *TSDB) ingestRows(ctx context.Context) {
	rows := make([]*Row, 0, defaultWriteBatch)
	tick := time.Tick(200 * time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			return

		case rs := <-tsdb.q:
			for i := 0; i < len(rs); i++ {
				rows = append(rows, rs[i])
			}

			if len(rows) >= defaultWriteBatch {
				head, err := tsdb.getHeadPartition()
				if err != nil {
					logger.Errorf("failed to get head partition: %v", head)
					continue
				}

				head.InsertRows(rows)
				rows = rows[:0]
			}

		case <-tick:
			head, err := tsdb.getHeadPartition()
			if err != nil {
				logger.Errorf("failed to get head partition: %v", head)
				continue
			}

			head.InsertRows(rows)
			rows = rows[:0]
		}
	}
}

func (tsdb *TSDB) getHeadPartition() (Segment, error) {
	tsdb.mut.Lock()
	defer tsdb.mut.Unlock()

	if tsdb.segs.head.Frozen() {
		head := tsdb.segs.head

		go func() {
			tsdb.wg.Add(1)
			defer tsdb.wg.Done()

			t0 := time.Now()
			prefix := filePrefix(head.MinTs(), head.MaxTs())
			_, err := writeToDisk(head)
			if err != nil {
				logger.Errorf("failed to flush data to disk, %v", err)
				return
			}

			fname := prefix + "data"
			mf, err := mmap.OpenMmapFile(fname)
			if err != nil {
				logger.Errorf("failed to make a mmap file %s, %v", fname, err)
				return
			}

			tsdb.segs.Add(newDiskSegment(mf, prefix+"meta", head.MinTs(), head.MaxTs()))
			logger.Infof("write file %s take: %v", fname, time.Since(t0))
		}()

		tsdb.segs.head = newMemorySegment()
	}

	return tsdb.segs.head, nil
}

type QueryRangeOptions struct {
	Metric  string   `json:"metric"`
	Labels  LabelSet `json:"labels"`
	Agg     string   `json:"agg"`
	GroupBy string   `json:"groupBy"`
	Start   int64    `json:"start"`
	End     int64    `json:"end"`
	Step    string   `json:"step"`
}

func (tsdb *TSDB) QueryRange(metric string, labels LabelSet, start, end int64) ([]MetricRet, error) {
	tsdb.wg.Wait()

	labels = labels.AddMetricName(metric)

	temp := make([]MetricRet, 0)
	for _, segment := range tsdb.segs.Get(start, end) {
		segment = segment.Load()
		data, err := segment.QueryRange(labels, start, end)
		if err != nil {
			return nil, err
		}

		temp = append(temp, data...)
	}

	return tsdb.MergeQueryRangeResult(temp...), nil
}

func (tsdb *TSDB) MergeQueryRangeResult(ret ...MetricRet) []MetricRet {
	metrics := make(map[uint64]*MetricRet)
	for _, r := range ret {
		h := LabelSet(r.Labels).Hash()
		v, ok := metrics[h]
		if !ok {
			metrics[h] = &MetricRet{
				Labels:     r.Labels,
				DataPoints: r.DataPoints,
			}
			continue
		}

		v.DataPoints = append(v.DataPoints, r.DataPoints...)
	}

	items := make([]MetricRet, 0, len(metrics))
	for _, v := range metrics {
		sort.Slice(v.DataPoints, func(i, j int) bool {
			return v.DataPoints[i].Ts < v.DataPoints[j].Ts
		})
		items = append(items, *v)
	}

	return items
}

func (tsdb *TSDB) QuerySeries(labels LabelSet, start, end int64) ([]map[string]string, error) {
	tsdb.wg.Wait()

	temp := make([]LabelSet, 0)
	for _, segment := range tsdb.segs.Get(start, end) {
		segment = segment.Load()
		data, err := segment.QuerySeries(labels)
		if err != nil {
			return nil, err
		}

		temp = append(temp, data...)
	}

	return tsdb.MergeQuerySeriesResult(temp...), nil
}

func (tsdb *TSDB) MergeQuerySeriesResult(ret ...LabelSet) []map[string]string {
	lbs := make(map[uint64]LabelSet)
	for _, r := range ret {
		lbs[r.Hash()] = r
	}

	items := make([]map[string]string, 0)
	for _, lb := range lbs {
		items = append(items, lb.Map())
	}

	return items
}

func (tsdb *TSDB) QueryLabelValues(label string, start, end int64) []string {
	temp := make(map[string]struct{})
	for _, segment := range tsdb.segs.Get(start, end) {
		values := segment.QueryLabelValues(label)
		for i := 0; i < len(values); i++ {
			temp[values[i]] = struct{}{}
		}
	}

	ret := make([]string, 0, len(temp))
	for k := range temp {
		ret = append(ret, k)
	}

	sort.Strings(ret)

	return ret
}

func (tsdb *TSDB) Close() {
	tsdb.wg.Wait()
	tsdb.cancel()

	for _, segment := range tsdb.segs.lst.All() {
		segment.(Segment).Close()
	}

	tsdb.segs.head.Close()
}

func (tsdb *TSDB) loadFiles() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic("BUG: failed to load data storage, error: " + err.Error())
	}

	// 确保文件按时间排序
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "seg-") {
			continue
		}

		if strings.HasSuffix(file.Name(), ".json") {
			bs, err := ioutil.ReadFile(file.Name())
			if err != nil {
				logger.Errorf("failed to read file: %s, err: %v", file.Name(), err)
				continue
			}

			desc := Desc{}
			if err := json.Unmarshal(bs, &desc); err != nil {
				logger.Errorf("failed to unmarshal descfile: %v", err)
				continue
			}

			datafname := strings.ReplaceAll(file.Name(), ".json", ".data")
			mf, err := mmap.OpenMmapFile(datafname)
			if err != nil {
				logger.Errorf("failed to open mmapfile %s, err: %v", file.Name(), err)
				continue
			}

			diskseg := &diskSegment{
				dataFd:  mf,
				metaF:   strings.ReplaceAll(file.Name(), ".json", ".meta"),
				minTs:   desc.MinTs,
				maxTs:   desc.MaxTs,
				labelVs: newLabelValueSet(),
			}
			tsdb.segs.Add(diskseg)
		}
	}
}

func OpenTSDB() *TSDB {
	tsdb := &TSDB{
		segs: newSegmentList(),
		q:    make(chan []*Row, defaultQSize),
		srv:  newServer(),
	}

	tsdb.srv.ref = tsdb
	tsdb.loadFiles()

	worker := runtime.GOMAXPROCS(-1)
	tsdb.ctx, tsdb.cancel = context.WithCancel(context.Background())

	for i := 0; i < worker; i++ {
		go tsdb.ingestRows(tsdb.ctx)
	}

	go tsdb.srv.Run(":8099")

	return tsdb
}
