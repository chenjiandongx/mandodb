package mandodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/chenjiandongx/logger"

	"github.com/chenjiandongx/mandodb/pkg/mmap"
)

type tsdbOptions struct {
	metaSerializer    MetaSerializer
	bytesCompressor   BytesCompressor
	retention         time.Duration
	segmentDuration   time.Duration
	writeTimeout      time.Duration
	onlyMemoryMode    bool
	enableOutdated    bool
	maxRowsPerSegment int64
	dataPath          string
}

var globalOpts = &tsdbOptions{
	metaSerializer:    newBinaryMetaSerializer(),
	bytesCompressor:   newNoopBytesCompressor(),
	segmentDuration:   2 * time.Hour,
	retention:         7 * 24 * time.Hour, // 7d
	writeTimeout:      30 * time.Second,
	onlyMemoryMode:    false,
	enableOutdated:    true,
	maxRowsPerSegment: 19960412,
	dataPath:          ".",
}

type Option func(c *tsdbOptions)

// WithMetaSerializerType 设置 Metadata 数据的序列化类型
// 目前只提供了 BinaryMetaSerializer
func WithMetaSerializerType(t MetaSerializerType) Option {
	return func(c *tsdbOptions) {
		switch t {
		default: // binary
			c.metaSerializer = newBinaryMetaSerializer()
		}
	}
}

// WithMetaBytesCompressorType 设置字节数据的压缩算法
// 目前提供了
// * 不压缩: NoopBytesCompressor（默认）
// * ZSTD: ZstdBytesCompressor
// * Snappy: SnappyBytesCompressor
func WithMetaBytesCompressorType(t BytesCompressorType) Option {
	return func(c *tsdbOptions) {
		switch t {
		case ZstdBytesCompressor:
			c.bytesCompressor = newZstdBytesCompressor()
		case SnappyBytesCompressor:
			c.bytesCompressor = newSnappyBytesCompressor()
		default: // noop
			c.bytesCompressor = newNoopBytesCompressor()
		}
	}
}

// WithOnlyMemoryMode 设置是否默认只存储在内存中
// 默认为 false
func WithOnlyMemoryMode(memoryMode bool) Option {
	return func(c *tsdbOptions) {
		c.onlyMemoryMode = memoryMode
	}
}

// WithEnabledOutdated 设置是否支持乱序写入 此特性会增加资源开销 但会提升数据完整性
// 默认为 true
func WithEnabledOutdated(outdated bool) Option {
	return func(c *tsdbOptions) {
		c.enableOutdated = outdated
	}
}

// WithMaxRowsPerSegment 设置单 Segment 最大允许存储的点数
// 默认为 19960412
func WithMaxRowsPerSegment(n int64) Option {
	return func(c *tsdbOptions) {
		c.maxRowsPerSegment = n
	}
}

// WithDataPath 设置 Segment 持久化存储文件夹
// 默认为 "."
func WithDataPath(d string) Option {
	return func(c *tsdbOptions) {
		c.dataPath = d
	}
}

// WithRetention 设置 Segment 持久化数据保存时长
// 默认为 7d
func WithRetention(t time.Duration) Option {
	return func(c *tsdbOptions) {
		c.retention = t
	}
}

// WithWriteTimeout 设置写入超时阈值
// 默认为 30s
func WithWriteTimeout(t time.Duration) Option {
	return func(c *tsdbOptions) {
		c.writeTimeout = t
	}
}

const (
	separator    = "/-/"
	defaultQSize = 128
)

// Point 表示一个数据点 (ts, value) 二元组
type Point struct {
	Ts    int64
	Value float64
}

func joinSeparator(a, b interface{}) string {
	return fmt.Sprintf("%v%s%v", a, separator, b)
}

func dirname(a, b int64) string {
	return path.Join(globalOpts.dataPath, fmt.Sprintf("seg-%d-%d", a, b))
}

// Row 一行时序数据 包括数据点和标签组合
type Row struct {
	Metric string
	Labels LabelSet
	Point  Point
}

// ID 使用 hash 计算 Series 的唯一标识
func (r Row) ID() string {
	return joinSeparator(xxhash.Sum64([]byte(r.Metric)), r.Labels.Hash())
}

type TSDB struct {
	segs *segmentList
	mut  sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	q  chan []*Row
	wg sync.WaitGroup
}

var timerPool sync.Pool

func getTimer(d time.Duration) *time.Timer {
	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		if t.Reset(d) {
			panic("active timer trapped to the pool")
		}
		return t
	}
	return time.NewTimer(d)
}

func putTimer(t *time.Timer) {
	if !t.Stop() {
		// Drain t.C if it wasn't obtained by the caller yet.
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}

func (tsdb *TSDB) InsertRows(rows []*Row) error {
	timer := getTimer(globalOpts.writeTimeout)
	select {
	case tsdb.q <- rows:
		putTimer(timer)
	case <-timer.C:
		putTimer(timer)
		return errors.New("failed to insert rows to database, write overloaded")
	}

	return nil
}

func (tsdb *TSDB) ingestRows(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case rs := <-tsdb.q:
			head, err := tsdb.getHeadPartition()
			if err != nil {
				logger.Errorf("failed to get head partition: %v", head)
				continue
			}
			head.InsertRows(rs)
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

			tsdb.segs.Add(head)

			t0 := time.Now()
			dn := dirname(head.MinTs(), head.MaxTs())

			if err := writeToDisk(head.(*memorySegment)); err != nil {
				logger.Errorf("failed to flush data to disk, %v", err)
				return
			}

			fname := path.Join(dn, "data")
			mf, err := mmap.OpenMmapFile(fname)
			if err != nil {
				logger.Errorf("failed to make a mmap file %s, %v", fname, err)
				return
			}

			tsdb.segs.Remove(head)
			tsdb.segs.Add(newDiskSegment(mf, path.Join(dn, "meta.json"), head.MinTs(), head.MaxTs()))
			logger.Infof("write file %s take: %v", fname, time.Since(t0))
		}()

		tsdb.segs.head = newMemorySegment()
	}

	return tsdb.segs.head, nil
}

type MetricRet struct {
	Labels LabelSet
	Points []Point
}

func (tsdb *TSDB) QueryRange(metric string, lms LabelMatcherSet, start, end int64) ([]MetricRet, error) {
	lms = lms.AddMetricName(metric)

	tmp := make([]MetricRet, 0)
	for _, segment := range tsdb.segs.Get(start, end) {
		segment = segment.Load()
		data, err := segment.QueryRange(lms, start, end)
		if err != nil {
			return nil, err
		}

		tmp = append(tmp, data...)
	}

	return tsdb.mergeQueryRangeResult(tmp...), nil
}

func (tsdb *TSDB) mergeQueryRangeResult(ret ...MetricRet) []MetricRet {
	metrics := make(map[uint64]*MetricRet)
	for _, r := range ret {
		h := r.Labels.Hash()
		v, ok := metrics[h]
		if !ok {
			metrics[h] = &MetricRet{
				Labels: r.Labels,
				Points: r.Points,
			}
			continue
		}

		v.Points = append(v.Points, r.Points...)
	}

	items := make([]MetricRet, 0, len(metrics))
	for _, v := range metrics {
		sort.Slice(v.Points, func(i, j int) bool {
			return v.Points[i].Ts < v.Points[j].Ts
		})

		items = append(items, *v)
	}

	return items
}

func (tsdb *TSDB) QuerySeries(lms LabelMatcherSet, start, end int64) ([]map[string]string, error) {
	tmp := make([]LabelSet, 0)
	for _, segment := range tsdb.segs.Get(start, end) {
		segment = segment.Load()
		data, err := segment.QuerySeries(lms)
		if err != nil {
			return nil, err
		}

		tmp = append(tmp, data...)
	}

	return tsdb.mergeQuerySeriesResult(tmp...), nil
}

func (tsdb *TSDB) mergeQuerySeriesResult(ret ...LabelSet) []map[string]string {
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
	tmp := make(map[string]struct{})
	for _, segment := range tsdb.segs.Get(start, end) {
		values := segment.QueryLabelValues(label)
		for i := 0; i < len(values); i++ {
			tmp[values[i]] = struct{}{}
		}
	}

	ret := make([]string, 0, len(tmp))
	for k := range tmp {
		ret = append(ret, k)
	}

	sort.Strings(ret)

	return ret
}

func (tsdb *TSDB) Close() {
	tsdb.wg.Wait()
	tsdb.cancel()

	it := tsdb.segs.lst.All()
	for it.Next() {
		it.Value().(Segment).Close()
	}

	tsdb.segs.head.Close()
}

func (tsdb *TSDB) removeExpires() {
	tick := time.Tick(5 * time.Minute)
	for {
		select {
		case <-tsdb.ctx.Done():
			return
		case <-tick:
			now := time.Now().Unix()

			var removed []Segment
			it := tsdb.segs.lst.All()
			for it.Next() {
				if now-it.Value().(Segment).MaxTs() > int64(globalOpts.retention.Seconds()) {
					removed = append(removed, it.Value().(Segment))
				}
			}

			for _, r := range removed {
				tsdb.segs.Remove(r)
			}
		}
	}
}

func (tsdb *TSDB) loadFiles() {
	err := filepath.Walk(globalOpts.dataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to read the dir: %s, err: %v", path, err)
		}

		if !info.IsDir() || !strings.HasPrefix(info.Name(), "seg-") {
			return nil
		}

		files, err := ioutil.ReadDir(info.Name())
		if err != nil {
			return fmt.Errorf("failed to load data storage, err: %v", err)
		}

		diskseg := &diskSegment{}

		for _, file := range files {
			fn := filepath.Join(info.Name(), file.Name())

			if file.Name() == "data" {
				mf, err := mmap.OpenMmapFile(fn)
				if err != nil {
					return fmt.Errorf("failed to open mmap file %s, err: %v", fn, err)
				}

				diskseg.dataFd = mf
				diskseg.dataFilename = fn
				diskseg.labelVs = newLabelValueSet()
			}

			if file.Name() == "meta.json" {
				bs, err := ioutil.ReadFile(fn)
				if err != nil {
					return fmt.Errorf("failed to read file: %s, err: %v", fn, err)
				}

				desc := Desc{}
				if err := json.Unmarshal(bs, &desc); err != nil {
					return fmt.Errorf("failed to unmarshal desc file: %v", err)
				}

				diskseg.minTs = desc.MinTs
				diskseg.maxTs = desc.MaxTs
			}
		}

		tsdb.segs.Add(diskseg)
		return nil
	})

	if err != nil {
		logger.Error(err)
	}
}

func OpenTSDB(opts ...Option) *TSDB {
	for _, opt := range opts {
		opt(globalOpts)
	}

	tsdb := &TSDB{
		segs: newSegmentList(),
		q:    make(chan []*Row, defaultQSize),
	}

	tsdb.loadFiles()

	worker := runtime.GOMAXPROCS(-1)
	tsdb.ctx, tsdb.cancel = context.WithCancel(context.Background())

	for i := 0; i < worker; i++ {
		go tsdb.ingestRows(tsdb.ctx)
	}
	go tsdb.removeExpires()

	return tsdb
}
