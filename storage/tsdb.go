package storage

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"sync"

	"github.com/chenjiandongx/mandodb/toolkit/mmap"
)

// TODO: list
// * 处理 Outdated 数据 -> skiplist
// * 归档数据使用 snappy/zstd 压缩
// * 磁盘文件合并 参考 leveldb
// * WAL 做灾备

type DataPoint struct {
	Ts    int64
	Value float64
}

func joinSeparator(a, b interface{}) string {
	const separator = "/-/"
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

func (r Row) ID() string {
	return joinSeparator(r.Metric, r.Labels.Hash())
}

type MetricRet struct {
	Labels     []Label
	DataPoints []DataPoint
}

type TSDB struct {
	segs *SegmentList
	mut  sync.Mutex
}

func (tsdb *TSDB) InsertRows(rows []*Row) error {
	// 加锁确保 head 的状态对外都是一致的
	// TODO: 这个锁对性能影响太大了 得想办法优化
	tsdb.mut.Lock()
	if tsdb.segs.head.Frozen() {
		prefix := filePrefix(tsdb.segs.head.MinTs(), tsdb.segs.head.MaxTs())
		meta, err := writeToDisk(tsdb.segs.head)
		if err != nil {
			return fmt.Errorf("failed to flush data to disk, %v", err)
		}

		mf, err := mmap.OpenMmapFile(prefix + "data")
		if err != nil {
			return fmt.Errorf("failed to make a mmap file, %v", err)
		}

		tsdb.segs.Add(newDiskSegment(mf, meta, tsdb.segs.head.MinTs(), tsdb.segs.head.MaxTs()))
		tsdb.segs.head = newMemorySegment()
	}
	tsdb.mut.Unlock()

	tsdb.segs.head.InsertRows(rows)
	return nil
}

func (tsdb *TSDB) QueryRange(metric string, labels LabelSet, start, end int64) {
	labels = labels.AddMetricName(metric)

	ret := tsdb.segs.Get(start, end)
	for _, r := range ret {
		fmt.Println("Query from:", r.Type())
		dps, err := r.QueryRange(labels, start, end)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Ret: %+v\n", dps)
	}
}

func (tsdb *TSDB) MergeResult(ret ...MetricRet) []MetricRet {
	return nil
}

func (tsdb *TSDB) Close() {
	for _, segment := range tsdb.segs.lst {
		segment.Close()
	}

	tsdb.segs.head.Close()
}

func (tsdb *TSDB) loadFiles() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic(err)
	}

	// 确保文件按时间排序
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "seg-") {
			continue
		}

		if strings.HasSuffix(file.Name(), ".meta") {
			bs, err := ioutil.ReadFile(file.Name())
			if err != nil {
				panic(err)
			}

			// TODO: 这里需要校验数据的合法性 CRC32...
			meta := Metadata{}
			UnmarshalMeta(bs, &meta)

			datafname := strings.ReplaceAll(file.Name(), ".meta", ".data")
			fmt.Println("datafname:", datafname)
			mf, err := mmap.OpenMmapFile(datafname)
			if err != nil {
				panic(err)
			}

			diskseg := &diskSegment{
				mf:        mf,
				metricIdx: buildIndexMapForDisk(meta.Labels),
				series:    meta.Series,
				minTs:     meta.MinTs,
				maxTs:     meta.MaxTs,
			}
			tsdb.segs.Add(diskseg)
		}
	}
}

func OpenTSDB() *TSDB {
	tsdb := &TSDB{segs: newSegmentList()}
	tsdb.loadFiles()

	return tsdb
}
