package mandodb

import (
	"bytes"
	"os"
	"path"
	"sync"
	"time"

	"github.com/chenjiandongx/logger"
	"github.com/dgryski/go-tsz"

	"github.com/chenjiandongx/mandodb/pkg/mmap"
)

// diskSegment 持久化 segment 磁盘数据使用 mmap 的方式按需加载
type diskSegment struct {
	dataFd       *mmap.MmapFile
	dataFilename string
	dir          string
	load         bool

	wg       sync.WaitGroup
	labelVs  *labelValueSet
	indexMap *diskIndexMap
	series   []metaSeries

	minTs int64
	maxTs int64

	seriesCount     int64
	dataPointsCount int64
}

type tocReader struct {
	reader *bytes.Reader
}

func (t *tocReader) Read() (int64, int64, error) {
	// 读取 dataBytes 长度
	dst := make([]byte, uint64Size)
	_, err := t.reader.ReadAt(dst, 0)
	if err != nil {
		return 0, 0, err
	}

	decf := newDecbuf()
	decf.UnmarshalUint64(dst)
	dataSize := decf.UnmarshalUint64(dst)

	// 读取 metaBytes 长度
	dst = make([]byte, uint64Size)
	_, err = t.reader.ReadAt(dst, uint64Size)
	if err != nil {
		return 0, 0, err
	}

	decf = newDecbuf()
	decf.UnmarshalUint64(dst)
	metaSize := decf.UnmarshalUint64(dst)

	return int64(dataSize), int64(metaSize), nil
}

func newDiskSegment(mf *mmap.MmapFile, dir string, minTs, maxTs int64) Segment {
	return &diskSegment{
		dataFd:       mf,
		dir:          dir,
		dataFilename: path.Join(dir, "meta.json"),
		minTs:        minTs,
		maxTs:        maxTs,
		labelVs:      newLabelValueSet(),
	}
}

func (ds *diskSegment) MinTs() int64 {
	return ds.minTs
}

func (ds *diskSegment) MaxTs() int64 {
	return ds.maxTs
}

func (ds *diskSegment) Frozen() bool {
	return true
}

func (ds *diskSegment) Type() SegmentType {
	return DiskSegmentType
}

func (ds *diskSegment) Close() error {
	ds.wg.Wait() // 确保没有进程在使用 fd
	return ds.dataFd.Close()
}

func (ds *diskSegment) Cleanup() error {
	return os.RemoveAll(ds.dir)
}

func (ds *diskSegment) shift() uint64 {
	return uint64Size * 2
}

func (ds *diskSegment) Load() Segment {
	// 仅加载一次即可
	if ds.load {
		return ds
	}

	t0 := time.Now()
	reader := bytes.NewReader(ds.dataFd.Bytes())

	tocRr := &tocReader{reader: reader}
	dataSize, metaSize, err := tocRr.Read()
	if err != nil {
		logger.Errorf("failed to read %s toc: %v", ds.dataFilename, err)
		return ds
	}

	metaBytes := make([]byte, metaSize)
	_, err = reader.ReadAt(metaBytes, uint64Size*2+int64(dataSize))
	if err != nil {
		logger.Errorf("failed to read %s meta-bytes: %v", ds.dataFilename, err)
		return ds
	}

	var meta Metadata
	if err := UnmarshalMeta(metaBytes, &meta); err != nil {
		logger.Errorf("failed to unmarshal meta: %v", err)
		return ds
	}

	for _, label := range meta.Labels {
		k, v := unmarshalLabelName(label.Name)
		if k != "" && v != "" {
			ds.labelVs.Set(k, v)
		}
	}

	ds.indexMap = newDiskIndexMap(meta.Labels)
	ds.series = meta.Series
	ds.load = true

	logger.Infof("load disk segment %s, take: %v", ds.dataFilename, time.Since(t0))
	return ds
}

func (ds *diskSegment) InsertRows(_ []*Row) {
	panic("BUG: disk segments are not mutable")
}

func (ds *diskSegment) QueryLabelValues(label string) []string {
	return ds.labelVs.Get(label)
}

func (ds *diskSegment) QuerySeries(lms LabelMatcherSet) ([]LabelSet, error) {
	sids := ds.indexMap.MatchSids(ds.labelVs, lms)
	ret := make([]LabelSet, 0)

	for _, sid := range sids {
		ret = append(ret, ds.indexMap.MatchLabels(ds.series[sid].Labels...))
	}

	return ret, nil
}

func (ds *diskSegment) QueryRange(lms LabelMatcherSet, start, end int64) ([]MetricRet, error) {
	ds.wg.Add(1)
	defer ds.wg.Done()

	sids := ds.indexMap.MatchSids(ds.labelVs, lms)

	ret := make([]MetricRet, 0)
	for _, sid := range sids {
		startOffset := ds.series[sid].StartOffset + ds.shift()
		endOffset := ds.series[sid].EndOffset + ds.shift()

		reader := bytes.NewReader(ds.dataFd.Bytes())
		dataBytes := make([]byte, endOffset-startOffset)
		_, err := reader.ReadAt(dataBytes, int64(startOffset))
		if err != nil {
			return nil, err
		}

		dataBytes, err = ByteDecompress(dataBytes)
		if err != nil {
			return nil, err
		}

		iter, err := tsz.NewIterator(dataBytes)
		if err != nil {
			return nil, err
		}

		points := make([]Point, 0)
		for iter.Next() {
			ts, val := iter.Values()
			if ts > uint32(end) {
				break
			}

			if ts >= uint32(start) && ts <= uint32(end) {
				points = append(points, Point{Ts: int64(ts), Value: val})
			}
		}

		lbs := ds.indexMap.MatchLabels(ds.series[sid].Labels...)
		ret = append(ret, MetricRet{Points: points, Labels: lbs})
	}

	return ret, nil
}
