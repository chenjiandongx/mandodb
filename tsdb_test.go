package mandodb

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/chenjiandongx/logger"
	"github.com/stretchr/testify/assert"
)

// 模拟一些监控指标
var metrics = []string{
	"cpu.busy", "cpu.load1", "cpu.load5", "cpu.load15", "cpu.iowait",
	"disk.write.ops", "disk.read.ops", "disk.used",
	"net.in.bytes", "net.out.bytes", "net.in.packages", "net.out.packages",
	"mem.used", "mem.idle", "mem.used.bytes", "mem.total.bytes",
}

func genPoints(ts int64, node, dc int) []*Row {
	points := make([]*Row, 0)
	for _, metric := range metrics {
		points = append(points, &Row{
			Metric: metric,
			Labels: []Label{
				{Name: "node", Value: "vm" + strconv.Itoa(node)},
				{Name: "dc", Value: strconv.Itoa(dc)},
			},
			Point: Point{Ts: ts, Value: float64(ts)},
		})
	}

	return points
}

func TestTSDB_QueryRange(t *testing.T) {
	tmpdir := "/tmp/tsdb"

	store := OpenTSDB(WithDataPath(tmpdir), WithLoggerConfig(&logger.Options{
		Stdout:      true,
		ConsoleMode: true,
		Level:       logger.ErrorLevel,
	}))
	defer store.Close()
	defer os.RemoveAll(tmpdir)

	var start int64 = 1600000000

	var now = start
	for i := 0; i < 720; i++ {
		for n := 0; n < 3; n++ {
			for j := 0; j < 24; j++ {
				_ = store.InsertRows(genPoints(now, n, j))
			}
		}

		now += 60 //1min
	}

	time.Sleep(time.Millisecond * 20)

	ret, err := store.QueryRange("cpu.busy", LabelMatcherSet{
		{Name: "node", Value: "vm1"},
		{Name: "dc", Value: "0"},
	}, start, start+120)
	assert.NoError(t, err)

	ret[0].Labels.Sorted()
	labels := LabelSet{
		{"__name__", "cpu.busy"},
		{"dc", "0"},
		{"node", "vm1"},
	}
	assert.Equal(t, ret[0].Labels, labels)

	values := []int64{start, start + 60, start + 120}

	for idx, d := range ret[0].Points {
		assert.Equal(t, d.Ts, values[idx])
		assert.Equal(t, d.Value, float64(values[idx]))
	}

	ret, err = store.QueryRange("cpu.busy", LabelMatcherSet{
		{Name: "node", Value: "vm1"},
		{Name: "dc", Value: "0"},
	}, now-120, now)
	assert.NoError(t, err)
	assert.Equal(t, len(ret[0].Points), 2)
}

func TestTSDB_QuerySeries(t *testing.T) {
	tmpdir := "/tmp/tsdb"

	store := OpenTSDB(WithDataPath(tmpdir))
	defer store.Close()
	defer os.RemoveAll(tmpdir)

	var start int64 = 1600000000

	var now = start
	for i := 0; i < 720; i++ {
		for n := 0; n < 3; n++ {
			for j := 0; j < 24; j++ {
				_ = store.InsertRows(genPoints(now, n, j))
			}
		}

		now += 60 //1min
	}

	time.Sleep(time.Millisecond * 20)

	ret, err := store.QuerySeries(LabelMatcherSet{
		{Name: "__name__", Value: "disk.*", IsRegx: true},
		{Name: "node", Value: "vm1"},
		{Name: "dc", Value: "0"},
	}, start, start+120)
	assert.NoError(t, err)
	assert.Equal(t, len(ret), 3)
}

func TestTSDB_QueryLabelValues(t *testing.T) {
	tmpdir := "/tmp/tsdb"

	store := OpenTSDB(WithDataPath(tmpdir))
	defer store.Close()
	defer os.RemoveAll(tmpdir)

	var start int64 = 1600000000

	var now = start
	for i := 0; i < 720; i++ {
		for n := 0; n < 3; n++ {
			for j := 0; j < 24; j++ {
				_ = store.InsertRows(genPoints(now, n, j))
			}
		}

		now += 60 //1min
	}

	time.Sleep(time.Millisecond * 20)

	ret := store.QueryLabelValues("idc", start, start+120)
	assert.Equal(t, ret, []string{"0", "1", "2"})
}
