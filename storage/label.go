package storage

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash"
)

var labelBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

type Label struct {
	Name  string
	Value string
}

func (l Label) MarshalName() string {
	return joinSeparator(l.Name, l.Value)
}

func unmarshalLabelName(s string) (string, string) {
	pair := strings.SplitN(s, separator, 2)
	if len(pair) != 2 {
		return "", ""
	}

	return pair[0], pair[1]
}

type labelValueSet struct {
	mut    sync.Mutex
	values map[string]map[string]struct{}
}

func newLabelValueSet() *labelValueSet {
	return &labelValueSet{
		values: map[string]map[string]struct{}{},
	}
}

func (lvs *labelValueSet) Set(label, value string) {
	lvs.mut.Lock()
	defer lvs.mut.Unlock()

	if _, ok := lvs.values[label]; !ok {
		lvs.values[label] = make(map[string]struct{})
	}

	lvs.values[label][value] = struct{}{}
}

func (lvs *labelValueSet) Get(label string) []string {
	lvs.mut.Lock()
	defer lvs.mut.Unlock()

	ret := make([]string, 0)
	vs, ok := lvs.values[label]
	if !ok {
		return ret
	}

	for k := range vs {
		ret = append(ret, k)
	}

	return ret
}

type LabelSet []Label

// filter 过滤空 kv 和重复数据
func (ls LabelSet) filter() LabelSet {
	mark := make(map[string]struct{})
	var size int
	for _, v := range ls {
		_, ok := mark[v.Name]
		if v.Name != "" && v.Value != "" && !ok {
			ls[size] = v // 复用原来的 slice
			size++
		}
		mark[v.Name] = struct{}{}
	}

	return ls[:size]
}

func (ls LabelSet) Map() map[string]string {
	m := make(map[string]string)
	for _, label := range ls {
		m[label.Name] = label.Value
	}

	return m
}

func (ls LabelSet) Len() int           { return len(ls) }
func (ls LabelSet) Less(i, j int) bool { return ls[i].Name < ls[j].Name }
func (ls LabelSet) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }

// AddMetricName 将指标名称也当成一个 label 处理 在存储的时候并不做特性的区分
// 每个指标的最后一个 label 就是 metricName
func (ls LabelSet) AddMetricName(metric string) LabelSet {
	labels := ls.filter()
	labels = append(labels, Label{
		Name:  metricName,
		Value: metric,
	})
	return labels
}

func (ls LabelSet) Hash() uint64 {
	sort.Sort(ls) // 保证每次 hash 结果一致
	b := labelBufPool.Get().([]byte)

	const sep = '\xff'
	for _, v := range ls {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	h := xxhash.Sum64(b)

	b = b[:0]
	labelBufPool.Put(b) // 复用 buffer

	return h
}

func (ls LabelSet) Has(name string) bool {
	for _, label := range ls {
		if label.Name == name {
			return true
		}
	}

	return false
}

// String 用户格式化输出
func (ls LabelSet) String() string {
	var b bytes.Buffer

	b.WriteByte('{')
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
	}
	b.WriteByte('}')
	return b.String()
}
