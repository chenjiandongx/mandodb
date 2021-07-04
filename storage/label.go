package storage

import (
	"bytes"
	"regexp"
	"regexp/syntax"
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

const (
	labelValuesRegxPrefix = "$Regx("
	labelValuesRegxSuffix = ")"
)

// fastRegexMatcher 是一种优化的正则匹配器 算法来自 prometheus
type fastRegexMatcher struct {
	re       *regexp.Regexp
	prefix   string
	suffix   string
	contains string
}

func newFastRegexMatcher(v string) (*fastRegexMatcher, error) {
	re, err := regexp.Compile("^(?:" + v + ")$")
	if err != nil {
		return nil, err
	}

	// 语法解析
	parsed, err := syntax.Parse(v, syntax.Perl)
	if err != nil {
		return nil, err
	}

	m := &fastRegexMatcher{
		re: re,
	}

	if parsed.Op == syntax.OpConcat {
		m.prefix, m.suffix, m.contains = optimizeConcatRegex(parsed)
	}

	return m, nil
}

func (m *fastRegexMatcher) MatchString(s string) bool {
	if m.prefix != "" && !strings.HasPrefix(s, m.prefix) {
		return false
	}

	if m.suffix != "" && !strings.HasSuffix(s, m.suffix) {
		return false
	}

	if m.contains != "" && !strings.Contains(s, m.contains) {
		return false
	}
	return m.re.MatchString(s)
}

func optimizeConcatRegex(r *syntax.Regexp) (prefix, suffix, contains string) {
	sub := r.Sub

	// 移除前缀空格
	if len(sub) > 0 && sub[0].Op == syntax.OpBeginText {
		sub = sub[1:]
	}

	// 移除后缀空格
	if len(sub) > 0 && sub[len(sub)-1].Op == syntax.OpEndText {
		sub = sub[:len(sub)-1]
	}

	if len(sub) == 0 {
		return
	}

	// 如果前缀和后缀是正常字符的话可以直接标记下来
	if sub[0].Op == syntax.OpLiteral {
		prefix = string(sub[0].Rune)
	}
	if last := len(sub) - 1; sub[last].Op == syntax.OpLiteral {
		suffix = string(sub[last].Rune)
	}

	// 这里已经去除首尾了 匹配中间的字符串
	for i := 1; i < len(sub)-1; i++ {
		if sub[i].Op == syntax.OpLiteral {
			contains = string(sub[i].Rune)
			break
		}
	}

	return
}

// Match 主要用于正则匹配 Labels 组合 函数标识 $Regx()
func (lvs *labelValueSet) Match(label, value string) []string {
	ret := make([]string, 0)
	if strings.HasPrefix(value, labelValuesRegxPrefix) && strings.HasSuffix(value, labelValuesRegxSuffix) {
		pattern, err := newFastRegexMatcher(value[len(labelValuesRegxPrefix) : len(value)-1])
		if err != nil {
			return []string{value}
		}

		for _, v := range lvs.Get(label) {
			if pattern.MatchString(v) {
				ret = append(ret, v)
			}
		}

		return ret
	}

	return []string{value}
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

// Metric 返回 __name__ 指标名称
func (ls LabelSet) Metric() string {
	for _, l := range ls {
		if l.Name == metricName {
			return l.Value
		}
	}

	return ""
}

// Map 将 Label 列表转换成 map
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

func (ls LabelSet) Sorted() {
	sort.Sort(ls)
}

// Hash 哈希计算 LabelSet 唯一标识符
func (ls LabelSet) Hash() uint64 {
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

// Has 判断 label 是否存在
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
