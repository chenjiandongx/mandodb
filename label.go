package mandodb

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

func unmarshalLabelName(s string) (string, string) {
	pair := strings.SplitN(s, separator, 2)
	if len(pair) != 2 {
		return "", ""
	}

	return pair[0], pair[1]
}

// Label 代表一个标签组合
type Label struct {
	Name  string
	Value string
}

func (l Label) MarshalName() string {
	return joinSeparator(l.Name, l.Value)
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

// fastRegexMatcher 是一种优化的正则匹配器 算法来自 Prometheus
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

// optimizeConcatRegex returns literal prefix/suffix text that can be safely
// checked against the label value before running the regexp matcher.
func optimizeConcatRegex(r *syntax.Regexp) (prefix, suffix, contains string) {
	sub := r.Sub

	// We can safely remove begin and end text matchers respectively
	// at the beginning and end of the regexp.
	if len(sub) > 0 && sub[0].Op == syntax.OpBeginText {
		sub = sub[1:]
	}
	if len(sub) > 0 && sub[len(sub)-1].Op == syntax.OpEndText {
		sub = sub[:len(sub)-1]
	}

	if len(sub) == 0 {
		return
	}

	// Given Prometheus regex matchers are always anchored to the begin/end
	// of the text, if the first/last operations are literals, we can safely
	// treat them as prefix/suffix.
	if sub[0].Op == syntax.OpLiteral && (sub[0].Flags&syntax.FoldCase) == 0 {
		prefix = string(sub[0].Rune)
	}
	if last := len(sub) - 1; sub[last].Op == syntax.OpLiteral && (sub[last].Flags&syntax.FoldCase) == 0 {
		suffix = string(sub[last].Rune)
	}

	// If contains any literal which is not a prefix/suffix, we keep the
	// 1st one. We do not keep the whole list of literals to simplify the
	// fast path.
	for i := 1; i < len(sub)-1; i++ {
		if sub[i].Op == syntax.OpLiteral && (sub[i].Flags&syntax.FoldCase) == 0 {
			contains = string(sub[i].Rune)
			break
		}
	}

	return
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

// Match 主要用于匹配 Labels 组合 支持正则匹配
func (lvs *labelValueSet) Match(matcher LabelMatcher) []string {
	ret := make([]string, 0)
	if matcher.IsRegx {
		pattern, err := newFastRegexMatcher(matcher.Value)
		if err != nil {
			return []string{matcher.Value}
		}

		for _, v := range lvs.Get(matcher.Name) {
			if pattern.MatchString(v) {
				ret = append(ret, v)
			}
		}

		return ret
	}

	return []string{matcher.Value}
}

// LabelSet 表示 Label 组合
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

// LabelMatcher Label 匹配器 支持正则
type LabelMatcher struct {
	Name   string
	Value  string
	IsRegx bool
}

// LabelMatcherSet 表示 LabelMatcher 组合
type LabelMatcherSet []LabelMatcher

// AddMetricName 将指标名称也当成一个 label 处理 在存储的时候并不做特性的区分
// 每个指标的最后一个 label 就是 metricName
func (lms LabelMatcherSet) AddMetricName(metric string) LabelMatcherSet {
	labels := lms.filter()

	newl := LabelMatcher{
		Name:  metricName,
		Value: metric,
	}
	labels = append(labels, newl)
	return labels
}

// filter 过滤空 kv 和重复数据
func (lms LabelMatcherSet) filter() LabelMatcherSet {
	mark := make(map[string]struct{})
	var size int
	for _, v := range lms {
		_, ok := mark[v.Name]
		if v.Name != "" && v.Value != "" && !ok {
			lms[size] = v // 复用原来的 slice
			size++
		}
		mark[v.Name] = struct{}{}
	}

	return lms[:size]
}
