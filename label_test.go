package mandodb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabels_String(t *testing.T) {
	cases := []struct {
		lables   LabelSet
		expected string
	}{
		{
			lables: LabelSet{
				{
					Name:  "t1",
					Value: "t1",
				},
				{
					Name:  "t2",
					Value: "t2",
				},
			},
			expected: "{t1=\"t1\", t2=\"t2\"}",
		},
		{
			lables:   LabelSet{},
			expected: "{}",
		},
		{
			lables:   nil,
			expected: "{}",
		},
	}
	for _, c := range cases {
		str := c.lables.String()
		require.Equal(t, c.expected, str)
	}
}

func TestLabels_Has(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{
			input:    "foo",
			expected: false,
		},
		{
			input:    "aaa",
			expected: true,
		},
	}

	labelsSet := LabelSet{
		{
			Name:  "aaa",
			Value: "111",
		},
		{
			Name:  "bbb",
			Value: "222",
		},
	}

	for i, test := range tests {
		got := labelsSet.Has(test.input)
		require.Equal(t, test.expected, got, "unexpected comparison result for test case %d", i)
	}
}

func TestLabels_Hash(t *testing.T) {
	lbls := LabelSet{
		{Name: "foo", Value: "bar"},
		{Name: "baz", Value: "qux"},
	}
	require.Equal(t, lbls.Hash(), lbls.Hash())
	require.NotEqual(t, lbls.Hash(), LabelSet{lbls[1], lbls[0]}.Hash(), "unordered labels match.")
	require.NotEqual(t, lbls.Hash(), LabelSet{lbls[0]}.Hash(), "different labels match.")
}


func TestLabels_WithoutEmpty(t *testing.T) {
	for _, test := range []struct {
		input    LabelSet
		expected LabelSet
	}{
		{
			input: LabelSet{
				{Name: "foo"},
				{Name: "bar"},
			},
			expected: LabelSet{},
		},
		{
			input: LabelSet{
				{Name: "foo"},
				{Name: "bar"},
				{Name: "baz"},
			},
			expected: LabelSet{},
		},
		{
			input: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "hostname", Value: "localhost"},
				{Name: "job", Value: "check"},
			},
			expected: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "hostname", Value: "localhost"},
				{Name: "job", Value: "check"},
			},
		},
		{
			input: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "hostname", Value: "localhost"},
				{Name: "bar"},
				{Name: "job", Value: "check"},
			},
			expected: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "hostname", Value: "localhost"},
				{Name: "job", Value: "check"},
			},
		},
		{
			input: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "foo"},
				{Name: "hostname", Value: "localhost"},
				{Name: "bar"},
				{Name: "job", Value: "check"},
			},
			expected: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "hostname", Value: "localhost"},
				{Name: "job", Value: "check"},
			},
		},
		{
			input: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "foo"},
				{Name: "baz"},
				{Name: "hostname", Value: "localhost"},
				{Name: "bar"},
				{Name: "job", Value: "check"},
			},
			expected: LabelSet{
				{Name: "__name__", Value: "test"},
				{Name: "hostname", Value: "localhost"},
				{Name: "job", Value: "check"},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, test.expected, test.input.filter())
		})
	}
}