package physicalplan

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestBuildIndexRanges(t *testing.T) {
	arr := []uint32{4, 6, 7, 8, 10}
	ranges := buildIndexRanges(arr)

	require.Equal(t, 3, len(ranges))
	require.Equal(t, []IndexRange{
		{Start: 4, End: 5},
		{Start: 6, End: 9},
		{Start: 10, End: 11},
	}, ranges)
}

func Test_BuildIndexRanges(t *testing.T) {
	tests := map[string]struct {
		indicies []uint32
		expected []IndexRange
	}{
		"no consecutive": {
			indicies: []uint32{1, 3, 5, 7, 9},
			expected: []IndexRange{{Start: 1, End: 2}, {Start: 3, End: 4}, {Start: 5, End: 6}, {Start: 7, End: 8}, {Start: 9, End: 10}},
		},
		"only consecutive": {
			indicies: []uint32{1, 2},
			expected: []IndexRange{{Start: 1, End: 3}},
		},
		"only 1": {
			indicies: []uint32{1},
			expected: []IndexRange{{Start: 1, End: 2}},
		},
		"multiple": {
			indicies: []uint32{1, 2, 7, 8, 9},
			expected: []IndexRange{{Start: 1, End: 3}, {Start: 7, End: 10}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.expected, buildIndexRanges(test.indicies))
		})
	}
}

type mockExpression struct {
	evalFn func(arrow.Record) (*Bitmap, error)
}

var _ BooleanExpression = mockExpression{}

func (e mockExpression) Eval(record arrow.Record) (*Bitmap, error) {
	return e.evalFn(record)
}

func (e mockExpression) String() string {
	return ""
}

func TestAndExprShortCircuits(t *testing.T) {
	left := mockExpression{evalFn: func(_ arrow.Record) (*Bitmap, error) {
		return NewBitmap(), nil
	}}
	right := mockExpression{evalFn: func(_ arrow.Record) (*Bitmap, error) {
		t.Fatal("right should not be evaluated")
		return nil, nil
	}}
	andExpr := AndExpr{
		Left:  left,
		Right: right,
	}
	result, err := andExpr.Eval(nil)
	require.NoError(t, err)
	require.True(t, result.IsEmpty())
}
