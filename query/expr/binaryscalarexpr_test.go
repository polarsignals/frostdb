package expr

import (
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type FakeColumnChunk struct {
	index     *FakeColumnIndex
	numValues int64
}

func (f *FakeColumnChunk) Type() parquet.Type                        { return nil }
func (f *FakeColumnChunk) Column() int                               { return 0 }
func (f *FakeColumnChunk) Pages() parquet.Pages                      { return nil }
func (f *FakeColumnChunk) ColumnIndex() (parquet.ColumnIndex, error) { return f.index, nil }
func (f *FakeColumnChunk) OffsetIndex() (parquet.OffsetIndex, error) { return nil, nil }
func (f *FakeColumnChunk) BloomFilter() parquet.BloomFilter          { return nil }
func (f *FakeColumnChunk) NumValues() int64                          { return f.numValues }

type FakeColumnIndex struct {
	numPages  int
	min       parquet.Value
	max       parquet.Value
	nullCount int64
}

func (f *FakeColumnIndex) NumPages() int {
	return f.numPages
}
func (f *FakeColumnIndex) NullCount(int) int64        { return f.nullCount }
func (f *FakeColumnIndex) NullPage(int) bool          { return false }
func (f *FakeColumnIndex) MinValue(int) parquet.Value { return f.min }
func (f *FakeColumnIndex) MaxValue(int) parquet.Value { return f.max }
func (f *FakeColumnIndex) IsAscending() bool          { return false }
func (f *FakeColumnIndex) IsDescending() bool         { return false }

// This is a regression test that ensures the Min/Max functions return a null
// value (instead of panicing) should they be passed a column chunk that only
// has null values.
func Test_MinMax_EmptyColumnChunk(t *testing.T) {
	fakeIndex := &FakeColumnIndex{
		numPages: 10,
	}

	v := Min(fakeIndex)
	require.True(t, v.IsNull())

	v = Max(fakeIndex)
	require.True(t, v.IsNull())
}

func TestBinaryScalarOperation(t *testing.T) {
	const numValues = 10
	for _, tc := range []struct {
		name string
		min  int
		max  int
		// -1 is interpreted as a null value.
		right     int
		nullCount int64
		op        logicalplan.Op
		// expectSatisfies is true if the predicate should be satisfied by the
		// column chunk.
		expectSatisfies bool
	}{
		{
			name:            "OpEqValueContained",
			min:             1,
			max:             10,
			right:           5,
			op:              logicalplan.OpEq,
			expectSatisfies: true,
		},
		{
			name:            "OpEqValueGt",
			min:             1,
			max:             10,
			right:           11,
			op:              logicalplan.OpEq,
			expectSatisfies: false,
		},
		{
			name:            "OpEqValueLt",
			min:             1,
			max:             10,
			right:           0,
			op:              logicalplan.OpEq,
			expectSatisfies: false,
		},
		{
			name:            "OpEqMaxBound",
			min:             1,
			max:             10,
			right:           10,
			op:              logicalplan.OpEq,
			expectSatisfies: true,
		},
		{
			name:            "OpEqMinBound",
			min:             1,
			max:             10,
			right:           1,
			op:              logicalplan.OpEq,
			expectSatisfies: true,
		},
		{
			name:            "OpEqNullValueNoMatch",
			right:           -1,
			nullCount:       0,
			op:              logicalplan.OpEq,
			expectSatisfies: false,
		},
		{
			name:            "OpEqNullValueMatch",
			right:           -1,
			nullCount:       1,
			op:              logicalplan.OpEq,
			expectSatisfies: true,
		},
		{
			name:            "OpEqNullColumn",
			right:           1,
			nullCount:       1,
			op:              logicalplan.OpEq,
			expectSatisfies: false,
		},
		{
			name:            "OpEqFullNullColumn",
			right:           1,
			nullCount:       10,
			op:              logicalplan.OpEq,
			expectSatisfies: false,
		},
		{
			name:            "OpGtFullNullColumn",
			right:           1,
			nullCount:       10,
			op:              logicalplan.OpGt,
			expectSatisfies: false,
		},
		{
			name:            "OpGtNullValueMatch",
			right:           -1,
			nullCount:       0,
			op:              logicalplan.OpGt,
			expectSatisfies: true,
		},
		{
			name:      "OpGtNullValueNoMatch",
			right:     -1,
			nullCount: 1,
			op:        logicalplan.OpGt,
			// expectSatisfies should probably be false once we optimize this.
			expectSatisfies: true,
		},
		{
			name:            "OpGtWithSomeNullValuesNoMatch",
			min:             1,
			max:             10,
			right:           11,
			nullCount:       1,
			op:              logicalplan.OpGt,
			expectSatisfies: false,
		},
		{
			name:            "OpGtWithSomeNullValuesMatch",
			min:             1,
			max:             10,
			right:           5,
			nullCount:       1,
			op:              logicalplan.OpGt,
			expectSatisfies: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.op == logicalplan.OpUnknown {
				t.Fatal("test programming error: remember to set operator")
			}
			minV := parquet.ValueOf(tc.min)
			maxV := parquet.ValueOf(tc.max)
			if tc.nullCount == numValues {
				// All values in page are null. Parquet doesn't have
				// well-defined min/max values in this case, but setting them
				// explicitly to null here will tickle some edge cases.
				minV = parquet.ValueOf(nil)
				maxV = parquet.ValueOf(nil)
			}
			fakeChunk := &FakeColumnChunk{
				index: &FakeColumnIndex{
					numPages:  1,
					min:       minV,
					max:       maxV,
					nullCount: tc.nullCount,
				},
				numValues: numValues,
			}
			var v parquet.Value
			if tc.right == -1 {
				v = parquet.ValueOf(nil)
			} else {
				v = parquet.ValueOf(tc.right)
			}
			res, err := BinaryScalarOperation(fakeChunk, v, tc.op)
			require.NoError(t, err)
			require.Equal(t, tc.expectSatisfies, res)
		})
	}
}
