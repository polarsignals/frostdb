package expr

import (
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

type fakeParticulate struct {
	schema       *parquet.Schema
	columnChunks []parquet.ColumnChunk
}

// newFakeParticulate creates a new fake particulate. maxValues[i] is the
// maximum value of the ith column. If maxValues[i] is -1, then the ith column
// index will return that it is full of nulls.
func newFakeParticulate(columnNames []string, maxValues []int64) fakeParticulate {
	if len(columnNames) != len(maxValues) {
		panic("columnNames and maxValues must have the same length")
	}
	vForName := make(map[string]int64)
	for i, name := range columnNames {
		vForName[name] = maxValues[i]
	}

	g := parquet.Group{}
	for _, name := range columnNames {
		g[name] = parquet.Int(64)
	}
	s := parquet.NewSchema("", g)

	columnChunks := make([]parquet.ColumnChunk, len(columnNames))
	// Iterate over the schema in creation order (not doing so mixes up which
	// column has which value).
	for i, f := range s.Fields() {
		numValues := int64(1)
		numNulls := int64(0)
		var maxV parquet.Value
		if v := vForName[f.Name()]; v == -1 {
			maxV = parquet.ValueOf(nil)
			numNulls = numValues
		} else {
			maxV = parquet.ValueOf(v)
		}
		columnChunks[i] = &FakeColumnChunk{
			index: &FakeColumnIndex{
				numPages:  1,
				min:       parquet.Value{},
				max:       maxV,
				nullCount: numNulls,
			},
			numValues: numValues,
		}
	}
	return fakeParticulate{
		schema:       s,
		columnChunks: columnChunks,
	}
}

func (f fakeParticulate) Schema() *parquet.Schema {
	return f.schema
}

func (f fakeParticulate) ColumnChunks() []parquet.ColumnChunk {
	return f.columnChunks
}

func TestMaxAgg(t *testing.T) {
	for _, tc := range []struct {
		name string
		agg  *MaxAgg
		// values[i] is the ith particulate.
		values []struct {
			p fakeParticulate
			// expected result of Eval(values[i]).
			expected bool
		}
	}{
		{
			name: "ConcreteColumn",
			agg: &MaxAgg{
				columnName: "a",
			},
			values: []struct {
				p        fakeParticulate
				expected bool
			}{
				{
					newFakeParticulate(
						[]string{"anotfound"},
						[]int64{3},
					),
					// Column not found.
					false,
				},
				{
					newFakeParticulate(
						[]string{"a.a"},
						[]int64{3},
					),
					// Column not found.
					false,
				},
				{
					newFakeParticulate(
						[]string{"a"},
						[]int64{2},
					),
					true,
				},
				{
					newFakeParticulate(
						[]string{"a"},
						[]int64{1},
					),
					// Values is less than the current max.
					false,
				},
				{
					newFakeParticulate(
						[]string{"b.a", "a"},
						[]int64{-1, 3},
					),
					true,
				},
			},
		},
		{
			name: "DynamicColumn",
			agg: &MaxAgg{
				columnName: "a",
				dynamic:    true,
			},
			values: []struct {
				p        fakeParticulate
				expected bool
			}{
				{
					newFakeParticulate(
						[]string{"a"},
						[]int64{3},
					),
					// Not a dynamic column.
					false,
				},
				{
					newFakeParticulate(
						[]string{"a.a", "a"},
						[]int64{-1, 3},
					),
					// Nulls in the dynamic column.
					false,
				},
				{
					newFakeParticulate(
						[]string{"a.a", "a.b", "a.c"},
						[]int64{3, 3, 3},
					),
					// This particulate should pass the filter. Also, this
					// particulate verifies that the filter memoizes the max for
					// all columns in the particulate for the future.
					true,
				},
				{
					newFakeParticulate(
						[]string{"a.a"},
						[]int64{1},
					),
					// Lower than max.
					false,
				},
				{
					newFakeParticulate(
						[]string{"a.b"},
						[]int64{1},
					),
					// Lower than max.
					false,
				},
				{
					newFakeParticulate(
						[]string{"a.c"},
						[]int64{1},
					),
					// Lower than max.
					false,
				},
				{
					newFakeParticulate(
						[]string{"a.a", "a.b"},
						[]int64{1, 4},
					),
					// First column is lower, but second column is higher.
					true,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i, v := range tc.values {
				got, err := tc.agg.Eval(v.p, false)
				require.NoError(t, err)
				require.Equal(t, v.expected, got, "index %d mismatch", i)
			}
		})
	}
}
