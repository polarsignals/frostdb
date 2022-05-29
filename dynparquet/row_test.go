package dynparquet

import (
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

type TestStructMiddleList struct {
	A int64
	B []int64
	C int64
}

type TestStructEndList struct {
	A int64
	B int64
	C []int64
}

func TestValuesForIndex(t *testing.T) {
	testCases := []struct {
		name   string
		input  interface{}
		index  int
		expect []parquet.Value
	}{{
		name: "middle-list-first",
		input: &TestStructMiddleList{
			A: 1,
			B: []int64{2, 3, 4},
			C: 5,
		},
		index:  0,
		expect: []parquet.Value{parquet.ValueOf(int64(1)).Level(0, 0, 0)},
	}, {
		name: "middle-list-middle",
		input: &TestStructMiddleList{
			A: 1,
			B: []int64{2, 3, 4},
			C: 5,
		},
		index: 1,
		expect: []parquet.Value{
			parquet.ValueOf(int64(2)).Level(0, 1, 1),
			parquet.ValueOf(int64(3)).Level(1, 1, 1),
			parquet.ValueOf(int64(4)).Level(1, 1, 1),
		},
	}, {
		name: "middle-list-middle-empty",
		input: &TestStructMiddleList{
			A: 1,
			B: []int64{},
			C: 5,
		},
		index: 1,
		expect: []parquet.Value{
			parquet.ValueOf(nil).Level(0, 0, 1),
		},
	}, {
		name: "middle-list-last",
		input: &TestStructMiddleList{
			A: 1,
			B: []int64{2, 3, 4},
			C: 5,
		},
		index:  2,
		expect: []parquet.Value{parquet.ValueOf(int64(5)).Level(0, 0, 2)},
	}, {
		name: "end-list-last",
		input: &TestStructEndList{
			A: 1,
			B: 2,
			C: []int64{3, 4, 5},
		},
		index: 2,
		expect: []parquet.Value{
			parquet.ValueOf(int64(3)).Level(0, 1, 2),
			parquet.ValueOf(int64(4)).Level(1, 1, 2),
			parquet.ValueOf(int64(5)).Level(1, 1, 2),
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := parquet.SchemaOf(tc.input)
			values := ValuesForIndex(s.Deconstruct(nil, tc.input), tc.index)
			require.Equal(t, tc.expect, values)
		})
	}
}

func TestLess(t *testing.T) {
	schema := NewSampleSchema()
	samples := NewTestSamples()

	rowGroups := []DynamicRowGroup{}
	for _, sample := range samples {
		s := Samples{sample}
		rg, err := s.ToBuffer(schema)
		require.NoError(t, err)
		rowGroups = append(rowGroups, rg)
	}

	var err error
	row1 := &DynamicRows{
		Schema:         rowGroups[0].Schema(),
		DynamicColumns: rowGroups[0].DynamicColumns(),
		Rows:           make([]parquet.Row, 1),
	}
	n, err := rowGroups[0].Rows().ReadRows(row1.Rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	row2 := &DynamicRows{
		Schema:         rowGroups[1].Schema(),
		DynamicColumns: rowGroups[1].DynamicColumns(),
		Rows:           make([]parquet.Row, 1),
	}
	n, err = rowGroups[1].Rows().ReadRows(row2.Rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	row3 := &DynamicRows{
		Schema:         rowGroups[2].Schema(),
		DynamicColumns: rowGroups[2].DynamicColumns(),
		Rows:           make([]parquet.Row, 1),
	}
	n, err = rowGroups[2].Rows().ReadRows(row3.Rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.True(t, schema.RowLessThan(row1.Get(0), row2.Get(0)))
	require.True(t, schema.RowLessThan(row1.Get(0), row3.Get(0)))
	require.True(t, schema.RowLessThan(row2.Get(0), row3.Get(0)))
	require.False(t, schema.RowLessThan(row2.Get(0), row1.Get(0)))
	require.False(t, schema.RowLessThan(row3.Get(0), row1.Get(0)))
	require.False(t, schema.RowLessThan(row3.Get(0), row2.Get(0)))
}

func TestLessWithDynamicSchemas(t *testing.T) {
	schema := NewSampleSchema()
	samples := Samples{{
		Labels: []Label{
			{Name: "label12", Value: "value12"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []Label{
			{Name: "label14", Value: "value14"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	rowGroups := []DynamicRowGroup{}
	for _, sample := range samples {
		s := Samples{sample}
		rg, err := s.ToBuffer(schema)
		require.NoError(t, err)
		rowGroups = append(rowGroups, rg)
	}

	var err error
	row1 := &DynamicRows{
		Schema:         rowGroups[0].Schema(),
		DynamicColumns: rowGroups[0].DynamicColumns(),
		Rows:           make([]parquet.Row, 1),
	}
	n, err := rowGroups[0].Rows().ReadRows(row1.Rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	row2 := &DynamicRows{
		Schema:         rowGroups[1].Schema(),
		DynamicColumns: rowGroups[1].DynamicColumns(),
		Rows:           make([]parquet.Row, 1),
	}
	n, err = rowGroups[1].Rows().ReadRows(row2.Rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.True(t, schema.RowLessThan(row2.Get(0), row1.Get(0)))
	require.False(t, schema.RowLessThan(row1.Get(0), row2.Get(0)))
}
