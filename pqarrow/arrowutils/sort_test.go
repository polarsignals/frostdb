package arrowutils

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/compute"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestSortRecord(t *testing.T) {
	null := func(v int64) *int64 {
		return &v
	}

	cases := []SortCase{
		{
			Name: "must provide at least one column",
			Samples: Samples{
				{},
			},
			Error: "expected missing column error",
		},

		{
			Name:    "No Nows",
			Samples: Samples{},
			Columns: []SortingColumn{{Index: 0}},
		},
		{
			Name: "One Row",
			Samples: Samples{
				{},
			},
			Columns: []SortingColumn{
				{
					Index: 0,
				},
			},
			Indices: []int32{0},
		},
		{
			Name: "By Integer column ascending",
			Samples: Samples{
				{Int: 3},
				{Int: 2},
				{Int: 1},
			},
			Columns: []SortingColumn{
				{Index: 0},
			},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Integer column descending",
			Samples: Samples{
				{Int: 1},
				{Int: 2},
				{Int: 3},
			},

			Columns: []SortingColumn{
				{Index: 0, Direction: Descending},
			},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Double column ascending",
			Samples: Samples{
				{Double: 3},
				{Double: 2},
				{Double: 1},
			},
			Columns: []SortingColumn{{Index: 1}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Double column descending",
			Samples: Samples{
				{Double: 1},
				{Double: 2},
				{Double: 3},
			},
			Columns: []SortingColumn{{Index: 1, Direction: Descending}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By String column ascending",
			Samples: Samples{
				{String: "3"},
				{String: "2"},
				{String: "1"},
			},
			Columns: []SortingColumn{{Index: 2}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By String column descending",
			Samples: Samples{
				{String: "1"},
				{String: "2"},
				{String: "3"},
			},
			Columns: []SortingColumn{{Index: 2, Direction: Descending}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Timestamp column ascending",
			Samples: Samples{
				{Timestamp: 3},
				{Timestamp: 2},
				{Timestamp: 1},
			},
			Columns: []SortingColumn{{Index: 5}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Timestamp column descending",
			Samples: Samples{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
			},
			Columns: []SortingColumn{{Index: 5, Direction: Descending}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Dict column ascending",
			Samples: Samples{
				{Dict: "3"},
				{Dict: "2"},
				{Dict: "1"},
			},
			Columns: []SortingColumn{{Index: 3}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Dict column descending",
			Samples: Samples{
				{Dict: "1"},
				{Dict: "2"},
				{Dict: "3"},
			},
			Columns: []SortingColumn{{Index: 3, Direction: Descending}},
			Indices: []int32{2, 1, 0},
		},
		{
			Name: "By Null column ascending",
			Samples: Samples{
				{},
				{},
				{Nullable: null(1)},
			},
			Columns: []SortingColumn{{Index: 4}},
			Indices: []int32{2, 0, 1},
		},
		{
			Name: "By Null column ascending nullsFirst",
			Samples: Samples{
				{},
				{},
				{Nullable: null(1)},
			},
			Columns: []SortingColumn{{Index: 4, NullsFirst: true}},
			Indices: []int32{0, 1, 2},
		},
		{
			Name: "By Null column descending",
			Samples: Samples{
				{},
				{},
				{Nullable: null(1)},
			},
			Columns: []SortingColumn{{Index: 4, Direction: Descending}},
			Indices: []int32{2, 0, 1},
		},
		{
			Name: "By Null column descending nullsFirst",
			Samples: Samples{
				{},
				{},
				{Nullable: null(1)},
			},
			Columns: []SortingColumn{{Index: 4, Direction: Descending, NullsFirst: true}},
			Indices: []int32{0, 1, 2},
		},
		{
			Name: "Multiple columns same direction",
			Samples: Samples{
				{String: "1", Int: 3},
				{String: "2", Int: 2},
				{String: "3", Int: 2},
				{String: "4", Int: 1},
			},
			Columns: []SortingColumn{
				{Index: 0},
				{Index: 2},
			},
			Indices: []int32{3, 1, 2, 0},
		},
		{
			Name: "Multiple columns different direction",
			Samples: Samples{
				{String: "1", Int: 3},
				{String: "2", Int: 2},
				{String: "3", Int: 2},
				{String: "4", Int: 1},
			},
			Columns: []SortingColumn{
				{Index: 0, Direction: Ascending},
				{Index: 2, Direction: Descending},
			},
			Indices: []int32{3, 2, 1, 0},
		},
	}

	for _, kase := range cases {
		t.Run(kase.Name, func(t *testing.T) {
			sortAndCompare(t, kase)
		})
	}
}

func TestSortRecordBuilderReuse(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	schema := arrow.NewSchema([]arrow.Field{{Name: "int64", Type: arrow.PrimitiveTypes.Int64}}, nil)

	b1 := array.NewInt64Builder(mem)
	b1.AppendValues([]int64{3, 2, 1}, nil)
	arr1 := b1.NewArray()
	r1 := array.NewRecord(schema, []arrow.Array{arr1}, 3)

	ms, err := newMultiColSorter(r1, []SortingColumn{{Index: 0}})
	require.Nil(t, err)
	sort.Sort(ms)
	sortedArr1 := ms.indices.NewArray().(*array.Int32)
	require.Equal(t, []int32{2, 1, 0}, sortedArr1.Int32Values())
	ms.Release() // usually defer

	b2 := array.NewInt64Builder(mem)
	b2.AppendValues([]int64{2, 1}, nil)
	arr2 := b2.NewArray()
	r2 := array.NewRecord(schema, []arrow.Array{arr2}, 2)

	ms, err = newMultiColSorter(r2, []SortingColumn{{Index: 0}})
	require.Nil(t, err)
	sort.Sort(ms)
	sortedArr2 := ms.indices.NewArray().(*array.Int32)
	require.Equal(t, []int32{1, 0}, sortedArr2.Int32Values())
	ms.Release() // usually defer

	// This failed before the fix because the builder's data was reused.
	require.Equal(t, []int32{2, 1, 0}, sortedArr1.Int32Values())
	require.Equal(t, []int32{1, 0}, sortedArr2.Int32Values())
}

func TestReorderRecord(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		b := array.NewRecordBuilder(mem, arrow.NewSchema(
			[]arrow.Field{
				{
					Name: "int",
					Type: arrow.PrimitiveTypes.Int64,
				},
			}, nil,
		))
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{3, 2, 1}, nil)
		r := b.NewRecord()
		defer r.Release()

		indices := array.NewInt32Builder(mem)
		indices.AppendValues([]int32{2, 1, 0}, nil)
		by := indices.NewInt32Array()
		result, err := Take(
			compute.WithAllocator(context.Background(), mem), r, by)
		require.Nil(t, err)
		defer result.Release()

		want := []int64{1, 2, 3}
		require.Equal(t, want, result.Column(0).(*array.Int64).Int64Values())
	})
	t.Run("WithDict", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		b := array.NewRecordBuilder(mem, arrow.NewSchema(
			[]arrow.Field{
				{
					Name: "dict",
					Type: &arrow.DictionaryType{
						IndexType: arrow.PrimitiveTypes.Int32,
						ValueType: arrow.BinaryTypes.String,
					},
				},
			}, nil,
		))
		defer b.Release()
		d := b.Field(0).(*array.BinaryDictionaryBuilder)
		require.NoError(t, d.AppendString("3"))
		require.NoError(t, d.AppendString("2"))
		require.NoError(t, d.AppendString("1"))
		d.AppendNull()
		require.NoError(t, d.AppendString("3"))
		r := b.NewRecord()
		defer r.Release()

		indices := array.NewInt32Builder(mem)
		indices.AppendValues([]int32{2, 1, 4, 0, 3}, nil)
		result, err := Take(compute.WithAllocator(context.Background(), mem), r, indices.NewInt32Array())
		require.NoError(t, err)
		defer result.Release()

		want := []string{"1", "2", "3", "3", ""}
		got := result.Column(0).(*array.Dictionary)
		require.Equal(t, len(want), got.Len())
		for i, v := range want {
			if v == "" {
				require.True(t, got.IsNull(i))
				continue
			}
			require.Equal(t, want[i], got.ValueStr(i))
		}
	})
	t.Run("List", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		b := array.NewRecordBuilder(mem, arrow.NewSchema(
			[]arrow.Field{
				{
					Name: "list",
					Type: arrow.ListOf(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}),
				},
			}, nil,
		))
		defer b.Release()
		lb := b.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.BinaryDictionaryBuilder)
		lb.Append(true)
		require.NoError(t, vb.AppendString("1"))
		require.NoError(t, vb.AppendString("2"))
		require.NoError(t, vb.AppendString("3"))
		require.NoError(t, vb.AppendString("1"))
		lb.Append(false)
		lb.Append(true)
		require.NoError(t, vb.AppendString("4"))
		require.NoError(t, vb.AppendString("5"))
		require.NoError(t, vb.AppendString("6"))
		lb.Append(true)
		require.NoError(t, vb.AppendString("3"))
		require.NoError(t, vb.AppendString("3"))
		require.NoError(t, vb.AppendString("3"))
		require.NoError(t, vb.AppendString("4"))
		r := b.NewRecord()
		defer r.Release()

		indices := array.NewInt32Builder(mem)
		indices.AppendValues([]int32{2, 1, 0, 3}, nil)
		result, err := Take(
			compute.WithAllocator(context.Background(), mem), r, indices.NewInt32Array())
		require.Nil(t, err)
		defer result.Release()

		got := result.Column(0).(*array.List)
		expected := []string{
			"[\"4\",\"5\",\"6\"]",
			"",
			"[\"1\",\"2\",\"3\",\"1\"]",
			"[\"3\",\"3\",\"3\",\"4\"]",
		}
		require.Equal(t, len(expected), got.Len())
		for i, v := range expected {
			if len(v) == 0 {
				require.True(t, got.IsNull(i), "expected null at %d", i)
				continue
			}
			require.Equal(t, expected[i], got.ValueStr(i), "unexpected value at %d", i)
		}
	})
}

// Use all supported sort field.
type Sample struct {
	Int       int64
	Double    float64
	String    string
	Dict      string
	Nullable  *int64
	Timestamp arrow.Timestamp
}

type Samples []Sample

func (s Samples) Record() arrow.Record {
	b := array.NewRecordBuilder(memory.NewGoAllocator(),
		arrow.NewSchema([]arrow.Field{
			{
				Name: "int",
				Type: arrow.PrimitiveTypes.Int64,
			},
			{
				Name: "double",
				Type: arrow.PrimitiveTypes.Float64,
			},
			{
				Name: "string",
				Type: arrow.BinaryTypes.String,
			},
			{
				Name: "dict",
				Type: &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.String,
				},
			},
			{
				Name:     "nullable",
				Type:     arrow.PrimitiveTypes.Int64,
				Nullable: true,
			},
			{
				Name:     "timestamp",
				Type:     &arrow.TimestampType{},
				Nullable: true,
			},
		}, nil),
	)

	fInt := b.Field(0).(*array.Int64Builder)
	fDouble := b.Field(1).(*array.Float64Builder)
	fString := b.Field(2).(*array.StringBuilder)
	fDict := b.Field(3).(*array.BinaryDictionaryBuilder)
	fNullable := b.Field(4).(*array.Int64Builder)
	fTimestamp := b.Field(5).(*array.TimestampBuilder)

	for _, v := range s {
		fInt.Append(v.Int)
		fDouble.Append(v.Double)
		fString.Append(v.String)
		if v.Timestamp == 0 {
			fTimestamp.AppendNull()
		} else {
			fTimestamp.Append(v.Timestamp)
		}
		_ = fDict.AppendString(v.Dict)
		if v.Nullable != nil {
			fNullable.Append(*v.Nullable)
		} else {
			fNullable.AppendNull()
		}
	}
	return b.NewRecord()
}

type SortCase struct {
	Name    string
	Samples Samples
	Columns []SortingColumn
	Indices []int32
	Error   string
}

func sortAndCompare(t *testing.T, kase SortCase) {
	t.Helper()

	got, err := SortRecord(kase.Samples.Record(), kase.Columns)
	if kase.Error != "" {
		require.NotNil(t, err, kase.Error)
		return
	}
	defer got.Release()

	require.Equal(t, kase.Indices, got.Int32Values())
}

func BenchmarkTake(b *testing.B) {
	const (
		numRows            = 1024
		numValsPerListElem = 4
	)
	mem := memory.NewGoAllocator()
	b.Run("Dict", func(b *testing.B) {
		rb := array.NewRecordBuilder(mem, arrow.NewSchema(
			[]arrow.Field{
				{
					Name: "dict",
					Type: &arrow.DictionaryType{
						IndexType: arrow.PrimitiveTypes.Int32,
						ValueType: arrow.BinaryTypes.Binary,
					},
				},
			}, nil,
		))
		defer rb.Release()
		d := rb.Field(0).(*array.BinaryDictionaryBuilder)
		for i := 0; i < numRows; i++ {
			// Interesting to benchmark with a string that appears every other row.
			// i.e. only one entry in the dict.
			require.NoError(b, d.AppendString("appearseveryotherrow"))
			require.NoError(b, d.AppendString(fmt.Sprintf("%d", i)))
		}
		r := rb.NewRecord()
		indices := array.NewInt32Builder(mem)
		for i := r.NumRows() - 1; i > 0; i-- {
			indices.Append(int32(i))
		}
		ctx := compute.WithAllocator(context.Background(), mem)
		indArr := indices.NewInt32Array()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := Take(ctx, r, indArr); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("List", func(b *testing.B) {
		listb := array.NewRecordBuilder(mem, arrow.NewSchema(
			[]arrow.Field{
				{
					Name: "list",
					Type: arrow.ListOf(
						&arrow.DictionaryType{
							IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary,
						},
					),
				},
			}, nil,
		))
		defer listb.Release()

		l := listb.Field(0).(*array.ListBuilder)
		vb := l.ValueBuilder().(*array.BinaryDictionaryBuilder)
		for i := 0; i < numRows; i++ {
			l.Append(true)
			for j := 0; j < numValsPerListElem-1; j++ {
				require.NoError(b, vb.AppendString(fmt.Sprintf("%d", i)))
			}
			require.NoError(b, vb.AppendString("appearseveryrow"))
		}

		r := listb.NewRecord()
		indices := array.NewInt32Builder(mem)
		for i := numRows - 1; i > 0; i-- {
			indices.Append(int32(i))
		}
		ctx := compute.WithAllocator(context.Background(), mem)
		indArr := indices.NewInt32Array()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := Take(ctx, r, indArr); err != nil {
				b.Fatal(err)
			}
		}
	})
}
