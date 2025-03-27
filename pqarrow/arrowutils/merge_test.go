package arrowutils_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/internal/records"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
)

func TestMerge(t *testing.T) {
	type row struct {
		Number *int64  `frostdb:",asc(0)"`
		Text   *string `frostdb:",asc(1)"`
	}
	int64Ptr := func(i int64) *int64 { return &i }
	stringPtr := func(s string) *string { return &s }

	// mem := memory.NewGoAllocator()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	t.Parallel()

	build := records.NewBuild[row](mem)
	defer build.Release()
	err := build.Append([]row{
		{Number: nil, Text: nil},
		{Number: int64Ptr(0), Text: stringPtr("a")},
		{Number: int64Ptr(1), Text: stringPtr("b")},
		{Number: int64Ptr(3), Text: stringPtr("d")},
		{Number: int64Ptr(5), Text: stringPtr("d")},
	}...)
	require.NoError(t, err)
	record1 := build.NewRecord()
	defer record1.Release()

	for _, tc := range []struct {
		name string
		// inputs
		records [][]row
		orderBy []arrowutils.SortingColumn
		limit   uint64
		// expected output
		expected []row
	}{{
		name: "merge-ascending",
		records: [][]row{{
			{Number: nil},         // 0
			{Number: int64Ptr(0)}, // 1
			{Number: int64Ptr(2)}, // 3
			{Number: int64Ptr(4)}, // 5
			{Number: int64Ptr(5)}, // 7
		}, {
			{Number: int64Ptr(1)}, // 2
			{Number: int64Ptr(3)}, // 4
			{Number: int64Ptr(5)}, // 6
			{Number: int64Ptr(6)}, // 8
			{Number: int64Ptr(7)}, // 9
		}},
		orderBy: []arrowutils.SortingColumn{
			{Index: 0, Direction: arrowutils.Ascending, NullsFirst: true},
		},

		expected: []row{
			{Number: nil, Text: nil},
			{Number: int64Ptr(0)},
			{Number: int64Ptr(1)},
			{Number: int64Ptr(2)},
			{Number: int64Ptr(3)},
			{Number: int64Ptr(4)},
			{Number: int64Ptr(5)},
			{Number: int64Ptr(5)},
			{Number: int64Ptr(6)},
			{Number: int64Ptr(7)},
		},
	}, {
		name: "merge-ascending-limit",
		records: [][]row{{
			{Number: nil},         // 0
			{Number: int64Ptr(0)}, // 1
			{Number: int64Ptr(2)}, // 3
			{Number: int64Ptr(4)}, // 5
			{Number: int64Ptr(5)}, // 7
		}, {
			{Number: int64Ptr(1)}, // 2
			{Number: int64Ptr(3)}, // 4
			{Number: int64Ptr(5)}, // 6
			{Number: int64Ptr(6)}, // 8
			{Number: int64Ptr(7)}, // 9
		}},
		orderBy: []arrowutils.SortingColumn{
			{Index: 0, Direction: arrowutils.Ascending, NullsFirst: true},
		},
		limit: 3,

		expected: []row{
			{Number: nil, Text: nil},
			{Number: int64Ptr(0)},
			{Number: int64Ptr(1)},
		},
	}, {
		name: "merge-descending",
		records: [][]row{{
			{Number: nil},         // 0
			{Number: int64Ptr(5)}, // 3
			{Number: int64Ptr(3)}, // 6
			{Number: int64Ptr(1)}, // 7
			{Number: int64Ptr(0)}, // 8
		}, {
			{Number: int64Ptr(7)}, // 1
			{Number: int64Ptr(6)}, // 2
			{Number: int64Ptr(5)}, // 4
			{Number: int64Ptr(4)}, // 5
		}},
		orderBy: []arrowutils.SortingColumn{
			{Index: 0, Direction: arrowutils.Descending, NullsFirst: true},
		},

		expected: []row{
			{Number: nil},
			{Number: int64Ptr(7)},
			{Number: int64Ptr(6)},
			{Number: int64Ptr(5)},
			{Number: int64Ptr(5)},
			{Number: int64Ptr(4)},
			{Number: int64Ptr(3)},
			{Number: int64Ptr(1)},
			{Number: int64Ptr(0)},
		},
	}, {
		name: "merge-descending-limit",
		records: [][]row{{
			{Number: nil},         // 0
			{Number: int64Ptr(5)}, // 4
			{Number: int64Ptr(3)}, // limited
			{Number: int64Ptr(1)}, // limited
			{Number: int64Ptr(0)}, // limited
		}, {
			{Number: int64Ptr(7)}, // 1
			{Number: int64Ptr(6)}, // 2
			{Number: int64Ptr(5)}, // 3
			{Number: int64Ptr(4)}, // 5
		}},
		orderBy: []arrowutils.SortingColumn{
			{Index: 0, Direction: arrowutils.Descending, NullsFirst: true},
		},
		limit: 6,

		expected: []row{
			{Number: nil},
			{Number: int64Ptr(7)},
			{Number: int64Ptr(6)},
			{Number: int64Ptr(5)},
			{Number: int64Ptr(5)},
			{Number: int64Ptr(4)},
		},
	}, {
		name: "multiple-ascending",
		records: [][]row{{
			{Number: int64Ptr(0), Text: stringPtr("a")}, // 1
			{Number: int64Ptr(0), Text: stringPtr("c")}, // 3
			{Number: int64Ptr(1), Text: stringPtr("e")}, // 4
		}, {
			{Number: nil, Text: nil},                    // 0
			{Number: int64Ptr(0), Text: stringPtr("b")}, // 2
			{Number: int64Ptr(1), Text: stringPtr("d")}, // 3
			{Number: int64Ptr(2), Text: stringPtr("f")}, // 5
		}},
		orderBy: []arrowutils.SortingColumn{
			{Index: 0, Direction: arrowutils.Ascending, NullsFirst: true},
			{Index: 1, Direction: arrowutils.Ascending, NullsFirst: true},
		},

		expected: []row{
			{Number: nil, Text: nil},
			{Number: int64Ptr(0), Text: stringPtr("a")},
			{Number: int64Ptr(0), Text: stringPtr("b")},
			{Number: int64Ptr(0), Text: stringPtr("c")},
			{Number: int64Ptr(1), Text: stringPtr("d")},
			{Number: int64Ptr(1), Text: stringPtr("e")},
			{Number: int64Ptr(2), Text: stringPtr("f")},
		},
	}, {
		name: "multiple-descending",
		records: [][]row{{
			{Number: int64Ptr(1), Text: stringPtr("e")},
			{Number: int64Ptr(0), Text: stringPtr("c")},
			{Number: int64Ptr(0), Text: stringPtr("a")},
		}, {
			{Number: nil, Text: nil},                    // 0
			{Number: int64Ptr(2), Text: stringPtr("f")}, //
			{Number: int64Ptr(1), Text: stringPtr("d")},
			{Number: int64Ptr(0), Text: stringPtr("b")},
		}},
		orderBy: []arrowutils.SortingColumn{
			{Index: 0, Direction: arrowutils.Descending, NullsFirst: true},
			{Index: 1, Direction: arrowutils.Descending, NullsFirst: true},
		},

		expected: []row{
			{Number: nil, Text: nil},
			{Number: int64Ptr(2), Text: stringPtr("f")},
			{Number: int64Ptr(1), Text: stringPtr("e")},
			{Number: int64Ptr(1), Text: stringPtr("d")},
			{Number: int64Ptr(0), Text: stringPtr("c")},
			{Number: int64Ptr(0), Text: stringPtr("b")},
			{Number: int64Ptr(0), Text: stringPtr("a")},
		},
	}, {
		name: "multiple-mixed",
		records: [][]row{{
			{Number: int64Ptr(1), Text: stringPtr("e")},
			{Number: int64Ptr(0), Text: stringPtr("a")},
			{Number: int64Ptr(0), Text: stringPtr("c")},
		}, {
			{Number: nil, Text: nil},
			{Number: int64Ptr(2), Text: stringPtr("f")},
			{Number: int64Ptr(1), Text: stringPtr("d")},
			{Number: int64Ptr(0), Text: stringPtr("b")},
		}},
		orderBy: []arrowutils.SortingColumn{
			{Index: 0, Direction: arrowutils.Descending, NullsFirst: true},
			{Index: 1, Direction: arrowutils.Ascending, NullsFirst: true},
		},

		expected: []row{
			{Number: nil, Text: nil},
			{Number: int64Ptr(2), Text: stringPtr("f")},
			{Number: int64Ptr(1), Text: stringPtr("d")},
			{Number: int64Ptr(1), Text: stringPtr("e")},
			{Number: int64Ptr(0), Text: stringPtr("a")},
			{Number: int64Ptr(0), Text: stringPtr("b")},
			{Number: int64Ptr(0), Text: stringPtr("c")},
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			builder := records.NewBuild[row](mem)
			defer builder.Release()

			records := make([]arrow.Record, 0, len(tc.records))
			defer func() {
				for _, record := range records {
					record.Release()
				}
			}()

			for _, rows := range tc.records {
				err := builder.Append(rows...)
				require.NoError(t, err)
				records = append(records, builder.NewRecord())
			}

			res, err := arrowutils.MergeRecords(mem, records, tc.orderBy, tc.limit)
			require.NoError(t, err)
			defer res.Release()

			numbers := res.Column(0).(*array.Int64)
			texts := res.Column(1).(*array.String)

			// TODO: Create an equivalent to the records.NewBuild for reading back into the struct.
			result := make([]row, res.NumRows())
			for i := 0; i < int(res.NumRows()); i++ {
				if numbers.IsValid(i) {
					result[i].Number = int64Ptr(numbers.Value(i))
				}
				if texts.IsValid(i) {
					result[i].Text = stringPtr(texts.Value(i))
				}
			}

			require.Equal(t, tc.expected, result)
		})
	}
}

func TestMergeNestedListStruct(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	rb := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "list", Type: arrow.ListOf(arrow.StructOf([]arrow.Field{
			{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64},
		}...))},
	}, nil))
	defer rb.Release()

	var recs []arrow.Record
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	int64Builder := rb.Field(0).(*array.Int64Builder)
	listBuilder := rb.Field(1).(*array.ListBuilder)
	listStructBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	listStructInt32Builder := listStructBuilder.FieldBuilder(0).(*array.Int32Builder)
	listStructUint64Builder := listStructBuilder.FieldBuilder(1).(*array.Uint64Builder)

	int64Builder.Append(-123)
	listBuilder.Append(true)
	listStructBuilder.Append(true)
	listStructInt32Builder.Append(123)
	listStructUint64Builder.Append(123 * 2)
	listStructBuilder.Append(true)
	listStructInt32Builder.Append(123 * 3)
	listStructUint64Builder.Append(123 * 4)
	recs = append(recs, rb.NewRecord())

	int64Builder.Append(-123 * 2)
	listBuilder.Append(true)
	listStructBuilder.Append(true)
	listStructInt32Builder.Append(123 * 5)
	listStructUint64Builder.Append(123 * 6)
	listStructBuilder.Append(true)
	listStructInt32Builder.Append(123 * 7)
	listStructUint64Builder.Append(123 * 8)
	listStructBuilder.Append(true)
	listStructInt32Builder.Append(123 * 9)
	listStructUint64Builder.Append(123 * 10)
	recs = append(recs, rb.NewRecord())

	mergeRecord, err := arrowutils.MergeRecords(mem, recs, []arrowutils.SortingColumn{
		{Index: 0, Direction: arrowutils.Ascending},
	}, 0)
	require.NoError(t, err)
	defer mergeRecord.Release()

	require.Equal(t, int64(2), mergeRecord.NumCols())
	require.Equal(t, int64(2), mergeRecord.NumRows())

	require.Equal(t, `[-246 -123]`, mergeRecord.Column(0).String())
	require.Equal(t, `[{[615 861 1107] [738 984 1230]} {[123 369] [246 492]}]`, mergeRecord.Column(1).String())
}

func BenchmarkMergeRecords(b *testing.B) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	var r1, r2 arrow.Record
	{
		build := records.NewBuild[Model](mem)
		err := build.Append(makeModels(40)...)
		require.NoError(b, err)
		r := build.NewRecord()
		defer r.Release()

		indices, err := arrowutils.SortRecord(r.Record, r.SortingColumns)
		require.NoError(b, err)
		defer indices.Release()

		r1, err = arrowutils.Take(ctx, r.Record, indices)
		require.NoError(b, err)
		defer r1.Release()
	}
	{
		build := records.NewBuild[Model](mem)
		err := build.Append(makeModels(60)...)
		require.NoError(b, err)
		r := build.NewRecord()
		defer r.Release()

		indices, err := arrowutils.SortRecord(r.Record, r.SortingColumns)
		require.NoError(b, err)
		defer indices.Release()

		r2, err = arrowutils.Take(ctx, r.Record, indices)
		require.NoError(b, err)
		defer r2.Release()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := arrowutils.MergeRecords(
			mem,
			[]arrow.Record{r1, r2},
			[]arrowutils.SortingColumn{
				{Index: 0, Direction: arrowutils.Ascending, NullsFirst: true},
			},
			0,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}
