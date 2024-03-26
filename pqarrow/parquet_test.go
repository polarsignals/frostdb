package pqarrow

import (
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

type noopWriter struct{}

func (w noopWriter) Schema() *parquet.Schema { return nil }

func (w noopWriter) Write(r []any) (int, error) { return len(r), nil }

func (w noopWriter) WriteRows(r []parquet.Row) (int, error) { return len(r), nil }

func (w noopWriter) Flush() error { return nil }

func (w noopWriter) Close() error { return nil }

func (w noopWriter) Reset(_ io.Writer) {}

func BenchmarkRecordsToFile(b *testing.B) {
	b.ReportAllocs()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int_column", Type: arrow.PrimitiveTypes.Int64},
		{Name: "string_column", Type: arrow.BinaryTypes.String},
		{
			Name: "dictionary_column",
			Type: &arrow.DictionaryType{
				ValueType: arrow.BinaryTypes.String,
				IndexType: arrow.PrimitiveTypes.Int32,
			},
		},
		{
			Name: "list_column",
			Type: arrow.ListOf(&arrow.DictionaryType{
				ValueType: arrow.BinaryTypes.Binary,
				IndexType: arrow.PrimitiveTypes.Int32,
			}),
		},
	}, nil)

	parquetFields := parquet.Group{}
	for _, f := range schema.Fields() {
		// No need to create actual nodes since the code only looks for field
		// names.
		parquetFields[f.Name] = parquet.Node(nil)
	}

	checked := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer checked.AssertSize(b, 0)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	const numRows = 1024
	for i := 0; i < numRows; i++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))

		builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("%d", i))

		dictBuilder := builder.Field(2).(*array.BinaryDictionaryBuilder)
		require.NoError(b, dictBuilder.Append([]byte(fmt.Sprintf("%d-key", i))))

		listBuilder := builder.Field(3).(*array.ListBuilder)
		listBuilder.Append(true)
		valueBuilder := listBuilder.ValueBuilder().(*array.BinaryDictionaryBuilder)
		require.NoError(b, valueBuilder.Append([]byte(fmt.Sprintf("string-1-%d", i))))
		require.NoError(b, valueBuilder.Append([]byte(fmt.Sprintf("string-2-%d", i))))
		require.NoError(b, valueBuilder.Append([]byte(fmt.Sprintf("string-3-%d", i))))
	}

	record := builder.NewRecord()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := recordToRows(
			noopWriter{}, record, 0, numRows, parquetFields.Fields(),
		); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRecordToRows_list(t *testing.T) {
	b := array.NewRecordBuilder(memory.NewGoAllocator(),
		arrow.NewSchema(
			[]arrow.Field{
				{
					Name:     "int",
					Type:     arrow.ListOf(arrow.PrimitiveTypes.Int64),
					Nullable: true,
				},
				{
					Name:     "double",
					Type:     arrow.ListOf(arrow.PrimitiveTypes.Float64),
					Nullable: true,
				},
				{
					Name:     "string",
					Type:     arrow.ListOf(arrow.BinaryTypes.String),
					Nullable: true,
				},
				{
					Name:     "binary",
					Type:     arrow.ListOf(arrow.BinaryTypes.Binary),
					Nullable: true,
				},
				{
					Name:     "bool",
					Type:     arrow.ListOf(arrow.FixedWidthTypes.Boolean),
					Nullable: true,
				},
			}, nil,
		),
	)
	defer b.Release()

	type Sample struct {
		Int    []int64
		Double []float64
		String []string
		Binary [][]byte
		Bool   []bool
	}
	samples := []Sample{
		{}, // handle nulls
		{
			Int:    []int64{1},
			Double: []float64{1},
			String: []string{"1"},
			Binary: [][]byte{
				[]byte("1"),
			},
			Bool: []bool{true},
		},
	}
	ints := b.Field(0).(*array.ListBuilder)
	intsBuild := ints.ValueBuilder().(*array.Int64Builder)
	double := b.Field(1).(*array.ListBuilder)
	doubleBuild := double.ValueBuilder().(*array.Float64Builder)
	str := b.Field(2).(*array.ListBuilder)
	strBuild := str.ValueBuilder().(*array.StringBuilder)
	bin := b.Field(3).(*array.ListBuilder)
	binBuild := bin.ValueBuilder().(*array.BinaryBuilder)
	boolean := b.Field(4).(*array.ListBuilder)
	booleanBuild := boolean.ValueBuilder().(*array.BooleanBuilder)

	for _, s := range samples {
		appendList[int64](ints, s.Int, intsBuild.Append)
		appendList[float64](double, s.Double, doubleBuild.Append)
		appendList[string](str, s.String, strBuild.Append)
		appendList[[]byte](bin, s.Binary, binBuild.Append)
		appendList[bool](boolean, s.Bool, booleanBuild.Append)
	}
	r := b.NewRecord()
	defer r.Release()

	parquetFields := parquet.Group{}
	for _, f := range r.Schema().Fields() {
		parquetFields[f.Name] = parquet.Required(parquet.Node(nil))
	}
	clone := &cloneWriter{}
	if err := recordToRows(
		clone, r, 0, int(r.NumRows()), parquetFields.Fields(),
	); err != nil {
		t.Fatal(err)
	}
	// parquetFields.Fields() changes the order of the rows
	// From
	//  int, double, string, binary, bool
	// To
	//  binary, bool, double, int, string
	want := []parquet.Row{
		{
			parquet.Value{}.Level(0, 0, 0),
			parquet.Value{}.Level(0, 0, 1),
			parquet.Value{}.Level(0, 0, 2),
			parquet.Value{}.Level(0, 0, 3),
			parquet.Value{}.Level(0, 0, 4),
		},
		{
			parquet.ByteArrayValue([]byte("1")).Level(0, 1, 0),
			parquet.BooleanValue(true).Level(0, 1, 1),
			parquet.DoubleValue(1).Level(0, 1, 2),
			parquet.Int64Value(1).Level(0, 1, 3),
			parquet.ByteArrayValue([]byte("1")).Level(0, 1, 4),
		},
	}
	require.Equal(t, len(want), len(clone.rows))
	for i := range want {
		require.True(t, want[i].Equal(clone.rows[i]))
	}
}

type cloneWriter struct {
	rows []parquet.Row
}

func (w cloneWriter) Schema() *parquet.Schema { return nil }

func (w cloneWriter) Write(r []any) (int, error) { return len(r), nil }

func (w *cloneWriter) WriteRows(r []parquet.Row) (int, error) {
	for i := range r {
		w.rows = append(w.rows, r[i].Clone())
	}
	return len(r), nil
}

func (w cloneWriter) Flush() error { return nil }

func (w cloneWriter) Close() error { return nil }

func (w cloneWriter) Reset(_ io.Writer) {}

func appendList[T any](ls *array.ListBuilder, values []T, add func(T)) {
	if values == nil {
		ls.AppendNull()
		return
	}
	ls.Append(true)
	for i := range values {
		add(values[i])
	}
}

func TestRecordDynamicCols(t *testing.T) {
	build := array.NewRecordBuilder(memory.NewGoAllocator(),
		arrow.NewSchema([]arrow.Field{
			{
				Name: "labels.label1",
				Type: arrow.BinaryTypes.String,
			},
			{
				Name: "labels.label2",
				Type: arrow.BinaryTypes.String,
			},
			{
				Name: "labels.label3",
				Type: arrow.BinaryTypes.String,
			},
		}, nil),
	)
	defer build.Release()
	r := build.NewRecord()

	res := RecordDynamicCols(r)
	require.Equal(t, map[string][]string{
		"labels": {"label1", "label2", "label3"},
	}, res)
}

func BenchmarkRecordDynamicCols(b *testing.B) {
	build := array.NewRecordBuilder(memory.NewGoAllocator(),
		arrow.NewSchema([]arrow.Field{
			{
				Name: "labels.label1",
				Type: arrow.BinaryTypes.String,
			},
			{
				Name: "labels.label2",
				Type: arrow.BinaryTypes.String,
			},
			{
				Name: "labels.label3",
				Type: arrow.BinaryTypes.String,
			},
		}, nil),
	)
	defer build.Release()
	r := build.NewRecord()
	defer r.Release()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = RecordDynamicCols(r)
	}
}

func Test_Uint64RecordToRow(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	build := array.NewRecordBuilder(alloc,
		arrow.NewSchema([]arrow.Field{
			{
				Name: "value",
				Type: arrow.PrimitiveTypes.Uint64,
			},
		}, nil),
	)
	defer build.Release()
	build.Field(0).(*array.Uint64Builder).AppendValues([]uint64{0, 1, 2, 3}, nil)
	r := build.NewRecord()
	defer r.Release()

	// Build with required
	parquetFields := parquet.Group{}
	parquetFields["value"] = parquet.Required(parquet.Int(64))
	schema := parquet.NewSchema("test", parquetFields)

	row, err := RecordToRow(schema, r, 2)
	require.NoError(t, err)

	require.Equal(t, "[2]", fmt.Sprintf("%v", row))

	// Build with optional
	parquetFields = parquet.Group{}
	parquetFields["value"] = parquet.Optional(parquet.Int(64))
	schema = parquet.NewSchema("test", parquetFields)

	row, err = RecordToRow(schema, r, 2)
	require.NoError(t, err)

	require.Equal(t, "[2]", fmt.Sprintf("%v", row))
}
