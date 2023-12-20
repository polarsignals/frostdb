package pqarrow

import (
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
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
	b.ResetTimer()
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
	for i := 0; i < b.N; i++ {
		if err := recordToRows(
			noopWriter{}, func(string) bool { return false }, record, 0, numRows, parquetFields.Fields(),
		); err != nil {
			b.Fatal(err)
		}
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
