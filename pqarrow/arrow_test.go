package pqarrow

import (
	"testing"

	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/google/uuid"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query/logicalplan"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	TestString string      `parquet:"test_string,dict"`
	UUIDList   []uuid.UUID `parquet:"uuid_list"`
	TestInt    int64       `parquet:"test_int"`
}

func TestArrowUUIDList(t *testing.T) {
	s := parquet.SchemaOf(&TestStruct{})
	b := parquet.NewBuffer(s)

	lists := []*TestStruct{
		{
			TestString: "test1",
			UUIDList: []uuid.UUID{
				uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			},
			TestInt: 1,
		},
		{
			TestString: "test1",
			UUIDList: []uuid.UUID{
				uuid.MustParse("44444444-4444-4444-4444-444444444444"),
				uuid.MustParse("44444444-4444-4444-4444-444444444444"),
			},
			TestInt: 2,
		},
		{
			TestString: "test3",
			UUIDList: []uuid.UUID{
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
			},
			TestInt: 3,
		},
	}

	for _, list := range lists {
		err := b.Write(list)
		require.NoError(t, err)
	}

	ar, err := ParquetRowGroupToArrowRecord(memory.DefaultAllocator, b, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(lists)), ar.NumRows())
	require.Equal(t, int64(3), ar.NumCols())
}

func TestProjections(t *testing.T) {
	s := parquet.SchemaOf(&TestStruct{})
	b := parquet.NewBuffer(s)

	lists := []*TestStruct{
		{
			TestString: "test1",
			UUIDList: []uuid.UUID{
				uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			},
			TestInt: 1,
		},
		{
			TestString: "test1",
			UUIDList: []uuid.UUID{
				uuid.MustParse("44444444-4444-4444-4444-444444444444"),
				uuid.MustParse("44444444-4444-4444-4444-444444444444"),
			},
			TestInt: 2,
		},
		{
			TestString: "test3",
			UUIDList: []uuid.UUID{
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				uuid.MustParse("66666666-6666-6666-6666-666666666666"),
			},
			TestInt: 3,
		},
	}

	for _, list := range lists {
		err := b.Write(list)
		require.NoError(t, err)
	}

	ar, err := ParquetRowGroupToArrowRecord(memory.DefaultAllocator, b, []logicalplan.ColumnMatcher{logicalplan.StaticColumnMatcher{
		ColumnName: "test_string",
	}}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(lists)), ar.NumRows())
	require.Equal(t, int64(1), ar.NumCols())
}

func TestMergeToArrow(t *testing.T) {
	schema := dynparquet.NewSampleSchema()

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf1, err := samples.ToBuffer(schema)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf2, err := samples.ToBuffer(schema)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf3, err := samples.ToBuffer(schema)
	require.NoError(t, err)

	merge, err := schema.MergeDynamicRowGroups([]dynparquet.DynamicRowGroup{buf1, buf2, buf3})
	require.NoError(t, err)

	ar, err := ParquetRowGroupToArrowRecord(memory.DefaultAllocator, merge, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(5), ar.NumRows())
	require.Equal(t, int64(8), ar.NumCols())
}
