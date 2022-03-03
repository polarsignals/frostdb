package pqarrow

import (
	"testing"

	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/google/uuid"
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

	ar, err := ParquetRowGroupToArrowRecord(memory.DefaultAllocator, b)
	require.NoError(t, err)
	require.Equal(t, int64(len(lists)), ar.NumRows())
	require.Equal(t, int64(3), ar.NumCols())
}
