package arrowutils_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/internal/records"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
)

func TestEnsureSameSchema(t *testing.T) {
	type struct1 struct {
		Field1 int64 `frostdb:",asc(0)"`
		Field2 int64 `frostdb:",asc(1)"`
	}
	type struct2 struct {
		Field1 int64 `frostdb:",asc(0)"`
		Field3 int64 `frostdb:",asc(1)"`
	}
	type struct3 struct {
		Field1 int64 `frostdb:",asc(0)"`
		Field2 int64 `frostdb:",asc(1)"`
		Field3 int64 `frostdb:",asc(1)"`
	}

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	build1 := records.NewBuild[struct1](mem)
	defer build1.Release()
	err := build1.Append([]struct1{
		{Field1: 1, Field2: 2},
		{Field1: 1, Field2: 3},
	}...)
	require.NoError(t, err)

	build2 := records.NewBuild[struct2](mem)
	defer build2.Release()
	err = build2.Append([]struct2{
		{Field1: 1, Field3: 2},
		{Field1: 1, Field3: 3},
	}...)
	require.NoError(t, err)

	build3 := records.NewBuild[struct3](mem)
	defer build3.Release()
	err = build3.Append([]struct3{
		{Field1: 1, Field2: 1, Field3: 1},
		{Field1: 2, Field2: 2, Field3: 2},
	}...)
	require.NoError(t, err)

	record1 := build1.NewRecord()
	record2 := build2.NewRecord()
	record3 := build3.NewRecord()

	recs := []arrow.Record{record1, record2, record3}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	recs, err = arrowutils.EnsureSameSchema(recs)
	require.NoError(t, err)

	expected := []struct3{
		// record1
		{Field1: 1, Field2: 2, Field3: 0},
		{Field1: 1, Field2: 3, Field3: 0},
		// record2
		{Field1: 1, Field2: 0, Field3: 2},
		{Field1: 1, Field2: 0, Field3: 3},
		// record3
		{Field1: 1, Field2: 1, Field3: 1},
		{Field1: 2, Field2: 2, Field3: 2},
	}

	reader := records.NewReader[struct3](recs...)
	rows := reader.NumRows()
	require.Equal(t, int64(len(expected)), rows)

	actual := make([]struct3, rows)
	for i := 0; i < int(rows); i++ {
		actual[i] = reader.Value(i)
	}
	require.Equal(t, expected, actual)
}
