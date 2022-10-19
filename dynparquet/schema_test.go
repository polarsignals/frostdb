package dynparquet

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestMergeRowBatches(t *testing.T) {
	schema := NewSampleSchema()
	samples := NewTestSamples()

	rowGroups := []DynamicRowGroup{}
	for _, sample := range samples {
		s := Samples{sample}
		rg, err := s.ToBuffer(schema)
		require.NoError(t, err)
		rowGroups = append(rowGroups, rg)
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(rowGroups), func(i, j int) { rowGroups[i], rowGroups[j] = rowGroups[j], rowGroups[i] })

	merge, err := schema.MergeDynamicRowGroups(rowGroups)
	require.NoError(t, err)

	b := bytes.NewBuffer(nil)
	require.NoError(t, err)
	w := parquet.NewWriter(b)
	_, err = w.WriteRowGroup(merge)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	buf := parquet.NewBuffer(merge.Schema())
	_, err = buf.WriteRowGroup(merge)
	require.NoError(t, err)

	// Check that the first label column has the exected values.
	rowBuf := make([]parquet.Row, 1)
	rows := buf.Rows()
	n, err := rows.ReadRows(rowBuf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	row := rowBuf[0]
	require.Equal(t, "test3", string(row[3].ByteArray()))

	n, err = rows.ReadRows(rowBuf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	row = rowBuf[0]
	require.True(t, row[3].IsNull())

	n, err = rows.ReadRows(rowBuf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	row = rowBuf[0]
	require.True(t, row[3].IsNull())
}

func TestMapMergedColumnNameIndexes(t *testing.T) {
	testCases := []struct {
		name             string
		merged, original []string
		expected         []int
	}{{
		name: "subset_empty_first",
		merged: []string{
			"example_type",
			"labels.container",
			"labels.namespace",
			"labels.node",
			"labels.pod",
			"timestamp",
			"value",
		},
		original: []string{
			"example_type",
			"labels.namespace",
			"labels.pod",
			"timestamp",
			"value",
		},
		expected: []int{
			0,
			-1,
			1,
			-1,
			2,
			3,
			4,
		},
	}, {
		name: "subset_non_empty_first",
		merged: []string{
			"example_type",
			"labels.container",
			"labels.namespace",
			"labels.node",
			"labels.pod",
			"timestamp",
			"value",
		},
		original: []string{
			"example_type",
			"labels.container",
			"labels.namespace",
			"timestamp",
			"value",
		},
		expected: []int{
			0,
			1,
			2,
			-1,
			-1,
			3,
			4,
		},
	}, {
		name: "full_subset",
		merged: []string{
			"example_type",
			"labels.container",
			"labels.namespace",
			"labels.node",
			"labels.pod",
			"timestamp",
			"value",
		},
		original: []string{
			"example_type",
			"labels.node",
			"timestamp",
			"value",
		},
		expected: []int{
			0,
			-1,
			-1,
			1,
			-1,
			2,
			3,
		},
	}}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v", tc.name), func(t *testing.T) {
			require.Equal(t, tc.expected, mapMergedColumnNameIndexes(tc.merged, tc.original))
		})
	}
}

func TestMultipleIterations(t *testing.T) {
	schema := NewSampleSchema()

	samples := Samples{{
		Labels: []Label{
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
		Labels: []Label{
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
		Labels: []Label{
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

	dbuf, err := samples.ToBuffer(schema)
	require.NoError(t, err)

	buf := dbuf.buffer

	rowBuf := make([]parquet.Row, 1)
	rows := buf.Rows()
	i := 0
	for {
		n, err := rows.ReadRows(rowBuf)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		i += n
	}
	require.Equal(t, 3, i)
	require.NoError(t, rows.Close())

	rows = buf.Rows()
	i = 0
	for {
		n, err := rows.ReadRows(rowBuf)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		i += n
	}
	require.Equal(t, 3, i)
	require.NoError(t, rows.Close())
}

func Test_SchemaFromParquetFile(t *testing.T) {
	schema := NewSampleSchema()

	samples := Samples{{
		Labels: []Label{
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
		Labels: []Label{
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
		Labels: []Label{
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

	dbuf, err := samples.ToBuffer(schema)
	require.NoError(t, err)

	b := bytes.NewBuffer(nil)
	require.NoError(t, schema.SerializeBuffer(b, dbuf))

	file, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(len(b.Bytes())))
	require.NoError(t, err)

	def, err := DefinitionFromParquetFile(file)
	require.NoError(t, err)
	require.Equal(t, SampleDefinition(), def)
}
