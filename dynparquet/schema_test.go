package dynparquet

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

func TestMergeRowBatches(t *testing.T) {
	schema := NewSampleSchema()
	samples := NewTestSamples()

	rowGroups := []DynamicRowGroup{}
	for _, sample := range samples {
		s := Samples{sample}
		rg, err := ToBuffer(s, schema)
		require.NoError(t, err)
		rowGroups = append(rowGroups, rg)
	}

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
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	dbuf, err := ToBuffer(samples, schema)
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
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	dbuf, err := ToBuffer(samples, schema)
	require.NoError(t, err)

	b := bytes.NewBuffer(nil)
	require.NoError(t, schema.SerializeBuffer(b, dbuf))

	file, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(len(b.Bytes())))
	require.NoError(t, err)

	def, err := DefinitionFromParquetFile(file)
	require.NoError(t, err)
	require.Equal(t, SampleDefinition(), def)
}

func TestIsDynamicColumn(t *testing.T) {
	for _, tc := range []struct {
		input      string
		cName      string
		notDynamic bool
		expected   bool
	}{
		{
			input:    "labels.label1",
			cName:    "labels",
			expected: true,
		},
		{
			input:    "labels.label1.cannothavetwoperiods",
			cName:    "labels",
			expected: false,
		},
		{
			input:    "columnnotfound.label1",
			cName:    "labels",
			expected: false,
		},
		{
			input:      "labels.columnnotdynamic",
			cName:      "labels",
			notDynamic: true,
			expected:   false,
		},
		{
			input:    "",
			cName:    "labels",
			expected: false,
		},
	} {
		def := &schemapb.Schema{
			Name: "test_schema",
			Columns: []*schemapb.Column{{
				Name: tc.cName,
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_INT64,
					Encoding: schemapb.StorageLayout_ENCODING_PLAIN_UNSPECIFIED,
				},
				Dynamic: !tc.notDynamic,
			}},
		}
		schema, err := SchemaFromDefinition(def)
		require.NoError(t, err)
		schema.FindDynamicColumnForConcreteColumn(tc.cName)
	}
}

func BenchmarkIsDynamicColumn(b *testing.B) {
	def := &schemapb.Schema{
		Name: "test_schema",
		Columns: []*schemapb.Column{{
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_INT64,
				Encoding: schemapb.StorageLayout_ENCODING_PLAIN_UNSPECIFIED,
			},
			Dynamic: true,
		}},
	}
	schema, err := SchemaFromDefinition(def)
	require.NoError(b, err)
	for i := 0; i < b.N; i++ {
		schema.FindDynamicColumnForConcreteColumn("labels.label1")
	}
}

func TestMergeDynamicColumnSets(t *testing.T) {
	sets := []map[string][]string{
		{"labels": {"label1", "label2"}},
		{"labels": {"label1", "label2"}},
		{"labels": {"label1", "label2", "label3"}},
		{
			"labels": {"label1", "label2"},
			"foo":    {"label1", "label2"},
		},
		{
			"labels": {"label1", "label2", "label3"},
			"foo":    {"label1", "label2", "label3"},
		},
	}
	require.Equal(t, map[string][]string{
		"foo":    {"label1", "label2", "label3"},
		"labels": {"label1", "label2", "label3"},
	}, MergeDynamicColumnSets(sets))
}

func BenchmarkMergeDynamicColumnSets(b *testing.B) {
	sets := []map[string][]string{
		{"labels": {"label1", "label2"}},
		{"labels": {"label1", "label2"}},
		{"labels": {"label1", "label2", "label3"}},
		{
			"labels": {"label1", "label2"},
			"foo":    {"label1", "label2"},
		},
		{
			"labels": {"label1", "label2", "label3"},
			"foo":    {"label1", "label2", "label3"},
		},
	}
	for i := 0; i < b.N; i++ {
		MergeDynamicColumnSets(sets)
	}
}
