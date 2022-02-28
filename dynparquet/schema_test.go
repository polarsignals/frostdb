package dynparquet

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

type Label struct {
	Name  string
	Value string
}

type Sample struct {
	ExampleType string
	Labels      []Label
	Timestamp   int64
	Value       int64
}

type Samples []Sample

func (s Samples) ToBuffer(schema *Schema) (*Buffer, error) {
	names := s.SampleLabelNames()

	pb, err := schema.NewBuffer(map[string]DynamicColumns{
		"labels": DynamicColumns{
			Column: "labels",
			Names:  names,
		},
	})
	if err != nil {
		return nil, err
	}

	for _, sample := range s {
		r := sample.ToParquetRow(names)
		err := pb.WriteRow(r)
		if err != nil {
			return nil, err
		}
	}

	return pb, nil
}

func (s Samples) SampleLabelNames() []string {
	names := []string{}
	seen := map[string]struct{}{}

	for _, sample := range s {
		for _, label := range sample.Labels {
			if _, ok := seen[label.Name]; !ok {
				names = append(names, label.Name)
				seen[label.Name] = struct{}{}
			}
		}
	}
	sort.Strings(names)

	return names
}

func (s Sample) ToParquetRow(labelNames []string) parquet.Row {
	// The order of these appends is important. Parquet values must be in the
	// order of the schema and the schema orders columns by their names.

	nameNumber := len(labelNames)
	labelLen := len(s.Labels)
	row := make([]parquet.Value, 0, nameNumber+3)

	row = append(row, parquet.ValueOf(s.ExampleType).Level(0, 0, 0))

	i, j := 0, 0
	for i < nameNumber {
		if labelNames[i] == s.Labels[j].Name {
			row = append(row, parquet.ValueOf(s.Labels[j].Value).Level(0, 1, i+1))
			i++
			j++

			if j >= labelLen {
				for ; i < nameNumber; i++ {
					row = append(row, parquet.ValueOf(nil).Level(0, 0, i+1))
				}
				break
			}
		} else {
			row = append(row, parquet.ValueOf(nil).Level(0, 0, i+1))
			i++
		}
	}

	row = append(row, parquet.ValueOf(s.Timestamp).Level(0, 0, nameNumber+1))
	row = append(row, parquet.ValueOf(s.Value).Level(0, 0, nameNumber+2))

	return row
}

func NewSampleSchema() *Schema {
	return NewSchema(
		"test",
		[]ColumnDefinition{{
			Name:          "example_type",
			StorageLayout: parquet.Encoded(parquet.String(), &parquet.RLEDictionary),
			Dynamic:       false,
		}, {
			Name:          "labels",
			StorageLayout: parquet.Encoded(parquet.Optional(parquet.String()), &parquet.RLEDictionary),
			Dynamic:       true,
		}, {
			Name:          "timestamp",
			StorageLayout: parquet.Int(64),
			Dynamic:       false,
		}, {
			Name:          "value",
			StorageLayout: parquet.Int(64),
			Dynamic:       false,
		}},
		[]SortingColumn{
			Ascending("example_type"),
			NullsFirst(Ascending("labels")),
			Ascending("timestamp"),
		},
	)
}

func NewTestSamples() Samples {
	return Samples{{
		ExampleType: "cpu",
		Labels: []Label{{
			Name:  "namespace",
			Value: "default",
		}, {
			Name:  "pod",
			Value: "test1",
		}},
		Timestamp: 2,
		Value:     3,
	}, {
		ExampleType: "cpu",
		Labels: []Label{{
			Name:  "container",
			Value: "test2",
		}, {
			Name:  "namespace",
			Value: "default",
		}},
		Timestamp: 2,
		Value:     3,
	}, {
		ExampleType: "cpu",
		Labels: []Label{{
			Name:  "node",
			Value: "test3",
		}},
		Timestamp: 2,
		Value:     5,
	}}
}

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

	merge, err := schema.MergeDynamicRowGroups(rowGroups)
	require.NoError(t, err)

	b := bytes.NewBuffer(nil)
	require.NoError(t, err)
	w := parquet.NewWriter(b)
	_, err = w.WriteRowGroup(merge)
	require.NoError(t, err)
	require.NoError(t, w.Close())
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
