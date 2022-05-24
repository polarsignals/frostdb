package dynparquet

import (
	"sort"

	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
)

type Label struct {
	Name  string
	Value string
}

type Sample struct {
	ExampleType string
	Labels      []Label
	Stacktrace  []uuid.UUID
	Timestamp   int64
	Value       int64
}

type Samples []Sample

func (s Samples) ToBuffer(schema *Schema) (*Buffer, error) {
	names := s.SampleLabelNames()

	pb, err := schema.NewBuffer(map[string][]string{
		"labels": names,
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
	for i, location := range s.Stacktrace {
		if i == 0 {
			row = append(row, parquet.ValueOf(location).Level(0, 1, nameNumber+1))
			continue
		}
		row = append(row, parquet.ValueOf(location).Level(1, 1, nameNumber+1))
	}
	row = append(row, parquet.ValueOf(s.Timestamp).Level(0, 0, nameNumber+2))
	row = append(row, parquet.ValueOf(s.Value).Level(0, 0, nameNumber+3))

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
			Name:          "stacktrace",
			StorageLayout: parquet.Encoded(parquet.Repeated(parquet.String()), &parquet.RLEDictionary),
			Dynamic:       false,
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
			NullsFirst(Ascending("stacktrace")),
			Ascending("timestamp"),
		},
	)
}

func NewTestSamples() Samples {
	return Samples{
		{
			ExampleType: "cpu",
			Labels: []Label{{
				Name:  "node",
				Value: "test3",
			}},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: 2,
			Value:     5,
		}, {
			ExampleType: "cpu",
			Labels: []Label{{
				Name:  "namespace",
				Value: "default",
			}, {
				Name:  "pod",
				Value: "test1",
			}},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
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
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: 2,
			Value:     3,
		},
	}
}
