package samples

import (
	"sort"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/internal/records"
)

type Sample struct {
	ExampleType string            `frostdb:"example_type,rle_dict,asc(0)"`
	Labels      map[string]string `frostdb:"labels,rle_dict,asc(1),null_first"`
	Stacktrace  []uuid.UUID       `frostdb:"stacktrace,rle_dict,asc(3),null_first"`
	Timestamp   int64             `frostdb:"timestamp,asc(2)"`
	Value       int64             `frostdb:"value"`
}

type Samples []Sample

func (s Samples) ToRecord() (arrow.Record, error) {
	b := records.NewBuild[Sample](memory.NewGoAllocator())
	defer b.Release()
	err := b.Append(s...)
	if err != nil {
		return nil, err
	}
	return b.NewRecord(), nil
}

func (s Samples) SampleLabelNames() []string {
	names := []string{}
	seen := map[string]struct{}{}

	for _, sample := range s {
		for label := range sample.Labels {
			if _, ok := seen[label]; !ok {
				names = append(names, label)
				seen[label] = struct{}{}
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
		if value, ok := s.Labels[labelNames[i]]; ok {
			row = append(row, parquet.ValueOf(value).Level(0, 1, i+1))
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
	row = append(row, parquet.ValueOf(ExtractLocationIDs(s.Stacktrace)).Level(0, 0, nameNumber+1))
	row = append(row, parquet.ValueOf(s.Timestamp).Level(0, 0, nameNumber+2))
	row = append(row, parquet.ValueOf(s.Value).Level(0, 0, nameNumber+3))

	return row
}

func ExtractLocationIDs(locs []uuid.UUID) []byte {
	b := make([]byte, len(locs)*16) // UUID are 16 bytes thus multiply by 16
	index := 0
	for i := len(locs) - 1; i >= 0; i-- {
		copy(b[index:index+16], locs[i][:])
		index += 16
	}
	return b
}

func PrehashedSampleDefinition() *schemapb.Schema {
	return &schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "example_type",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: false,
		}, {
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Nullable: true,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: true,
			Prehash: true,
		}, {
			Name: "stacktrace",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: false,
			Prehash: true,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "example_type",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "labels",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}, {
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "stacktrace",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}},
	}
}

func SampleDefinition() *schemapb.Schema {
	return &schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "example_type",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: false,
		}, {
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Nullable: true,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: true,
		}, {
			Name: "stacktrace",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: false,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "example_type",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "labels",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}, {
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "stacktrace",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}},
	}
}

// Adds a float column to the SampleDefinition to be able to test
// aggregations with float values.
func SampleDefinitionWithFloat() *schemapb.Schema {
	sample := SampleDefinition()
	sample.Columns = append(sample.Columns, &schemapb.Column{
		Name: "floatvalue",
		StorageLayout: &schemapb.StorageLayout{
			Type:     schemapb.StorageLayout_TYPE_DOUBLE,
			Nullable: true,
		},
		Dynamic: false,
	})
	return sample
}

func NewTestSamples() Samples {
	return Samples{
		{
			ExampleType: "cpu",
			Labels: map[string]string{
				"node": "test3",
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: 2,
			Value:     5,
		}, {
			ExampleType: "cpu",
			Labels: map[string]string{
				"namespace": "default",
				"pod":       "test1",
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: 2,
			Value:     3,
		}, {
			ExampleType: "cpu",
			Labels: map[string]string{
				"container": "test2",
				"namespace": "default",
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: 2,
			Value:     3,
		},
	}
}

func GenerateTestSamples(n int) Samples {
	s := Samples{}
	for i := 0; i < n; i++ {
		s = append(s,
			Sample{
				ExampleType: "cpu",
				Labels: map[string]string{
					"node": "test3",
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: int64(i),
				Value:     int64(i),
			})
	}
	return s
}
