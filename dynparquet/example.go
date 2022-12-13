package dynparquet

import (
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	schemav2pb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha2"
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

	rows := make([]parquet.Row, len(s))
	for i, sample := range s {
		rows[i] = sample.ToParquetRow(names)
	}

	_, err = pb.WriteRows(rows)
	if err != nil {
		return nil, err
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
			Name:       "stacktrace",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}, {
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
}

func NewSampleSchema() *Schema {
	s, err := SchemaFromDefinition(SampleDefinition())
	if err != nil {
		panic(err)
	}
	return s
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

func NewNestedSampleSchema(t *testing.T) *Schema {
	def := &schemav2pb.Schema{
		Root: &schemav2pb.Group{
			Name: "nested",
			Nodes: []*schemav2pb.Node{
				{
					Type: &schemav2pb.Node_Group{
						Group: &schemav2pb.Group{
							Name: "labels",
							Nodes: []*schemav2pb.Node{
								{
									Type: &schemav2pb.Node_Leaf{
										Leaf: &schemav2pb.Leaf{
											Name: "label1",
											StorageLayout: &schemav2pb.StorageLayout{
												Type:     schemav2pb.StorageLayout_TYPE_STRING,
												Nullable: true,
												Encoding: schemav2pb.StorageLayout_ENCODING_RLE_DICTIONARY,
											},
										},
									},
								},
								{
									Type: &schemav2pb.Node_Leaf{
										Leaf: &schemav2pb.Leaf{
											Name: "label2",
											StorageLayout: &schemav2pb.StorageLayout{
												Type:     schemav2pb.StorageLayout_TYPE_STRING,
												Nullable: true,
												Encoding: schemav2pb.StorageLayout_ENCODING_RLE_DICTIONARY,
											},
										},
									},
								},
							},
						},
					},
				},
				{ // NOTE that this nested group structure for a list of ints is how parquet is converted from an arrow list of int64s
					Type: &schemav2pb.Node_Group{
						Group: &schemav2pb.Group{
							Name: "timestamps",
							Nodes: []*schemav2pb.Node{
								{
									Type: &schemav2pb.Node_Group{
										Group: &schemav2pb.Group{
											Name:     "list",
											Repeated: true,
											Nodes: []*schemav2pb.Node{
												{
													Type: &schemav2pb.Node_Leaf{
														Leaf: &schemav2pb.Leaf{
															Name: "element",
															StorageLayout: &schemav2pb.StorageLayout{
																Type:     schemav2pb.StorageLayout_TYPE_INT64,
																Nullable: true,
																Encoding: schemav2pb.StorageLayout_ENCODING_RLE_DICTIONARY,
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Type: &schemav2pb.Node_Group{
						Group: &schemav2pb.Group{
							Name: "values",
							Nodes: []*schemav2pb.Node{
								{
									Type: &schemav2pb.Node_Group{
										Group: &schemav2pb.Group{
											Name:     "list",
											Repeated: true,
											Nodes: []*schemav2pb.Node{
												{
													Type: &schemav2pb.Node_Leaf{
														Leaf: &schemav2pb.Leaf{
															Name: "element",
															StorageLayout: &schemav2pb.StorageLayout{
																Type:     schemav2pb.StorageLayout_TYPE_INT64,
																Nullable: true,
																Encoding: schemav2pb.StorageLayout_ENCODING_RLE_DICTIONARY,
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		SortingColumns: []*schemav2pb.SortingColumn{
			{
				Path:       "labels",
				Direction:  schemav2pb.SortingColumn_DIRECTION_ASCENDING,
				NullsFirst: true,
			},
			{
				Path:      "timestamp",
				Direction: schemav2pb.SortingColumn_DIRECTION_ASCENDING,
			},
		},
	}

	schema, err := SchemaFromDefinition(def)
	require.NoError(t, err)

	return schema
}
