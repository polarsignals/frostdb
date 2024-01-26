package dynparquet

import (
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/samples"
)

type (
	Samples = samples.Samples
	Sample  = samples.Sample
)

var (
	SampleDefinition          = samples.SampleDefinition
	NewTestSamples            = samples.NewTestSamples
	PrehashedSampleDefinition = samples.PrehashedSampleDefinition
	SampleDefinitionWithFloat = samples.SampleDefinitionWithFloat
	GenerateTestSamples       = samples.GenerateTestSamples
)

func ToBuffer(s Samples, schema *Schema) (*Buffer, error) {
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

func NewSampleSchema() *Schema {
	s, err := SchemaFromDefinition(SampleDefinition())
	if err != nil {
		panic(err)
	}
	return s
}
