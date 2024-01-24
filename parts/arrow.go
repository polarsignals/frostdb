package parts

import (
	"github.com/apache/arrow/go/v14/arrow"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
)

// arrow implments the Part interface backed by an Arrow record.
type arrowPart struct {
	basePart

	schema *dynparquet.Schema
	record arrow.Record
	size   uint64
}

// NewArrowPart returns a new Arrow part.
func NewArrowPart(tx uint64, record arrow.Record, size uint64, schema *dynparquet.Schema, options ...Option) Part {
	p := &arrowPart{
		basePart: basePart{
			tx: tx,
		},
		schema: schema,
		record: record,
		size:   size,
	}

	for _, option := range options {
		option(&p.basePart)
	}

	return p
}

func (p *arrowPart) Retain() { p.record.Retain() }

func (p *arrowPart) Record() arrow.Record {
	return p.record
}

func (p *arrowPart) Release() { p.record.Release() }

func (p *arrowPart) SerializeBuffer(schema *dynparquet.Schema, w dynparquet.ParquetWriter) error {
	return pqarrow.RecordToFile(schema, w, p.record)
}

func (p *arrowPart) AsSerializedBuffer(schema *dynparquet.Schema) (*dynparquet.SerializedBuffer, error) {
	return pqarrow.SerializeRecord(p.record, schema)
}

func (p *arrowPart) NumRows() int64 {
	return p.record.NumRows()
}

func (p *arrowPart) Size() int64 {
	return int64(p.size)
}

// Least returns the least row  in the part.
func (p *arrowPart) Least() (*dynparquet.DynamicRow, error) {
	if p.minRow != nil {
		return p.minRow, nil
	}

	dynCols := pqarrow.RecordDynamicCols(p.record)
	pooledSchema, err := p.schema.GetDynamicParquetSchema(dynCols)
	if err != nil {
		return nil, err
	}
	defer p.schema.PutPooledParquetSchema(pooledSchema)
	p.minRow, err = pqarrow.RecordToDynamicRow(p.schema, pooledSchema.Schema, p.record, dynCols, 0)
	if err != nil {
		return nil, err
	}

	return p.minRow, nil
}

func (p *arrowPart) Most() (*dynparquet.DynamicRow, error) {
	if p.maxRow != nil {
		return p.maxRow, nil
	}

	dynCols := pqarrow.RecordDynamicCols(p.record)
	pooledSchema, err := p.schema.GetDynamicParquetSchema(dynCols)
	if err != nil {
		return nil, err
	}
	defer p.schema.PutPooledParquetSchema(pooledSchema)
	p.maxRow, err = pqarrow.RecordToDynamicRow(p.schema, pooledSchema.Schema, p.record, dynCols, int(p.record.NumRows()-1))
	if err != nil {
		return nil, err
	}

	return p.maxRow, nil
}

func (p *arrowPart) OverlapsWith(schema *dynparquet.Schema, otherPart Part) (bool, error) {
	a, err := p.Least()
	if err != nil {
		return false, err
	}
	b, err := p.Most()
	if err != nil {
		return false, err
	}
	c, err := otherPart.Least()
	if err != nil {
		return false, err
	}
	d, err := otherPart.Most()
	if err != nil {
		return false, err
	}

	return schema.Cmp(a, d) <= 0 && schema.Cmp(c, b) <= 0, nil
}
