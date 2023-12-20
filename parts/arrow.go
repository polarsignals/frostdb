package parts

import (
	"bytes"
	"sort"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/util"
	"github.com/parquet-go/parquet-go"

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

func (p *arrowPart) Record() arrow.Record {
	return p.record
}

func (p *arrowPart) Release() { p.record.Release() }

func (p *arrowPart) SerializeBuffer(schema *dynparquet.Schema, w dynparquet.ParquetWriter) error {
	return pqarrow.RecordToFile(schema, w, p.record)
}

func (p *arrowPart) AsSerializedBuffer(schema *dynparquet.Schema) (*dynparquet.SerializedBuffer, error) {
	// If this is a Arrow record part, convert the record into a serialized buffer
	b := &bytes.Buffer{}

	w, err := schema.GetWriter(b, pqarrow.RecordDynamicCols(p.record), false)
	if err != nil {
		return nil, err
	}
	defer schema.PutWriter(w)
	if err := p.SerializeBuffer(schema, w.ParquetWriter); err != nil {
		return nil, err
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		return nil, err
	}

	buf, err := dynparquet.NewSerializedBuffer(f)
	if err != nil {
		return nil, err
	}

	return buf, nil
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

type ArrowCompact struct {
	mem    memory.Allocator
	schema *dynparquet.Schema
	fields []arrow.Field
	seen   map[string]struct{}
	byName map[string]array.Builder
}

func NewArrowCompact(mem memory.Allocator, schema *dynparquet.Schema) *ArrowCompact {
	return &ArrowCompact{
		mem:    mem,
		schema: schema,
		fields: make([]arrow.Field, 0, 64),
		seen:   make(map[string]struct{}),
		byName: make(map[string]array.Builder),
	}
}

func (a *ArrowCompact) Compact(parts []Part, opts ...Option) (newParts []Part, preSize, postSize int64, err error) {
	a.fields = a.fields[:0]
	clear(a.seen)
	clear(a.byName)
	for _, part := range parts {
		schema := part.Record().Schema()
		for _, f := range schema.Fields() {
			if _, ok := a.seen[f.Name]; ok {
				continue
			}
			a.fields = append(a.fields, f)
			a.seen[f.Name] = struct{}{}
		}
	}
	// To be consistent we make sure that fields are always sorted
	sort.Slice(a.fields, func(i, j int) bool {
		return a.fields[i].Name < a.fields[j].Name
	})
	schema := arrow.NewSchema(a.fields, nil)
	r := array.NewRecordBuilder(a.mem, schema)

	for i := range a.fields {
		a.byName[a.fields[i].Name] = r.Field(i)
	}
	defer r.Release()
	seen := make(map[string]struct{})
	for _, part := range parts {
		clear(seen)
		record := part.Record()
		preSize += util.TotalRecordSize(record)
		for i := 0; i < int(record.NumCols()); i++ {
			name := record.ColumnName(i)
			appendColumn(a.byName[name], record.Column(i))
			seen[name] = struct{}{}
		}
		for n, b := range a.byName {
			if _, ok := seen[n]; !ok {
				// This dynamic column was not found in the current appended part. Fill the
				// column with nulls.
				appendNulls(b, int(record.NumRows()))
			}
		}
	}
	result := r.NewRecord()
	postSize = util.TotalRecordSize(result)
	newParts = []Part{
		NewArrowPart(0, result, uint64(postSize),
			a.schema, opts...),
	}
	return
}

// appends array a to array builder b .It is named appendColumn because it is
// used to build record columns.
//
// supports
//   - all base type
//   - nullable base types
//   - repeated arrays of all base types(nullable and non nullable)
func appendColumn(b array.Builder, a arrow.Array) {
	switch e := b.(type) {
	case *array.Int64Builder:
		appendArray[int64](e, a.(*array.Int64))
	case *array.Float64Builder:
		appendArray[float64](e, a.(*array.Float64))
	case *array.BooleanBuilder:
		appendArray[bool](e, a.(*array.Boolean))
	case *array.StringBuilder:
		appendArray[[]byte](e, &wrapString{String: a.(*array.String)})
	case *array.BinaryDictionaryBuilder:
		e.Reserve(a.Len())
		d := a.(*array.Dictionary)
		v := d.Dictionary().(*array.String)
		for i := 0; i < a.Len(); i++ {
			if d.IsNull(i) {
				e.AppendNull()
				continue
			}
			_ = e.AppendString(v.Value(d.GetValueIndex(i)))
		}
	case *array.ListBuilder:
		switch value := e.ValueBuilder().(type) {
		case *array.Int64Builder:
			appendList[int64](e, value, a.(*array.List), func(a arrow.Array) baseArray[int64] {
				return a.(*array.Int64)
			})
		case *array.Float64Builder:
			appendList[float64](e, value, a.(*array.List), func(a arrow.Array) baseArray[float64] {
				return a.(*array.Float64)
			})
		case *array.BooleanBuilder:
			appendList[bool](e, value, a.(*array.List), func(a arrow.Array) baseArray[bool] {
				return a.(*array.Boolean)
			})
		case *array.StringBuilder:
			appendList[[]byte](e, value, a.(*array.List), func(a arrow.Array) baseArray[[]byte] {
				return &wrapString{
					String: a.(*array.String),
				}
			})
		case *array.BinaryDictionaryBuilder:
			ls := a.(*array.List)
			values := ls.ListValues()
			for i := 0; i < a.Len(); i++ {
				if ls.IsNull(i) {
					e.AppendNull()
					continue
				}
				start, end := ls.ValueOffsets(i)
				va := array.NewSlice(values, start, end).(*array.Dictionary)
				de := va.Dictionary().(*array.String)
				value.Reserve(va.Len())
				for j := 0; j < va.Len(); j++ {
					if va.IsNull(j) {
						value.AppendNull()
						continue
					}
					_ = value.AppendString(de.Value(va.GetValueIndex(j)))
				}
				va.Release()
			}
		}
	}
}

type arrayBuilder[T any] interface {
	Reserve(int)
	AppendNull()
	UnsafeAppend(T)
}

type baseArray[T any] interface {
	Len() int
	IsNull(int) bool
	Value(int) T
	Release()
}

type wrapString struct {
	*array.String
}

func (a *wrapString) Value(i int) []byte {
	return []byte(a.String.Value(i))
}

func appendList[T any](
	b *array.ListBuilder,
	valueBuild arrayBuilder[T],
	ls *array.List,
	a func(arrow.Array) baseArray[T],
) {
	values := ls.ListValues()
	for i := 0; i < ls.Len(); i++ {
		if ls.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(true)
		start, end := ls.ValueOffsets(i)
		v := a(array.NewSlice(values, start, end))
		valueBuild.Reserve(v.Len())
		for j := 0; j < v.Len(); j++ {
			valueBuild.UnsafeAppend(v.Value(j))
		}
		v.Release()
	}
}

func appendArray[T any](b arrayBuilder[T], a baseArray[T]) {
	b.Reserve(a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.UnsafeAppend(a.Value(i))
	}
}

func appendNulls(b array.Builder, n int) {
	b.Reserve(n)
	for i := 0; i < n; i++ {
		b.UnsafeAppendBoolToBitmap(true)
	}
}
