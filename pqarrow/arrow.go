package pqarrow

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow/convert"
	"github.com/polarsignals/frostdb/pqarrow/writer"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// ParquetRowGroupToArrowSchema converts a parquet row group to an arrow schema.
func ParquetRowGroupToArrowSchema(
	ctx context.Context,
	rg parquet.RowGroup,
	projections []logicalplan.ColumnMatcher,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.ColumnMatcher,
) (*arrow.Schema, error) {
	parquetFields := rg.Schema().Fields()

	if len(distinctColumns) == 1 && filterExpr == nil {
		// We can use the faster path for a single distinct column by just
		// returning its dictionary.
		fields := make([]arrow.Field, 0, 1)
		for _, field := range parquetFields {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				name := field.Name()
				if distinctColumns[0].Match(name) {
					af, err := convert.ParquetFieldToArrowField(field)
					if err != nil {
						return nil, err
					}
					fields = append(fields, af)
				}
			}
		}
		return arrow.NewSchema(fields, nil), nil
	}

	fields := make([]arrow.Field, 0, len(parquetFields))

	for _, parquetField := range parquetFields {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			if includedProjection(projections, parquetField.Name()) {
				af, err := convert.ParquetFieldToArrowField(parquetField)
				if err != nil {
					return nil, err
				}
				fields = append(fields, af)
			}
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func includedProjection(projections []logicalplan.ColumnMatcher, name string) bool {
	if len(projections) == 0 {
		return true
	}

	for _, p := range projections {
		if p.Match(name) {
			return true
		}
	}
	return false
}

// ParquetRowGroupToArrowRecord converts a parquet row group to an arrow record.
func ParquetRowGroupToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	rg parquet.RowGroup,
	schema *arrow.Schema,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.ColumnMatcher,
) (arrow.Record, error) {
	switch rg.(type) {
	case *dynparquet.MergedRowGroup:
		return rowBasedParquetRowGroupToArrowRecord(ctx, pool, rg, schema)
	default:
		return contiguousParquetRowGroupToArrowRecord(
			ctx,
			pool,
			rg,
			schema,
			filterExpr,
			distinctColumns,
		)
	}
}

var rowBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]parquet.Row, 64) // Random guess.
	},
}

// rowBasedParquetRowGroupToArrowRecord converts a parquet row group to an arrow record row by row.
func rowBasedParquetRowGroupToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	rg parquet.RowGroup,
	schema *arrow.Schema,
) (arrow.Record, error) {
	parquetFields := rg.Schema().Fields()

	if len(schema.Fields()) != len(parquetFields) {
		return nil, fmt.Errorf("inconsistent schema between arrow and parquet")
	}

	// Create arrow writers from arrow and parquet schema
	writers := make([]writer.ValueWriter, len(parquetFields))
	b := array.NewRecordBuilder(pool, schema)
	for i, field := range b.Fields() {
		_, newValueWriter, err := convert.ParquetNodeToTypeWithWriterFunc(parquetFields[i])
		if err != nil {
			return nil, err
		}
		writers[i] = newValueWriter(field, 0)
	}

	rows := rg.Rows()
	defer rows.Close()
	rowBuf := rowBufPool.Get().([]parquet.Row)
	defer rowBufPool.Put(rowBuf[:cap(rowBuf)])

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		rowBuf = rowBuf[:cap(rowBuf)]
		n, err := rows.ReadRows(rowBuf)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read row: %w", err)
		}
		rowBuf = rowBuf[:n]

		for i, writer := range writers {
			for _, row := range rowBuf {
				values := dynparquet.ValuesForIndex(row, i)
				writer.Write(values)
			}
		}
		if err == io.EOF {
			break
		}
	}

	return b.NewRecord(), nil
}

// contiguousParquetRowGroupToArrowRecord converts a parquet row group to an arrow record.
func contiguousParquetRowGroupToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	rg parquet.RowGroup,
	schema *arrow.Schema,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.ColumnMatcher,
) (arrow.Record, error) {
	s := rg.Schema()
	parquetColumns := rg.ColumnChunks()
	parquetFields := s.Fields()

	if len(distinctColumns) == 1 && filterExpr == nil {
		// We can use the faster path for a single distinct column by just
		// returning its dictionary.
		cols := make([]arrow.Array, 0, 1)
		rows := int64(0)
		for i, field := range parquetFields {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				name := field.Name()
				if distinctColumns[0].Match(name) {
					array, err := parquetColumnToArrowArray(
						pool,
						field,
						parquetColumns[i],
						true,
					)
					if err != nil {
						return nil, fmt.Errorf("convert parquet column to arrow array: %w", err)
					}
					cols = append(cols, array)
					rows = int64(array.Len())
				}
			}
		}

		return array.NewRecord(schema, cols, rows), nil
	}

	cols := make([]arrow.Array, len(schema.Fields()))

	for i, parquetField := range parquetFields {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			if schema.HasField(parquetField.Name()) {
				a, err := parquetColumnToArrowArray(
					pool,
					parquetField,
					parquetColumns[i],
					false,
				)
				if err != nil {
					return nil, fmt.Errorf("convert parquet column to arrow array: %w", err)
				}

				index := schema.FieldIndices(parquetField.Name())[0]
				cols[index] = a
			}
		}
	}

	rows := rg.NumRows()

	for i, field := range schema.Fields() {
		if cols[i] == nil {
			// If the column is empty we need to append NULL as often as we have rows
			// TODO: Is there a faster or better way?
			b := array.NewBuilder(pool, field.Type)
			for j := int64(0); j < rows; j++ {
				b.AppendNull()
			}
			cols[i] = b.NewArray()
		}
	}

	return array.NewRecord(schema, cols, rows), nil
}

// parquetColumnToArrowArray converts a single parquet column to an arrow array
// and returns the type, nullability, and the actual resulting arrow array. If
// a column is a repeated type, it automatically boxes it into the appropriate
// arrow equivalent.
func parquetColumnToArrowArray(
	pool memory.Allocator,
	n parquet.Node,
	c parquet.ColumnChunk,
	dictionaryOnly bool,
) (
	arrow.Array,
	error,
) {
	at, newValueWriter, err := convert.ParquetNodeToTypeWithWriterFunc(n)
	if err != nil {
		return nil, fmt.Errorf("convert ParquetNodeToTypeWithWriterFunc failed: %v", err)
	}

	var (
		w  writer.ValueWriter
		b  array.Builder
		lb *array.ListBuilder

		repeated = false
	)

	optional := n.Optional()

	// Using the retrieved arrow type and whether the type is repeated we can
	// build a type-safe `writeValues` function that only casts the resulting
	// builder once and can perform optimized transfers of the page values to
	// the target array.
	if n.Repeated() {
		// If the column is repeated, we need to box it into a list.
		lt := arrow.ListOf(at)
		lt.SetElemNullable(optional)
		at = lt

		repeated = true

		lb = array.NewBuilder(pool, at).(*array.ListBuilder)
		// A list builder actually expects all values of all sublists to be
		// written contiguously, the offsets where litsts start are recorded in
		// the offsets array below.
		w = newValueWriter(lb.ValueBuilder(), int(c.NumValues()))
		b = lb
	} else {
		b = array.NewBuilder(pool, at)
		w = newValueWriter(b, int(c.NumValues()))
	}
	defer b.Release()

	err = writePagesToArray(
		c.Pages(),
		optional,
		repeated,
		lb,
		w,
		dictionaryOnly,
	)
	if err != nil {
		return nil, fmt.Errorf("writePagesToArray failed: %v", err)
	}

	arr := b.NewArray()

	// Is this a bug in arrow? We already set the nullability above, but it
	// doesn't appear to transfer into the resulting array's type. Needs to be
	// investigated.
	switch t := arr.DataType().(type) {
	case *arrow.ListType:
		t.SetElemNullable(optional)
	}

	return arr, nil
}

// writePagesToArray reads all pages of a page iterator and writes the values
// to an array builder. If the type is a repeated type it will also write the
// starting offsets of lists to the list builder.
func writePagesToArray(
	pages parquet.Pages,
	optional bool,
	repeated bool,
	lb *array.ListBuilder,
	w writer.ValueWriter,
	dictionaryOnly bool,
) error {
	defer pages.Close()
	// We are potentially writing multiple pages to the same array, so we need
	// to keep track of the index of the offsets in case this is a List-type.
	i := 0
	for {
		p, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read page: %w", err)
		}
		dict := p.Dictionary()

		switch {
		case !repeated && dictionaryOnly && dict != nil:
			// If we are only writing the dictionary, we don't need to read
			// the values.
			err = w.WritePage(dict.Page())
			if err != nil {
				return fmt.Errorf("write dictionary page: %w", err)
			}
		case !repeated && !optional && dict == nil:
			// If the column is not optional, we can read all values at once
			// consecutively without worrying about null values.
			err = w.WritePage(p)
			if err != nil {
				return fmt.Errorf("write page: %w", err)
			}
		default:
			values := make([]parquet.Value, p.NumValues())
			reader := p.Values()
			_, err = reader.ReadValues(values)
			// We're reading all values in the page so we always expect an io.EOF.
			if err != nil && err != io.EOF {
				return fmt.Errorf("read values: %w", err)
			}

			w.Write(values)

			if repeated {
				offsets := []int32{}
				validity := []bool{}
				for _, v := range values {
					rep := v.RepetitionLevel()
					def := v.DefinitionLevel()
					if rep == 0 && def == 1 {
						offsets = append(offsets, int32(i))
						validity = append(validity, true)
					}
					if rep == 0 && def == 0 {
						offsets = append(offsets, int32(i))
						validity = append(validity, false)
					}
					// rep == 1 && def == 1 means the item in the list is not null which is handled by the value writer
					// rep == 1 && def == 0 means the item in the list is null which is handled by the value writer
					i++
				}

				lb.AppendValues(offsets, validity)
			}
		}
	}

	return nil
}
