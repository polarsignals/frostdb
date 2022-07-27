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
	schema *dynparquet.Schema,
	rg parquet.RowGroup,
	physicalProjections []logicalplan.Expr,
	projections []logicalplan.Expr,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
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
				if distinctColumns[0].MatchColumn(name) {
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
			if includedProjection(physicalProjections, parquetField.Name()) {
				af, err := convert.ParquetFieldToArrowField(parquetField)
				if err != nil {
					return nil, err
				}
				fields = append(fields, af)
			}
		}
	}

	for _, distinctExpr := range distinctColumns {
		if distinctExpr.Computed() {
			dataType, err := distinctExpr.DataType(schema)
			if err != nil {
				return nil, err
			}
			fields = append(fields, arrow.Field{
				Name:     distinctExpr.Name(),
				Type:     dataType,
				Nullable: true, // TODO: This should be determined by the expression and underlying column(s).
			})
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func includedProjection(projections []logicalplan.Expr, name string) bool {
	if len(projections) == 0 {
		return true
	}

	for _, p := range projections {
		if p.MatchColumn(name) {
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
	distinctColumns []logicalplan.Expr,
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
	distinctColumns []logicalplan.Expr,
) (arrow.Record, error) {
	s := rg.Schema()
	parquetColumns := rg.ColumnChunks()
	parquetFields := s.Fields()

	if filterExpr == nil && len(distinctColumns) == 0 && SingleMatchingColumn(distinctColumns, parquetFields) {
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
				if distinctColumns[0].MatchColumn(name) {
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

	if filterExpr == nil && len(distinctColumns) != 0 {
		// Since we're not filtering, we can use a faster path for distinct
		// columns. If all the distinct columns are dictionary encoded, we can
		// check their dictionaries and if all of them have a single value, we
		// can just return a single row with each of their values.
		res, err := distinctColumnsToArrowRecord(
			ctx,
			pool,
			schema,
			parquetFields,
			parquetColumns,
			distinctColumns,
		)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
		// If we get here, we couldn't use the fast path.
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

	r := array.NewRecord(schema, cols, rows)
	return r, nil
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

	err = writeColumnToArray(
		c,
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

// writeColumnToArray writes the values of a single parquet column to an arrow
// array. It will attempt to make shortcuts if possible to not read the whole
// column. Possilibities why it might not read the whole column:
//
// * If it has been requested to only read the dictionary it will only do that
// (provided it's not a repeated type).
//
// If the type is a repeated type it will also write the starting offsets of
// lists to the list builder.
func writeColumnToArray(
	columnChunk parquet.ColumnChunk,
	optional bool,
	repeated bool,
	lb *array.ListBuilder,
	w writer.ValueWriter,
	dictionaryOnly bool,
) error {
	pages := columnChunk.Pages()
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

// SingleMatchingColumn returns true if there is only a single matching column for the given column matchers.
func SingleMatchingColumn(distinctColumns []logicalplan.Expr, fields []parquet.Field) bool {
	count := 0
	for _, col := range distinctColumns {
		for _, field := range fields {
			name := field.Name()
			if col.MatchColumn(name) {
				count++
				if count > 1 {
					return false
				}
			}
		}
	}

	return count == 1
}

func distinctColumnsToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	schema *arrow.Schema,
	parquetFields []parquet.Field,
	parquetColumns []parquet.ColumnChunk,
	distinctColumns []logicalplan.Expr,
) (arrow.Record, error) {
	cols := make([]arrow.Array, len(schema.Fields()))
	for i, field := range parquetFields {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			name := field.Name()
			for _, distinctColumn := range distinctColumns {
				matchers := distinctColumn.ColumnsUsedExprs()
				if len(matchers) != 1 {
					// The expression is more complex than just a single binary
					// expression, so we can't apply the optimization.
					return nil, nil
				}
				if matchers[0].MatchColumn(name) {
					// Fast path for distinct queries.
					array, err := writeDistinctColumnToArray(
						pool,
						field,
						parquetColumns[i],
						distinctColumn,
					)
					if err != nil {
						return nil, err
					}
					if array == nil {
						// Optimization could not be applied.
						return nil, nil
					}

					index := schema.FieldIndices(distinctColumn.Name())[0]
					cols[index] = array
				}
			}
		}
	}

	if allArraysNoRows(cols) {
		for i, field := range schema.Fields() {
			if cols[i] == nil {
				cols[i] = array.NewBuilder(pool, field.Type).NewArray()
			}
		}
		return array.NewRecord(schema, cols, 0), nil
	}

	count, maxRows, maxColIndex := countArraysWithRowsLargerThanOne(cols)
	if count > 1 {
		// Can't apply the optimization as we can't know the combination of
		// distinct values.
		for _, col := range cols {
			if col != nil {
				col.Release()
			}
		}
		return nil, nil
	}

	// At this point we know there is at most one column with more than one
	// row. Therefore we can repeat the values of the other columns as those
	// are the only possible combinations within the rowgroup.

	for i, field := range schema.Fields() {
		// Columns that had no values are just backfilled with null values.
		if cols[i] == nil {
			b := array.NewBuilder(pool, field.Type)
			for j := 0; j < maxRows; j++ {
				b.AppendNull()
			}
			cols[i] = b.NewArray()
		}

		// Columns with a single value must be repeated to match the maximum
		// number of rows. Also we need to make sure the column is not the one
		// with the maximum values as that array we want to retain the exact
		// values.
		if maxRows > 1 && maxColIndex != i {
			cols[i] = repeatArray(pool, cols[i], maxRows)
		}
	}
	return array.NewRecord(schema, cols, int64(maxRows)), nil
}

func allArraysNoRows(arrays []arrow.Array) bool {
	for _, arr := range arrays {
		if arr == nil {
			continue
		}
		if arr.Len() != 0 {
			return false
		}
	}
	return true
}

func countArraysWithRowsLargerThanOne(arrays []arrow.Array) (int, int, int) {
	count := 0
	maxRows := 0
	maxColIndex := 0
	for i, arr := range arrays {
		if arr == nil {
			continue
		}
		len := arr.Len()
		if len > maxRows {
			maxRows = len
			maxColIndex = i
		}
		if len > 1 {
			count++
		}
	}
	return count, maxRows, maxColIndex
}

func writeDistinctColumnToArray(
	pool memory.Allocator,
	node parquet.Node,
	columnChunk parquet.ColumnChunk,
	distinctExpr logicalplan.Expr,
) (arrow.Array, error) {
	switch expr := distinctExpr.(type) {
	case *logicalplan.BinaryExpr:
		return binaryDistinctExpr(
			pool,
			node.Type(),
			columnChunk,
			expr,
		)
	case *logicalplan.Column:
		return parquetColumnToArrowArray(
			pool,
			node,
			columnChunk,
			true,
		)
	default:
		return nil, nil
	}
}

type PreExprVisitorFunc func(expr logicalplan.Expr) bool

func (f PreExprVisitorFunc) PreVisit(expr logicalplan.Expr) bool {
	return f(expr)
}

func (f PreExprVisitorFunc) PostVisit(expr logicalplan.Expr) bool {
	return false
}

func binaryDistinctExpr(
	pool memory.Allocator,
	typ parquet.Type,
	columnChunk parquet.ColumnChunk,
	expr *logicalplan.BinaryExpr,
) (arrow.Array, error) {
	var (
		value parquet.Value
		err   error
	)
	expr.Right.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
		switch e := expr.(type) {
		case *logicalplan.LiteralExpr:
			value, err = ArrowScalarToParquetValue(e.Value)
			return false
		}
		return true
	}))
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case logicalplan.OpGt:
		index := columnChunk.ColumnIndex()
		allGreater, noneGreater := allOrNoneGreaterThan(
			typ,
			index,
			value,
		)

		if allGreater || noneGreater {
			b := array.NewBooleanBuilder(pool)
			defer b.Release()
			if allGreater {
				b.Append(true)
			}
			if noneGreater {
				b.Append(false)
			}
			return b.NewArray(), nil
		}
	default:
		return nil, nil
	}

	return nil, nil
}

func allOrNoneGreaterThan(
	typ parquet.Type,
	index parquet.ColumnIndex,
	value parquet.Value,
) (bool, bool) {
	numPages := index.NumPages()
	allTrue := true
	allFalse := true
	for i := 0; i < numPages; i++ {
		min := index.MinValue(i)
		max := index.MaxValue(i)

		if typ.Compare(max, value) <= 0 {
			allTrue = false
		}

		if typ.Compare(min, value) > 0 {
			allFalse = false
		}
	}

	return allTrue, allFalse
}

func repeatArray(
	pool memory.Allocator,
	arr arrow.Array,
	count int,
) arrow.Array {
	defer arr.Release()
	switch arr := arr.(type) {
	case *array.Boolean:
		return repeatBooleanArray(pool, arr, count)
	case *array.Binary:
		return repeatBinaryArray(pool, arr, count)
	case *array.Int64:
		return repeatInt64Array(pool, arr, count)
	case *array.Uint64:
		return repeatUint64Array(pool, arr, count)
	case *array.Float64:
		return repeatFloat64Array(pool, arr, count)
	default:
		panic(fmt.Sprintf("unsupported array type: %T", arr))
	}
}

func repeatBooleanArray(
	pool memory.Allocator,
	arr *array.Boolean,
	count int,
) *array.Boolean {
	b := array.NewBooleanBuilder(pool)
	defer b.Release()
	val := arr.Value(0)
	vals := make([]bool, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
	return b.NewBooleanArray()
}

func repeatBinaryArray(
	pool memory.Allocator,
	arr *array.Binary,
	count int,
) *array.Binary {
	b := array.NewBinaryBuilder(pool, &arrow.BinaryType{})
	defer b.Release()
	val := arr.Value(0)
	vals := make([][]byte, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
	return b.NewBinaryArray()
}

func repeatInt64Array(
	pool memory.Allocator,
	arr *array.Int64,
	count int,
) *array.Int64 {
	b := array.NewInt64Builder(pool)
	defer b.Release()
	val := arr.Value(0)
	vals := make([]int64, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
	return b.NewInt64Array()
}

func repeatUint64Array(
	pool memory.Allocator,
	arr *array.Uint64,
	count int,
) *array.Uint64 {
	b := array.NewUint64Builder(pool)
	defer b.Release()
	val := arr.Value(0)
	vals := make([]uint64, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
	return b.NewUint64Array()
}

func repeatFloat64Array(
	pool memory.Allocator,
	arr *array.Float64,
	count int,
) *array.Float64 {
	b := array.NewFloat64Builder(pool)
	defer b.Release()
	val := arr.Value(0)
	vals := make([]float64, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
	return b.NewFloat64Array()
}
