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
			dataType, err := distinctExpr.DataType(rg.Schema())
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
// The result is appended to builder.
func ParquetRowGroupToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	rg parquet.RowGroup,
	schema *arrow.Schema,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
	builder *array.RecordBuilder,
) error {
	switch rg.(type) {
	case *dynparquet.MergedRowGroup:
		return rowBasedParquetRowGroupToArrowRecord(ctx, pool, rg, schema, builder)
	default:
		return contiguousParquetRowGroupToArrowRecord(
			ctx,
			pool,
			rg,
			schema,
			filterExpr,
			distinctColumns,
			builder,
		)
	}
}

var rowBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]parquet.Row, 64) // Random guess.
	},
}

// rowBasedParquetRowGroupToArrowRecord converts a parquet row group to an arrow
// record row by row. The result is appended to b.
func rowBasedParquetRowGroupToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	rg parquet.RowGroup,
	schema *arrow.Schema,
	builder *array.RecordBuilder,
) error {
	parquetFields := rg.Schema().Fields()

	if len(schema.Fields()) != len(parquetFields) {
		return fmt.Errorf("inconsistent schema between arrow and parquet")
	}

	// Create arrow writers from arrow and parquet schema
	writers := make([]writer.ValueWriter, len(parquetFields))
	for i, field := range builder.Fields() {
		_, newValueWriter, err := convert.ParquetNodeToTypeWithWriterFunc(parquetFields[i])
		if err != nil {
			return err
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
			return ctx.Err()
		default:
		}
		rowBuf = rowBuf[:cap(rowBuf)]
		n, err := rows.ReadRows(rowBuf)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("read row: %w", err)
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

	return nil
}

// contiguousParquetRowGroupToArrowRecord converts a parquet row group to an
// arrow record. The result is written to builder.
func contiguousParquetRowGroupToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	rg parquet.RowGroup,
	schema *arrow.Schema,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
	builder *array.RecordBuilder,
) error {
	s := rg.Schema()
	parquetColumns := rg.ColumnChunks()
	parquetFields := s.Fields()

	if filterExpr == nil && len(distinctColumns) == 0 && SingleMatchingColumn(distinctColumns, parquetFields) {
		// We can use the faster path for a single distinct column by just
		// writing its dictionary.
		for i, field := range parquetFields {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// TODO(asubiotto): builder.Field(i) will panic as soon as the
				// if statement is modified.
				name := field.Name()
				if distinctColumns[0].MatchColumn(name) {
					if err := parquetColumnToArrowArray(
						pool,
						field,
						parquetColumns[i],
						true,
						builder.Field(i),
					); err != nil {
						return fmt.Errorf("convert parquet column to arrow array: %w", err)
					}
				}
			}
		}

		return nil
	}

	if filterExpr == nil && len(distinctColumns) != 0 {
		// Since we're not filtering, we can use a faster path for distinct
		// columns. If all the distinct columns are dictionary encoded, we can
		// check their dictionaries and if all of them have a single value, we
		// can just return a single row with each of their values.
		appliedOptimization, err := distinctColumnsToArrowRecord(
			ctx,
			pool,
			schema,
			parquetFields,
			parquetColumns,
			distinctColumns,
			builder,
		)
		if err != nil {
			return err
		}
		if appliedOptimization {
			return nil
		}
		// If we get here, we couldn't use the fast path.
	}

	for i, parquetField := range parquetFields {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if indices := schema.FieldIndices(parquetField.Name()); len(indices) > 0 {
				if err := parquetColumnToArrowArray(
					pool,
					parquetField,
					parquetColumns[i],
					false,
					builder.Field(indices[0]),
				); err != nil {
					return fmt.Errorf("convert parquet column to arrow array: %w", err)
				}
			}
		}
	}

	maxLen, _, anomaly := recordBuilderLength(builder)
	if !anomaly {
		return nil
	}

	for _, field := range builder.Fields() {
		if fieldLen := field.Len(); fieldLen < maxLen {
			// If the column is not the same length as the maximum length
			// column, we need to append NULL as often as we have rows
			// TODO: Is there a faster or better way?
			for i := 0; i < maxLen-fieldLen; i++ {
				field.AppendNull()
			}
		}
	}

	return nil
}

// parquetColumnToArrowArray converts a single parquet column to an arrow array
// and writes the result to builder. If a column is a repeated type, it
// automatically boxes it into the appropriate arrow equivalent.
func parquetColumnToArrowArray(
	pool memory.Allocator,
	n parquet.Node,
	c parquet.ColumnChunk,
	dictionaryOnly bool,
	builder array.Builder,
) error {
	_, newValueWriter, err := convert.ParquetNodeToTypeWithWriterFunc(n)
	if err != nil {
		return fmt.Errorf("convert ParquetNodeToTypeWithWriterFunc failed: %v", err)
	}

	if err := writeColumnToArray(
		n.Type(),
		c,
		n.Optional(),
		n.Repeated(),
		newValueWriter(builder, int(c.NumValues())),
		dictionaryOnly,
	); err != nil {
		return fmt.Errorf("writePagesToArray failed: %v", err)
	}

	return nil
}

// writeColumnToArray writes the values of a single parquet column to an arrow
// array. It will attempt to make shortcuts if possible to not read the whole
// column. Possilibities why it might not read the whole column:
//
// * If it has been requested to only read the dictionary it will only do that
// (provided it's not a repeated type). Additionally, decompression of all pages
// are avoided if the column index indicates that there is only one value in the
// column.
//
// If the type is a repeated type it will also write the starting offsets of
// lists to the list builder.
func writeColumnToArray(
	t parquet.Type,
	columnChunk parquet.ColumnChunk,
	optional bool,
	repeated bool,
	w writer.ValueWriter,
	dictionaryOnly bool,
) error {
	if !repeated && dictionaryOnly {
		// Check all the page indexes of the column chunk. If they are
		// trustworthy and there is only one value contained in the column
		// chunk, we can avoid reading any pages and construct a dictionary from
		// the index values.
		// TODO(asubiotto): This optimization can be applied at a finer
		// granularity at the page level as well.
		columnIndex := columnChunk.ColumnIndex()
		columnType := columnChunk.Type()

		globalMinValue := columnIndex.MinValue(0)
		readPages := false
		for pageIdx := 0; pageIdx < columnIndex.NumPages(); pageIdx++ {
			if columnType.Length() == 0 {
				// Variable-length datatype. The index can only be trusted if
				// the size of the values is less than the column index size,
				// since we cannot otherwise know if the index values are
				// truncated.
				if len(columnIndex.MinValue(pageIdx).Bytes()) >= dynparquet.ColumnIndexSize ||
					len(columnIndex.MaxValue(pageIdx).Bytes()) >= dynparquet.ColumnIndexSize {
					readPages = true
					break
				}
			}

			minValue := columnIndex.MinValue(pageIdx)
			maxValue := columnIndex.MaxValue(pageIdx)
			if columnType.Compare(minValue, maxValue) != 0 ||
				columnType.Compare(globalMinValue, minValue) != 0 {
				readPages = true
				break
			}
		}

		if !readPages {
			w.Write([]parquet.Value{globalMinValue})
			return nil
		}
	}

	pages := columnChunk.Pages()
	defer pages.Close()
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
			if err := w.WritePage(dict.Page()); err != nil {
				return fmt.Errorf("write dictionary page: %w", err)
			}
		case !repeated && !optional && dict == nil:
			// If the column is not optional, we can read all values at once
			// consecutively without worrying about null values.
			if err := w.WritePage(p); err != nil {
				return fmt.Errorf("write page: %w", err)
			}
		default:
			values := make([]parquet.Value, p.NumValues())
			reader := p.Values()

			// We're reading all values in the page so we always expect an io.EOF.
			if _, err := reader.ReadValues(values); err != nil && err != io.EOF {
				return fmt.Errorf("read values: %w", err)
			}

			w.Write(values)
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

// recordBuilderLength returns the maximum length of all of the
// array.RecordBuilder's fields, the number of columns that have this maximum
// length, and a boolean for convenience to indicate if this last number is
// equal to the number of fields in the RecordBuilder (i.e. there is no anomaly
// in the length of each field).
func recordBuilderLength(rb *array.RecordBuilder) (maxLength, maxLengthFields int, anomaly bool) {
	fields := rb.Fields()
	maxLength = fields[0].Len()
	maxLengthFields = 0
	for _, field := range fields {
		if fieldLen := field.Len(); fieldLen != maxLength {
			if fieldLen > maxLength {
				maxLengthFields = 1
				maxLength = fieldLen
			}
		} else {
			maxLengthFields++
		}
	}
	return maxLength, maxLengthFields, !(maxLengthFields == len(rb.Fields()))
}

func distinctColumnsToArrowRecord(
	ctx context.Context,
	pool memory.Allocator,
	schema *arrow.Schema,
	parquetFields []parquet.Field,
	parquetColumns []parquet.ColumnChunk,
	distinctColumns []logicalplan.Expr,
	builder *array.RecordBuilder,
) (bool, error) {
	initialLength, _, anomaly := recordBuilderLength(builder)
	if anomaly {
		return false, fmt.Errorf("expected all fields in record builder to have equal length")
	}
	for i, field := range parquetFields {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
			name := field.Name()
			for _, distinctColumn := range distinctColumns {
				matchers := distinctColumn.ColumnsUsedExprs()
				if len(matchers) != 1 {
					// The expression is more complex than just a single binary
					// expression, so we can't apply the optimization.
					return false, nil
				}
				if matchers[0].MatchColumn(name) {
					// Fast path for distinct queries.
					optimizationApplied, err := writeDistinctColumnToArray(
						pool,
						field,
						parquetColumns[i],
						distinctColumn,
						// Note: The builder has to be lazily passed in because
						// the index evaluation may panic in certain cases when
						// the optimizations cannot be applied (e.g. in the case
						// of dynamic columns).
						func() array.Builder {
							return builder.Field(schema.FieldIndices(distinctColumn.Name())[0])
						},
					)
					if err != nil || !optimizationApplied {
						return optimizationApplied, err
					}
				}
			}
		}
	}

	atLeastOneRow := false
	for _, field := range builder.Fields() {
		if field.Len() > initialLength {
			atLeastOneRow = true
			break
		}
	}
	if !atLeastOneRow {
		// Exit early if no rows were written.
		return true, nil
	}

	newLength, maxLengthFields, anomaly := recordBuilderLength(builder)
	if !anomaly {
		// All columns have the same number of values.
		return true, nil
	}

	if newLength > initialLength+1 && maxLengthFields > 1 {
		// Can't apply the optimization as more than one column has more than
		// one distinct value. We can't know the combination of distinct values.
		for _, field := range builder.Fields() {
			if field.Len() == initialLength {
				continue
			}
			resetBuilderToLength(field, initialLength)
		}
		return false, nil
	}

	// At this point we know there is at most one column with more than one
	// row. Therefore we can repeat the values of the other columns as those
	// are the only possible combinations within the rowgroup.

	for _, field := range builder.Fields() {
		// Columns that had no values are just backfilled with null values.
		if fieldLen := field.Len(); fieldLen == initialLength {
			for j := initialLength; j < newLength; j++ {
				field.AppendNull()
			}
		} else if fieldLen < newLength {
			arr := field.NewArray()
			// TODO(asubiotto): NewArray resets the builder, copy all the values
			// again. There *must* be a better way to do this.
			copyArrToBuilder(field, arr, fieldLen)
			repeatLastValue(field, arr, newLength-fieldLen)
			arr.Release()
		}
	}
	return true, nil
}

// writeDistinctColumnToArray checks if the distinct expression can be optimized
// at the scan level and returns whether the optimization was successful or not.
func writeDistinctColumnToArray(
	pool memory.Allocator,
	node parquet.Node,
	columnChunk parquet.ColumnChunk,
	distinctExpr logicalplan.Expr,
	builder func() array.Builder,
) (bool, error) {
	switch expr := distinctExpr.(type) {
	case *logicalplan.BinaryExpr:
		return binaryDistinctExpr(
			pool,
			node.Type(),
			columnChunk,
			expr,
			builder(),
		)
	case *logicalplan.Column:
		if err := parquetColumnToArrowArray(
			pool,
			node,
			columnChunk,
			true,
			builder(),
		); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}

type PreExprVisitorFunc func(expr logicalplan.Expr) bool

func (f PreExprVisitorFunc) PreVisit(expr logicalplan.Expr) bool {
	return f(expr)
}

func (f PreExprVisitorFunc) PostVisit(expr logicalplan.Expr) bool {
	return false
}

// binaryDistinctExpr checks the columnChunk's column index to see if the
// expression can be evaluated on the index without reading the page values.
// Returns whether the optimization was successful or not.
func binaryDistinctExpr(
	pool memory.Allocator,
	typ parquet.Type,
	columnChunk parquet.ColumnChunk,
	expr *logicalplan.BinaryExpr,
	builder array.Builder,
) (bool, error) {
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
		return false, err
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
			b := builder.(*array.BooleanBuilder)
			if allGreater {
				b.Append(true)
			}
			if noneGreater {
				b.Append(false)
			}
			return true, nil
		}
	default:
		return false, nil
	}

	return false, nil
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

// resetBuilderToLength resets the builder to the given length, it is logically
// equivalent to b = b[0:l]. It is unfortunately pretty expensive, since there
// is currently no way to recreate a builder from a sliced array.
func resetBuilderToLength(builder array.Builder, l int) {
	arr := builder.NewArray()
	copyArrToBuilder(builder, arr, l)
	arr.Release()
}

func copyArrToBuilder(builder array.Builder, arr arrow.Array, toCopy int) {
	// TODO(asubiotto): Is there a better way to do this in the arrow
	// library? Maybe by copying buffers over, but I'm not sure if it's
	// cheaper to convert the byte slices to valid slices/offsets.
	// In any case, we should probably move this to a utils file.
	// One other idea is to create a thin layer on top of a builder that
	// only flushes writes when told to (will help with all these
	// optimizations where we aren't sure we can apply them until the end).
	builder.Reserve(toCopy)
	switch arr := arr.(type) {
	case *array.Boolean:
		b := builder.(*array.BooleanBuilder)
		for i := 0; i < toCopy; i++ {
			if arr.IsNull(i) {
				b.UnsafeAppendBoolToBitmap(false)
			} else {
				b.UnsafeAppend(arr.Value(i))
			}
		}
	case *array.Binary:
		b := builder.(*array.BinaryBuilder)
		for i := 0; i < toCopy; i++ {
			if arr.IsNull(i) {
				b.UnsafeAppendBoolToBitmap(false)
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.Int64:
		b := builder.(*array.Int64Builder)
		for i := 0; i < toCopy; i++ {
			if arr.IsNull(i) {
				b.UnsafeAppendBoolToBitmap(false)
			} else {
				b.UnsafeAppend(arr.Value(i))
			}
		}
	case *array.Uint64:
		b := builder.(*array.Uint64Builder)
		for i := 0; i < toCopy; i++ {
			if arr.IsNull(i) {
				b.UnsafeAppendBoolToBitmap(false)
			} else {
				b.UnsafeAppend(arr.Value(i))
			}
		}
	case *array.Float64:
		b := builder.(*array.Float64Builder)
		for i := 0; i < toCopy; i++ {
			if arr.IsNull(i) {
				b.UnsafeAppendBoolToBitmap(false)
			} else {
				b.UnsafeAppend(arr.Value(i))
			}
		}
	default:
		panic(fmt.Sprintf("unsupported array type: %T", arr))
	}
}

// repeatLastValue repeat's arr's last value count times and writes it to
// builder.
func repeatLastValue(
	builder array.Builder,
	arr arrow.Array,
	count int,
) {
	switch arr := arr.(type) {
	case *array.Boolean:
		repeatBooleanArray(builder.(*array.BooleanBuilder), arr, count)
	case *array.Binary:
		repeatBinaryArray(builder.(*array.BinaryBuilder), arr, count)
	case *array.Int64:
		repeatInt64Array(builder.(*array.Int64Builder), arr, count)
	case *array.Uint64:
		repeatUint64Array(builder.(*array.Uint64Builder), arr, count)
	case *array.Float64:
		repeatFloat64Array(builder.(*array.Float64Builder), arr, count)
	default:
		panic(fmt.Sprintf("unsupported array type: %T", arr))
	}
}

func repeatBooleanArray(
	b *array.BooleanBuilder,
	arr *array.Boolean,
	count int,
) {
	val := arr.Value(arr.Len() - 1)
	vals := make([]bool, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	// TODO(asubiotto): are we ignoring a possible null?
	b.AppendValues(vals, nil)
}

func repeatBinaryArray(
	b *array.BinaryBuilder,
	arr *array.Binary,
	count int,
) {
	val := arr.Value(arr.Len() - 1)
	vals := make([][]byte, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
}

func repeatInt64Array(
	b *array.Int64Builder,
	arr *array.Int64,
	count int,
) {
	val := arr.Value(arr.Len() - 1)
	vals := make([]int64, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
}

func repeatUint64Array(
	b *array.Uint64Builder,
	arr *array.Uint64,
	count int,
) {
	val := arr.Value(arr.Len() - 1)
	vals := make([]uint64, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
}

func repeatFloat64Array(
	b *array.Float64Builder,
	arr *array.Float64,
	count int,
) {
	val := arr.Value(arr.Len() - 1)
	vals := make([]float64, count)
	for i := 0; i < count; i++ {
		vals[i] = val
	}
	b.AppendValues(vals, nil)
}
