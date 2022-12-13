package pqarrow

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow/builder"
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
				af, err := parquetFieldToArrowField("", field, distinctColumns)
				if err != nil {
					return nil, err
				}
				if af.Name != "" {
					fields = append(fields, af)
				}
			}
		}
		return arrow.NewSchema(fields, nil), nil
	}

	fields := make([]arrow.Field, 0, len(parquetFields))
	for _, f := range parquetFields {
		f, err := parquetFieldToArrowField("", f, physicalProjections)
		if err != nil {
			return nil, err
		}
		if f.Name != "" {
			fields = append(fields, f)
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

func parquetFieldToArrowField(prefix string, field parquet.Field, physicalProjections []logicalplan.Expr) (arrow.Field, error) {
	switch field.Leaf() {
	case true:
		if includedProjection(physicalProjections, fullPath(prefix, field)) {
			af, err := convert.ParquetFieldToArrowField(field)
			if err != nil {
				return arrow.Field{}, err
			}
			return af, nil
		}
	default:
		switch {
		case includedProjection(physicalProjections, fullPath(prefix, field)):
			af, err := convert.ParquetFieldToArrowField(field)
			if err != nil {
				return arrow.Field{}, err
			}
			return af, nil
		case includedPathProjection(physicalProjections, fullPath(prefix, field)):
			group := []arrow.Field{}
			for _, f := range field.Fields() {
				af, err := parquetFieldToArrowField(fullPath(prefix, field), f, physicalProjections)
				if err != nil {
					return arrow.Field{}, err
				}
				if af.Name != "" {
					group = append(group, af)
				}
			}
			if len(group) > 0 {
				return arrow.Field{
					Name:     field.Name(),
					Type:     arrow.StructOf(group...),
					Nullable: field.Optional(),
				}, nil
			}
		}
	}
	return arrow.Field{}, nil
}

func fullPath(prefix string, parquetField parquet.Field) string {
	if prefix == "" {
		return parquetField.Name()
	}
	return strings.Join([]string{prefix, parquetField.Name()}, ".")
}

func includedPathProjection(projections []logicalplan.Expr, name string) bool {
	if len(projections) == 0 {
		return true
	}

	for _, p := range projections {
		if p.MatchPath(name) {
			return true
		}
	}
	return false
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

type parquetConverterMode int

const (
	// normal is the ParquetConverter's normal execution mode. No special
	// optimizations are applied.
	normal parquetConverterMode = iota
	// singleDistinctColumn is an execution mode when a single distinct column
	// is specified with no filter.
	singleDistinctColumn
	// multiDistinctColumn is an execution mode where there are multiple
	// distinct columns specified with no filter. Note that only "simple"
	// distinct expressions are supported in this mode (i.e. multiple columns
	// are not specified in the same distinct expression).
	multiDistinctColumn
)

// singleDistinctColumn is unused for now, see TODO in execution code.
var _ = singleDistinctColumn

// distinctColInfo stores metadata for a distinct expression.
type distinctColInfo struct {
	// parquetIndex is the index of the physical parquet column the distinct
	// expression reads from.
	parquetIndex int

	// v may be used in cases to store a literal expression value.
	v *parquet.Value

	// w and b are fields that the output is written to.
	w writer.ValueWriter
	b builder.ColumnBuilder
}

// ParquetConverter converts parquet.RowGroups into arrow.Records. The converted
// results are accumulated in the converter and can be retrieved by calling
// NewRecord, at which point the converter is reset.
type ParquetConverter struct {
	mode parquetConverterMode

	pool memory.Allocator
	// distinctColumns and distinctColInfos have a 1:1 mapping.
	distinctColInfos []*distinctColInfo

	// Output fields, for each outputSchema.Field(i) there will always be a
	// corresponding builder.Field(i).
	outputSchema *arrow.Schema
	iterOpts     logicalplan.IterOptions
	builder      *builder.RecordBuilder

	// writers are wrappers over a subset of builder.Fields().
	writers []MultiColumnWriter

	// prevSchema is stored to check for a different parquet schema on each
	// Convert call. This avoids performing duplicate work (e.g. finding
	// distinct column indices).
	prevSchema *parquet.Schema

	// scratchValues is an array of parquet.Values that is reused during
	// decoding to avoid allocations.
	scratchValues []parquet.Value
}

func NewParquetConverter(
	pool memory.Allocator,
	iterOpts logicalplan.IterOptions,
) *ParquetConverter {
	c := &ParquetConverter{
		mode:             normal,
		pool:             pool,
		iterOpts:         iterOpts,
		distinctColInfos: make([]*distinctColInfo, len(iterOpts.DistinctColumns)),
	}

	if iterOpts.Filter == nil && len(iterOpts.DistinctColumns) != 0 {
		simpleDistinctExprs := true
		for _, distinctColumn := range iterOpts.DistinctColumns {
			if _, ok := distinctColumn.(*logicalplan.DynamicColumn); ok ||
				len(distinctColumn.ColumnsUsedExprs()) != 1 {
				simpleDistinctExprs = false
				break
			}
		}
		if simpleDistinctExprs {
			// TODO(asubiotto): Note that the singleDistinctColumn mode is not
			// used yet given a bug in the current optimization (it was never
			// executed).
			c.mode = multiDistinctColumn
		}
	}

	return c
}

func (c *ParquetConverter) Convert(ctx context.Context, rg parquet.RowGroup) error {
	schema, err := ParquetRowGroupToArrowSchema(ctx, rg, c.iterOpts.PhysicalProjection, c.iterOpts.Projection, c.iterOpts.Filter, c.iterOpts.DistinctColumns)
	if err != nil {
		return err
	}
	// If the schema has no fields we simply ignore this RowGroup that has no data.
	if len(schema.Fields()) == 0 {
		return nil
	}

	if c.outputSchema == nil {
		c.outputSchema = schema
		c.builder = builder.NewRecordBuilder(c.pool, c.outputSchema)
	} else if !schema.Equal(c.outputSchema) { // new output schema; append new fields onto record builder
		c.outputSchema = mergeArrowSchemas([]*arrow.Schema{c.outputSchema, schema})
		c.builder.ExpandSchema(c.outputSchema)
		// Since we expanded the field we need to append nulls to the new field to match the max column length
		if maxLen, _, anomaly := recordBuilderLength(c.builder); anomaly {
			for _, field := range c.builder.Fields() {
				if fieldLen := field.Len(); fieldLen < maxLen {
					if ob, ok := field.(builder.OptimizedBuilder); ok {
						ob.AppendNulls(maxLen - fieldLen)
						continue
					}
					// If the column is not the same length as the maximum length
					// column, we need to append NULL as often as we have rows
					// TODO: Is there a faster or better way?
					for i := 0; i < maxLen-fieldLen; i++ {
						field.AppendNull()
					}
				}
			}
		}
	}

	if _, ok := rg.(*dynparquet.MergedRowGroup); ok {
		return rowBasedParquetRowGroupToArrowRecord(ctx, c.pool, rg, c.outputSchema, c.builder)
	}

	parquetSchema := rg.Schema()
	parquetColumns := rg.ColumnChunks()
	parquetFields := parquetSchema.Fields()

	if !parquetSchemaEqual(c.prevSchema, parquetSchema) {
		if err := c.schemaChanged(parquetFields); err != nil {
			return err
		}
		c.prevSchema = parquetSchema
	}

	if c.mode == multiDistinctColumn {
		// Since we're not filtering, we can use a faster path for distinct
		// columns. If all the distinct columns are dictionary encoded, we can
		// check their dictionaries and if all of them have a single value, we
		// can just return a single row with each of their values.
		appliedOptimization, err := c.writeDistinctAllColumns(
			ctx,
			parquetFields,
			parquetColumns,
		)
		if err != nil {
			return err
		}

		if appliedOptimization {
			return nil
		}
		// If we get here, we couldn't use the fast path.
	}

	for _, w := range c.writers {
		for _, col := range w.colIdx {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := c.writeColumnToArray(
					parquetFields[w.fieldIdx],
					parquetColumns[col],
					false,
					w.writer,
				); err != nil {
					return fmt.Errorf("convert parquet column to arrow array: %w", err)
				}
			}
		}
	}

	maxLen, _, anomaly := recordBuilderLength(c.builder)
	if !anomaly {
		return nil
	}

	for _, field := range c.builder.Fields() {
		if fieldLen := field.Len(); fieldLen < maxLen {
			if ob, ok := field.(builder.OptimizedBuilder); ok {
				ob.AppendNulls(maxLen - fieldLen)
				continue
			}
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

func (c *ParquetConverter) Fields() []builder.ColumnBuilder {
	if c.builder == nil {
		return nil
	}
	return c.builder.Fields()
}

func (c *ParquetConverter) NumRows() int {
	// NumRows assumes all fields have the same length. If not, this is a bug.
	return c.builder.Field(0).Len()
}

func (c *ParquetConverter) NewRecord() arrow.Record {
	if c.builder != nil {
		return c.builder.NewRecord()
	}

	return nil
}

func (c *ParquetConverter) Close() {
	if c.builder != nil {
		c.builder.Release()
	}
}

func numLeaves(f parquet.Field) int {
	if f.Leaf() {
		return 1
	}

	leaves := 0
	for _, field := range f.Fields() {
		switch field.Leaf() {
		case true:
			leaves++
		default:
			leaves += numLeaves(field)
		}
	}

	return leaves
}

// schemaChanged is called when a rowgroup to convert has a different schema
// than previously seen. This causes a recalculation of helper fields.
func (c *ParquetConverter) schemaChanged(parquetFields []parquet.Field) error {
	c.writers = c.writers[:0]
	parquetIndexToWriterMap := make(map[int]writer.ValueWriter)
	colOffset := 0
	for i, field := range parquetFields {
		indices := c.outputSchema.FieldIndices(field.Name())
		if len(indices) == 0 {
			// This column can be skipped, it's not needed by the output.
			colOffset += numLeaves(field)
			continue
		}

		newWriter, err := convert.GetWriter(i, field)
		if err != nil {
			return err
		}
		writer := newWriter(c.builder.Field(indices[0]), 0)
		cols := make([]int, numLeaves(field))
		for i := range cols {
			cols[i] = colOffset
			colOffset++
		}
		c.writers = append(c.writers, MultiColumnWriter{
			writer:   writer,
			fieldIdx: i,
			colIdx:   cols,
		})
		parquetIndexToWriterMap[i] = writer
	}

	if c.mode != multiDistinctColumn {
		return nil
	}

	// For distinct columns, we need to iterate to find the physical parquet
	// column to read from. Note that a sanity check has already been completed
	// in the constructor to ensure that only one column per distinct expression
	// is read in multiDistinctColumn mode.
	for i := range c.iterOpts.DistinctColumns {
		c.distinctColInfos[i] = nil
	}

	for i, expr := range c.iterOpts.DistinctColumns {
		for j, field := range parquetFields {
			if !expr.ColumnsUsedExprs()[0].MatchColumn(field.Name()) {
				continue
			}

			c.distinctColInfos[i] = &distinctColInfo{
				parquetIndex: j,
				w:            parquetIndexToWriterMap[j],
				b:            c.builder.Field(c.outputSchema.FieldIndices(expr.Name())[0]),
			}
		}
	}
	return nil
}

func (c *ParquetConverter) writeDistinctAllColumns(
	ctx context.Context,
	parquetFields []parquet.Field,
	parquetColumns []parquet.ColumnChunk,
) (bool, error) {
	initialLength := c.NumRows()

	for i, info := range c.distinctColInfos {
		if info == nil {
			// The parquet field the distinct expression operates on was not
			// found in this row group, skip it.
			continue
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
			optimizationApplied, err := c.writeDistinctSingleColumn(
				parquetFields[info.parquetIndex],
				parquetColumns[info.parquetIndex],
				c.iterOpts.DistinctColumns[i],
				info,
			)
			if err != nil {
				return false, err
			}

			if !optimizationApplied {
				// Builder must be reset to its initial length in case other
				// columns were processed before this one.
				for _, field := range c.builder.Fields() {
					if field.Len() == initialLength {
						continue
					}
					resetBuilderToLength(field, initialLength)
				}
				return false, nil
			}
		}
	}

	atLeastOneRow := false
	for _, field := range c.builder.Fields() {
		if field.Len() > initialLength {
			atLeastOneRow = true
			break
		}
	}
	if !atLeastOneRow {
		// Exit early if no rows were written.
		return true, nil
	}

	countRowsLargerThanOne := 0
	maxLen := 0
	for _, field := range c.builder.Fields() {
		fieldLen := field.Len()
		if fieldLen > initialLength+1 {
			countRowsLargerThanOne++
			if countRowsLargerThanOne > 1 {
				break
			}
		}
		if fieldLen > maxLen {
			maxLen = fieldLen
		}
	}

	if countRowsLargerThanOne > 1 {
		// More than one column had more than one distinct value. This means
		// that there is no way to know the correct number of distinct values,
		// so we must fall back to the non-optimized path.
		for _, field := range c.builder.Fields() {
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

	for _, field := range c.builder.Fields() {
		// Columns that had no values are just backfilled with null values.
		if fieldLen := field.Len(); fieldLen == initialLength {
			if ob, ok := field.(builder.OptimizedBuilder); ok {
				ob.AppendNulls(maxLen - initialLength)
				continue
			}
			for j := initialLength; j < maxLen; j++ {
				field.AppendNull()
			}
		} else if fieldLen < maxLen {
			repeatTimes := maxLen - fieldLen
			if ob, ok := field.(builder.OptimizedBuilder); ok {
				ob.RepeatLastValue(repeatTimes)
				continue
			}
			arr := field.NewArray()
			// TODO(asubiotto): NewArray resets the builder, copy all the values
			// again. There *must* be a better way to do this.
			copyArrToBuilder(field, arr, fieldLen)
			repeatLastValue(field, arr, repeatTimes)
			arr.Release()
		}
	}
	return true, nil
}

// writeDistinctSingleColumn checks if the distinct expression can be optimized
// at the scan level and returns whether the optimization was successful or not.
// Writer and builder point to the same memory and are both passed in for
// convenience (TODO(asubiotto): This should be cleaned up by having
// binaryDistinctExpr write to a writer instead of a builder or extending the
// writer interface).
func (c *ParquetConverter) writeDistinctSingleColumn(
	node parquet.Node,
	columnChunk parquet.ColumnChunk,
	distinctExpr logicalplan.Expr,
	info *distinctColInfo,
) (bool, error) {
	switch expr := distinctExpr.(type) {
	case *logicalplan.BinaryExpr:
		return binaryDistinctExpr(
			node.Type(),
			columnChunk,
			expr,
			info,
		)
	case *logicalplan.Column:
		if err := c.writeColumnToArray(
			node,
			columnChunk,
			true,
			info.w,
		); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
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
	builder *builder.RecordBuilder,
) error {
	parquetFields := rg.Schema().Fields()

	if len(schema.Fields()) != len(parquetFields) {
		return fmt.Errorf("inconsistent schema between arrow and parquet")
	}

	// Create arrow writers from arrow and parquet schema
	writers := make([]writer.ValueWriter, len(parquetFields))
	for i, field := range builder.Fields() {
		newValueWriter, err := convert.GetWriter(i, parquetFields[i])
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
func (c *ParquetConverter) writeColumnToArray(
	n parquet.Node,
	columnChunk parquet.ColumnChunk,
	dictionaryOnly bool,
	w writer.ValueWriter,
) error {
	repeated := n.Repeated()
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
			if columnIndex.NullCount(pageIdx) > 0 {
				// NULLs are not represented in the column index, so fall back
				// to the non-optimized path.
				// TODO(asubiotto): This is unexpected, verify upstream.
				readPages = true
				break
			}

			minValue := globalMinValue
			if pageIdx != 0 {
				// MinValue/MaxValue are relatively expensive calls, so we avoid
				// them as much as possible.
				minValue = columnIndex.MinValue(pageIdx)
			}
			maxValue := columnIndex.MaxValue(pageIdx)
			if columnType.Length() == 0 {
				// Variable-length datatype. The index can only be trusted if
				// the size of the values is less than the column index size,
				// since we cannot otherwise know if the index values are
				// truncated.
				if len(minValue.Bytes()) >= dynparquet.ColumnIndexSize ||
					len(maxValue.Bytes()) >= dynparquet.ColumnIndexSize {
					readPages = true
					break
				}
			}

			if columnType.Compare(minValue, maxValue) != 0 ||
				(pageIdx != 0 && columnType.Compare(globalMinValue, minValue) != 0) {
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
		case !repeated && dictionaryOnly && dict != nil && p.NumNulls() == 0:
			// If we are only writing the dictionary, we don't need to read
			// the values.
			if err := w.WritePage(dict.Page()); err != nil {
				return fmt.Errorf("write dictionary page: %w", err)
			}
		case !repeated && p.NumNulls() == 0 && dict == nil:
			// If the column has no nulls, we can read all values at once
			// consecutively without worrying about null values.
			if err := w.WritePage(p); err != nil {
				return fmt.Errorf("write page: %w", err)
			}
		default:
			if n := p.NumValues(); int64(cap(c.scratchValues)) < n {
				c.scratchValues = make([]parquet.Value, n)
			} else {
				c.scratchValues = c.scratchValues[:n]
			}

			// We're reading all values in the page so we always expect an io.EOF.
			reader := p.Values()
			if _, err := reader.ReadValues(c.scratchValues); err != nil && err != io.EOF {
				return fmt.Errorf("read values: %w", err)
			}

			w.Write(c.scratchValues)
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
func recordBuilderLength(rb *builder.RecordBuilder) (maxLength, maxLengthFields int, anomaly bool) {
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

// parquetSchemaEqual returns whether the two input schemas are equal. For now,
// only the field names are checked. In the future, it might be good to flesh
// out this check and commit it upstream.
func parquetSchemaEqual(schema1, schema2 *parquet.Schema) bool {
	switch {
	case schema1 == schema2:
		return true
	case schema1 == nil || schema2 == nil:
		return false
	case len(schema1.Fields()) != len(schema2.Fields()):
		return false
	}

	s1Fields := schema1.Fields()
	s2Fields := schema2.Fields()

	for i := range s1Fields {
		if s1Fields[i].Name() != s2Fields[i].Name() {
			return false
		}
	}

	return true
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
	typ parquet.Type,
	columnChunk parquet.ColumnChunk,
	expr *logicalplan.BinaryExpr,
	info *distinctColInfo,
) (bool, error) {
	if info.v == nil {
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
		info.v = &value
	}

	value := *info.v
	switch expr.Op {
	case logicalplan.OpGt:
		index := columnChunk.ColumnIndex()
		allGreater, noneGreater := allOrNoneGreaterThan(
			typ,
			index,
			value,
		)

		if allGreater || noneGreater {
			b := info.b.(*builder.OptBooleanBuilder)
			if allGreater {
				b.AppendParquetValues([]parquet.Value{parquet.ValueOf(true)})
			}
			if noneGreater {
				b.AppendParquetValues([]parquet.Value{parquet.ValueOf(false)})
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
func resetBuilderToLength(b builder.ColumnBuilder, l int) {
	if ob, ok := b.(builder.OptimizedBuilder); ok {
		ob.ResetToLength(l)
		return
	}
	arr := b.NewArray()
	copyArrToBuilder(b, arr, l)
	arr.Release()
}

func copyArrToBuilder(builder builder.ColumnBuilder, arr arrow.Array, toCopy int) {
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
				// We cannot use unsafe appends with the binary builder
				// because offsets won't be appended.
				b.AppendNull()
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
	builder builder.ColumnBuilder,
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

func mergeArrowSchemas(schemas []*arrow.Schema) *arrow.Schema {
	fieldNames := make([]string, 0, 16)
	fieldsMap := make(map[string]arrow.Field)

	for _, schema := range schemas {
		for _, f := range schema.Fields() {
			if _, ok := fieldsMap[f.Name]; !ok {
				fieldNames = append(fieldNames, f.Name)
				fieldsMap[f.Name] = f
			}
		}
	}

	sort.Strings(fieldNames)

	fields := make([]arrow.Field, 0, len(fieldNames))
	for _, name := range fieldNames {
		fields = append(fields, fieldsMap[name])
	}

	return arrow.NewSchema(fields, nil)
}

type MultiColumnWriter struct {
	writer   writer.ValueWriter
	fieldIdx int
	colIdx   []int
}
