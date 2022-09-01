package frostdb

import (
	"errors"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/format"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

// ParquetFileParticulate extends parquet.File with Particulate functions.
type ParquetFileParticulate struct {
	*parquet.File
}

// Schema implements the Particulate interafce.
func (p *ParquetFileParticulate) Schema() *parquet.Schema {
	return p.File.Schema()
}

// VirtualSparseColumnChunk is a virtualization of a sparse ColumnChunk.
type VirtualSparseColumnChunk struct {
	i parquet.ColumnIndex
}

// Type implements the parquet.ColumnChunk interface; It is intentionally unimplemented.
func (v VirtualSparseColumnChunk) Type() parquet.Type { return nil }

// Column implements the parquet.ColumnChunk interface; It is intentionally unimplemented.
func (v VirtualSparseColumnChunk) Column() int { return -1 }

// Pages implements the parquet.ColumnChunk interface; It is intentionally unimplemented.
func (v VirtualSparseColumnChunk) Pages() parquet.Pages { return nil }

// OffsetIndex implements the parquet.ColumnChunk interface; It is intentionally unimplemented.
func (v VirtualSparseColumnChunk) OffsetIndex() parquet.OffsetIndex { return nil }

// BloomFilter implements the parquet.ColumnChunk interface; It is intentionally unimplemented.
func (v VirtualSparseColumnChunk) BloomFilter() parquet.BloomFilter { return nil }

// NumValues implements the parquet.ColumnChunk interface; It is intentionally unimplemented.
func (v VirtualSparseColumnChunk) NumValues() int64 { return -1 }

// ColumnIndex returns a column index for the VirtualSparseColumnChunk.
func (v VirtualSparseColumnChunk) ColumnIndex() parquet.ColumnIndex { return v.i }

type VirtualSparseColumnIndex struct {
	Min parquet.Value
	Max parquet.Value
}

// NumPages implements the parquet.ColumnIndex interface.
func (v VirtualSparseColumnIndex) NumPages() int { return 1 }

// NullCount implements the parquet.ColumnIndex interface.
func (v VirtualSparseColumnIndex) NullCount(int) int64 { return 0 }

// NullPage implements the parquet.ColumnIndex interface.
func (v VirtualSparseColumnIndex) NullPage(int) bool { return false }

// MinValue implements the parquet.ColumnIndex interface.
func (v VirtualSparseColumnIndex) MinValue(int) parquet.Value { return v.Min }

// MaxValue implements the parquet.ColumnIndex interface.
func (v VirtualSparseColumnIndex) MaxValue(int) parquet.Value { return v.Max }

// IsAscending implements the parquet.ColumnIndex interface.
func (v VirtualSparseColumnIndex) IsAscending() bool { return true }

// IsDescending implements the parquet.ColumnIndex interface.
func (v VirtualSparseColumnIndex) IsDescending() bool { return false }

// ColumnChunks implements the Particulate interafce.
func (p *ParquetFileParticulate) ColumnChunks() []parquet.ColumnChunk {
	var chunks []parquet.ColumnChunk
	for i := range p.ColumnIndexes() {
		switch *p.Metadata().Schema[i+1].Type {
		case format.Int32:
			chunks = append(chunks, VirtualSparseColumnChunk{parquet.NewColumnIndex(parquet.Int32, &p.ColumnIndexes()[i])})
		case format.Int64:
			chunks = append(chunks, VirtualSparseColumnChunk{parquet.NewColumnIndex(parquet.Int64, &p.ColumnIndexes()[i])})
		case format.Float:
			chunks = append(chunks, VirtualSparseColumnChunk{parquet.NewColumnIndex(parquet.Float, &p.ColumnIndexes()[i])})
		case format.Double:
			chunks = append(chunks, VirtualSparseColumnChunk{parquet.NewColumnIndex(parquet.Double, &p.ColumnIndexes()[i])})
		case format.ByteArray:
			chunks = append(chunks, VirtualSparseColumnChunk{parquet.NewColumnIndex(parquet.ByteArray, &p.ColumnIndexes()[i])})
		case format.FixedLenByteArray:
			chunks = append(chunks, VirtualSparseColumnChunk{parquet.NewColumnIndex(parquet.FixedLenByteArray, &p.ColumnIndexes()[i])})
		default:
			panic("unimplemented format type")
		}
	}

	return chunks
}

type ColumnRef struct {
	ColumnName string
}

func (c *ColumnRef) Column(p Particulate) (parquet.ColumnChunk, bool, error) {
	columnIndex := findColumnIndex(p.Schema(), c.ColumnName)
	var columnChunk parquet.ColumnChunk
	// columnChunk can be nil if the column is not present in the row group.
	if columnIndex != -1 {
		columnChunk = p.ColumnChunks()[columnIndex]
	}

	return columnChunk, columnIndex != -1, nil
}

func findColumnIndex(s *parquet.Schema, columnName string) int {
	for i, field := range s.Fields() {
		if field.Name() == columnName {
			return i
		}
	}
	return -1
}

type BinaryScalarExpr struct {
	Left  *ColumnRef
	Op    logicalplan.Op
	Right parquet.Value
}

func (e BinaryScalarExpr) Eval(p Particulate) (bool, error) {
	leftData, exists, err := e.Left.Column(p)
	if err != nil {
		return false, err
	}

	// TODO: This needs a bunch of test cases to validate edge cases like non
	// existant columns or null values. I'm pretty sure this is completely
	// wrong and needs per operation, per type specific behavior.
	if !exists {
		// only handling string for now.
		if e.Right.Kind() == parquet.ByteArray || e.Right.Kind() == parquet.FixedLenByteArray {
			switch {
			case e.Op == logicalplan.OpEq && e.Right.String() == "":
				return true, nil
			case e.Op == logicalplan.OpNotEq && e.Right.String() != "":
				return true, nil
			}
		}
		return false, nil
	}

	return BinaryScalarOperation(leftData, e.Right, e.Op)
}

var ErrUnsupportedBinaryOperation = errors.New("unsupported binary operation")

func BinaryScalarOperation(left parquet.ColumnChunk, right parquet.Value, operator logicalplan.Op) (bool, error) {
	switch operator {
	case logicalplan.OpEq:
		bloomFilter := left.BloomFilter()
		if bloomFilter == nil {
			// If there is no bloom filter then we cannot make a statement about true negative, instead check the min max values of the column chunk
			return compare(right, Max(left)) <= 0 || compare(right, Min(left)) >= -1, nil
		}

		ok, err := bloomFilter.Check(right)
		if err != nil {
			return true, err
		}
		if !ok {
			// Bloom filters may return false positives, but never return false
			// negatives, we know this column chunk does not contain the value.
			return false, nil
		}

		return true, nil
	case logicalplan.OpLtEq:
		return compare(Min(left), right) <= 0, nil
	case logicalplan.OpLt:
		return compare(Min(left), right) < 0, nil
	case logicalplan.OpGt:
		return compare(Max(left), right) > 0, nil
	case logicalplan.OpGtEq:
		return compare(Max(left), right) >= 0, nil
	default:
		return true, nil
	}
}

// Min returns the minimum value found in the column chunk across all pages.
func Min(chunk parquet.ColumnChunk) parquet.Value {
	columnIndex := chunk.ColumnIndex()
	min := columnIndex.MinValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		if v := columnIndex.MinValue(i); compare(min, v) == 1 {
			min = v
		}
	}

	return min
}

// Max returns the maximum value found in the column chunk across all pages.
func Max(chunk parquet.ColumnChunk) parquet.Value {
	columnIndex := chunk.ColumnIndex()
	max := columnIndex.MaxValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		if v := columnIndex.MaxValue(i); compare(max, v) == -1 {
			max = v
		}
	}

	return max
}

// compares two parquet values. 0 if they are equal, -1 if v1 < v2, 1 if v1 > v2.
func compare(v1, v2 parquet.Value) int {
	switch v1.Kind() {
	case parquet.Int32:
		return parquet.Int32Type.Compare(v1, v2)
	case parquet.Int64:
		return parquet.Int64Type.Compare(v1, v2)
	case parquet.Float:
		return parquet.FloatType.Compare(v1, v2)
	case parquet.Double:
		return parquet.DoubleType.Compare(v1, v2)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return parquet.ByteArrayType.Compare(v1, v2)
	default:
		panic("unsupported value comparison")
	}
}
