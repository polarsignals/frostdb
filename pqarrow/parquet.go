package pqarrow

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
)

func ArrowScalarToParquetValue(sc scalar.Scalar) (parquet.Value, error) {
	switch s := sc.(type) {
	case *scalar.String:
		return parquet.ValueOf(string(s.Data())), nil
	case *scalar.Int64:
		return parquet.ValueOf(s.Value), nil
	case *scalar.Int32:
		return parquet.ValueOf(s.Value), nil
	case *scalar.FixedSizeBinary:
		width := s.Type.(*arrow.FixedSizeBinaryType).ByteWidth
		v := [16]byte{}
		copy(v[:width], s.Data())
		return parquet.ValueOf(v), nil
	case *scalar.Boolean:
		return parquet.ValueOf(s.Value), nil
	case *scalar.Null:
		return parquet.NullValue(), nil
	case nil:
		return parquet.Value{}, nil
	default:
		return parquet.Value{}, fmt.Errorf("unsupported scalar type %T", s)
	}
}

// singlePassThroughWriter is used to keep a reference to the rows written to
// a parquet writer when only converting a single row. Calling WriteRows more
// than once is unsupported.
type singlePassThroughWriter struct {
	rows []parquet.Row
}

var _ dynparquet.ParquetWriter = (*singlePassThroughWriter)(nil)

func (w *singlePassThroughWriter) Schema() *parquet.Schema { return nil }

func (w *singlePassThroughWriter) Write(_ []any) (int, error) { panic("use WriteRows instead") }

func (w *singlePassThroughWriter) WriteRows(rows []parquet.Row) (int, error) {
	if w.rows != nil {
		panic("cannot call WriteRows more than once")
	}
	w.rows = rows
	return len(rows), nil
}

func (w *singlePassThroughWriter) Flush() error { return nil }

func (w *singlePassThroughWriter) Close() error { return nil }

func (w *singlePassThroughWriter) Reset(_ io.Writer) {}

// RecordToRow converts an arrow record with dynamic columns into a row using a dynamic parquet schema.
func RecordToRow(schema *dynparquet.Schema, final *parquet.Schema, record arrow.Record, index int) (parquet.Row, error) {
	w := &singlePassThroughWriter{}
	if err := recordToRows(w, schema.IsDynamicColumn, record, index, index+1, final.Fields()); err != nil {
		return nil, err
	}
	return w.rows[0], nil
}

type dynamicColumnVerifier func(string) bool

// recordToRows converts a full arrow record to parquet rows which are written
// to the parquet writer.
// The caller should use recordStart=0 and recordEnd=record.NumRows() to convert
// the entire record. Alternatively, the caller may only convert a subset of
// rows by specifying a range of [recordStart, recordEnd).
func recordToRows(w dynparquet.ParquetWriter, dcv dynamicColumnVerifier, record arrow.Record, recordStart, recordEnd int, finalFields []parquet.Field) error {
	numRows := recordEnd - recordStart
	schema := record.Schema()
	row := make(parquet.Row, len(finalFields))
	writers := make([]arrowToParquet, len(finalFields))
	for i := range writers {
		f := finalFields[i]
		name := f.Name()
		def := 0
		if dcv(name) {
			def = 1
		}
		idx := schema.FieldIndices(name)
		if len(idx) == 0 {
			writers[i] = writeNull(i)
			continue
		}
		column := record.Column(idx[0])
		switch a := column.(type) {
		case *array.List:
			ls, err := writeList(def, i, recordStart, a)
			if err != nil {
				return err
			}
			writers[i] = ls
		case *array.Dictionary:
			writers[i] = writeDictionary(def, i, recordStart, a)
		case *array.Int32:
			writers[i] = writeInt32(def, i, recordStart, a)
		case *array.Int64:
			writers[i] = writeInt64(def, i, recordStart, a)
		case *array.String:
			writers[i] = writeString(def, i, recordStart, a)
		case *array.Binary:
			writers[i] = writeBinary(def, i, recordStart, a)
		default:
			writers[i] = writeGeneral(def, i, recordStart, a)
		}
	}
	rows := make([]parquet.Row, 1)
	for i := 0; i < numRows; i++ {
		row = row[:0]
		for j := range writers {
			row = writers[j](row, i)
		}
		rows[0] = row
		_, err := w.WriteRows(rows)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeGeneral(def, column, startIdx int, a arrow.Array) arrowToParquet {
	return func(w parquet.Row, row int) parquet.Row {
		if a.IsNull(row + startIdx) {
			return append(w,
				parquet.Value{}.Level(0, 0, column),
			)
		}
		return append(w,
			parquet.ValueOf(a.GetOneForMarshal(row+startIdx)).Level(0, def, column),
		)
	}
}

func writeInt64(def, column, startIdx int, a *array.Int64) arrowToParquet {
	return func(w parquet.Row, row int) parquet.Row {
		if a.IsNull(row + startIdx) {
			return append(w,
				parquet.Value{}.Level(0, 0, column),
			)
		}
		return append(w,
			parquet.Int64Value(a.Value(row+startIdx)).Level(0, def, column),
		)
	}
}

func writeInt32(def, column, startIdx int, a *array.Int32) arrowToParquet {
	return func(w parquet.Row, row int) parquet.Row {
		if a.IsNull(row + startIdx) {
			return append(w,
				parquet.Value{}.Level(0, 0, column),
			)
		}
		return append(w,
			parquet.Int32Value(a.Value(row+startIdx)).Level(0, def, column),
		)
	}
}

func writeBinary(def, column, startIdx int, a *array.Binary) arrowToParquet {
	return func(w parquet.Row, row int) parquet.Row {
		if a.IsNull(row + startIdx) {
			return append(w,
				parquet.Value{}.Level(0, 0, column),
			)
		}
		return append(w,
			parquet.ByteArrayValue(a.Value(row+startIdx)).Level(0, def, column),
		)
	}
}

func writeString(def, column, startIdx int, a *array.String) arrowToParquet {
	return func(w parquet.Row, row int) parquet.Row {
		if a.IsNull(row + startIdx) {
			return append(w,
				parquet.Value{}.Level(0, 0, column),
			)
		}
		return append(w,
			parquet.ByteArrayValue([]byte(a.Value(row+startIdx))).Level(0, def, column),
		)
	}
}

func writeDictionary(def, column, startIdx int, a *array.Dictionary) arrowToParquet {
	value := func(row int) parquet.Value {
		return parquet.ValueOf(a.GetOneForMarshal(row))
	}
	if x, ok := a.Dictionary().(*array.Binary); ok {
		value = func(row int) parquet.Value {
			return parquet.ByteArrayValue(
				x.Value(a.GetValueIndex(row)),
			)
		}
	}
	return func(w parquet.Row, row int) parquet.Row {
		if a.IsNull(row + startIdx) {
			return append(w,
				parquet.Value{}.Level(0, 0, column),
			)
		}
		return append(w,
			value(row+startIdx).Level(0, def, column),
		)
	}
}

func writeList(def, column, startIdx int, a *array.List) (arrowToParquet, error) {
	var lw arrowToParquet
	switch e := a.ListValues().(type) {
	case *array.Int32:
		// WHile this is not base type. To avoid breaking things I have left it here.
		lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
			return parquet.Int32Value(e.Value(idx))
		})
	case *array.Int64:
		lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
			return parquet.Int64Value(e.Value(idx))
		})
	case *array.Boolean:
		lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
			return parquet.BooleanValue(e.Value(idx))
		})
	case *array.Float64:
		lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
			return parquet.DoubleValue(e.Value(idx))
		})
	case *array.String:
		lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
			return parquet.ByteArrayValue([]byte(e.Value(idx)))
		})
	case *array.Binary:
		lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
			return parquet.ByteArrayValue([]byte(e.Value(idx)))
		})
	case *array.Dictionary:
		switch d := e.Dictionary().(type) {
		case *array.Binary:
			lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
				return parquet.ByteArrayValue(
					d.Value(e.GetValueIndex(idx)),
				)
			})
		case *array.String:
			lw = writeListOf(def, column, startIdx, a, func(idx int) parquet.Value {
				return parquet.ByteArrayValue(
					[]byte(d.Value(e.GetValueIndex(idx))),
				)
			})
		default:
			return nil, fmt.Errorf("list dictionary not of expected type: %T", d)
		}
	default:
		return nil, fmt.Errorf("list not of expected type: %T", e)
	}
	return func(w parquet.Row, row int) parquet.Row {
		if a.IsNull(row + startIdx) {
			return append(w,
				parquet.Value{}.Level(0, 0, column),
			)
		}
		return lw(w, row)
	}, nil
}

func writeListOf(def, column, startIdx int, a *array.List, value func(idx int) parquet.Value) arrowToParquet {
	return func(w parquet.Row, row int) parquet.Row {
		start, end := a.ValueOffsets(row + startIdx)
		for k := start; k < end; k++ {
			rep := 0
			if k != start {
				rep = 1
			}
			w = append(w, value(int(k)).Level(rep, def+1, column))
		}
		return w
	}
}

func writeNull(column int) arrowToParquet {
	return func(w parquet.Row, row int) parquet.Row {
		return append(w, parquet.Value{}.Level(0, 0, column))
	}
}

type arrowToParquet func(w parquet.Row, row int) parquet.Row

func RecordDynamicCols(record arrow.Record) (columns map[string][]string) {
	dyncols := make(map[string]struct{})
	for i := 0; i < record.Schema().NumFields(); i++ {
		af := record.Schema().Field(i)
		if strings.Contains(af.Name, ".") {
			dyncols[af.Name] = struct{}{}
		}
	}
	columns = make(map[string][]string)
	for s := range dyncols {
		name, part, _ := strings.Cut(s, ".")
		columns[name] = append(columns[name], part)
	}
	for k := range columns {
		sort.Strings(columns[k])
	}
	return
}

func RecordToDynamicRow(dynSchema *dynparquet.Schema, pqSchema *parquet.Schema, record arrow.Record, dyncols map[string][]string, index int) (*dynparquet.DynamicRow, error) {
	if index >= int(record.NumRows()) {
		return nil, io.EOF
	}

	row, err := RecordToRow(dynSchema, pqSchema, record, index)
	if err != nil {
		return nil, err
	}

	return dynparquet.NewDynamicRow(row, pqSchema, dyncols, pqSchema.Fields()), nil
}

func SerializeRecord(r arrow.Record, schema *dynparquet.Schema) (*dynparquet.SerializedBuffer, error) {
	b := &bytes.Buffer{}
	w, err := schema.GetWriter(b, RecordDynamicCols(r), false)
	if err != nil {
		return nil, err
	}
	defer schema.PutWriter(w)
	if err := RecordToFile(schema, w.ParquetWriter, r); err != nil {
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

func RecordToFile(schema *dynparquet.Schema, w dynparquet.ParquetWriter, r arrow.Record) error {
	return RecordsToFile(schema, w, []arrow.Record{r})
}

func RecordsToFile(schema *dynparquet.Schema, w dynparquet.ParquetWriter, recs []arrow.Record) error {
	dynColSets := make([]map[string][]string, 0, len(recs))
	for _, r := range recs {
		dynColSets = append(dynColSets, RecordDynamicCols(r))
	}
	dynCols := dynparquet.MergeDynamicColumnSets(dynColSets)
	defer w.Close()

	ps, err := schema.GetDynamicParquetSchema(dynCols)
	if err != nil {
		return err
	}
	defer schema.PutPooledParquetSchema(ps)

	finalFields := ps.Schema.Fields()

	for _, r := range recs {
		if err := recordToRows(w, schema.IsDynamicColumn, r, 0, int(r.NumRows()), finalFields); err != nil {
			return err
		}
	}

	return nil
}
