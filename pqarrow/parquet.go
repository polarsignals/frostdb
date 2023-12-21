package pqarrow

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/bufutils"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
)

func ArrowScalarToParquetValue(sc scalar.Scalar) (parquet.Value, error) {
	switch s := sc.(type) {
	case *scalar.String:
		return parquet.ValueOf(string(s.Data())), nil
	case *scalar.Int64:
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
	row := make([]parquet.Value, 0, len(finalFields))
	for i := 0; i < numRows; i++ {
		row = row[:0]
		for j, f := range finalFields {
			columnIndexes := record.Schema().FieldIndices(f.Name())
			if len(columnIndexes) == 0 {
				// Column not found in record, append null to row.
				row = append(row, parquet.ValueOf(nil).Level(0, 0, j))
				continue
			}

			def := 0
			if dcv(f.Name()) {
				def = 1
			}

			col := record.Column(columnIndexes[0])
			indexIntoRecord := recordStart + i
			if col.IsNull(indexIntoRecord) {
				row = append(
					row, parquet.ValueOf(nil).Level(0, 0, j),
				)
				continue
			}

			switch arr := col.(type) {
			case *array.List:
				dictionaryList, binaryDictionaryList, err := arrowutils.ToConcreteList(arr)
				if err != nil {
					return err
				}

				start, end := arr.ValueOffsets(indexIntoRecord)
				for k := start; k < end; k++ {
					v := binaryDictionaryList.Value(dictionaryList.GetValueIndex(int(k)))
					rep := 0
					if k != start {
						rep = 1
					}
					row = append(row, parquet.ByteArrayValue(v).Level(rep, def+1, j))
				}
			case *array.Dictionary:
				vidx := arr.GetValueIndex(indexIntoRecord)
				switch dict := arr.Dictionary().(type) {
				case *array.Binary:
					row = append(row, parquet.ByteArrayValue(dict.Value(vidx)).Level(0, def, j))
				default:
					row = append(row, parquet.ValueOf(dict.GetOneForMarshal(vidx)).Level(0, def, j))
				}
			case *array.Int32:
				row = append(
					row, parquet.Int32Value(arr.Value(indexIntoRecord)).Level(0, def, j),
				)
			case *array.Int64:
				row = append(
					row, parquet.Int64Value(arr.Value(indexIntoRecord)).Level(0, def, j),
				)
			default:
				row = append(
					row, parquet.ValueOf(col.GetOneForMarshal(indexIntoRecord)).Level(0, def, j),
				)
			}
		}

		if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
			return err
		}
	}

	return nil
}

func RecordDynamicCols(record arrow.Record) map[string][]string {
	dyncols := map[string][]string{}
	for i := 0; i < record.Schema().NumFields(); i++ {
		af := record.Schema().Field(i)
		parts := strings.SplitN(af.Name, ".", 2)
		if len(parts) == 2 { // dynamic column
			dyncols[parts[0]] = append(dyncols[parts[0]], parts[1])
		}
	}

	return bufutils.Dedupe(dyncols)
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
