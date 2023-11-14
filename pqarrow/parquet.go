package pqarrow

import (
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

// RecordToRow converts an arrow record with dynamic columns into a row using a dynamic parquet schema.
func RecordToRow(schema *dynparquet.Schema, final *parquet.Schema, record arrow.Record, index int) (parquet.Row, error) {
	rows, err := recordToRows(schema, record, index, index+1, final.Fields(), nil)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("converted wrong number of rows, expected 1, got %d", len(rows))
	}
	return rows[0], err
}

// recordToRows converts a full arrow record to a slice of parquet rows.
// scratchRows is used to append the conversion results to.
// The caller should use recordStart=0 and recordEnd=record.NumRows() to convert
// the entire record. Alternatively, the caller may only convert a subset of
// rows by specifying a range of [recordStart, recordEnd).
func recordToRows(
	schema *dynparquet.Schema,
	record arrow.Record,
	recordStart, recordEnd int,
	finalFields []parquet.Field,
	scratchRows []parquet.Row,
) ([]parquet.Row, error) {
	numRows := recordEnd - recordStart
	if cap(scratchRows) < numRows {
		scratchRows = make([]parquet.Row, 0, numRows)
	}
	scratchRows = scratchRows[:numRows]
	for i := range scratchRows {
		if cap(scratchRows[i]) < len(finalFields) {
			scratchRows[i] = make([]parquet.Value, 0, len(finalFields))
		}
		scratchRows[i] = scratchRows[i][:0]
	}

	// This code converts the arrow record column-by-column for better cache
	// locality.
	for j, f := range finalFields {
		columnIndexes := record.Schema().FieldIndices(f.Name())
		if len(columnIndexes) == 0 {
			// Column not found in record, append null to all rows.
			for i := 0; i < numRows; i++ {
				scratchRows[i] = append(
					scratchRows[i], parquet.ValueOf(nil).Level(0, 0, j),
				)
			}
			continue
		}

		def := 0
		if schema.IsDynamicColumn(f.Name()) {
			def = 1
		}

		col := record.Column(columnIndexes[0])
		switch arr := col.(type) {
		case *array.List:
			dictionaryList, binaryDictionaryList, err := arrowutils.ToConcreteList(arr)
			if err != nil {
				return nil, err
			}
			for i := 0; i < numRows; i++ {
				indexIntoRecord := recordStart + i
				if col.IsNull(indexIntoRecord) {
					scratchRows[i] = append(
						scratchRows[i], parquet.ValueOf(nil).Level(0, 0, j),
					)
					continue
				}

				start, end := arr.ValueOffsets(indexIntoRecord)
				for k := start; k < end; k++ {
					v := binaryDictionaryList.Value(dictionaryList.GetValueIndex(int(k)))
					rep := 0
					if k != start {
						rep = 1
					}
					scratchRows[i] = append(scratchRows[i], parquet.ValueOf(v).Level(rep, def+1, j))
				}
			}
		default:
			for i := 0; i < numRows; i++ {
				indexIntoRecord := recordStart + i
				if col.IsNull(indexIntoRecord) {
					scratchRows[i] = append(
						scratchRows[i], parquet.ValueOf(nil).Level(0, 0, j),
					)
					continue
				}
				scratchRows[i] = append(
					scratchRows[i], parquet.ValueOf(col.GetOneForMarshal(indexIntoRecord)).Level(0, def, j),
				)
			}
		}
	}
	return scratchRows, nil
}

func RecordDynamicCols(record arrow.Record) map[string][]string {
	dyncols := map[string][]string{}
	for _, af := range record.Schema().Fields() {
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

	var rows []parquet.Row
	for _, r := range recs {
		rows, err = recordToRows(schema, r, 0, int(r.NumRows()), finalFields, rows)
		if err != nil {
			return err
		}
		if _, err := w.WriteRows(rows); err != nil {
			return err
		}
	}

	return nil
}
