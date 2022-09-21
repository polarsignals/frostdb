package dynparquet

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/segmentio/parquet-go"
)

// ArrowRecordToBuffer takes a schema an a record and returns a Buffer
func ArrowRecordToBuffer(schema *Schema, r arrow.Record) (*Buffer, error) {
	dynamicColumns := map[string][]string{}

	isDynamic := func(name string) (string, string, bool) {
		names := strings.Split(name, ".")
		if len(names) > 1 { // this may be a dynamic column
			def, ok := schema.ColumnByName(names[0])
			return names[0], strings.Join(names[1:], "."), ok && def.Dynamic
		}
		return "", "", false
	}

	for _, f := range r.Schema().Fields() {
		if col, val, ok := isDynamic(f.Name); ok {
			dynamicColumns[col] = append(dynamicColumns[col], val)
		}
	}

	pb, err := schema.NewBuffer(dynamicColumns)
	if err != nil {
		return nil, err
	}

	// Create rows
	rows := []parquet.Row{}
	for i := 0; int64(i) < r.NumRows(); i++ {
		row := []parquet.Value{}
		for j := 0; int64(j) < r.NumCols(); j++ {
			col := r.Column(j)
			definitionLevel := 0
			if _, _, ok := isDynamic(r.ColumnName(j)); ok {
				definitionLevel = 1
			}
			switch col.DataType().ID() {
			case arrow.BINARY:
				a := col.(*array.Binary)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, definitionLevel, j))
			case arrow.STRING:
				a := col.(*array.String)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, definitionLevel, j))
			case arrow.INT64:
				a := col.(*array.Int64)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, definitionLevel, j))
			case arrow.UINT64:
				a := col.(*array.Uint64)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, definitionLevel, j))
			case arrow.FLOAT64:
				a := col.(*array.Float64)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, definitionLevel, j))
			default:
				panic(fmt.Sprintf("at the disco %v", col.DataType().ID()))
			}
		}
		rows = append(rows, row)
	}

	_, err = pb.WriteRows(rows)
	if err != nil {
		return nil, err
	}

	return pb, nil
}
