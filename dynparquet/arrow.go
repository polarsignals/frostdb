package dynparquet

import (
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/segmentio/parquet-go"
)

// ArrowRecordToBuffer takes a schema an a record and returns a Buffer
func ArrowRecordToBuffer(schema *Schema, r arrow.Record) (*Buffer, error) {
	dynamicColumns := map[string][]string{}

	for _, f := range r.Schema().Fields() {
		names := strings.Split(f.Name, ".")
		if len(names) > 1 { // this may be a dynamic column
			def, ok := schema.ColumnByName(names[0])
			if ok && def.Dynamic {
				dynamicColumns[names[0]] = append(dynamicColumns[names[0]], strings.Join(names[1:], "."))
			}
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
			switch col.DataType().ID() {
			case arrow.STRING:
				a := col.(*array.String)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, 0, j))
			case arrow.INT64:
				a := col.(*array.Int64)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, 0, j))
			case arrow.UINT64:
				a := col.(*array.Uint64)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, 0, j))
			case arrow.FLOAT64:
				a := col.(*array.Float64)
				row = append(row, parquet.ValueOf(a.Value(i)).Level(0, 0, j))
			default:
				panic("at the disco")
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
