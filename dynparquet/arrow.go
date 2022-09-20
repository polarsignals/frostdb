package dynparquet

import (
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/segmentio/parquet-go"
)

// ArrowRecordToBuffer takes a schema an a record and returns a Buffer
func ArrowRecordToBuffer(schema *Schema, r arrow.Record) (*Buffer, error) {
	rows := []parquet.Row{}
	dynamicColumns := map[string][]string{}

	rschema := r.Schema()
	for _, f := range rschema.Fields() {
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
	for i := int64(0); i < r.NumCols(); i++ {
	}

	_, err = pb.WriteRows(rows)
	if err != nil {
		return nil, err
	}

	return pb, nil
}
