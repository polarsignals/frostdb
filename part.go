package arcticdb

import (
	"io"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

type Part struct {
	Buf *dynparquet.SerializedBuffer

	least *dynparquet.DynamicRow

	// transaction id that this part was inserted under
	tx uint64
}

func NewPart(tx uint64, buf *dynparquet.SerializedBuffer) *Part {

	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	reader := buf.DynamicRowGroup(0).DynamicRows()
	_, err := reader.ReadRows(rowBuf)
	if err == io.EOF {
		panic("tODO")
	}
	if err != nil {
		panic("tODO")
	}
	row := rowBuf.GetCopy(0)
	if err := reader.Close(); err != nil {
		panic("tODO")
	}

	return &Part{
		least: row,
		tx:    tx,
		Buf:   buf,
	}
}
