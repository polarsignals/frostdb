package arcticdb

import (
	"fmt"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

type Part struct {
	Buf *dynparquet.SerializedBuffer

	// transaction id that this part was inserted under
	tx uint64
}

func NewPart(tx uint64, buf *dynparquet.SerializedBuffer) *Part {
	return &Part{
		tx:  tx,
		Buf: buf,
	}
}

// Least returns the least row  in the part
func (p *Part) Least() (*dynparquet.DynamicRow, error) {
	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	reader := p.Buf.DynamicRowGroup(0).DynamicRows()
	n, err := reader.ReadRows(rowBuf)
	if err != nil {
		return nil, fmt.Errorf("read first row of part: %w", err)
	}
	if n != 1 {
		return nil, fmt.Errorf("expected to read exactly 1 row, but read %d", n)
	}
	r := rowBuf.GetCopy(0)
	if err := reader.Close(); err != nil {
		return nil, err
	}

	return r, nil
}
