package frostdb

import (
	"fmt"

	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
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

// Least returns the least row  in the part.
func (p *Part) Least() (*dynparquet.DynamicRow, error) {
	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	f, err := p.Buf.Open()
	if err != nil {
		return nil, err
	}
	rg, err := f.DynamicRowGroup(0)
	if err != nil {
		return nil, err
	}
	reader := rg.DynamicRows()
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
