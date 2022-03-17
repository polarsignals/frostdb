package columnstore

import (
	"github.com/parca-dev/parca/pkg/columnstore/dynparquet"
)

type Part struct {
	Buf *dynparquet.SerializedBuffer

	// transaction id that this part was indserted under
	tx uint64
}

func NewPart(tx uint64, buf *dynparquet.SerializedBuffer) *Part {
	return &Part{
		tx:  tx,
		Buf: buf,
	}
}
