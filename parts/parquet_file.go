package parts

import (
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/frostdb/dynparquet"
	snapshotpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/snapshot/v1alpha1"
)

// parquetFilePart is a Part that is backed by an on-disk Parquet file.
type parquetFilePart struct {
	offset, size int64
	Part
}

func NewParquetFilePart(tx uint64, file io.ReaderAt, offset, size int64, options ...Option) (Part, error) {
	pf, err := parquet.OpenFile(io.NewSectionReader(file, offset, size), size)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	buf, err := dynparquet.NewSerializedBuffer(pf)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet buffer: %w", err)
	}

	return &parquetFilePart{
		offset: offset,
		size:   size,
		Part:   NewParquetPart(tx, buf, options...),
	}, nil
}

func (p *parquetFilePart) Meta() *snapshotpb.Part {
	return &snapshotpb.Part{
		Tx:              p.Part.TX(),
		StartOffset:     p.offset,
		EndOffset:       p.offset + p.size,
		CompactionLevel: uint64(p.Part.CompactionLevel()),
		Encoding:        snapshotpb.Part_ENCODING_PARQUET_FILE,
	}
}
