package arcticdb

import (
	"bytes"
	"context"
	"path"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

// WriteBlock writes a block somewhere
func (block *TableBlock) WriteToDisk() error {
	data, err := block.Serialize()
	if err != nil {
		return err
	}
	fullName := filepath.Join(block.table.StorePath(), block.ulid.String(), "data.parquet")
	return block.table.db.columnStore.bucket.Upload(context.Background(), fullName, bytes.NewReader(data))
}

func (t *Table) readFileFromBucket(ctx context.Context, fileName string) (*bytes.Reader, error) {
	attribs, err := t.db.columnStore.bucket.Attributes(ctx, fileName)
	if err != nil {
		return nil, err
	}

	reader, err := t.db.columnStore.bucket.Get(ctx, fileName)
	if err != nil {
		return nil, err
	}

	data := make([]byte, attribs.Size)
	if _, err := reader.Read(data); err != nil {
		return nil, err
	}
	return bytes.NewReader(data), err
}

func (t *Table) IterateDiskBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicRowGroup) bool, lastBlockTimestamp uint64) error {
	if t.db.columnStore.bucket == nil {
		return nil
	}

	n := 0

	ctx := context.Background()
	err := t.db.columnStore.bucket.Iter(ctx, t.StorePath(), func(blockDir string) error {
		blockUlid, err := ulid.Parse(path.Base(blockDir))
		if err != nil {
			return err
		}

		if blockUlid.Time() >= lastBlockTimestamp {
			return nil
		}

		reader, err := t.readFileFromBucket(ctx, filepath.Join(blockDir, "data.parquet"))
		if err != nil {
			return err
		}

		file, err := parquet.OpenFile(reader, int64(reader.Len()))
		if err != nil {
			return err
		}

		// Get a reader from the file bytes
		buf, err := dynparquet.NewSerializedBuffer(file)
		if err != nil {
			return err
		}

		n++
		for i := 0; i < buf.NumRowGroups(); i++ {
			rg := buf.DynamicRowGroup(i)
			var mayContainUsefulData bool
			mayContainUsefulData, err = filter.Eval(rg)
			if err != nil {
				return err
			}
			if mayContainUsefulData {
				if continu := iterator(rg); !continu {
					return err
				}
			}
		}
		return nil
	})
	level.Debug(logger).Log("msg", "read blocks", "n", n)
	return err
}
