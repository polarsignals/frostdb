package arcticdb

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

func generateULID(t time.Time) string {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	ul := ulid.MustNew(ulid.Timestamp(t), entropy)
	return ul.String()
}

func getBlockFileName(block *TableBlock) string {
	return generateULID(block.timestamp) + ".parquet"
}

func parseBlockFileName(name string) (ulid.ULID, error) {
	return ulid.Parse(strings.TrimSuffix(name, ".parquet"))
}

// WriteBlock writes a block somewhere
func (block *TableBlock) WriteToDisk(fileName string) error {
	data, err := block.Serialize()
	if err != nil {
		return err
	}
	fullName := filepath.Join(block.table.StorePath(), fileName)
	return block.table.db.columnStore.bucket.Upload(context.Background(), fullName, bytes.NewReader(data))
}

// MemDynamicRowGroup is a row group that is in memory and nop the close function
type MemDynamicRowGroup struct {
	dynparquet.DynamicRowGroup
	io.Closer
}

func (MemDynamicRowGroup) Close() error {
	return nil
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

func (t *Table) IterateDiskBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicCloserRowGroup) bool, lastBlockTimestamp time.Time) error {
	if t.db.columnStore.bucket == nil {
		return nil
	}

	n := 0

	ctx := context.Background()
	err := t.db.columnStore.bucket.Iter(ctx, t.StorePath(), func(fileName string) error {
		blockUlid, err := parseBlockFileName(path.Base(fileName))
		if err != nil {
			return err
		}

		if blockUlid.Time() >= uint64(lastBlockTimestamp.UnixMilli()) {
			return nil
		}

		reader, err := t.readFileFromBucket(ctx, fileName)
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
				continu := iterator(&MemDynamicRowGroup{
					DynamicRowGroup: rg,
				})
				if !continu {
					return err
				}
			}
		}
		return nil
	})
	level.Info(logger).Log("msg", "read blocks", "n", n)
	return err
}
