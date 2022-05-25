package arcticdb

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

func writeToFile(data []byte, dir string, fileName string) error {
	tempFile, err := os.CreateTemp(dir, fileName)
	if err != nil {
		return err
	}

	if _, err := tempFile.Write(data); err != nil {
		return err
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	return os.Rename(tempFile.Name(), path.Join(dir, fileName))
}

func getBlockFileName(block *TableBlock) string {
	return strconv.FormatUint(block.timestamp, 10) + ".parquet"
}

func parseBlockFileName(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".parquet"), 0, 0)
}

// WriteBlock writes a block somewhere
func (block *TableBlock) WriteToDisk() error {
	data, err := block.Serialize()
	if err != nil {
		return err
	}
	fileName := filepath.Join(block.table.StorePath(), getBlockFileName(block))
	return block.table.db.columnStore.bucket.Upload(context.Background(), fileName, bytes.NewReader(data))
}

// FileDynamicRowGroup is a dynamic row group that is backed by a file object
type FileDynamicRowGroup struct {
	dynparquet.DynamicRowGroup
	io.Closer
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

func (t *Table) IterateDiskBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicCloserRowGroup) bool, lastBlockTimestamp uint64) error {
	if t.db.columnStore.bucket == nil {
		return nil
	}

	n := 0

	ctx := context.Background()
	err := t.db.columnStore.bucket.Iter(ctx, t.StorePath(), func(fileName string) error {
		timestamp, err := parseBlockFileName(path.Base(fileName))
		if err != nil {
			return err
		}

		if timestamp >= lastBlockTimestamp {
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
