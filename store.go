package frostdb

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/protobuf/jsonpb"
	"github.com/oklog/ulid"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore"

	"github.com/polarsignals/frostdb/dynparquet"
)

// Persist uploads the block to the underlying bucket.
func (t *TableBlock) Persist() error {
	if t.table.db.bucket == nil {
		return nil
	}

	data, err := t.Serialize()
	if err != nil {
		return err
	}
	fileName := filepath.Join(t.table.name, t.ulid.String(), dataFileName)
	err = t.table.db.bucket.Upload(context.Background(), fileName, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to upload table block: %w", err)
	}

	return t.persistTableSchema(context.Background())
}

// persistTableSchema will write a schema.json file to storage if enabled.
func (t *TableBlock) persistTableSchema(ctx context.Context) error {
	m := &jsonpb.Marshaler{}
	b := &bytes.Buffer{}
	if err := m.Marshal(b, t.table.Schema().Definition()); err != nil {
		return fmt.Errorf("failed to marshal schema definition: %w", err)
	}

	h := fnv.New64()
	_, _ = h.Write(b.Bytes())

	name := filepath.Join(schemasPrefix, fmt.Sprintf(schemaFileNameFormat, h.Sum64()))
	return t.table.db.bucket.Upload(ctx, filepath.Join(t.table.name, name), b)
}

func (t *Table) IterateBucketBlocks(ctx context.Context, logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicRowGroup) bool, lastBlockTimestamp uint64) error {
	if t.db.bucket == nil || t.db.ignoreStorageOnQuery {
		return nil
	}

	n := 0
	err := t.db.bucket.Iter(ctx, t.name, func(blockDir string) error {
		f := filepath.Base(blockDir)
		if f != dataFileName {
			return nil
		}

		blockUlid, err := ulid.Parse(f)
		if err != nil {
			return err
		}

		if blockUlid.Time() >= lastBlockTimestamp {
			return nil
		}

		blockName := filepath.Join(blockDir, dataFileName)
		attribs, err := t.db.bucket.Attributes(ctx, blockName)
		if err != nil {
			return err
		}

		b := &BucketReaderAt{
			name:   blockName,
			ctx:    ctx,
			Bucket: t.db.bucket,
		}

		file, err := parquet.OpenFile(b, attribs.Size)
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

// BucketReaderAt is an objstore.Bucket wrapper that supports the io.ReaderAt interface.
type BucketReaderAt struct {
	name string
	ctx  context.Context
	objstore.Bucket
}

// ReadAt implements the io.ReaderAt interface.
func (b *BucketReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := b.GetRange(b.ctx, b.name, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer func() {
		err = rc.Close()
	}()

	return rc.Read(p)
}
