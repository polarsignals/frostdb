package frostdb

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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

	r, w := io.Pipe()
	var err error
	go func() {
		defer w.Close()
		err = t.Serialize(w)
	}()
	defer r.Close()

	fileName := filepath.Join(t.table.name, t.ulid.String(), "data.parquet")
	if err := t.table.db.bucket.Upload(context.Background(), fileName, r); err != nil {
		return fmt.Errorf("failed to upload block %v", err)
	}

	if err != nil {
		return fmt.Errorf("failed to serialize block: %v", err)
	}
	return nil
}

// BlockFilterFunc takes a block ULID and returns true if this block can be ignored during this query
type BlockFilterFunc func(ulid.ULID) bool

// A BlockFilter is a logical construction of multiple block filters
type BlockFilter struct {
	BlockFilterFunc
	input *BlockFilter
}

// Filter will recursively call input filters until one returns true or the final filter is reached
func (b *BlockFilter) Filter(block ulid.ULID) bool {
	if b.BlockFilterFunc == nil {
		return false
	}

	if b.BlockFilterFunc(block) {
		return true
	}

	return b.input.Filter(block)
}

// LastBlockTimestamp adds a LastBlockTimestamp filter to the block filter
// TODO better description
func (b *BlockFilter) LastBlockTimestamp(lastBlockTimestamp uint64) *BlockFilter {
	return &BlockFilter{
		BlockFilterFunc: func(block ulid.ULID) bool {
			return lastBlockTimestamp != 0 && block.Time() >= lastBlockTimestamp
		},
		input: b,
	}
}

type BlockFilterIf interface {
	Filter(ulid.ULID) bool
}

func (t *Table) IterateBucketBlocks(ctx context.Context, logger log.Logger, blockFilter BlockFilterIf, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicRowGroup) bool) error {
	if t.db.bucket == nil || t.db.ignoreStorageOnQuery {
		return nil
	}

	n := 0
	err := t.db.bucket.Iter(ctx, t.name, func(blockDir string) error {
		blockUlid, err := ulid.Parse(filepath.Base(blockDir))
		if err != nil {
			return err
		}

		if blockFilter.Filter(blockUlid) {
			return nil
		}

		blockName := filepath.Join(blockDir, "data.parquet")
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
