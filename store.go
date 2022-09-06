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
	"go.opentelemetry.io/otel/attribute"

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

func (t *Table) IterateBucketBlocks(
	ctx context.Context,
	logger log.Logger,
	lastBlockTimestamp uint64,
	filter TrueNegativeFilter,
	rowGroups chan<- dynparquet.DynamicRowGroup,
) error {
	if t.db.bucket == nil || t.db.ignoreStorageOnQuery {
		return nil
	}
	ctx, span := t.tracer.Start(ctx, "Table/IterateBucketBlocks")
	span.SetAttributes(attribute.Int64("lastBlockTimestamp", int64(lastBlockTimestamp)))
	defer span.End()

	n := 0
	err := t.db.bucket.Iter(ctx, t.name, func(blockDir string) error {
		ctx, span := t.tracer.Start(ctx, "Table/IterateBucketBlocks/Iter")
		defer span.End()

		blockUlid, err := ulid.Parse(filepath.Base(blockDir))
		if err != nil {
			return err
		}

		span.SetAttributes(attribute.String("ulid", blockUlid.String()))

		if lastBlockTimestamp != 0 && blockUlid.Time() >= lastBlockTimestamp {
			return nil
		}

		blockName := filepath.Join(blockDir, "data.parquet")
		attribs, err := t.db.bucket.Attributes(ctx, blockName)
		if err != nil {
			return err
		}

		span.SetAttributes(attribute.Int64("size", attribs.Size))

		file, err := t.openBlockFile(ctx, blockName, attribs.Size)
		if err != nil {
			return err
		}

		// Check if the block can be filtered out
		mayContainUsefulData, err := filter.Eval(&ParquetFileParticulate{File: file})
		if err != nil {
			return err
		}
		if !mayContainUsefulData { // skip the block entirely
			return nil
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
				rowGroups <- rg
			}
		}
		return nil
	})
	level.Debug(logger).Log("msg", "read blocks", "n", n)
	return err
}

func (t *Table) openBlockFile(ctx context.Context, blockName string, size int64) (*parquet.File, error) {
	ctx, span := t.tracer.Start(ctx, "Table/IterateBucketBlocks/Iter/OpenFile")
	defer span.End()
	r, err := t.db.bucket.GetReaderAt(ctx, blockName)
	if err != nil {
		return nil, err
	}

	file, err := parquet.OpenFile(
		r,
		size,
		parquet.ReadBufferSize(5*1024*1024), // 5MB read buffers
	)
	if err != nil {
		return nil, err
	}

	return file, nil
}
