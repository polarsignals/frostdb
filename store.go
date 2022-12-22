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
	"golang.org/x/sync/errgroup"

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
	rowGroups chan<- any,
) error {
	if t.db.bucket == nil || t.db.ignoreStorageOnQuery {
		return nil
	}
	ctx, span := t.tracer.Start(ctx, "Table/IterateBucketBlocks")
	span.SetAttributes(attribute.Int64("lastBlockTimestamp", int64(lastBlockTimestamp)))
	defer span.End()

	n := 0
	errg := &errgroup.Group{}
	errg.SetLimit(t.config.blockReaderLimit)
	err := t.db.bucket.Iter(ctx, t.name, func(blockDir string) error {
		n++
		errg.Go(func() error { return t.ProcessFile(ctx, blockDir, lastBlockTimestamp, filter, rowGroups) })
		return nil
	})
	if err != nil {
		return err
	}

	level.Debug(logger).Log("msg", "read blocks", "n", n)
	return errg.Wait()
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
		parquet.SkipBloomFilters(true),
		parquet.FileReadMode(parquet.ReadModeAsync),
	)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// ProcessFile will process a bucket block parquet file.
func (t *Table) ProcessFile(ctx context.Context, blockDir string, lastBlockTimestamp uint64, filter TrueNegativeFilter, rowGroups chan<- any) error {
	ctx, span := t.tracer.Start(ctx, "Table/IterateBucketBlocks/Iter/ProcessFile")
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

	// Get a reader from the file bytes
	buf, err := dynparquet.NewSerializedBuffer(file)
	if err != nil {
		return err
	}

	return t.filterRowGroups(ctx, buf, filter, rowGroups)
}

func (t *Table) filterRowGroups(ctx context.Context, buf *dynparquet.SerializedBuffer, filter TrueNegativeFilter, rowGroups chan<- any) error {
	_, span := t.tracer.Start(ctx, "Table/filterRowGroups")
	defer span.End()
	span.SetAttributes(attribute.Int("row_groups", buf.NumRowGroups()))

	for i := 0; i < buf.NumRowGroups(); i++ {
		rg := buf.DynamicRowGroup(i)
		mayContainUsefulData, err := filter.Eval(rg)
		if err != nil {
			return err
		}
		if mayContainUsefulData {
			rowGroups <- rg
		}
	}

	return nil
}
