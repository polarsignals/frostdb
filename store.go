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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/storage"
)

// Persist uploads the block to the underlying bucket.
func (t *TableBlock) Persist() error {
	if len(t.table.db.sinks) != 0 {
		return nil
	}

	for i, sink := range t.table.db.sinks {
		if i > 0 {
			return fmt.Errorf("multiple sinks not supported")
		}
		r, w := io.Pipe()
		var err error
		go func() {
			defer w.Close()
			err = t.Serialize(w)
		}()
		defer r.Close()

		fileName := filepath.Join(t.table.name, t.ulid.String(), "data.parquet")
		if err := sink.Upload(context.Background(), fileName, r); err != nil {
			return fmt.Errorf("failed to upload block %v", err)
		}

		if err != nil {
			return fmt.Errorf("failed to serialize block: %v", err)
		}
	}
	return nil
}

// DefaultObjstoreBucket is the default implementation of the DataSource and DataSink interface.
type DefaultObjstoreBucket struct {
	storage.Bucket
	tracer trace.Tracer
	logger log.Logger

	blockReaderLimit int
}

type DefaultObjstoreBucketOption func(*DefaultObjstoreBucket)

func StorageWithBlockReaderLimit(limit int) DefaultObjstoreBucketOption {
	return func(b *DefaultObjstoreBucket) {
		b.blockReaderLimit = limit
	}
}

func StorageWithTracer(tracer trace.Tracer) DefaultObjstoreBucketOption {
	return func(b *DefaultObjstoreBucket) {
		b.tracer = tracer
	}
}

func StorageWithLogger(logger log.Logger) DefaultObjstoreBucketOption {
	return func(b *DefaultObjstoreBucket) {
		b.logger = logger
	}
}

func NewDefaultObjstoreBucket(b objstore.Bucket, options ...DefaultObjstoreBucketOption) *DefaultObjstoreBucket {
	d := &DefaultObjstoreBucket{
		Bucket: storage.NewBucketReaderAt(b),
		tracer: trace.NewNoopTracerProvider().Tracer(""),
		logger: log.NewNopLogger(),
	}

	for _, option := range options {
		option(d)
	}

	return d
}

func (b *DefaultObjstoreBucket) String() string {
	return b.Bucket.Name()
}

func (b *DefaultObjstoreBucket) TableScan(ctx context.Context, prefix string, _ *dynparquet.Schema, filter logicalplan.Expr, lastBlockTimestamp uint64, stream chan<- any) error {
	ctx, span := b.tracer.Start(ctx, "Source/RowGroupIterator")
	span.SetAttributes(attribute.Int64("lastBlockTimestamp", int64(lastBlockTimestamp)))
	defer span.End()

	f, err := BooleanExpr(filter)
	if err != nil {
		return err
	}

	n := 0
	errg := &errgroup.Group{}
	errg.SetLimit(int(b.blockReaderLimit))
	err = b.Iter(ctx, prefix, func(blockDir string) error {
		n++
		errg.Go(func() error { return b.ProcessFile(ctx, blockDir, lastBlockTimestamp, f, stream) })
		return nil
	})
	if err != nil {
		return err
	}

	level.Debug(b.logger).Log("msg", "read blocks", "n", n)
	return errg.Wait()
}

func (b *DefaultObjstoreBucket) openBlockFile(ctx context.Context, blockName string, size int64) (*parquet.File, error) {
	ctx, span := b.tracer.Start(ctx, "Source/IterateBucketBlocks/Iter/OpenFile")
	defer span.End()
	r, err := b.GetReaderAt(ctx, blockName)
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
func (b *DefaultObjstoreBucket) ProcessFile(ctx context.Context, blockDir string, lastBlockTimestamp uint64, filter TrueNegativeFilter, rowGroups chan<- any) error {
	ctx, span := b.tracer.Start(ctx, "Source/IterateBucketBlocks/Iter/ProcessFile")
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
	attribs, err := b.Attributes(ctx, blockName)
	if err != nil {
		return err
	}

	span.SetAttributes(attribute.Int64("size", attribs.Size))

	file, err := b.openBlockFile(ctx, blockName, attribs.Size)
	if err != nil {
		return err
	}

	// Get a reader from the file bytes
	buf, err := dynparquet.NewSerializedBuffer(file)
	if err != nil {
		return err
	}

	return b.filterRowGroups(ctx, buf, filter, rowGroups)
}

func (b *DefaultObjstoreBucket) filterRowGroups(ctx context.Context, buf *dynparquet.SerializedBuffer, filter TrueNegativeFilter, rowGroups chan<- any) error {
	_, span := b.tracer.Start(ctx, "Source/filterRowGroups")
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
