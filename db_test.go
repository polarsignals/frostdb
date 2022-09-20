package frostdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

func TestDBWithWALAndBucket(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	logger := newTestLogger(t)

	dir, err := os.MkdirTemp("", "frostdb-with-wal-test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	bucket, err := filesystem.NewBucket(dir)
	require.NoError(t, err)

	c, err := New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithBucketStorage(bucket),
		WithActiveMemorySize(100*1024),
	)

	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		buf, err := samples.ToBuffer(table.Schema())
		require.NoError(t, err)
		_, err = table.InsertBuffer(ctx, buf)
		require.NoError(t, err)
	}
	table.Sync()
	require.NoError(t, db.wal.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithBucketStorage(bucket),
		WithActiveMemorySize(100*1024),
	)
	require.NoError(t, err)
	require.NoError(t, c.ReplayWALs(context.Background()))
}

func TestDBWithWAL(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	logger := newTestLogger(t)

	dir, err := os.MkdirTemp("", "frostdb-with-wal-test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	c, err := New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
	)
	require.NoError(t, err)

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
	)
	require.NoError(t, err)

	require.NoError(t, c.ReplayWALs(context.Background()))

	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err = db.Table("test", config)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		as, err := table.ArrowSchema(ctx, tx, pool, logicalplan.IterOptions{})
		if err != nil {
			return err
		}

		return table.Iterator(
			ctx,
			tx,
			pool,
			as,
			logicalplan.IterOptions{},
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				t.Log(ar)
				defer ar.Release()
				return nil
			}},
		)
	})
	require.NoError(t, err)

	uuid1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	uuid2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	// One granule with 3 parts
	require.Equal(t, 1, table.active.Index().Len())
	require.Equal(t, uint64(3), table.active.Index().Min().(*Granule).parts.total.Load())
	require.Equal(t, parquet.Row{
		parquet.ValueOf("test").Level(0, 0, 0),
		parquet.ValueOf("value1").Level(0, 1, 1),
		parquet.ValueOf("value2").Level(0, 1, 2),
		parquet.ValueOf(nil).Level(0, 0, 3),
		parquet.ValueOf(nil).Level(0, 0, 4),
		parquet.ValueOf(append(uuid1[:], uuid2[:]...)).Level(0, 0, 5),
		parquet.ValueOf(1).Level(0, 0, 6),
		parquet.ValueOf(1).Level(0, 0, 7),
	}, (*dynparquet.DynamicRow)(table.active.Index().Min().(*Granule).metadata.least.Load()).Row)
	require.Equal(t, 1, table.active.Index().Len())
}

func Test_DB_WithStorage(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	// TODO(asubiotto): These buckets are not deleted from disk, resulting in
	// test failures if run multiple times. There seems to be an attempt below
	// to delete them, but it doesn't have an effect. What's the magic
	// incantation?
	bucket, err := filesystem.NewBucket(".")
	require.NoError(t, err)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
		WithBucketStorage(bucket),
	)
	require.NoError(t, err)

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	defer os.RemoveAll(t.Name())
	table, err := db.Table(t.Name(), config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Gracefully close the db to persist blocks
	c.Close()

	pool := memory.NewGoAllocator()
	engine := query.NewEngine(pool, db.TableProvider())
	err = engine.ScanTable(t.Name()).
		Filter(logicalplan.Col("timestamp").GtEq(logicalplan.Literal(2))).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			require.Equal(t, int64(1), r.NumCols())
			require.Equal(t, int64(2), r.NumRows())
			return nil
		})
	require.NoError(t, err)
}

func Test_DB_ColdStart(t *testing.T) {
	sanitize := func(name string) string {
		return strings.Replace(name, "/", "-", -1)
	}

	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	bucket, err := filesystem.NewBucket(".")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(sanitize(t.Name()))
	})

	logger := newTestLogger(t)

	tests := map[string]struct {
		newColumnstore func(t *testing.T) *ColumnStore
	}{
		"cold start with storage": {
			newColumnstore: func(t *testing.T) *ColumnStore {
				c, err := New(
					WithLogger(logger),
					WithBucketStorage(bucket),
				)
				require.NoError(t, err)
				return c
			},
		},
		"cold start with storage and wal": {
			newColumnstore: func(t *testing.T) *ColumnStore {
				dir, err := os.MkdirTemp("", "cold-start-with-storage-and-wal")
				require.NoError(t, err)
				t.Cleanup(func() {
					os.RemoveAll(dir) // clean up
				})
				c, err := New(
					WithLogger(logger),
					WithBucketStorage(bucket),
					WithWAL(),
					WithStoragePath(dir),
				)
				require.NoError(t, err)
				return c
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			c := test.newColumnstore(t)
			db, err := c.DB(context.Background(), sanitize(t.Name()))
			require.NoError(t, err)
			table, err := db.Table(sanitize(t.Name()), config)
			require.NoError(t, err)
			t.Cleanup(func() {
				os.RemoveAll(sanitize(t.Name()))
			})

			samples := dynparquet.Samples{
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 1,
					Value:     1,
				},
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 2,
					Value:     2,
				},
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 3,
					Value:     3,
				},
			}

			buf, err := samples.ToBuffer(table.Schema())
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertBuffer(ctx, buf)
			require.NoError(t, err)

			// Gracefully close the db to persist blocks
			c.Close()

			// Open a new database pointed to the same bucket storage
			c, err = New(
				WithLogger(logger),
				WithBucketStorage(bucket),
			)
			require.NoError(t, err)

			// connect to our test db
			db, err = c.DB(context.Background(), sanitize(t.Name()))
			require.NoError(t, err)

			pool := memory.NewGoAllocator()
			engine := query.NewEngine(pool, db.TableProvider())
			require.NoError(t, engine.ScanTable(sanitize(t.Name())).Execute(
				context.Background(), func(ctx context.Context, r arrow.Record) error {
					require.Equal(t, int64(6), r.NumCols())
					require.Equal(t, int64(3), r.NumRows())
					return nil
				},
			))
		})
	}
}

func Test_DB_ColdStart_MissingColumn(t *testing.T) {
	schemaDef := &schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{
			{
				Name: "example_type",
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				},
				Dynamic: false,
			},
			{
				Name: "labels",
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Nullable: true,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				},
				Dynamic: true,
			},
			{
				Name: "pprof_labels",
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Nullable: true,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				},
				Dynamic: true,
			},
		},
		SortingColumns: []*schemapb.SortingColumn{
			{
				Name:      "example_type",
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
			{
				Name:       "labels",
				Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
				NullsFirst: true,
			},
			{
				Name:       "pprof_labels",
				Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
				NullsFirst: true,
			},
		},
	}

	s, err := dynparquet.SchemaFromDefinition(schemaDef)
	require.NoError(t, err)
	config := NewTableConfig(s)

	bucket, err := filesystem.NewBucket(".")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
		WithBucketStorage(bucket),
	)
	require.NoError(t, err)

	db, err := c.DB(context.Background(), t.Name())
	require.NoError(t, err)
	table, err := db.Table(t.Name(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	buf, err := s.NewBuffer(map[string][]string{
		"labels": {
			"label1",
			"label2",
		},
		"pprof_labels": {},
	})
	require.NoError(t, err)

	_, err = buf.WriteRows([]parquet.Row{
		{
			parquet.ValueOf("test").Level(0, 0, 0),
			parquet.ValueOf("value1").Level(0, 1, 1),
			parquet.ValueOf("value1").Level(0, 1, 2),
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Gracefully close the db to persist blocks
	c.Close()

	// Open a new database pointed to the same bucket storage
	c, err = New(
		WithLogger(logger),
		WithBucketStorage(bucket),
	)
	require.NoError(t, err)

	// connect to our test db
	db, err = c.DB(context.Background(), t.Name())
	require.NoError(t, err)

	// fetch new table
	table, err = db.Table(t.Name(), config)
	require.NoError(t, err)

	buf, err = s.NewBuffer(map[string][]string{
		"labels": {
			"label1",
			"label2",
		},
		"pprof_labels": {},
	})
	require.NoError(t, err)

	_, err = buf.WriteRows([]parquet.Row{
		{
			parquet.ValueOf("test").Level(0, 0, 0),
			parquet.ValueOf("value2").Level(0, 1, 1),
			parquet.ValueOf("value2").Level(0, 1, 2),
		},
	})
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)
}

func Test_DB_Filter_Block(t *testing.T) {
	sanitize := func(name string) string {
		return strings.Replace(name, "/", "-", -1)
	}

	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	bucket, err := filesystem.NewBucket(".")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(sanitize(t.Name()))
	})

	logger := newTestLogger(t)

	tests := map[string]struct {
		newColumnstore func(t *testing.T) *ColumnStore
		filterExpr     logicalplan.Expr
		projections    []logicalplan.Expr
		distinct       []logicalplan.Expr
		rows           int64
		cols           int64
	}{
		"dynamic projection no optimization": {
			filterExpr: logicalplan.And(
				logicalplan.Col("timestamp").GtEq(logicalplan.Literal(2)),
			),
			projections: []logicalplan.Expr{logicalplan.DynCol("labels")},
			rows:        2,
			cols:        2,
			newColumnstore: func(t *testing.T) *ColumnStore {
				c, err := New(
					WithLogger(logger),
					WithBucketStorage(bucket),
				)
				require.NoError(t, err)
				return c
			},
		},
		"distinct": {
			filterExpr:  nil,
			distinct:    []logicalplan.Expr{logicalplan.DynCol("labels")},
			projections: nil,
			rows:        1,
			cols:        2,
			newColumnstore: func(t *testing.T) *ColumnStore {
				c, err := New(
					WithLogger(logger),
					WithBucketStorage(bucket),
				)
				require.NoError(t, err)
				return c
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			c := test.newColumnstore(t)
			db, err := c.DB(context.Background(), sanitize(t.Name()))
			require.NoError(t, err)
			table, err := db.Table(sanitize(t.Name()), config)
			require.NoError(t, err)
			t.Cleanup(func() {
				os.RemoveAll(sanitize(t.Name()))
			})

			samples := dynparquet.Samples{
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 1,
					Value:     1,
				},
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 2,
					Value:     2,
				},
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 3,
					Value:     3,
				},
			}

			buf, err := samples.ToBuffer(table.Schema())
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertBuffer(ctx, buf)
			require.NoError(t, err)

			// Gracefully close the db to persist blocks
			c.Close()

			// Open a new database pointed to the same bucket storage
			c, err = New(
				WithLogger(logger),
				WithBucketStorage(bucket),
			)
			require.NoError(t, err)

			// connect to our test db
			db, err = c.DB(context.Background(), sanitize(t.Name()))
			require.NoError(t, err)

			engine := query.NewEngine(
				memory.NewGoAllocator(),
				db.TableProvider(),
			)

			query := engine.ScanTable(sanitize(t.Name()))
			if test.filterExpr != nil {
				query = query.Filter(test.filterExpr)
			}
			if test.projections != nil {
				query = query.Project(test.projections...)
			}
			if test.distinct != nil {
				query = query.Distinct(test.distinct...)
			}
			err = query.Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
				require.Equal(t, test.rows, ar.NumRows())
				require.Equal(t, test.cols, ar.NumCols())
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// ErrorBucket is an objstore.Bucket implementation that supports error injection.
type ErrorBucket struct {
	iter             func(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error
	get              func(ctx context.Context, name string) (io.ReadCloser, error)
	getRange         func(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)
	exists           func(ctx context.Context, name string) (bool, error)
	isObjNotFoundErr func(err error) bool
	attributes       func(ctx context.Context, name string) (objstore.ObjectAttributes, error)

	upload func(ctx context.Context, name string, r io.Reader) error
	delete func(ctx context.Context, name string) error
	close  func() error
}

func (e *ErrorBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if e.iter != nil {
		return e.iter(ctx, dir, f, options...)
	}

	return nil
}

func (e *ErrorBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if e.get != nil {
		return e.get(ctx, name)
	}

	return nil, nil
}

func (e *ErrorBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if e.getRange != nil {
		return e.getRange(ctx, name, off, length)
	}

	return nil, nil
}

func (e *ErrorBucket) Exists(ctx context.Context, name string) (bool, error) {
	if e.exists != nil {
		return e.exists(ctx, name)
	}

	return false, nil
}

func (e *ErrorBucket) IsObjNotFoundErr(err error) bool {
	if e.isObjNotFoundErr != nil {
		return e.isObjNotFoundErr(err)
	}

	return false
}

func (e *ErrorBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if e.attributes != nil {
		return e.attributes(ctx, name)
	}

	return objstore.ObjectAttributes{}, nil
}

func (e *ErrorBucket) Close() error {
	if e.close != nil {
		return e.close()
	}

	return nil
}

func (e *ErrorBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	if e.upload != nil {
		return e.upload(ctx, name, r)
	}

	return nil
}

func (e *ErrorBucket) Delete(ctx context.Context, name string) error {
	if e.delete != nil {
		return e.delete(ctx, name)
	}

	return nil
}

func (e *ErrorBucket) Name() string { return "error bucket" }

func Test_DB_OpenError(t *testing.T) {
	logger := newTestLogger(t)

	temp := true
	tempErr := fmt.Errorf("injected temporary error")
	e := &ErrorBucket{
		iter: func(context.Context, string, func(string) error, ...objstore.IterOption) error {
			if temp {
				temp = false
				return tempErr
			}
			return nil
		},
	}

	c, err := New(
		WithLogger(logger),
		WithBucketStorage(e),
	)
	require.NoError(t, err)

	// First time returns temporary error and triggers the chicken switch
	db, err := c.DB(context.Background(), "test")
	require.Error(t, err)
	require.Nil(t, db)
	require.True(t, errors.Is(err, tempErr))

	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, db)
}

func Test_DB_Block_Optimization(t *testing.T) {
	sanitize := func(name string) string {
		return strings.Replace(name, "/", "-", -1)
	}

	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	bucket, err := filesystem.NewBucket(".")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(sanitize(t.Name()))
	})

	logger := newTestLogger(t)

	now := time.Now()
	ts := now.UnixMilli()

	tests := map[string]struct {
		newColumnstore func(t *testing.T) *ColumnStore
		filterExpr     logicalplan.Expr
		projections    []logicalplan.Expr
		distinct       []logicalplan.Expr
		rows           int64
		cols           int64
	}{
		"include block in filter": {
			filterExpr:  logicalplan.Col("timestamp").GtEq(logicalplan.Literal(now.Add(-1 * time.Minute).UnixMilli())),
			projections: []logicalplan.Expr{logicalplan.DynCol("labels")},
			rows:        3,
			cols:        2,
			newColumnstore: func(t *testing.T) *ColumnStore {
				c, err := New(
					WithLogger(logger),
					WithBucketStorage(bucket),
				)
				require.NoError(t, err)
				return c
			},
		},
		"exclude block in filter": {
			filterExpr:  logicalplan.Col("timestamp").GtEq(logicalplan.Literal(now.Add(time.Minute).UnixMilli())),
			projections: []logicalplan.Expr{logicalplan.DynCol("labels")},
			rows:        0,
			cols:        0,
			newColumnstore: func(t *testing.T) *ColumnStore {
				c, err := New(
					WithLogger(logger),
					WithBucketStorage(bucket),
				)
				require.NoError(t, err)
				return c
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			c := test.newColumnstore(t)
			db, err := c.DB(context.Background(), sanitize(t.Name()))
			require.NoError(t, err)
			table, err := db.Table(sanitize(t.Name()), config)
			require.NoError(t, err)
			t.Cleanup(func() {
				os.RemoveAll(sanitize(t.Name()))
			})

			samples := dynparquet.Samples{
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: ts,
					Value:     1,
				},
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: ts,
					Value:     2,
				},
				{
					ExampleType: "test",
					Labels: []dynparquet.Label{
						{Name: "label1", Value: "value1"},
						{Name: "label2", Value: "value2"},
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: ts,
					Value:     3,
				},
			}

			buf, err := samples.ToBuffer(table.Schema())
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertBuffer(ctx, buf)
			require.NoError(t, err)

			// Gracefully close the db to persist blocks
			c.Close()

			// Open a new database pointed to the same bucket storage
			c, err = New(
				WithLogger(logger),
				WithBucketStorage(bucket),
			)
			require.NoError(t, err)

			// connect to our test db
			db, err = c.DB(context.Background(), sanitize(t.Name()))
			require.NoError(t, err)

			engine := query.NewEngine(
				memory.NewGoAllocator(),
				db.TableProvider(),
			)

			query := engine.ScanTable(sanitize(t.Name()))
			if test.filterExpr != nil {
				query = query.Filter(test.filterExpr)
			}
			if test.projections != nil {
				query = query.Project(test.projections...)
			}
			if test.distinct != nil {
				query = query.Distinct(test.distinct...)
			}
			rows := int64(0)
			cols := int64(0)
			err = query.Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
				rows += ar.NumRows()
				cols += ar.NumCols()
				return nil
			})
			require.Equal(t, test.rows, rows)
			require.Equal(t, test.cols, cols)
			require.NoError(t, err)
		})
	}
}

func Test_DB_TableWrite_FlatSchema(t *testing.T) {
	ctx := context.Background()
	flatDefinition := &schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "example_type",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: false,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "example_type",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
	schema, err := dynparquet.SchemaFromDefinition(flatDefinition)
	require.NoError(t, err)
	config := NewTableConfig(schema)

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)

	db, err := c.DB(ctx, "flatschema")
	require.NoError(t, err)

	table, err := db.Table("test", config)
	require.NoError(t, err)

	s := struct {
		ExampleType string
		Timestamp   int64
		Value       int64
	}{
		ExampleType: "hello-world",
		Timestamp:   7,
		Value:       8,
	}

	_, err = table.Write(ctx, s)
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	err = engine.ScanTable("test").Execute(ctx, func(ctx context.Context, ar arrow.Record) error {
		require.Equal(t, int64(1), ar.NumRows())
		require.Equal(t, int64(3), ar.NumCols())
		return nil
	})
	require.NoError(t, err)
}

func Test_DB_TableWrite_DynamicSchema(t *testing.T) {
	ctx := context.Background()
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)

	db, err := c.DB(ctx, "sampleschema")
	require.NoError(t, err)

	table, err := db.Table("test", config)
	require.NoError(t, err)

	now := time.Now()
	ts := now.UnixMilli()
	samples := dynparquet.Samples{
		{
			ExampleType: "test",
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: ts,
			Value:     1,
		},
		{
			ExampleType: "test",
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
				{Name: "label3", Value: "value3"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: ts,
			Value:     2,
		},
		{
			ExampleType: "test",
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: ts,
			Value:     3,
		},
	}

	_, err = table.Write(ctx, samples[0], samples[1], samples[2])
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	err = engine.ScanTable("test").Execute(ctx, func(ctx context.Context, ar arrow.Record) error {
		require.Equal(t, int64(3), ar.NumRows())
		require.Equal(t, int64(7), ar.NumCols())
		return nil
	})
	require.NoError(t, err)
}

func Test_DB_MaterializedView(t *testing.T) {
	ctx := context.Background()
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)

	db, err := c.DB(ctx, "sampleschema")
	require.NoError(t, err)

	table, err := db.Table("test", config)
	require.NoError(t, err)

	now := time.Now()
	ts := now.UnixMilli()
	samples := dynparquet.Samples{
		{
			ExampleType: "test",
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: ts,
			Value:     1,
		},
		{
			ExampleType: "test",
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
				{Name: "label3", Value: "value3"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: ts,
			Value:     2,
		},
		{
			ExampleType: "test",
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: ts,
			Value:     3,
		},
	}

	_, err = table.Write(ctx, samples[0], samples[1], samples[2])
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	viewCfg := NewMaterializedView(engine.ScanTable("test").Distinct(logicalplan.DynCol("labels")))

	_, err = db.Table("labels_view", viewCfg)
	require.NoError(t, err)

	engine.ScanTable("labels_view").Execute(ctx, func(ctx context.Context, ar arrow.Record) error {
		fmt.Println(ar)
		return nil
	})
}
