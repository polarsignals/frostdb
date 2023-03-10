package frostdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

func TestDBWithWALAndBucket(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	dir := t.TempDir()
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
	require.NoError(t, table.EnsureCompaction())
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithBucketStorage(bucket),
		WithActiveMemorySize(100*1024),
	)
	require.NoError(t, err)
	defer c.Close()
}

func TestDBWithWAL(t *testing.T) {
	ctx := context.Background()
	test := func(t *testing.T, isArrow bool) {
		config := NewTableConfig(
			dynparquet.SampleDefinition(),
		)

		logger := newTestLogger(t)

		dir := t.TempDir()
		c, err := New(
			WithLogger(logger),
			WithWAL(),
			WithStoragePath(dir),
		)
		require.NoError(t, err)
		defer c.Close()

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

		switch isArrow {
		case true:

			ps, err := table.Schema().DynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2", "label3", "label4"},
			})
			require.NoError(t, err)

			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps, logicalplan.IterOptions{})
			require.NoError(t, err)

			rec, err := samples.ToRecord(sc)
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, rec)
			require.NoError(t, err)

		default:
			buf, err := samples.ToBuffer(table.Schema())
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertBuffer(ctx, buf)
			require.NoError(t, err)
		}

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

		switch isArrow {
		case true:
			ps, err := table.Schema().DynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2"},
			})
			require.NoError(t, err)

			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps, logicalplan.IterOptions{})
			require.NoError(t, err)

			rec, err := samples.ToRecord(sc)
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, rec)
			require.NoError(t, err)
		default:
			buf, err := samples.ToBuffer(table.Schema())
			require.NoError(t, err)

			_, err = table.InsertBuffer(ctx, buf)
			require.NoError(t, err)
		}

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

		switch isArrow {
		case true:
			ps, err := table.Schema().DynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2", "label3"},
			})
			require.NoError(t, err)

			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps, logicalplan.IterOptions{})
			require.NoError(t, err)

			rec, err := samples.ToRecord(sc)
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, rec)
			require.NoError(t, err)
		default:
			buf, err := samples.ToBuffer(table.Schema())
			require.NoError(t, err)

			_, err = table.InsertBuffer(ctx, buf)
			require.NoError(t, err)
		}

		require.NoError(t, c.Close())

		c, err = New(
			WithLogger(logger),
			WithWAL(),
			WithStoragePath(dir),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err = c.DB(context.Background(), "test")
		require.NoError(t, err)
		table, err = db.Table("test", config)
		require.NoError(t, err)

		pool := memory.NewGoAllocator()
		records := []arrow.Record{}
		err = table.View(ctx, func(ctx context.Context, tx uint64) error {
			return table.Iterator(
				ctx,
				tx,
				pool,
				[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
					ar.Retain()
					records = append(records, ar)
					return nil
				}},
			)
		})
		require.NoError(t, err)

		// Validate returned data
		rows := int64(0)
		for _, r := range records {
			rows += r.NumRows()
			r.Release()
		}
		require.Equal(t, int64(5), rows)
	}

	t.Run("parquet", func(t *testing.T) {
		test(t, false)
	})
	t.Run("arrow", func(t *testing.T) {
		test(t, true)
	})
}

func Test_DB_WithStorage(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	bucket, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
		WithBucketStorage(bucket),
	)
	require.NoError(t, err)

	db, err := c.DB(context.Background(), t.Name())
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
		dynparquet.SampleDefinition(),
	)

	bucket, err := filesystem.NewBucket(t.TempDir())
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
				c, err := New(
					WithLogger(logger),
					WithBucketStorage(bucket),
					WithWAL(),
					WithStoragePath(t.TempDir()),
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
			defer c.Close()

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

	config := NewTableConfig(schemaDef)

	bucket, err := filesystem.NewBucket(t.TempDir())
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

	buf, err := table.schema.NewBuffer(map[string][]string{
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
	defer c.Close()

	// connect to our test db
	db, err = c.DB(context.Background(), t.Name())
	require.NoError(t, err)

	// fetch new table
	table, err = db.Table(t.Name(), config)
	require.NoError(t, err)

	buf, err = table.schema.NewBuffer(map[string][]string{
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
		dynparquet.SampleDefinition(),
	)

	bucket, err := filesystem.NewBucket(t.TempDir())
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
			defer c.Close()

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
	defer c.Close()

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
		dynparquet.SampleDefinition(),
	)

	bucket, err := filesystem.NewBucket(t.TempDir())
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
			defer c.Close()

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
	config := NewTableConfig(flatDefinition)

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)
	defer c.Close()

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
		dynparquet.SampleDefinition(),
	)

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)
	defer c.Close()

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

func Test_DB_TableNotExist(t *testing.T) {
	ctx := context.Background()

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(ctx, "test")
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	err = engine.ScanTable("does-not-exist").Execute(ctx, func(ctx context.Context, ar arrow.Record) error {
		return nil
	})
	require.Error(t, err)
}

func Test_DB_TableWrite_ArrowRecord(t *testing.T) {
	ctx := context.Background()
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(ctx, "sampleschema")
	require.NoError(t, err)

	table, err := db.Table("test", config)
	require.NoError(t, err)

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
			Timestamp: 10,
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
			Timestamp: 11,
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
			Timestamp: 12,
			Value:     3,
		},
	}

	ps, err := table.Schema().DynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3"},
	})
	require.NoError(t, err)

	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps, logicalplan.IterOptions{})
	require.NoError(t, err)

	r, err := samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	tests := map[string]struct {
		filter   logicalplan.Expr
		distinct logicalplan.Expr
		rows     int64
		cols     int64
	}{
		"none": {
			rows: 3,
			cols: 7,
		},
		"timestamp filter": {
			filter: logicalplan.Col("timestamp").GtEq(logicalplan.Literal(12)),
			rows:   1,
			cols:   7,
		},
		"distinct": {
			distinct: logicalplan.DynCol("labels"),
			rows:     2,
			cols:     3,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			bldr := engine.ScanTable("test")
			if test.filter != nil {
				bldr = bldr.Filter(test.filter)
			}
			if test.distinct != nil {
				bldr = bldr.Distinct(test.distinct)
			}
			err = bldr.Execute(ctx, func(ctx context.Context, ar arrow.Record) error {
				require.Equal(t, test.rows, ar.NumRows())
				require.Equal(t, test.cols, ar.NumCols())
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func Test_DB_ReadOnlyQuery(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	dir := t.TempDir()
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
	require.NoError(t, table.EnsureCompaction())
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithBucketStorage(bucket),
		WithActiveMemorySize(100*1024),
	)
	require.NoError(t, err)
	defer c.Close()

	// Query with an aggregat query
	pool := memory.NewGoAllocator()
	engine := query.NewEngine(pool, db.TableProvider())
	err = engine.ScanTable("test").
		Aggregate(
			[]logicalplan.Expr{logicalplan.Sum(logicalplan.Col("value"))},
			[]logicalplan.Expr{logicalplan.Col("labels.label2")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			return nil
		})
	require.NoError(t, err)
}

// TestDBRecover verifies correct DB recovery with both a WAL and snapshots as
// well as a block rotation (in which case no duplicate data should be in the
// database).
func TestDBRecover(t *testing.T) {
	ctx := context.Background()
	const (
		dbAndTableName = "test"
		numInserts     = 3
	)
	dir := t.TempDir()
	func() {
		c, err := New(
			WithLogger(newTestLogger(t)),
			WithStoragePath(dir),
			WithWAL(),
			WithSnapshotTriggerSize(1),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, dbAndTableName)
		require.NoError(t, err)
		schema := dynparquet.SampleDefinition()
		table, err := db.Table(dbAndTableName, NewTableConfig(schema))
		require.NoError(t, err)

		// Insert 3 txns.
		var lastWriteTx uint64
		for i := 0; i < numInserts; i++ {
			samples := dynparquet.NewTestSamples()
			for i := range samples {
				samples[i].Timestamp = int64(i)
			}
			buf, err := samples.ToBuffer(table.schema)
			require.NoError(t, err)
			writeTx, err := table.InsertBuffer(ctx, buf)
			require.NoError(t, err)
			if i > 0 {
				// Wait until a snapshot is written for each write (it is the txn
				// immediately preceding the write). This has to be done in a loop,
				// otherwise writes may not cause a snapshot given that there
				// might be a snapshot in progress.
				db.Wait(writeTx - 1)
				lastWriteTx = writeTx
			}
		}
		// At this point, there should be 2 snapshots. One was triggered before
		// the second write, and the second was triggered before the third write.
		// A block rotation should trigger the third snapshot.
		require.NoError(t, table.RotateBlock(ctx, table.ActiveBlock()))
		// Wait for both the new block txn, and the old block rotation txn.
		db.Wait(lastWriteTx + 2)

		files, err := os.ReadDir(db.snapshotsDir())
		require.NoError(t, err)
		snapshotTxns := make([]uint64, 0, len(files))
		for _, f := range files {
			tx, err := getTxFromSnapshotFileName(f.Name())
			require.NoError(t, err)
			snapshotTxns = append(snapshotTxns, tx)
		}
		// Verify that there are now 3 snapshots and their txns.
		require.Equal(t, []uint64{3, 5, 8}, snapshotTxns)
	}()

	c, err := New(
		WithLogger(newTestLogger(t)),
		WithStoragePath(dir),
		WithWAL(),
		WithSnapshotTriggerSize(1),
	)
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(ctx, dbAndTableName)
	require.NoError(t, err)
	// Simulate corruption of the snapshot taken during block rotation. This
	// will cause recovery to use the snapshot with all the data before block
	// rotation. However, the WAL replay should notice that this data has
	// already been persisted.
	require.NoError(t, os.Remove(filepath.Join(db.snapshotsDir(), snapshotFileName(8))))

	engine := query.NewEngine(memory.DefaultAllocator, db.TableProvider())
	nrows := 0
	require.NoError(t, engine.ScanTable(dbAndTableName).
		Distinct(logicalplan.Col("timestamp")).
		Execute(
			ctx,
			func(_ context.Context, r arrow.Record) error {
				nrows += int(r.NumRows())
				return nil
			}))
	// No more timestamps if querying in-memory only, since the data has
	// been rotated.
	require.Equal(t, 0, nrows)
}

func Test_DB_WalReplayTableConfig(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
		WithRowGroupSize(10),
	)

	logger := newTestLogger(t)

	dir := t.TempDir()

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
	require.Equal(t, uint64(10), table.config.RowGroupSize)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		buf, err := samples.ToBuffer(table.Schema())
		require.NoError(t, err)
		_, err = table.InsertBuffer(ctx, buf)
		require.NoError(t, err)
	}
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
	)
	require.NoError(t, err)
	defer c.Close()

	db, err = c.DB(ctx, "test")
	require.NoError(t, err)

	table, err = db.Table("test", nil) // Pass nil because we expect the table to already exist because of wal replay
	require.NoError(t, err)
	require.Equal(t, uint64(10), table.config.RowGroupSize)
}
