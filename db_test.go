package frostdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestDBWithWALAndBucket(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	dir := t.TempDir()
	bucket := objstore.NewInMemBucket()

	sinksource := NewDefaultObjstoreBucket(bucket)

	c, err := New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithReadWriteStorage(sinksource),
		WithActiveMemorySize(100*1024),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"node", "namespace", "pod", "container"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		buf, err := samples.ToRecord(sc)
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, buf)
		require.NoError(t, err)
	}
	require.NoError(t, table.EnsureCompaction())
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithReadWriteStorage(sinksource),
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

			ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2", "label3", "label4"},
			})
			require.NoError(t, err)
			defer table.Schema().PutPooledParquetSchema(ps)

			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
			require.NoError(t, err)

			rec, err := samples.ToRecord(sc)
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, rec)
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
			ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2"},
			})
			require.NoError(t, err)
			defer table.Schema().PutPooledParquetSchema(ps)

			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
			require.NoError(t, err)

			rec, err := samples.ToRecord(sc)
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, rec)
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
			ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2", "label3"},
			})
			require.NoError(t, err)
			defer table.Schema().PutPooledParquetSchema(ps)

			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
			require.NoError(t, err)

			rec, err := samples.ToRecord(sc)
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, rec)
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

		// Perform an aggregate query against the replayed data
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
	t.Run("arrow", func(t *testing.T) {
		test(t, true)
	})
}

func Test_DB_WithStorage(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	bucket := objstore.NewInMemBucket()
	sinksource := NewDefaultObjstoreBucket(bucket)
	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
		WithReadWriteStorage(sinksource),
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

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3", "label4"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err := samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, buf)
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

	bucket := objstore.NewInMemBucket()
	sinksource := NewDefaultObjstoreBucket(bucket)
	logger := newTestLogger(t)

	tests := map[string]struct {
		newColumnstore func(t *testing.T) *ColumnStore
	}{
		"cold start with storage": {
			newColumnstore: func(t *testing.T) *ColumnStore {
				c, err := New(
					WithLogger(logger),
					WithReadWriteStorage(sinksource),
				)
				require.NoError(t, err)
				return c
			},
		},
		"cold start with storage and wal": {
			newColumnstore: func(t *testing.T) *ColumnStore {
				c, err := New(
					WithLogger(logger),
					WithReadWriteStorage(sinksource),
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

			ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2"},
			})
			require.NoError(t, err)
			defer table.Schema().PutPooledParquetSchema(ps)

			ctx := context.Background()
			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
			require.NoError(t, err)

			buf, err := samples.ToRecord(sc)
			require.NoError(t, err)

			_, err = table.InsertRecord(ctx, buf)
			require.NoError(t, err)

			// Gracefully close the db to persist blocks
			c.Close()

			// Open a new database pointed to the same bucket storage
			c, err = New(
				WithLogger(logger),
				WithReadWriteStorage(sinksource),
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

func Test_DB_Filter_Block(t *testing.T) {
	sanitize := func(name string) string {
		return strings.Replace(name, "/", "-", -1)
	}

	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	bucket := objstore.NewInMemBucket()
	sinksource := NewDefaultObjstoreBucket(bucket)
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
					WithReadWriteStorage(sinksource),
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
					WithReadWriteStorage(sinksource),
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

			ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2"},
			})
			require.NoError(t, err)
			defer table.Schema().PutPooledParquetSchema(ps)

			ctx := context.Background()
			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
			require.NoError(t, err)

			buf, err := samples.ToRecord(sc)
			require.NoError(t, err)

			_, err = table.InsertRecord(ctx, buf)
			require.NoError(t, err)

			// Gracefully close the db to persist blocks
			c.Close()

			// Open a new database pointed to the same bucket storage
			c, err = New(
				WithLogger(logger),
				WithReadWriteStorage(sinksource),
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
	sinksource := NewDefaultObjstoreBucket(e)

	c, err := New(
		WithLogger(logger),
		WithReadWriteStorage(sinksource),
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

	bucket := objstore.NewInMemBucket()
	sinksource := NewDefaultObjstoreBucket(bucket)
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
					WithReadWriteStorage(sinksource),
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
					WithReadWriteStorage(sinksource),
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

			ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
				"labels": {"label1", "label2"},
			})
			require.NoError(t, err)
			defer table.Schema().PutPooledParquetSchema(ps)

			ctx := context.Background()
			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
			require.NoError(t, err)

			buf, err := samples.ToRecord(sc)
			require.NoError(t, err)

			_, err = table.InsertRecord(ctx, buf)
			require.NoError(t, err)

			// Gracefully close the db to persist blocks
			c.Close()

			// Open a new database pointed to the same bucket storage
			c, err = New(
				WithLogger(logger),
				WithReadWriteStorage(sinksource),
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

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
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
	bucket := objstore.NewInMemBucket()
	sinksource := NewDefaultObjstoreBucket(bucket)

	c, err := New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithReadWriteStorage(sinksource),
		WithActiveMemorySize(100*1024),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"node", "namespace", "pod", "container"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		buf, err := samples.ToRecord(sc)
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, buf)
		require.NoError(t, err)
	}
	require.NoError(t, table.EnsureCompaction())
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithReadWriteStorage(sinksource),
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

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"pod", "node", "namespace", "container"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		buf, err := samples.ToRecord(sc)
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, buf)
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

func TestDBMinTXPersisted(t *testing.T) {
	ctx := context.Background()
	c, err := New()
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(ctx, "test")
	require.NoError(t, err)

	schema := dynparquet.SampleDefinition()
	table, err := db.Table("test", NewTableConfig(schema))
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"node", "namespace", "pod", "container"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)
	buf, err := samples.ToRecord(sc)
	require.NoError(t, err)
	writeTx, err := table.InsertRecord(ctx, buf)
	require.NoError(t, err)

	require.NoError(t, table.RotateBlock(ctx, table.ActiveBlock()))
	// Writing the block is asynchronous, so wait for both the new table block
	// txn and the block persistence txn.
	db.Wait(writeTx + 2)

	require.Equal(t, uint64(1), db.getMinTXPersisted())

	_, err = db.Table("other", NewTableConfig(schema))
	require.NoError(t, err)

	require.Equal(t, uint64(0), db.getMinTXPersisted())
}

// TestReplayBackwardsCompatibility is a test that verifies that new versions of
// the code gracefully handle old versions of the WAL. If this test fails, it
// is likely that production code will break unless old WAL files are cleaned
// up.
// If it is expected that this test will fail, update testdata/oldwal with the
// new WAL files but make sure to delete old WAL files in production before
// deploying new code.
func TestReplayBackwardsCompatibility(t *testing.T) {
	const storagePath = "testdata/oldwal"
	c, err := New(WithWAL(), WithStoragePath(storagePath))
	require.NoError(t, err)
	defer c.Close()
}
