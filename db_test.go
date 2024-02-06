package frostdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
	"github.com/polarsignals/frostdb/recovery"
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
		WithActiveMemorySize(100*KiB),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.NoError(t, table.EnsureCompaction())
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithReadWriteStorage(sinksource),
		WithActiveMemorySize(100*KiB),
	)
	require.NoError(t, err)
	defer c.Close()
	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err = db.Table("test", config)
	require.NoError(t, err)

	// Validate that a read can be performed of the persisted data
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	rows := int64(0)
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		return table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				rows += ar.NumRows()
				return nil
			}},
		)
	})
	require.NoError(t, err)
	require.Equal(t, int64(300), rows)
}

func TestDBWithWAL(t *testing.T) {
	ctx := context.Background()
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
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	rec, err := samples.ToRecord()
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, rec)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	rec, err = samples.ToRecord()
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, rec)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	rec, err = samples.ToRecord()
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, rec)
	require.NoError(t, err)

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
			[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
			[]logicalplan.Expr{logicalplan.Col("labels.label2")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			return nil
		})
	require.NoError(t, err)
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
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	r, err := samples.ToRecord()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()
	engine := query.NewEngine(pool, db.TableProvider())
	var inMemory arrow.Record
	err = engine.ScanTable(t.Name()).
		Filter(logicalplan.Col("timestamp").GtEq(logicalplan.Literal(2))).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			r.Retain()
			inMemory = r
			return nil
		})
	require.NoError(t, err)

	// Gracefully close the db to persist blocks
	c.Close()

	c, err = New(
		WithLogger(logger),
		WithReadWriteStorage(sinksource),
	)
	require.NoError(t, err)
	defer c.Close()

	db, err = c.DB(context.Background(), t.Name())
	require.NoError(t, err)
	engine = query.NewEngine(pool, db.TableProvider())
	var onDisk arrow.Record
	err = engine.ScanTable(t.Name()).
		Filter(logicalplan.Col("timestamp").GtEq(logicalplan.Literal(2))).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			r.Retain()
			onDisk = r
			return nil
		})
	require.NoError(t, err)

	require.True(t, array.RecordEqual(inMemory, onDisk))
	require.Equal(t, int64(1), onDisk.NumCols())
	require.Equal(t, int64(2), onDisk.NumRows())
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 3,
					Value:     3,
				},
			}

			r, err := samples.ToRecord()
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, r)
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
	table, err := db.Table(t.Name(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "example_type", Type: arrow.BinaryTypes.Binary},
		{Name: "labels.label1", Type: arrow.BinaryTypes.Binary},
		{Name: "labels.label2", Type: arrow.BinaryTypes.Binary},
	}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.BinaryBuilder).Append([]byte("test"))
	bldr.Field(1).(*array.BinaryBuilder).Append([]byte("value1"))
	bldr.Field(2).(*array.BinaryBuilder).Append([]byte("value1"))

	r := bldr.NewRecord()
	defer r.Release()

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
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
	db, err = c.DB(context.Background(), t.Name())
	require.NoError(t, err)

	// fetch new table
	table, err = db.Table(t.Name(), config)
	require.NoError(t, err)

	bldr.Field(0).(*array.BinaryBuilder).Append([]byte("test"))
	bldr.Field(1).(*array.BinaryBuilder).Append([]byte("value2"))
	bldr.Field(2).(*array.BinaryBuilder).Append([]byte("value2"))

	r = bldr.NewRecord()
	defer r.Release()

	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: 3,
					Value:     3,
				},
			}

			r, err := samples.ToRecord()
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, r)
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
	iter                      func(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error
	get                       func(ctx context.Context, name string) (io.ReadCloser, error)
	getRange                  func(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)
	exists                    func(ctx context.Context, name string) (bool, error)
	isObjNotFoundErr          func(err error) bool
	isCustomerManagedKeyError func(err error) bool
	attributes                func(ctx context.Context, name string) (objstore.ObjectAttributes, error)

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

func (e *ErrorBucket) IsCustomerManagedKeyError(err error) bool {
	if e.isCustomerManagedKeyError != nil {
		return e.isCustomerManagedKeyError(err)
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
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
					Labels: map[string]string{
						"label1": "value1",
						"label2": "value2",
					},
					Stacktrace: []uuid.UUID{
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
					},
					Timestamp: ts,
					Value:     3,
				},
			}

			r, err := samples.ToRecord()
			require.NoError(t, err)

			ctx := context.Background()
			_, err = table.InsertRecord(ctx, r)
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

func Test_DB_TableWrite_FlatSchema(t *testing.T) {
	ctx := context.Background()

	type Flat struct {
		ExampleType string `frostdb:",rle_dict,asc(0)"`
		Timestamp   int64  `frostdb:",asc(1)"`
		Value       int64
	}

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(ctx, "flatschema")
	require.NoError(t, err)

	table, err := NewGenericTable[Flat](db, "test", memory.NewGoAllocator())
	require.NoError(t, err)
	defer table.Release()

	_, err = table.Write(ctx, Flat{
		ExampleType: "hello-world",
		Timestamp:   7,
		Value:       8,
	})
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

	c, err := New(WithLogger(newTestLogger(t)))
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(ctx, "sampleschema")
	require.NoError(t, err)

	table, err := NewGenericTable[dynparquet.Sample](db, "test", memory.NewGoAllocator())
	require.NoError(t, err)
	defer table.Release()

	now := time.Now()
	ts := now.UnixMilli()
	samples := dynparquet.Samples{
		{
			ExampleType: "test",
			Labels: map[string]string{
				"label1": "value1",
				"label2": "value2",
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
			Labels: map[string]string{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
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
			Labels: map[string]string{
				"label1": "value1",
				"label2": "value2",
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
	for _, schema := range []proto.Message{
		dynparquet.SampleDefinition(),
		dynparquet.PrehashedSampleDefinition(),
	} {
		ctx := context.Background()
		config := NewTableConfig(
			schema,
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
				Labels: map[string]string{
					"label1": "value1",
					"label2": "value2",
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
				Labels: map[string]string{
					"label1": "value1",
					"label2": "value2",
					"label3": "value3",
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
				Labels: map[string]string{
					"label1": "value1",
					"label2": "value2",
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: 12,
				Value:     3,
			},
		}

		r, err := samples.ToRecord()
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
				cols:   1,
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
		WithActiveMemorySize(100*KiB),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.NoError(t, table.EnsureCompaction())
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithReadWriteStorage(sinksource),
		WithActiveMemorySize(100*KiB),
	)
	require.NoError(t, err)
	defer c.Close()

	// Query with an aggregat query
	pool := memory.NewGoAllocator()
	engine := query.NewEngine(pool, db.TableProvider())
	err = engine.ScanTable("test").
		Aggregate(
			[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
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
	setup := func(t *testing.T, blockRotation bool, options ...Option) string {
		dir := t.TempDir()
		c, err := New(
			append([]Option{
				WithLogger(newTestLogger(t)),
				WithStoragePath(dir),
				WithWAL(),
				WithSnapshotTriggerSize(1),
				// Disable reclaiming disk space on snapshot (i.e. deleting
				// old snapshots and WAL). This allows us to modify on-disk
				// state for some tests.
				WithTestingNoDiskSpaceReclaimOnSnapshot(),
			},
				options...,
			)...,
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, dbAndTableName)
		require.NoError(t, err)
		schema := dynparquet.SampleDefinition()
		table, err := db.Table(dbAndTableName, NewTableConfig(schema))
		require.NoError(t, err)

		// Insert 3 txns.
		for i := 0; i < numInserts; i++ {
			samples := dynparquet.NewTestSamples()
			for j := range samples {
				samples[j].Timestamp = int64(i)
			}
			r, err := samples.ToRecord()
			require.NoError(t, err)
			_, err = table.InsertRecord(ctx, r)
			require.NoError(t, err)
			// Wait until a snapshot is written for each write (it is the txn
			// immediately preceding the write). This has to be done in a loop,
			// otherwise writes may not cause a snapshot given that there
			// might be a snapshot in progress.
			if i > 0 {
				require.Eventually(t, func() bool {
					files, err := os.ReadDir(db.snapshotsDir())
					if err != nil {
						return false
					}
					return len(files) == i && !db.snapshotInProgress.Load()
				}, 30*time.Second, 100*time.Millisecond)
			}
		}
		// At this point, there should be 2 snapshots. One was triggered before
		// the second write, and the second was triggered before the third write.
		if blockRotation {
			// A block rotation should trigger the third snapshot.
			require.NoError(t, table.RotateBlock(ctx, table.ActiveBlock(), false))
			// Wait for the snapshot to complete
			require.Eventually(t, func() bool {
				files, err := os.ReadDir(db.snapshotsDir())
				if err != nil {
					return false
				}
				return len(files) == 3 && !db.snapshotInProgress.Load()
			}, 30*time.Second, 100*time.Millisecond)
		}

		// Verify that there are now 3 snapshots and their txns.
		files, err := os.ReadDir(db.snapshotsDir())
		require.NoError(t, err)
		snapshotTxns := make([]uint64, 0, len(files))
		for _, f := range files {
			tx, err := getTxFromSnapshotFileName(f.Name())
			require.NoError(t, err)
			snapshotTxns = append(snapshotTxns, tx)
		}
		expectedSnapshots := []uint64{2, 3}
		if blockRotation {
			expectedSnapshots = append(expectedSnapshots, 6)
		}
		require.Equal(t, expectedSnapshots, snapshotTxns)
		return dir
	}

	t.Run("BlockRotation", func(t *testing.T) {
		dir := setup(t, true)
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
	})

	// The ability to write and expect a WAL record to be logged is vital on
	// database recovery. If it is not the case, writing to the WAL will be
	// stuck.
	newWriteAndExpectWALRecord := func(t *testing.T, db *DB, table *Table) {
		t.Helper()
		samples := dynparquet.NewTestSamples()
		for i := range samples {
			samples[i].Timestamp = numInserts
		}
		r, err := samples.ToRecord()
		require.NoError(t, err)

		writeTx, err := table.InsertRecord(ctx, r)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			lastIndex, err := db.wal.LastIndex()
			require.NoError(t, err)
			return lastIndex >= writeTx
		}, time.Second, 10*time.Millisecond)
	}

	// Ensure that the WAL is written to after loading from a snapshot. This
	// tests a regression detailed in:
	// https://github.com/polarsignals/frostdb/issues/390
	t.Run("Issue390", func(t *testing.T) {
		dir := setup(t, false)
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
		table, err := db.GetTable(dbAndTableName)
		require.NoError(t, err)
		newWriteAndExpectWALRecord(t, db, table)
	})

	// OutOfDateSnapshots verifies a scenario in which the WAL has records with
	// higher txns than the latest snapshot
	t.Run("OutOfDateSnapshots", func(t *testing.T) {
		dir := setup(t, false)

		snapshotsPath := filepath.Join(dir, "databases", dbAndTableName, "snapshots")
		// Since we snapshot on close, the latest snapshot might not have been
		// written yet.
		var files []os.DirEntry
		require.Eventually(t, func() bool {
			var err error
			files, err = os.ReadDir(snapshotsPath)
			require.NoError(t, err)
			return len(files) == 3
		}, 1*time.Second, 100*time.Millisecond)
		require.NoError(t, os.RemoveAll(filepath.Join(snapshotsPath, files[len(files)-1].Name())))
		files, err := os.ReadDir(snapshotsPath)
		require.NoError(t, err)
		require.Equal(t, 2, len(files))

		c, err := New(
			WithLogger(newTestLogger(t)),
			WithStoragePath(dir),
			WithWAL(),
			WithSnapshotTriggerSize(1),
		)
		require.NoError(t, err)
		defer c.Close()
	})

	// WithBucket ensures normal behavior of recovery in case of graceful
	// shutdown of a column store with bucket storage.
	t.Run("WithBucket", func(t *testing.T) {
		bucket := objstore.NewInMemBucket()
		sinksource := NewDefaultObjstoreBucket(bucket)
		dir := setup(t, true, WithReadWriteStorage(sinksource))

		// The previous wal and snapshots directories should be empty since data
		// is persisted on Close, rendering the directories useless.
		databasesDir := filepath.Join(dir, "databases")
		entries, err := os.ReadDir(databasesDir)
		require.NoError(t, err)
		for _, e := range entries {
			dbEntries, err := os.ReadDir(filepath.Join(databasesDir, e.Name()))
			require.NoError(t, err)
			if len(dbEntries) > 0 {
				entryNames := make([]string, 0, len(dbEntries))
				for _, e := range dbEntries {
					entryNames = append(entryNames, e.Name())
				}
				t.Fatalf("expected an empty dir but found the following entries: %v", entryNames)
			}
		}

		c, err := New(
			WithLogger(newTestLogger(t)),
			WithStoragePath(dir),
			WithWAL(),
			WithSnapshotTriggerSize(1),
			WithReadWriteStorage(sinksource),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, dbAndTableName)
		require.NoError(t, err)
		table, err := db.Table(dbAndTableName, NewTableConfig(dynparquet.SampleDefinition()))
		require.NoError(t, err)
		newWriteAndExpectWALRecord(t, db, table)
	})

	// SnapshotOnRecovery verifies that a snapshot is taken on recovery if the
	// WAL indicates that a block was rotated but no snapshot was taken.
	t.Run("SnapshotOnRecovery", func(t *testing.T) {
		dir := setup(t, false)
		c, err := New(
			WithLogger(newTestLogger(t)),
			WithStoragePath(dir),
			WithWAL(),
			// This option will disable snapshots on block rotation.
			WithSnapshotTriggerSize(0),
		)
		require.NoError(t, err)

		snapshotsPath := filepath.Join(dir, "databases", dbAndTableName, "snapshots")
		snapshots, err := os.ReadDir(snapshotsPath)
		require.NoError(t, err)

		seenSnapshots := make(map[string]struct{})
		for _, s := range snapshots {
			seenSnapshots[s.Name()] = struct{}{}
		}

		db, err := c.DB(ctx, dbAndTableName)
		require.NoError(t, err)
		table, err := db.GetTable(dbAndTableName)
		require.NoError(t, err)

		require.NoError(t, table.RotateBlock(ctx, table.ActiveBlock(), false))

		rec, err := dynparquet.NewTestSamples().ToRecord()
		require.NoError(t, err)

		insertTx, err := table.InsertRecord(ctx, rec)
		require.NoError(t, err)

		// RotateBlock again, this should log a couple of persisted block WAL
		// entries.
		require.NoError(t, table.RotateBlock(ctx, table.ActiveBlock(), false))
		require.NoError(t, c.Close())

		c, err = New(
			WithLogger(newTestLogger(t)),
			WithStoragePath(dir),
			WithWAL(),
			// Enable snapshots.
			WithSnapshotTriggerSize(1),
		)
		require.NoError(t, err)
		defer c.Close()

		snapshots, err = os.ReadDir(snapshotsPath)
		require.NoError(t, err)

		for _, s := range snapshots {
			tx, err := getTxFromSnapshotFileName(s.Name())
			require.NoError(t, err)
			require.GreaterOrEqual(t, tx, insertTx, "expected only snapshots after insert txn")
		}
		db, err = c.DB(ctx, dbAndTableName)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			numBlockPersists := 0
			require.NoError(t, db.wal.Replay(0, func(tx uint64, entry *walpb.Record) error {
				if _, ok := entry.Entry.EntryType.(*walpb.Entry_TableBlockPersisted_); ok {
					numBlockPersists++
				}
				return nil
			}))
			return numBlockPersists <= 1
		}, 1*time.Second, 10*time.Millisecond,
			"expected at most one block persist entry; the others should have been snapshot and truncated",
		)
	})
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
	require.Equal(t, uint64(10), table.config.Load().RowGroupSize)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
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

	table, err = db.GetTable("test")
	require.NoError(t, err)
	require.Equal(t, uint64(10), table.config.Load().RowGroupSize)
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
	r, err := samples.ToRecord()
	require.NoError(t, err)
	writeTx, err := table.InsertRecord(ctx, r)
	require.NoError(t, err)

	require.NoError(t, table.RotateBlock(ctx, table.ActiveBlock(), false))
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

func Test_DB_Limiter(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	c, err := New(
		WithLogger(newTestLogger(t)),
	)
	defer func() {
		_ = c.Close()
	}()
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	r, err := samples.ToRecord()
	require.NoError(t, err)
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	for i := 0; i < 1024; i++ {
		t.Run(fmt.Sprintf("limit-%v", i), func(t *testing.T) {
			debug := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer debug.AssertSize(t, 0)
			pool := query.NewLimitAllocator(int64(i), debug)
			engine := query.NewEngine(pool, db.TableProvider())
			err = engine.ScanTable("test").
				Filter(
					logicalplan.And(
						logicalplan.Col("labels.namespace").Eq(logicalplan.Literal("default")),
					),
				).
				Aggregate(
					[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
					[]logicalplan.Expr{logicalplan.Col("labels.namespace")},
				).
				Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
					return nil
				})
		})
	}
}

// DropStorage ensures that a database can continue on after drop storage is called.
func Test_DB_DropStorage(t *testing.T) {
	logger := newTestLogger(t)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	dir := t.TempDir()

	c, err := New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithActiveMemorySize(1*MiB),
	)
	defer func() {
		_ = c.Close()
	}()
	require.NoError(t, err)
	const dbName = "test"
	db, err := c.DB(context.Background(), dbName)
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		defer r.Release()
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	countRows := func(expected int) {
		rows := 0
		engine := query.NewEngine(mem, db.TableProvider())
		err = engine.ScanTable("test").
			Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
				rows += int(r.NumRows())
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, expected, rows)
	}
	countRows(300)

	level.Debug(logger).Log("msg", "dropping storage")
	require.NoError(t, c.DropDB(dbName))

	// Open a new store against the dropped storage, and expect empty db
	c, err = New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(dir),
		WithActiveMemorySize(1*MiB),
	)
	defer func() {
		_ = c.Close()
	}()
	require.NoError(t, err)
	level.Debug(logger).Log("msg", "opening new db")
	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)
	_, err = db.Table("test", config)
	require.NoError(t, err)
	countRows(0)
}

func Test_DB_EngineInMemory(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	dir := t.TempDir()
	bucket := objstore.NewInMemBucket()

	sinksource := NewDefaultObjstoreBucket(bucket)

	c, err := New(
		WithLogger(logger),
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

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.NoError(t, c.Close())

	c, err = New(
		WithLogger(logger),
		WithStoragePath(dir),
		WithReadWriteStorage(sinksource),
		WithActiveMemorySize(100*1024),
	)
	require.NoError(t, err)
	defer c.Close()

	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)

	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	engine := query.NewEngine(pool, db.TableProvider(), query.WithPhysicalplanOptions(physicalplan.WithInMemoryOnly()))
	err = engine.ScanTable("test").
		Aggregate(
			[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
			[]logicalplan.Expr{logicalplan.Col("labels.namespace")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			t.FailNow() // should not be called
			return nil
		})
	require.NoError(t, err)

	engine = query.NewEngine(pool, db.TableProvider())
	err = engine.ScanTable("test").
		Aggregate(
			[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
			[]logicalplan.Expr{logicalplan.Col("labels.namespace")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			require.Equal(t, int64(2), r.NumRows())
			return nil
		})
	require.NoError(t, err)
}

func Test_DB_SnapshotOnClose(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)
	dir := t.TempDir()

	c, err := New(
		WithLogger(logger),
		WithStoragePath(dir),
		WithWAL(),
		WithActiveMemorySize(1*GiB),
		WithSnapshotTriggerSize(1*GiB),
		WithManualBlockRotation(),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.NoError(t, c.Close())

	// Check that we have a snapshot
	found := false
	require.NoError(t, filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
		if filepath.Ext(path) == ".fdbs" {
			found = true
		}
		return nil
	}))
	require.True(t, found)
}

func Test_DB_All(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(context.Background(), t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(t.Name())
	table, err := db.Table(t.Name(), config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	r, err := samples.ToRecord()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()
	engine := query.NewEngine(pool, db.TableProvider())
	err = engine.ScanTable(t.Name()).
		Project(logicalplan.All()).
		Filter(logicalplan.Col("timestamp").GtEq(logicalplan.Literal(2))).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			require.Equal(t, int64(2), r.NumRows())
			require.Equal(t, int64(8), r.NumCols())
			return nil
		})
	require.NoError(t, err)
}

func Test_DB_PrehashedStorage(t *testing.T) {
	config := NewTableConfig(
		dynparquet.PrehashedSampleDefinition(),
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
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	r, err := samples.ToRecord()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	// Gracefully close the db to persist blocks
	c.Close()

	c, err = New(
		WithLogger(logger),
		WithReadWriteStorage(sinksource),
	)
	require.NoError(t, err)
	defer c.Close()

	db, err = c.DB(context.Background(), t.Name())
	require.NoError(t, err)
	table, err = db.Table(t.Name(), config)
	require.NoError(t, err)

	// Read the raw data back and expect prehashed columns to be returned
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer allocator.AssertSize(t, 0)
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		return table.Iterator(
			ctx,
			tx,
			allocator,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				require.Equal(t, int64(3), ar.NumRows())
				require.Equal(t, int64(13), ar.NumCols())
				return nil
			}},
		)
	})
	require.NoError(t, err)
}

// TestDBConcurrentOpen verifies that concurrent calls to open a DB do not
// result in a panic (most likely due to duplicate metrics registration).
func TestDBConcurrentOpen(t *testing.T) {
	const (
		concurrency = 16
		dbName      = "test"
	)

	bucket := objstore.NewInMemBucket()
	sinksource := NewDefaultObjstoreBucket(bucket)
	logger := newTestLogger(t)
	tempDir := t.TempDir()

	c, err := New(
		WithLogger(logger),
		WithReadWriteStorage(sinksource),
		WithWAL(),
		WithStoragePath(tempDir),
	)
	require.NoError(t, err)
	defer c.Close()

	var errg errgroup.Group
	for i := 0; i < concurrency; i++ {
		errg.Go(func() error {
			return recovery.Do(func() error {
				_, err := c.DB(context.Background(), dbName)
				return err
			})()
		})
	}
	require.NoError(t, errg.Wait())
}

func Test_DB_WithParquetDiskCompaction(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	cfg := DefaultIndexConfig()
	cfg[0].Type = index.CompactionTypeParquetDisk // Create disk compaction
	cfg[1].Type = index.CompactionTypeParquetDisk
	c, err := New(
		WithLogger(logger),
		WithIndexConfig(cfg),
		WithWAL(),
		WithStoragePath(t.TempDir()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.NoError(t, table.EnsureCompaction())

	// Ensure that disk compacted data can be recovered
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	rows := int64(0)
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		return table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				rows += ar.NumRows()
				return nil
			}},
		)
	})
	require.NoError(t, err)
	require.Equal(t, int64(300), rows)
}

func Test_DB_PersistentDiskCompaction(t *testing.T) {
	t.Parallel()
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)
	logger := newTestLogger(t)

	type write struct {
		n                int
		ensureCompaction bool
		op               func(table *Table, dir string)
	}

	defaultWrites := []write{
		{
			n:                100,
			ensureCompaction: true,
		},
		{
			n:                100,
			ensureCompaction: true,
		},
		{
			n:                100,
			ensureCompaction: true,
		},
		{
			n: 100,
		},
	}

	tests := map[string]struct {
		beforeReplay func(table *Table, dir string)
		beforeClose  func(table *Table, dir string)
		lvl2Parts    int
		lvl1Parts    int
		finalRows    int64
		writes       []write
	}{
		"happy path": {
			beforeReplay: func(_ *Table, _ string) {},
			beforeClose:  func(_ *Table, _ string) {},
			lvl2Parts:    3,
			lvl1Parts:    1,
			finalRows:    1200,
			writes:       defaultWrites,
		},
		"corrupted L1": {
			beforeClose: func(table *Table, dir string) {},
			lvl2Parts:   3,
			lvl1Parts:   1,
			finalRows:   1200,
			writes:      defaultWrites,
			beforeReplay: func(table *Table, dir string) {
				// Corrupt the LSM file; this should trigger a recovery from the WAL
				l1Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L1")
				levelFile := filepath.Join(l1Dir, fmt.Sprintf("00000000000000000000%s", index.IndexFileExtension))
				info, err := os.Stat(levelFile)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(levelFile, info.Size()-1)) // truncate the last byte to pretend the write didn't finish
			},
		},
		"corrupted L2": {
			beforeClose: func(table *Table, dir string) {},
			lvl2Parts:   4,
			lvl1Parts:   0,
			finalRows:   1200,
			beforeReplay: func(table *Table, dir string) {
				// Corrupt the LSM file; this should trigger a recovery from the WAL
				l2Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L2")
				levelFile := filepath.Join(l2Dir, fmt.Sprintf("00000000000000000000%s", index.IndexFileExtension))
				info, err := os.Stat(levelFile)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(levelFile, info.Size()-1)) // truncate the last byte to pretend the write didn't finish
			},
			writes: []write{
				{
					n:                100,
					ensureCompaction: true,
				},
				{
					n:                100,
					ensureCompaction: true,
				},
				{
					n:                100,
					ensureCompaction: true,
				},
				{
					n:                100,
					ensureCompaction: true,
				},
			},
		},
		"L1 cleanup failure with corruption": { // Compaction from L1->L2 happens, but L1 is not cleaned up and L2 gets corrupted
			beforeClose: func(table *Table, dir string) {},
			lvl2Parts:   1,
			lvl1Parts:   0,
			finalRows:   300,
			beforeReplay: func(table *Table, dir string) {
				// Copy the L2 file back to L1 to simulate that it was not cleaned up
				l2Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L2")
				l2File := filepath.Join(l2Dir, fmt.Sprintf("00000000000000000000%s", index.IndexFileExtension))
				l2, err := os.Open(l2File)
				require.NoError(t, err)

				l1Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L1")
				l1File := filepath.Join(l1Dir, fmt.Sprintf("00000000000000000000%s", index.IndexFileExtension))
				l1, err := os.Create(l1File)
				require.NoError(t, err)

				_, err = io.Copy(l1, l2)
				require.NoError(t, err)
				require.NoError(t, l2.Close())
				require.NoError(t, l1.Close())

				// Corrupt the L2 file
				info, err := os.Stat(l2File)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(l2File, info.Size()-1)) // truncate the last byte to pretend the write didn't finish
			},
			writes: []write{
				{
					n:                100,
					ensureCompaction: true,
				},
			},
		},
		"L1 cleanup failure": { // Compaction from L1->L2 happens, but L1 is not cleaned up
			beforeClose: func(table *Table, dir string) {},
			lvl2Parts:   1,
			lvl1Parts:   0,
			finalRows:   300,
			beforeReplay: func(table *Table, dir string) {
				// Copy the L2 file back to L1 to simulate that it was not cleaned up
				l2Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L2")
				l2File := filepath.Join(l2Dir, fmt.Sprintf("00000000000000000000%s", index.IndexFileExtension))
				l2, err := os.Open(l2File)
				require.NoError(t, err)

				l1Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L1")
				l1File := filepath.Join(l1Dir, fmt.Sprintf("00000000000000000000%s", index.IndexFileExtension))
				l1, err := os.Create(l1File)
				require.NoError(t, err)

				_, err = io.Copy(l1, l2)
				require.NoError(t, err)
				require.NoError(t, l2.Close())
				require.NoError(t, l1.Close())
			},
			writes: []write{
				{
					n:                100,
					ensureCompaction: true,
				},
			},
		},
		"L1 cleanup failure with corruption after snapshot": { // Snapshot happens after L1 compaction; Then compaction happens from L1->L2, but L1 is not cleaned up and L2 gets corrupted
			beforeClose: func(table *Table, dir string) {},
			lvl2Parts:   1,
			lvl1Parts:   0,
			finalRows:   303,
			beforeReplay: func(table *Table, dir string) {
				// Move the save files back into L1 to simulate an unssuccessful cleanup after L1 compaction
				l1Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L1")
				saveDir := filepath.Join(table.db.indexDir(), "save")
				require.NoError(t, filepath.WalkDir(saveDir, func(path string, info fs.DirEntry, err error) error {
					if filepath.Ext(path) == index.IndexFileExtension {
						// Move the save file back to the original file
						sv, err := os.Open(path)
						require.NoError(t, err)
						defer sv.Close()

						lvl, err := os.Create(filepath.Join(l1Dir, filepath.Base(path)))
						require.NoError(t, err)
						defer lvl.Close()

						_, err = io.Copy(lvl, sv)
						require.NoError(t, err)
					}
					return nil
				}))

				// Corrupt the L2 file to simulate that the failure occurred when writing the L2 file
				l2Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L2")
				l2File := filepath.Join(l2Dir, fmt.Sprintf("00000000000000000000%s", index.IndexFileExtension))
				info, err := os.Stat(l2File)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(l2File, info.Size()-1)) // truncate the last byte to pretend the write didn't finish
			},
			writes: []write{
				{ // Get to L1 compaction
					n: 100,
					op: func(table *Table, dir string) {
						require.Eventually(t, func() bool {
							return table.active.index.LevelSize(index.L1) != 0
						}, time.Second, time.Millisecond*10)
					},
				},
				{ // trigger a snapshot (leaving the only valid data in L1)
					op: func(table *Table, dir string) {
						tx := table.db.highWatermark.Load()
						require.NoError(t, table.db.snapshotAtTX(context.Background(), tx, table.db.snapshotWriter(tx)))
					},
				},
				{
					op: func(table *Table, dir string) { // save the L1 files to restore them
						l1Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L1")
						saveDir := filepath.Join(dir, "save")
						require.NoError(t, os.MkdirAll(saveDir, 0o755)) // Create a directory to save the files
						require.NoError(t, filepath.WalkDir(l1Dir, func(path string, info fs.DirEntry, err error) error {
							if filepath.Ext(path) == index.IndexFileExtension {
								lvl, err := os.Open(path)
								require.NoError(t, err)
								defer lvl.Close()

								sv, err := os.Create(filepath.Join(saveDir, filepath.Base(path)))
								require.NoError(t, err)
								defer sv.Close()

								_, err = io.Copy(sv, lvl)
								require.NoError(t, err)
							}
							return nil
						}))
					},
				},
				{ // Compact L1->L2
					n:                1, // New write to "trigger" the compaction
					ensureCompaction: true,
				},
			},
		},
		"snapshot": {
			beforeReplay: func(_ *Table, _ string) {},
			beforeClose: func(table *Table, dir string) {
				// trigger a snapshot
				success := false
				table.db.snapshot(context.Background(), false, func() {
					success = true
				})
				require.True(t, success)

				// Write more to the table and trigger a compaction
				samples := dynparquet.NewTestSamples()
				for j := 0; j < 100; j++ {
					r, err := samples.ToRecord()
					require.NoError(t, err)
					_, err = table.InsertRecord(context.Background(), r)
					require.NoError(t, err)
				}
				require.Eventually(t, func() bool {
					return table.active.index.LevelSize(index.L1) != 0
				}, time.Second, time.Millisecond*10)
			},
			lvl2Parts: 3,
			lvl1Parts: 2,
			finalRows: 1500,
			writes:    defaultWrites,
		},
		"corruption after snapshot": {
			beforeReplay: func(table *Table, dir string) {
				l1Dir := filepath.Join(table.db.indexDir(), "test", table.active.ulid.String(), "L1")
				levelFile := filepath.Join(l1Dir, fmt.Sprintf("00000000000000000001%v", index.IndexFileExtension))
				info, err := os.Stat(levelFile)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(levelFile, info.Size()-1)) // truncate the last byte to pretend the write didn't finish
			},
			beforeClose: func(table *Table, dir string) {
				// trigger a snapshot
				tx := table.db.highWatermark.Load()
				require.NoError(t, table.db.snapshotAtTX(context.Background(), tx, table.db.snapshotWriter(tx)))

				// Write more to the table and trigger a compaction
				samples := dynparquet.NewTestSamples()
				for j := 0; j < 100; j++ {
					r, err := samples.ToRecord()
					require.NoError(t, err)
					_, err = table.InsertRecord(context.Background(), r)
					require.NoError(t, err)
				}
				require.Eventually(t, func() bool {
					return table.active.index.LevelSize(index.L1) != 0
				}, time.Second, time.Millisecond*10)
			},
			lvl2Parts: 3,
			lvl1Parts: 2,
			finalRows: 1500,
			writes:    defaultWrites,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cfg := []*index.LevelConfig{
				{Level: index.L0, MaxSize: 25700, Type: index.CompactionTypeParquetDisk},
				{Level: index.L1, MaxSize: 1024 * 1024 * 128, Type: index.CompactionTypeParquetDisk},
				{Level: index.L2, MaxSize: 1024 * 1024 * 512},
			}
			c, err := New(
				WithLogger(logger),
				WithIndexConfig(cfg),
				WithStoragePath(dir),
				WithWAL(),
			)
			t.Cleanup(func() {
				require.NoError(t, c.Close())
			})
			require.NoError(t, err)
			db, err := c.DB(context.Background(), "test")
			require.NoError(t, err)
			table, err := db.Table("test", config)
			require.NoError(t, err)

			samples := dynparquet.NewTestSamples()

			ctx := context.Background()
			insertedRows := int64(0)
			for _, w := range test.writes {
				for j := 0; j < w.n; j++ {
					r, err := samples.ToRecord()
					require.NoError(t, err)
					_, err = table.InsertRecord(ctx, r)
					insertedRows += r.NumRows()
					require.NoError(t, err)
				}
				if w.ensureCompaction {
					require.NoError(t, table.EnsureCompaction())
				}
				if w.op != nil {
					w.op(table, db.indexDir())
				}
			}

			if test.lvl1Parts > 0 {
				require.Eventually(t, func() bool {
					return table.active.index.LevelSize(index.L1) != 0
				}, 30*time.Second, time.Millisecond*10)
			}

			rows := func(db *DB) int64 {
				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer pool.AssertSize(t, 0)
				rows := int64(0)
				engine := query.NewEngine(pool, db.TableProvider())
				err = engine.ScanTable("test").
					Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
						rows += r.NumRows()
						return nil
					})
				require.NoError(t, err)
				return rows
			}

			// Ensure that disk compacted data can be recovered
			require.Equal(t, insertedRows, rows(db))

			test.beforeClose(table, dir)

			// Close the database
			require.NoError(t, c.Close())

			// Run the beforeReplay hook
			test.beforeReplay(table, dir)

			// Reopen database; expect it to recover from the LSM files and WAL
			c, err = New(
				WithLogger(logger),
				WithIndexConfig(cfg),
				WithStoragePath(dir),
				WithWAL(),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, c.Close())
			})
			db, err = c.DB(context.Background(), "test")
			require.NoError(t, err)
			table, err = db.Table("test", config)
			require.NoError(t, err)

			// Check the final row count
			require.Equal(t, test.finalRows, rows(db))

			// Now write more data after a recovery into the levels and ensure that it can be read back.

			// L1 writes
			l1Before := table.active.index.LevelSize(index.L1)
			for j := 0; j < 100; j++ {
				r, err := samples.ToRecord()
				require.NoError(t, err)
				_, err = table.InsertRecord(ctx, r)
				require.NoError(t, err)
			}
			require.Eventually(t, func() bool {
				return table.active.index.LevelSize(index.L1) != l1Before
			}, 30*time.Second, time.Millisecond*10)
			require.Equal(t, test.finalRows+300, rows(db))

			// Move that data to L2
			require.NoError(t, table.EnsureCompaction())
			require.Equal(t, test.finalRows+300, rows(db))
		})
	}
}

// Ensure data integrity when a block is rotated.
func Test_DB_PersistentDiskCompaction_BlockRotation(t *testing.T) {
	t.Parallel()
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)
	logger := newTestLogger(t)
	bucket := objstore.NewInMemBucket()
	sinksource := NewDefaultObjstoreBucket(bucket)

	dir := t.TempDir()
	cfg := []*index.LevelConfig{
		{Level: index.L0, MaxSize: 25700, Type: index.CompactionTypeParquetDisk},
		{Level: index.L1, MaxSize: 1024 * 1024 * 128, Type: index.CompactionTypeParquetDisk},
		{Level: index.L2, MaxSize: 1024 * 1024 * 512},
	}
	c, err := New(
		WithLogger(logger),
		WithIndexConfig(cfg),
		WithStoragePath(dir),
		WithWAL(),
		WithManualBlockRotation(),
		WithReadWriteStorage(sinksource),
	)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()

	ctx := context.Background()
	compactions := 3 // 3 compacted parts in L2 file
	for i := 0; i < compactions; i++ {
		for j := 0; j < 100; j++ {
			r, err := samples.ToRecord()
			require.NoError(t, err)
			_, err = table.InsertRecord(ctx, r)
			require.NoError(t, err)
		}
		require.NoError(t, table.EnsureCompaction())
	}

	// Get data in L1
	for j := 0; j < 100; j++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return table.active.index.LevelSize(index.L1) != 0
	}, time.Second, time.Millisecond*10)

	validateRows := func(expected int64) {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		rows := int64(0)
		engine := query.NewEngine(pool, db.TableProvider())
		err = engine.ScanTable("test").
			Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
				rows += r.NumRows()
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, expected, rows)
	}

	validateRows(1200)

	// Rotate block
	require.NoError(t, table.RotateBlock(context.Background(), table.ActiveBlock(), false))

	validateRows(1200)

	for i := 0; i < compactions; i++ {
		for j := 0; j < 100; j++ {
			r, err := samples.ToRecord()
			require.NoError(t, err)
			_, err = table.InsertRecord(ctx, r)
			require.NoError(t, err)
		}
		require.NoError(t, table.EnsureCompaction())
	}

	// Get data in L1
	for j := 0; j < 100; j++ {
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return table.active.index.LevelSize(index.L1) != 0
	}, time.Second, time.Millisecond*10)

	validateRows(2400)
}

func Test_DB_PersistentDiskCompaction_NonOverlappingCompaction(t *testing.T) {
	t.Parallel()
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)
	logger := newTestLogger(t)

	dir := t.TempDir()
	cfg := []*index.LevelConfig{
		{Level: index.L0, MaxSize: 336, Type: index.CompactionTypeParquetDisk},
		{Level: index.L1, MaxSize: 1024 * 1024 * 128, Type: index.CompactionTypeParquetDisk},
		{Level: index.L2, MaxSize: 1024 * 1024 * 512},
	}
	c, err := New(
		WithLogger(logger),
		WithIndexConfig(cfg),
		WithStoragePath(dir),
		WithWAL(),
		WithManualBlockRotation(),
	)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 3; i++ { // Create non-overlapping samples for compaction
		samples := dynparquet.GenerateTestSamples(1)
		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return table.active.index.LevelSize(index.L1) != 0
	}, time.Second, time.Millisecond*10)

	// Snapshot the file
	success := false
	table.db.snapshot(context.Background(), false, func() {
		success = true
	})
	require.True(t, success)

	// Close the database
	require.NoError(t, c.Close())

	// Recover from the snapshot
	c, err = New(
		WithLogger(logger),
		WithIndexConfig(cfg),
		WithStoragePath(dir),
		WithWAL(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})
	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err = db.Table("test", config)
	require.NoError(t, err)

	validateRows := func(expected int64) {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		rows := int64(0)
		engine := query.NewEngine(pool, db.TableProvider())
		err = engine.ScanTable("test").
			Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
				rows += r.NumRows()
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, expected, rows)
	}

	validateRows(3)
}

func TestDBWatermarkBubbling(t *testing.T) {
	c, err := New()
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	const nTxns = 100
	var wg sync.WaitGroup
	wg.Add(nTxns)
	for i := 0; i < nTxns; i++ {
		_, _, commit := db.begin()
		go func() {
			defer wg.Done()
			commit()
		}()
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return db.HighWatermark() == nTxns
	}, time.Second, 10*time.Millisecond)
}
