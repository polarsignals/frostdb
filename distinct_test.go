package frostdb

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestDistinct(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value1"},
			{Name: "label5", Value: "value1"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
			{Name: "label5", Value: "value1"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(context.Background(), buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value1"},
			{Name: "label4", Value: "value4"},
			{Name: "label5", Value: "value1"},
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

	_, err = table.InsertBuffer(context.Background(), buf)
	require.NoError(t, err)

	tests := map[string]struct {
		columns []logicalplan.Expr
		values  [][]string
	}{
		// Empty strings are actually NULL in the DB.
		"label1": {
			columns: logicalplan.Cols("labels.label1"),
			values: [][]string{
				// label1
				{"value1"}, // row 0
				{"value2"}, // row 1
				{"value3"}, // row 2
			},
		},
		"label2": {
			columns: logicalplan.Cols("labels.label2"),
			values: [][]string{
				// label2
				{"value1"}, // row 0
				{"value2"}, // row 1
			},
		},
		"label1,label2": {
			columns: logicalplan.Cols("labels.label1", "labels.label2"),
			values: [][]string{
				// label1, label2
				{"value1", "value1"}, // row
				{"value2", "value2"}, // row
				{"value3", "value1"}, // row
			},
		},
		"label1,label2,label3": {
			columns: logicalplan.Cols("labels.label1", "labels.label2", "labels.label3"),
			values: [][]string{
				// label1, label2, label3
				{"value1", "value1", ""},       // row
				{"value2", "value2", "value3"}, // row
				{"value3", "value1", ""},       // row
			},
		},
		"label1,label2,label4": {
			columns: logicalplan.Cols("labels.label1", "labels.label2", "labels.label4"),
			values: [][]string{
				// label1,label2,label4
				{"value1", "value1", ""},       // row
				{"value2", "value2", ""},       // row
				{"value3", "value1", "value4"}, // row
			},
		},
		"label1,label2,label5": {
			columns: logicalplan.Cols("labels.label1", "labels.label2", "labels.label5"),
			values: [][]string{
				// label1,label2,label5
				{"value1", "value1", "value1"}, // row
				{"value2", "value2", "value1"}, // row
				{"value3", "value1", "value1"}, // row
			},
		},
		"label1,label2,label3,label4": {
			columns: logicalplan.Cols("labels.label1", "labels.label2", "labels.label3", "labels.label4"),
			values: [][]string{
				// label1,label2,label3,label4
				{"value1", "value1", "", ""},       // row
				{"value2", "value2", "value3", ""}, // row
				{"value3", "value1", "", "value4"}, // row
			},
		},
		"labels": {
			columns: []logicalplan.Expr{logicalplan.DynCol("labels")},
			values: [][]string{
				// label1,label2,label3,label4,label5
				{"value1", "value1", "", "", "value1"},       // row
				{"value2", "value2", "value3", "", "value1"}, // row
				{"value3", "value1", "", "value4", "value1"}, // row
			},
		},
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	t.Parallel()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			seenRows := map[string]struct{}{}
			for _, values := range test.values {
				seenRows[strings.Join(values, ",")] = struct{}{}
			}

			err := engine.ScanTable("test").
				Distinct(test.columns...).
				Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
					defer ar.Release()
					require.Equal(t, len(test.values[0]), int(ar.NumCols()))

					for row := 0; row < int(ar.NumRows()); row++ {
						rowValues := make([]string, 0, ar.NumCols())
						for col := 0; col < int(ar.NumCols()); col++ {
							rowValues = append(rowValues, ar.Column(col).(*array.Binary).ValueString(row))
						}
						delete(seenRows, strings.Join(rowValues, ","))
					}

					return nil
				})
			require.NoError(t, err)
			require.Lenf(t, seenRows, 0, "Not all expected rows were seen")
		})
	}
}

// TestDistinctProjectionScanOptimization verifies that the scan layer and a
// projection operator play nicely together in case the scan layer only
// partially optimizes a binary expression.
func TestDistinctPartialScanOptimization(t *testing.T) {
	var (
		ctx    = context.Background()
		config = NewTableConfig(
			dynparquet.NewSampleSchema(),
		)
		logger = newTestLogger(t)
	)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	for _, samples := range []dynparquet.Samples{
		// First set of samples (first row group). This should not be optimized
		// at the scan level because there are two fields with more than one
		// distinct value.
		{
			{
				ExampleType: "value1",
				Timestamp:   0,
				Value:       1,
			},
			{
				ExampleType: "value2",
				Timestamp:   1,
				Value:       1,
			},
		},
		// Second set of samples (second row group). This should be optimized.
		{
			{
				ExampleType: "value2",
				Timestamp:   1,
				Value:       1,
			},
			{
				ExampleType: "value2",
				Timestamp:   1,
				Value:       1,
			},
		},
	} {
		buf, err := samples.ToBuffer(table.Schema())
		require.NoError(t, err)
		_, err = table.InsertBuffer(ctx, buf)
		require.NoError(t, err)
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)
	var seen int64
	require.NoError(t, engine.ScanTable("test").
		Distinct(
			logicalplan.Col("example_type"),
			logicalplan.Col("timestamp"),
			logicalplan.Col("value").Gt(logicalplan.Literal(int64(0))),
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			// t.Log(ar)
			seen += ar.NumRows()
			return nil
		}),
	)
	require.Equal(t, int64(2), seen)
}

func TestDistinctProjectionAlwaysTrue(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
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

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(context.Background(), buf)
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	var r arrow.Record
	err = engine.ScanTable("test").
		Distinct(
			logicalplan.Col("labels.label1"),
			logicalplan.Col("labels.label2"),
			logicalplan.Col("timestamp").Gt(logicalplan.Literal(int64(0))),
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			ar.Retain()
			r = ar

			return nil
		})
	require.NoError(t, err)
	defer r.Release()

	// t.Log(r)
	require.Equal(t, int64(3), r.NumCols())
	require.Equal(t, int64(1), r.NumRows())
}

func TestDistinctProjectionAlwaysFalse(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     0,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     0,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(context.Background(), buf)
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	var r arrow.Record
	err = engine.ScanTable("test").
		Distinct(
			logicalplan.Col("labels.label1"),
			logicalplan.Col("labels.label2"),
			logicalplan.Col("value").Gt(logicalplan.Literal(int64(0))),
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			ar.Retain()
			r = ar

			return nil
		})
	require.NoError(t, err)
	defer r.Release()

	// t.Log(r)
	require.Equal(t, int64(3), r.NumCols())
	require.Equal(t, int64(1), r.NumRows())
}

func TestDistinctProjectionMixedBinaryProjection(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     0,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     0,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(context.Background(), buf)
	require.NoError(t, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	var r arrow.Record
	err = engine.ScanTable("test").
		Distinct(
			logicalplan.Col("labels.label1"),
			logicalplan.Col("labels.label2"),
			logicalplan.Col("value").Gt(logicalplan.Literal(int64(0))),
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			ar.Retain()
			r = ar

			return nil
		})
	require.NoError(t, err)
	defer r.Release()

	// t.Log(r)
	require.Equal(t, int64(3), r.NumCols())
	require.Equal(t, int64(2), r.NumRows())
}

func TestDistinctProjectionAllNull(t *testing.T) {
	// TODO(asubiotto): This test should check the returned results. What should
	// the semantics here be? We're currently ignoring row groups that don't
	// include the physical column distinct expressions operate on.
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     0,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     0,
	}}

	for i := range samples {
		buf, err := samples[i : i+1].ToBuffer(table.Schema())
		require.NoError(t, err)

		_, err = table.InsertBuffer(context.Background(), buf)
		require.NoError(t, err)
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	err = engine.ScanTable("test").
		Distinct(
			logicalplan.Col("labels.label2"),
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			return nil
		})
	require.NoError(t, err)
}
