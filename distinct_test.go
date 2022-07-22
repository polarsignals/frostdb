package frostdb

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestDistinct(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	reg := prometheus.NewRegistry()
	logger := newTestLogger(t)

	c, err := New(
		logger,
		reg,
	)
	require.NoError(t, err)
	db, err := c.DB("test")
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

	_, err = table.InsertBuffer(context.Background(), buf)
	require.NoError(t, err)

	tests := map[string]struct {
		columns []logicalplan.Expr
		rows    int64
	}{
		"label1": {
			columns: logicalplan.Cols("labels.label1"),
			rows:    3,
		},
		"label2": {
			columns: logicalplan.Cols("labels.label2"),
			rows:    1,
		},
		"label1,label2": {
			columns: logicalplan.Cols("labels.label1", "labels.label2"),
			rows:    3,
		},
		"label1,label2,label3": {
			columns: logicalplan.Cols("labels.label1", "labels.label2", "labels.label3"),
			rows:    3,
		},
		"label1,label2,label4": {
			columns: logicalplan.Cols("labels.label1", "labels.label2", "labels.label4"),
			rows:    3,
		},
		"label1,label2,label3, label4": {
			columns: logicalplan.Cols("labels.label1", "labels.label2", "labels.label3", "labels.label4"),
			rows:    3,
		},
		"labels": {
			columns: []logicalplan.Expr{logicalplan.DynCol("labels")},
			rows:    3,
		},
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	t.Parallel()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			rows := int64(0)
			err := engine.ScanTable("test").
				Distinct(test.columns...).
				Execute(context.Background(), func(ar arrow.Record) error {
					rows += ar.NumRows()
					defer ar.Release()

					return nil
				})
			require.NoError(t, err)
			require.Equal(t, test.rows, rows)
		})
	}
}

func TestDistinctProjection(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	reg := prometheus.NewRegistry()
	logger := newTestLogger(t)

	c, err := New(
		logger,
		reg,
	)
	require.NoError(t, err)
	db, err := c.DB("test")
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
			logicalplan.Col("timestamp").GT(logicalplan.Literal(1)),
		).
		Execute(context.Background(), func(ar arrow.Record) error {
			ar.Retain()
			r = ar

			return nil
		})
	require.NoError(t, err)
	defer r.Release()

	t.Log(r)
	require.Equal(t, int64(3), r.NumCols())
}
