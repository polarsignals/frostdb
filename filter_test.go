package frostdb

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestFilter(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	c := New(
		nil,
		8192,
		512*1024*1024,
	)
	db, err := c.DB("test")
	require.NoError(t, err)
	table, err := db.Table("test", config, newTestLogger(t))
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

	for i := 0; i < len(samples); i++ {
		buf, err := samples[i : i+1].ToBuffer(table.Schema())
		require.NoError(t, err)

		_, err = table.InsertBuffer(context.Background(), buf)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		filterExpr logicalplan.Expr
		cols       int64
		rows       int64
	}{
		">= int64": {
			filterExpr: logicalplan.Col("timestamp").GTE(logicalplan.Literal(2)),
			cols:       1,
			rows:       2,
		},
		"== string": {
			filterExpr: logicalplan.Col("labels.label4").Eq(logicalplan.Literal("value4")),
			cols:       1,
			rows:       1,
		},
		"regexp and == string": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label1").RegexMatch("value."),
				logicalplan.Col("labels.label2").Eq(logicalplan.Literal("value2")),
			),
			cols: 2,
			rows: 3,
		},
		"regexp missing colum": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label5").RegexMatch(""),
			),
			cols: 0,
			rows: 0,
		},
		"not regexp missing colum": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label5").RegexNotMatch("foo"),
			),
			cols: 0,
			rows: 0,
		},
		"regexp mixed of missing/not missing colum": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label3").RegexMatch("value."),
				logicalplan.Col("labels.label5").RegexMatch(""),
				logicalplan.Col("labels.label2").Eq(logicalplan.Literal("value2")),
			),
			cols: 2,
			rows: 1,
		},
		"=! missing colum": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label5").NotEq(logicalplan.Literal("value4")),
			),
			cols: 0,
			rows: 0,
		},
		"== missing colum": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label5").Eq(logicalplan.Literal("")),
			),
			cols: 0,
			rows: 0,
		},
		"regexp and == string and != string": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label1").RegexMatch("value."),
				logicalplan.Col("labels.label2").Eq(logicalplan.Literal("value2")),
				logicalplan.Col("labels.label1").NotEq(logicalplan.Literal("value3")),
			),
			cols: 2,
			rows: 2,
		},
		"regexp simple match": {
			filterExpr: logicalplan.Col("labels.label1").RegexMatch("value."),
			cols:       1,
			rows:       3,
		},
		"regexp no match": {
			filterExpr: logicalplan.Col("labels.label1").RegexMatch("values."),
			cols:       0,
			rows:       0,
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
			cols := int64(0)
			err := engine.ScanTable("test").
				Filter(test.filterExpr).
				Execute(context.Background(), func(ar arrow.Record) error {
					cols = ar.NumCols()
					rows += ar.NumRows()
					defer ar.Release()

					return nil
				})
			require.NoError(t, err)
			require.Equal(t, test.cols, cols)
			require.Equal(t, test.rows, rows)
		})
	}
}

func Test_Projection(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	c := New(
		nil,
		8192,
		512*1024*1024,
	)
	db, err := c.DB("test")
	require.NoError(t, err)
	table, err := db.Table("test", config, newTestLogger(t))
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

	for i := 0; i < len(samples); i++ {
		buf, err := samples[i : i+1].ToBuffer(table.Schema())
		require.NoError(t, err)

		_, err = table.InsertBuffer(context.Background(), buf)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		filterExpr  logicalplan.Expr
		projections []logicalplan.Expr
		rows        int64
		cols        int64
	}{
		"dynamic projections": {
			filterExpr: logicalplan.And(
				logicalplan.Col("timestamp").GTE(logicalplan.Literal(2)),
			),
			projections: []logicalplan.Expr{logicalplan.DynCol("labels"), logicalplan.Col("timestamp")},
			rows:        2,
			cols:        10,
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
			cols := int64(0)
			err := engine.ScanTable("test").
				Project(test.projections...).
				Filter(test.filterExpr).
				Execute(context.Background(), func(ar arrow.Record) error {
					rows += ar.NumRows()
					cols += ar.NumCols()
					defer ar.Release()

					return nil
				})
			require.NoError(t, err)
			require.Equal(t, test.rows, rows)
			require.Equal(t, test.cols, cols)
		})
	}
}
