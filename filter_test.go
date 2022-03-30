package arcticdb

import (
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query"
	"github.com/polarsignals/arcticdb/query/logicalplan"
)

func TestFilter(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
		8192,
	)

	c := New(nil)
	db := c.DB("test")
	table := db.Table("test", config, newTestLogger(t))

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

		err = table.Insert(buf)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		filterExpr logicalplan.Expr
		rows       int64
	}{
		">= int64": {
			filterExpr: logicalplan.Col("timestamp").GTE(logicalplan.Literal(2)),
			rows:       2,
		},
		"== string": {
			filterExpr: logicalplan.Col("labels.label4").Eq(logicalplan.Literal("value4")),
			rows:       1,
		},
		"regexp and == string": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label1").RegexMatch("value."),
				logicalplan.Col("labels.label2").Eq(logicalplan.Literal("value2")),
			),
			rows: 3,
		},
		"regexp and == string and != string": {
			filterExpr: logicalplan.And(
				logicalplan.Col("labels.label1").RegexMatch("value."),
				logicalplan.Col("labels.label2").Eq(logicalplan.Literal("value2")),
				logicalplan.Col("labels.label1").NotEq(logicalplan.Literal("value3")),
			),
			rows: 2,
		},
		"regexp simple match": {
			filterExpr: logicalplan.Col("labels.label1").RegexMatch("value."),
			rows:       3,
		},
		"regexp no match": {
			filterExpr: logicalplan.Col("labels.label1").RegexMatch("values."),
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
			err := engine.ScanTable("test").
				Filter(test.filterExpr).
				Execute(func(ar arrow.Record) error {
					rows += ar.NumRows()
					defer ar.Release()

					return nil
				})
			require.NoError(t, err)
			require.Equal(t, test.rows, rows)
		})
	}
}
