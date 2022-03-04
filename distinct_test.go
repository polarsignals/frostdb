package columnstore

import (
	"testing"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/google/uuid"
	"github.com/parca-dev/parca/pkg/columnstore/dynparquet"
	"github.com/stretchr/testify/require"
)

func TestDistinct(t *testing.T) {
	table := basicTable(t, 2^12)

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

	err = table.Insert(buf)
	require.NoError(t, err)

	tests := map[string]struct {
		columns []ArrowFieldMatcher
		rows    int64
	}{
		"label1": {
			columns: []ArrowFieldMatcher{
				DynamicColumnRef("labels").Column("label1").ArrowFieldMatcher(),
			},
			rows: 3,
		},
		"label2": {
			columns: []ArrowFieldMatcher{
				DynamicColumnRef("labels").Column("label2").ArrowFieldMatcher(),
			},
			rows: 1,
		},
		"label1,label2": {
			columns: []ArrowFieldMatcher{
				DynamicColumnRef("labels").Column("label1").ArrowFieldMatcher(),
				DynamicColumnRef("labels").Column("label2").ArrowFieldMatcher(),
			},
			rows: 3,
		},
		"label1,label2,label3": {
			columns: []ArrowFieldMatcher{
				DynamicColumnRef("labels").Column("label1").ArrowFieldMatcher(),
				DynamicColumnRef("labels").Column("label2").ArrowFieldMatcher(),
				DynamicColumnRef("labels").Column("label3").ArrowFieldMatcher(),
			},
			rows: 3,
		},
		"label1,label2,label4": {
			columns: []ArrowFieldMatcher{
				DynamicColumnRef("labels").Column("label1").ArrowFieldMatcher(),
				DynamicColumnRef("labels").Column("label2").ArrowFieldMatcher(),
				DynamicColumnRef("labels").Column("label4").ArrowFieldMatcher(),
			},
			rows: 3,
		},
	}

	pool := memory.NewGoAllocator()
	t.Parallel()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			rows := int64(0)
			err = table.Iterator(pool, Distinct(pool, test.columns, func(ar arrow.Record) error {
				rows += ar.NumRows()
				defer ar.Release()

				return nil
			}).Callback)
			require.NoError(t, err)
			require.Equal(t, test.rows, rows)
		})
	}
}
