package physicalplan

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/stretchr/testify/require"
)

type TestPlan struct {
	finish   func() error
	callback func(ctx context.Context, r arrow.Record) error
}

func (t *TestPlan) Callback(ctx context.Context, r arrow.Record) error {
	if t.callback != nil {
		return t.callback(ctx, r)
	}
	return nil
}
func (t *TestPlan) Finish(ctx context.Context) error {
	if t.finish != nil {
		return t.finish()
	}
	return nil
}
func (t *TestPlan) SetNext(next PhysicalPlan) {}
func (t *TestPlan) Draw() *Diagram            { return nil }
func (t *TestPlan) Close()                    {}

func Test_Sampler(t *testing.T) {
	ctx := context.Background()
	tests := map[string]struct {
		reservoirSize int64
		numRows       int
		recordSize    int
	}{
		"basic single row records": {
			reservoirSize: 10,
			numRows:       100,
			recordSize:    1,
		},
		"basic multi row records": {
			reservoirSize: 10,
			numRows:       100,
			recordSize:    10,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Create a new sampler
			s := NewReservoirSampler(test.reservoirSize)
			called := false
			total := int64(0)
			s.SetNext(&TestPlan{
				callback: func(ctx context.Context, r arrow.Record) error {
					called = true
					total += r.NumRows()
					return nil
				},
			})

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			}, nil)
			bldr := array.NewRecordBuilder(memory.NewGoAllocator(), schema)

			for i := 0; i < test.numRows/test.recordSize; i++ {
				for j := 0; j < test.recordSize; j++ {
					bldr.Field(0).(*array.Int64Builder).Append(int64((i * test.recordSize) + j))
				}
				r := bldr.NewRecord()
				t.Cleanup(r.Release)
				s.Callback(ctx, r)
			}

			s.Finish(ctx)
			require.True(t, called)
			require.Equal(t, test.reservoirSize, total)
		})
	}
}
