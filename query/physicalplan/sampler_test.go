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
	finish   func(ctx context.Context) error
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
		return t.finish(ctx)
	}
	return nil
}

func (t *TestPlan) SetNext(_ PhysicalPlan) {}
func (t *TestPlan) Draw() *Diagram         { return nil }
func (t *TestPlan) Close()                 {}

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
				require.NoError(t, s.Callback(ctx, r))
			}

			require.NoError(t, s.Finish(ctx))
			require.True(t, called)
			require.Equal(t, test.reservoirSize, total)
		})
	}
}

// Test_Sampler_Randomness tests the randomness of the sampler by checking the distribution of the samples.
func Test_Sampler_Randomness(t *testing.T) {
	ctx := context.Background()
	reservoirSize := int64(10)
	numRows := int64(100)
	recordSize := int64(1)
	iterations := int64(10_000)
	bins := make([]int64, numRows)

	// Create a new sampler
	for i := int64(0); i < iterations; i++ {
		s := NewReservoirSampler(reservoirSize)
		s.SetNext(&TestPlan{
			callback: func(ctx context.Context, r arrow.Record) error {
				for _, v := range r.Column(0).(*array.Int64).Int64Values() {
					bins[v]++
				}
				return nil
			},
		})

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		bldr := array.NewRecordBuilder(memory.NewGoAllocator(), schema)

		for i := int64(0); i < numRows/recordSize; i++ {
			for j := int64(0); j < recordSize; j++ {
				bldr.Field(0).(*array.Int64Builder).Append(int64((i * recordSize) + j))
			}
			r := bldr.NewRecord()
			t.Cleanup(r.Release)
			require.NoError(t, s.Callback(ctx, r))
		}

		require.NoError(t, s.Finish(ctx))
	}

	// Any given number has a reservoirSize/numRows or for current settings a 10/100 chance of being selected
	// 10/100 * 10000 iterations = 1000. So we expect each number to be selected roughly 1000 times.
	// Using a tolerance of 10% we expect each number to be selected between 900 and 1100 times.
	tolerance := 0.10
	expectation := float64(reservoirSize) / float64(numRows) * float64(iterations)
	lowerBound := expectation - expectation*tolerance
	upperBound := expectation + expectation*tolerance
	for _, count := range bins {
		require.GreaterOrEqual(t, float64(count), lowerBound)
		require.LessOrEqual(t, float64(count), upperBound)
	}
}
