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
			allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
			t.Cleanup(func() {
				allocator.AssertSize(t, 0)
			})

			// Create a new sampler
			s := NewReservoirSampler(test.reservoirSize, 10_000, allocator)
			called := false
			total := int64(0)
			s.SetNext(&TestPlan{
				callback: func(_ context.Context, r arrow.Record) error {
					called = true
					total += r.NumRows()
					return nil
				},
			})

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			}, nil)
			bldr := array.NewRecordBuilder(allocator, schema)
			t.Cleanup(bldr.Release)

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
			s.Close()
			require.Zero(t, s.sizeInBytes)
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
		allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
		t.Cleanup(func() {
			allocator.AssertSize(t, 0)
		})
		s := NewReservoirSampler(reservoirSize, 10_000, allocator)
		s.SetNext(&TestPlan{
			callback: func(_ context.Context, r arrow.Record) error {
				for _, v := range r.Column(0).(*array.Int64).Int64Values() {
					bins[v]++
				}
				return nil
			},
		})

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		bldr := array.NewRecordBuilder(allocator, schema)
		t.Cleanup(bldr.Release)

		for i := int64(0); i < numRows/recordSize; i++ {
			for j := int64(0); j < recordSize; j++ {
				bldr.Field(0).(*array.Int64Builder).Append(int64((i * recordSize) + j))
			}
			r := bldr.NewRecord()
			t.Cleanup(r.Release)
			require.NoError(t, s.Callback(ctx, r))
		}

		require.NoError(t, s.Finish(ctx))
		s.Close()
		require.Zero(t, s.sizeInBytes)
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

func Benchmark_Sampler(b *testing.B) {
	ctx := context.Background()
	tests := map[string]struct {
		reservoirSize int64
		numRows       int
		recordSize    int
	}{
		"10%_10_000_x10": {
			reservoirSize: 1000,
			numRows:       10_000,
			recordSize:    10,
		},
	}

	for name, test := range tests {
		b.Run(name, func(b *testing.B) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			}, nil)
			bldr := array.NewRecordBuilder(memory.NewGoAllocator(), schema)

			recordCount := test.numRows / test.recordSize
			records := make([]arrow.Record, 0, recordCount)
			b.Cleanup(func() {
				for _, r := range records {
					r.Release()
				}
			})
			for i := 0; i < recordCount; i++ {
				for j := 0; j < test.recordSize; j++ {
					bldr.Field(0).(*array.Int64Builder).Append(int64((i * test.recordSize) + j))
				}
				records = append(records, bldr.NewRecord())
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Create a new sampler
				s := NewReservoirSampler(test.reservoirSize, 10_000, memory.NewGoAllocator())
				total := int64(0)
				s.SetNext(&TestPlan{
					callback: func(_ context.Context, r arrow.Record) error {
						total += r.NumRows()
						return nil
					},
				})

				for _, r := range records {
					require.NoError(b, s.Callback(ctx, r))
				}
				require.NoError(b, s.Finish(ctx))
				require.Equal(b, test.reservoirSize, total)
			}
		})
	}
}

func Test_Sampler_Materialize(t *testing.T) {
	ctx := context.Background()
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() {
		allocator.AssertSize(t, 0)
	})
	s := NewReservoirSampler(10, 10_000, allocator)
	s.SetNext(&TestPlan{})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr := array.NewRecordBuilder(allocator, schema)
	t.Cleanup(bldr.Release)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			bldr.Field(0).(*array.Int64Builder).Append(int64((i * 10) + j))
		}
		r := bldr.NewRecord()
		t.Cleanup(r.Release)
		require.NoError(t, s.Callback(ctx, r))
	}

	// Create a new schema for records
	schema = arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr = array.NewRecordBuilder(allocator, schema)
	t.Cleanup(bldr.Release)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			bldr.Field(0).(*array.Int64Builder).Append(int64((i * 10) + j))
			bldr.Field(1).(*array.Int64Builder).Append(int64((i * 10) + j))
		}
		r := bldr.NewRecord()
		t.Cleanup(r.Release)
		require.NoError(t, s.Callback(ctx, r))
	}

	require.NoError(t, s.materialize(allocator))
	s.Close()
	require.Zero(t, s.sizeInBytes)
}

type AccountingAllocator struct {
	allocator *memory.CheckedAllocator
	maxUsed   int
}

func (a *AccountingAllocator) Allocate(size int) []byte {
	b := a.allocator.Allocate(size)
	if current := a.allocator.CurrentAlloc(); current > a.maxUsed {
		a.maxUsed = current
	}
	return b
}

func (a *AccountingAllocator) Reallocate(size int, data []byte) []byte {
	b := a.allocator.Reallocate(size, data)
	if current := a.allocator.CurrentAlloc(); current > a.maxUsed {
		a.maxUsed = current
	}
	return b
}

func (a *AccountingAllocator) Free(data []byte) {
	a.allocator.Free(data)
	if current := a.allocator.CurrentAlloc(); current > a.maxUsed {
		a.maxUsed = current
	}
}

func Test_Sampler_MaxSizeAllocation(t *testing.T) {
	ctx := context.Background()
	allocator := &AccountingAllocator{
		allocator: memory.NewCheckedAllocator(memory.NewGoAllocator()),
	}
	t.Cleanup(func() {
		require.LessOrEqual(t, allocator.maxUsed, 1024) // Expect the most we allocated was 1024 bytes during materialization
		allocator.allocator.AssertSize(t, 0)
	})
	s := NewReservoirSampler(10, 200, allocator)
	s.SetNext(&TestPlan{})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(bldr.Release)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			bldr.Field(0).(*array.Int64Builder).Append(int64((i * 10) + j))
		}
		r := bldr.NewRecord()
		t.Cleanup(r.Release)
		require.NoError(t, s.Callback(ctx, r))
	}

	// Create a new schema for records
	schema = arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr = array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(bldr.Release)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			bldr.Field(0).(*array.Int64Builder).Append(int64((i * 10) + j))
			bldr.Field(1).(*array.Int64Builder).Append(int64((i * 10) + j))
		}
		r := bldr.NewRecord()
		t.Cleanup(r.Release)
		require.NoError(t, s.Callback(ctx, r))
	}

	s.Close()
	require.Zero(t, s.sizeInBytes)
}
