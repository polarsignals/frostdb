package dynparquet

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBuild(t *testing.T) {

	t.Run("NewBuild", func(t *testing.T) {
		b := NewBuild[Sample](memory.DefaultAllocator)
		defer b.Release()

		ptr := NewBuild[*Sample](memory.DefaultAllocator)
		defer ptr.Release()
	})

	t.Run("Schema", func(t *testing.T) {
		b := NewBuild[Sample](memory.DefaultAllocator)
		defer b.Release()
		got := b.Schema("test")
		want := SampleDefinition()
		require.True(t, proto.Equal(want, got))
	})

	t.Run("NewRecord", func(t *testing.T) {
		b := NewBuild[Sample](memory.DefaultAllocator)
		defer b.Release()
		samples := NewTestSamples()
		b.Append(samples...)
		r := b.NewRecord()
		require.Equal(t, int64(len(samples)), r.NumRows())
		want := `[{"example_type":"cpu","labels.container":null,"labels.namespace":null,"labels.node":"test3","labels.pod":null,"stacktrace":"AAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAE=","timestamp":2,"value":5}
,{"example_type":"cpu","labels.container":null,"labels.namespace":"default","labels.node":null,"labels.pod":"test1","stacktrace":"AAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAE=","timestamp":2,"value":3}
,{"example_type":"cpu","labels.container":"test2","labels.namespace":"default","labels.node":null,"labels.pod":null,"stacktrace":"AAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAE=","timestamp":2,"value":3}
]`
		got, err := r.MarshalJSON()
		require.Nil(t, err)
		require.JSONEq(t, want, string(got))
	})
}

func BenchmarkBuild_Append_Then_NewRecord(b *testing.B) {
	// The way the record builder is used consist of calling Append followed by
	// NewRecord
	//
	// They are separate methods because we can't ignore benefits of buffering.
	build := NewBuild[Sample](memory.DefaultAllocator)
	defer build.Release()
	samples := NewTestSamples()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		build.Append(samples...)
		r := build.NewRecord()
		r.Release()
	}
}
