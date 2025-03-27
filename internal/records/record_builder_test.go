package records_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/polarsignals/frostdb/internal/records"
	"github.com/polarsignals/frostdb/samples"
)

func TestBuild(t *testing.T) {
	t.Run("NewBuild", func(_ *testing.T) {
		b := records.NewBuild[samples.Sample](memory.DefaultAllocator)
		defer b.Release()

		ptr := records.NewBuild[*samples.Sample](memory.DefaultAllocator)
		defer ptr.Release()
	})

	t.Run("Schema", func(t *testing.T) {
		b := records.NewBuild[samples.Sample](memory.DefaultAllocator)
		defer b.Release()
		got := b.Schema("test")
		want := samples.SampleDefinition()
		require.True(t, proto.Equal(want, got))
	})

	t.Run("NewRecord", func(t *testing.T) {
		b := records.NewBuild[samples.Sample](memory.DefaultAllocator)
		defer b.Release()
		samples := samples.NewTestSamples()
		err := b.Append(samples...)
		require.Nil(t, err)
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

	t.Run("Repeated", func(t *testing.T) {
		type Repeated struct {
			Int        []int64
			Float      []float64
			Bool       []bool
			String     []string
			StringDict []string `frostdb:",rle_dict"`
			Uint64     []uint64
		}
		b := records.NewBuild[Repeated](memory.DefaultAllocator)
		defer b.Release()

		wantSchema := `{
  "name": "repeated",
  "columns": [
    {
      "name": "int",
      "storageLayout": {
        "type": "TYPE_INT64",
        "nullable": true,
        "repeated": true
      }
    },
    {
      "name": "float",
      "storageLayout": {
        "type": "TYPE_DOUBLE",
        "nullable": true,
        "repeated": true
      }
    },
    {
      "name": "bool",
      "storageLayout": {
        "type": "TYPE_BOOL",
        "nullable": true,
        "repeated": true
      }
    },
    {
      "name": "string",
      "storageLayout": {
        "type": "TYPE_STRING",
        "nullable": true,
        "repeated": true
      }
    },
    {
      "name": "string_dict",
      "storageLayout": {
        "type": "TYPE_STRING",
        "encoding": "ENCODING_RLE_DICTIONARY",
        "nullable": true,
        "repeated": true
      }
    },
 	{
      "name": "uint64",
      "storageLayout": {
        "type": "TYPE_UINT64",
        "nullable": true,
        "repeated": true
      }
    }
  ]
}`
		m := protojson.MarshalOptions{Multiline: true}
		d, _ := m.Marshal(b.Schema("repeated"))
		require.JSONEq(t, wantSchema, string(d))

		err := b.Append(
			Repeated{}, // nulls
			Repeated{
				Int:        []int64{1, 2},
				Float:      []float64{1, 2},
				Bool:       []bool{true, true},
				String:     []string{"a", "b"},
				StringDict: []string{"a", "b"},
				Uint64:     []uint64{1, 2},
			},
			Repeated{
				Int:        []int64{1, 2},
				Float:      []float64{1, 2},
				Bool:       []bool{true, true},
				String:     []string{"a", "b"},
				StringDict: []string{"c", "d"},
				Uint64:     []uint64{1, 2},
			},
		)
		require.Nil(t, err)
		want := `[{"bool":null,"float":null,"int":null,"string":null,"string_dict":null, "uint64":null}
,{"bool":[true,true],"float":[1,2],"int":[1,2],"string":["a","b"],"string_dict":["a","b"],"uint64":[1, 2]}
,{"bool":[true,true],"float":[1,2],"int":[1,2],"string":["a","b"],"string_dict":["c","d"],"uint64":[1, 2]}
]`
		r := b.NewRecord()
		data, _ := r.MarshalJSON()
		require.JSONEq(t, want, string(data))
	})
}

func TestBuild_pointer_base_types(t *testing.T) {
	type PointerBase struct {
		Int     *int64
		Double  *float64
		String  *string
		Dynamic map[string]*string
	}

	b := records.NewBuild[PointerBase](memory.NewGoAllocator())
	defer b.Release()

	err := b.Append(
		PointerBase{},
		PointerBase{
			Int:    point[int64](1),
			Double: point[float64](1),
			String: point[string]("1"),
			Dynamic: map[string]*string{
				"one": point[string]("1"),
			},
		},
	)
	require.Nil(t, err)
	r := b.NewRecord()
	defer r.Release()

	want := `[{"double":null,"dynamic.one":null,"int":null,"string":null}
,{"double":1,"dynamic.one":"1","int":1,"string":"1"}
]`

	got, err := r.MarshalJSON()
	require.Nil(t, err)
	require.JSONEq(t, want, string(got))
}

func point[T any](t T) *T {
	return &t
}

func BenchmarkBuild_Append_Then_NewRecord(b *testing.B) {
	// The way the record builder is used consist of calling Append followed by
	// NewRecord
	//
	// They are separate methods because we can't ignore benefits of buffering.
	build := records.NewBuild[samples.Sample](memory.DefaultAllocator)
	defer build.Release()
	samples := samples.NewTestSamples()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = build.Append(samples...)
		r := build.NewRecord()
		r.Release()
	}
}
