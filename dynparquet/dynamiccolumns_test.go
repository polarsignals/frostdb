package dynparquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDynamicColumnsSerialization(t *testing.T) {
	input := map[string][]string{
		"labels": {
			"container",
			"namespace",
			"node",
			"pod",
		},
		"pprof_labels": {
			"profile_id",
		},
	}

	str := serializeDynamicColumns(input)
	require.Equal(t, "labels:container,namespace,node,pod;pprof_labels:profile_id", str)

	output, err := deserializeDynamicColumns(str)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestDynamicColumnsDeserialization(t *testing.T) {
	input := "labels:__name__;pprof_labels:;pprof_num_labels:bytes"
	output, err := deserializeDynamicColumns(input)
	require.NoError(t, err)

	expected := map[string][]string{
		"labels": {
			"__name__",
		},
		"pprof_labels": {},
		"pprof_num_labels": {
			"bytes",
		},
	}
	require.Equal(t, expected, output)
}

func TestDynamicColumnsDeserialization_NoDynamicColumns(t *testing.T) {
	input := ""
	output, err := deserializeDynamicColumns(input)
	require.NoError(t, err)

	expected := map[string][]string{}
	require.Equal(t, expected, output)
}

func BenchmarkDeserialization(b *testing.B) {
	input := "labels:__name__;pprof_labels:;pprof_num_labels:bytes"
	for i := 0; i < b.N; i++ {
		_, _ = deserializeDynamicColumns(input)
	}
}
