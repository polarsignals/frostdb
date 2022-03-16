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
