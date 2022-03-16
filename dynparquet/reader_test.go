package dynparquet

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	schema := NewSampleSchema()
	samples := NewTestSamples()
	buf, err := samples.ToBuffer(schema)
	require.NoError(t, err)

	b := bytes.NewBuffer(nil)
	w, err := schema.NewWriter(b, map[string][]string{
		"labels": samples.SampleLabelNames(),
	})
	require.NoError(t, err)

	_, err = parquet.CopyRows(w, buf.Rows())
	require.NoError(t, err)

	require.NoError(t, w.Close())

	_, err = ReaderFromBytes(b.Bytes())
	require.NoError(t, err)
}
