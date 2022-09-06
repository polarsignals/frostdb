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

	serBuf := ReaderFromBytes(b.Bytes())
	require.NoError(t, err)
	f, err := serBuf.Open()
	require.NoError(t, err)
	require.Equal(t, int64(3), f.NumRows())
}

func TestSerializedReader(t *testing.T) {
	schema := NewSampleSchema()
	samples := NewTestSamples()
	buf, err := samples.ToBuffer(schema)
	require.NoError(t, err)

	b, err := schema.SerializeBuffer(buf)
	require.NoError(t, err)

	serBuf := ReaderFromBytes(b)
	require.NoError(t, err)
	f, err := serBuf.Open()
	require.NoError(t, err)
	require.Equal(t, int64(3), f.NumRows())
}
