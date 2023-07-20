package dynparquet

import (
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestNilChunk(t *testing.T) {
	c := NewNilColumnChunk(parquet.String().Type(), 0, 10)
	pages := c.Pages()
	p, err := pages.ReadPage()
	require.NoError(t, err)
	vals := make([]parquet.Value, 10)
	n, err := p.Values().ReadValues(vals)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 10, n)
	require.Equal(t, []parquet.Value{
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
		parquet.ValueOf(nil).Level(0, 0, 0),
	}, vals)
	_, err = pages.ReadPage()
	require.Equal(t, io.EOF, err)
}
