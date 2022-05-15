package arcticdb

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogFile(t *testing.T) {
	file, err := ioutil.TempFile("", "data.log")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	defer os.Remove(file.Name())

	lf, err := OpenBlockFile(file.Name())
	require.NoError(t, err)

	bufs := make([][]byte, 0)
	for i := 0; i < 100; i++ {
		buf := make([]byte, 1024)
		rand.Read(buf)

		bufs = append(bufs, buf)

		err := lf.WriteBlock(0, buf)
		require.NoError(t, err)
	}

	require.NoError(t, lf.Close())

	lfRead, err := OpenBlockFile(file.Name())
	require.NoError(t, err)

	it := lfRead.NewIterator()

	i := 0
	for it.HasNext() {
		require.Less(t, i, len(bufs))

		_, data, err := it.NextBlock()
		require.NoError(t, err)

		require.Equal(t, data, bufs[i])
		i++
	}
	require.Equal(t, i, len(bufs))
}
