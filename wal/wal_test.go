package wal

import (
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
)

func TestWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "frostdb-wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	w, err := Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		dir,
	)
	require.NoError(t, err)

	require.NoError(t, w.Log(1, &walpb.Record{
		Entry: &walpb.Entry{
			EntryType: &walpb.Entry_Write_{
				Write: &walpb.Entry_Write{
					Data:      []byte("test-data"),
					TableName: "test-table",
				},
			},
		},
	}))

	require.NoError(t, w.Close())

	w, err = Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		dir,
	)
	require.NoError(t, err)

	err = w.Replay(func(tx uint64, r *walpb.Record) error {
		require.Equal(t, uint64(1), tx)
		require.Equal(t, []byte("test-data"), r.Entry.GetWrite().Data)
		require.Equal(t, "test-table", r.Entry.GetWrite().TableName)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, w.Log(2, &walpb.Record{
		Entry: &walpb.Entry{
			EntryType: &walpb.Entry_Write_{
				Write: &walpb.Entry_Write{
					Data:      []byte("test-data-2"),
					TableName: "test-table",
				},
			},
		},
	}))

	require.NoError(t, w.Close())

	w, err = Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		dir,
	)
	require.NoError(t, err)

	err = w.Replay(func(tx uint64, r *walpb.Record) error {
		return nil
	})
	require.NoError(t, err)
}
