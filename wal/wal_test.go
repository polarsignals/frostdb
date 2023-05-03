package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
)

func TestWAL(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		dir,
	)
	require.NoError(t, err)
	w.RunAsync()

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
	w.RunAsync()

	err = w.Replay(0, func(tx uint64, r *walpb.Record) error {
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

	// Test after removing wal dir.
	require.NoError(t, os.RemoveAll(dir))
	w, err = Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		dir,
	)
	require.NoError(t, err)
	w.RunAsync()
	defer w.Close()
}

func TestCorruptWAL(t *testing.T) {
	path := t.TempDir()

	w, err := Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		path,
	)
	require.NoError(t, err)
	w.RunAsync()

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

	corruptFilePath := filepath.Join(path, "00000000000000003466")
	// Write a file that has the correct WAL index format with some garbage.
	require.NoError(t, os.WriteFile(corruptFilePath, []byte("garbage"), 0o644))

	w, err = Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		path,
	)
	require.NoError(t, err)
	w.RunAsync()
	defer w.Close()

	lastIdx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), lastIdx)
}

// TestUnexpectedTxn verifies that the WAL can make progress when an unexpected
// txn (one that has already been seen) is logged. This should never happen but
// we should protect the WAL from getting into a deadlock. This test is likely
// to fail due to timeout.
func TestUnexpectedTxn(t *testing.T) {
	walDir := t.TempDir()
	func() {
		w, err := Open(
			log.NewNopLogger(),
			prometheus.NewRegistry(),
			walDir,
		)
		require.NoError(t, err)
		w.RunAsync()
		defer w.Close()

		emptyRecord := &walpb.Record{}
		require.NoError(t, w.Log(1, emptyRecord))
		require.NoError(t, w.Log(2, emptyRecord))

		// Note that an error cannot be returned from Log because it is hard to make
		// it aware of all txn ids that have already been seen (maintaining a map is
		// too expensive). An alternative would be to require the txn id to be
		// monotonically increasing.
		require.NoError(t, w.Log(1, emptyRecord))
	}()
	w, err := Open(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		walDir,
	)
	require.NoError(t, err)
	w.RunAsync()
	defer w.Close()
	lastIndex, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, lastIndex, uint64(2))
}

func TestWALTruncate(t *testing.T) {
	for i, tc := range []string{"BeforeLog", "AfterLog"} {
		t.Run(tc, func(t *testing.T) {
			dir := t.TempDir()
			w, err := Open(
				log.NewNopLogger(),
				prometheus.NewRegistry(),
				dir,
			)
			require.NoError(t, err)
			defer w.Close()
			w.RunAsync()

			for i := uint64(0); i < 10; i++ {
				require.NoError(t, w.Log(i, &walpb.Record{
					Entry: &walpb.Entry{
						EntryType: &walpb.Entry_Write_{
							Write: &walpb.Entry_Write{
								Data:      []byte(fmt.Sprintf("test-data-%d", i)),
								TableName: "test-table",
							},
						},
					},
				}))
			}
			if i == 1 {
				// Wait until the last entry is written before issuing the
				// truncate call.
				require.Eventually(t, func() bool {
					tx, _ := w.LastIndex()
					return tx == 9
				}, time.Second, 10*time.Millisecond)
			}
			require.NoError(t, w.Truncate(9))

			// Wait for the WAL to asynchronously log and truncate.
			require.Eventually(t, func() bool {
				tx, _ := w.FirstIndex()
				return tx == 9
			}, time.Second, 10*time.Millisecond)

			numRecords := 0
			require.NoError(
				t,
				w.Replay(0, func(tx uint64, r *walpb.Record) error {
					numRecords++
					require.Equal(t, uint64(9), tx)
					require.Equal(t, []byte("test-data-9"), r.Entry.GetWrite().Data)
					return nil
				}),
			)
			require.Equal(t, 1, numRecords)
		})
	}
}
