package cmd

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/wal"
)

var walCmd = &cobra.Command{
	Use:     "wal",
	Example: "parquet-tool wal </path/to/wal/directory> [columns to dump]",
	Short:   "Interact with a WAL directory",
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return inspectWAL(args[0], args[1:]...)
	},
}

func inspectWAL(dir string, columns ...string) error {
	wal, err := wal.Open(log.NewNopLogger(), prometheus.NewRegistry(), dir)
	if err != nil {
		return err
	}

	return wal.Replay(0, func(tx uint64, record *walpb.Record) error {
		switch e := record.Entry.EntryType.(type) {
		case *walpb.Entry_Write_:
			reader, err := ipc.NewReader(bytes.NewReader(e.Write.Data))
			if err != nil {
				return err
			}

			record, err := reader.Read()
			if err != nil {
				return err
			}

			inspectRecord(record, columns)
		default:
			// NOTE: just looking for writes
			return nil
		}
		return nil
	})
}
