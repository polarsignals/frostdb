package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "parquet-tool",
	Short: "Explort parquet files",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(dumpCmd)
	rootCmd.AddCommand(findCmd)
	rootCmd.AddCommand(walCmd)
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(rowgroupCmd)
}
