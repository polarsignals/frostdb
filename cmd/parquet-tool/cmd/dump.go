package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var dumpCmd = &cobra.Command{
	Use:     "dump",
	Example: "parquet-tool dump <file.parquet>",
	Short:   "dump the database",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return dump(args[0])
	},
}

func dump(file string) error {
	pf, closer, err := openParquetFile(file)
	if err != nil {
		return err
	}
	defer closer.Close()

	fmt.Println("schema:", pf.Schema())
	meta := pf.Metadata()
	fmt.Println("Num Rows:", meta.NumRows)

	for i, rg := range meta.RowGroups {
		fmt.Println("\t Row group:", i)
		fmt.Println("\t\t Row Count:", rg.NumRows)
		fmt.Println("\t\t Row size:", humanize.Bytes(uint64(rg.TotalByteSize)))
		fmt.Println("\t\t Columns:")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Col", "Type", "NumVal", "Encoding", "TotalCompressedSize", "TotalUncompressedSize", "Compression", "%"})
		for _, ds := range rg.Columns {
			table.Append(
				[]string{
					strings.Join(ds.MetaData.PathInSchema, "/"),
					ds.MetaData.Type.String(),
					fmt.Sprintf("%d", ds.MetaData.NumValues),
					fmt.Sprintf("%s", ds.MetaData.Encoding),
					humanize.Bytes(uint64(ds.MetaData.TotalCompressedSize)),
					humanize.Bytes(uint64(ds.MetaData.TotalUncompressedSize)),
					fmt.Sprintf("%.2f", float64(ds.MetaData.TotalUncompressedSize-ds.MetaData.TotalCompressedSize)/float64(ds.MetaData.TotalCompressedSize)*100),
					fmt.Sprintf("%.2f", float64(ds.MetaData.TotalCompressedSize)/float64(rg.TotalByteSize)*100),
				})
		}
		table.Render()
	}

	return nil
}
