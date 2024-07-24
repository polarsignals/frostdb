package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var statsCmd = &cobra.Command{
	Use:     "stats",
	Example: "parquet-tool stats <file.parquet>",
	Short:   "print total stats of a parquet file",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runStats(args[0])
	},
}

type stats struct {
	Type                  string
	NumVal                int64
	Encoding              string
	TotalCompressedSize   int64
	TotalUncompressedSize int64
	TotalByteSize         int64
}

func runStats(file string) error {
	pf, closer, err := openParquetFile(file)
	if err != nil {
		return err
	}
	defer closer.Close()

	meta := pf.Metadata()

	s := map[string]stats{}
	for _, rg := range meta.RowGroups {
		for _, ds := range rg.Columns {
			col := strings.Join(ds.MetaData.PathInSchema, "/")
			typ := ds.MetaData.Type.String()
			enc := ""
			for _, e := range ds.MetaData.Encoding {
				enc += e.String() + " "
			}

			if _, ok := s[col]; !ok {
				s[col] = stats{
					Type:                  typ,
					NumVal:                ds.MetaData.NumValues,
					Encoding:              enc,
					TotalCompressedSize:   ds.MetaData.TotalCompressedSize,
					TotalUncompressedSize: ds.MetaData.TotalUncompressedSize,
					TotalByteSize:         rg.TotalByteSize,
				}
			} else {
				s[col] = stats{
					Type:                  typ,
					NumVal:                s[col].NumVal + ds.MetaData.NumValues,
					Encoding:              enc,
					TotalCompressedSize:   s[col].TotalCompressedSize + ds.MetaData.TotalCompressedSize,
					TotalUncompressedSize: s[col].TotalUncompressedSize + ds.MetaData.TotalUncompressedSize,
					TotalByteSize:         s[col].TotalByteSize + rg.TotalByteSize,
				}
			}
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Col", "Type", "NumVal", "Encoding", "TotalCompressedSize", "TotalUncompressedSize", "Compression", "%"})
	keys := maps.Keys(s)
	slices.Sort(keys)

	for _, k := range keys {
		row := s[k]
		table.Append(
			[]string{
				k,
				row.Type,
				fmt.Sprintf("%d", row.NumVal),
				row.Encoding,
				humanize.Bytes(uint64(row.TotalCompressedSize)),
				humanize.Bytes(uint64(row.TotalUncompressedSize)),
				fmt.Sprintf("%.2f", float64(row.TotalUncompressedSize-row.TotalCompressedSize)/float64(row.TotalCompressedSize)*100),
				fmt.Sprintf("%.2f", float64(row.TotalUncompressedSize)/float64(row.TotalByteSize)*100),
			})
	}
	table.Render()

	return nil
}
