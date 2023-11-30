package cmd

import (
	"fmt"
	"strconv"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/spf13/cobra"
)

var rowgroupCmd = &cobra.Command{
	Use:     "rowgroup",
	Example: "parquet-tool rg </path/to/parquet-file> <row_group_index>",
	Short:   "Dump the column index for a row group",
	Args:    cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		rg, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		return rowgroup(args[0], rg)
	},
}

func rowgroup(file string, rg int) error {
	f, err := openParquetFile(file)
	if err != nil {
		return err
	}

	t := table.New().
		Border(lipgloss.NormalBorder()).
		BorderStyle(lipgloss.NewStyle().Foreground(lipgloss.Color("99"))).
		StyleFunc(func(row, col int) lipgloss.Style {
			switch {
			case row == 0:
				return HeaderStyle
			case row%2 == 0:
				return EvenRowStyle
			default:
				return OddRowStyle
			}
		}).
		Headers("Column", "Page", "Min", "Max", "Nulls")
	defer fmt.Println(t)

	rowgroup := f.RowGroups()[rg]
	fields := rowgroup.Schema().Fields()

	for i, chunk := range rowgroup.ColumnChunks() {
		index := chunk.ColumnIndex()
		for j := 0; j < index.NumPages(); j++ {
			t.Row(
				fields[i%len(fields)].Name(),
				strconv.Itoa(j),
				fmt.Sprintf("%v", index.MinValue(j)),
				fmt.Sprintf("%v", index.MaxValue(j)),
				fmt.Sprintf("%v", index.NullCount(j)),
			)
		}
	}
	return nil
}
