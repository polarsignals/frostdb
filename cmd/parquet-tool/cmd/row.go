package cmd

import (
	"fmt"
	"io"
	"strconv"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

var rowCmd = &cobra.Command{
	Use:     "row",
	Example: "parquet-tool row <row_start> <num_rows> <parqeut-file>",
	Short:   "print out row(s) for a given file",
	Args:    cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		rowStart, err := strconv.Atoi(args[0])
		if err != nil {
			return err
		}
		numRows, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		return row(args[2], rowStart, numRows)
	},
}

func row(file string, rowStart, numRows int) error {
	pf, closer, err := openParquetFile(file)
	if err != nil {
		return fmt.Errorf("failed to open file: ", err)
	}
	defer closer.Close()

	// Find the row group that contains the row we want
	var rowgroup parquet.RowGroup
	rowsSeen := 0
	rgoffset := 0
	for _, rg := range pf.RowGroups() {
		if rowsSeen+int(rg.NumRows()) >= rowStart { // This row group contains the row we want
			rowgroup = rg
			rgoffset = rowStart - rowsSeen
			break
		}
		rowsSeen += int(rg.NumRows())
	}

	headers := []string{"column"}
	for i := rowStart; i < rowStart+numRows; i++ {
		headers = append(headers, strconv.Itoa(i))
	}
	fields := rowgroup.Schema().Fields()
	tbl := table.New().
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
		Headers(headers...)
	defer fmt.Println(tbl)

	for i, chunk := range rowgroup.ColumnChunks() {
		if err := printPageSubset(tbl, fields[i].Name(), chunk.Pages(), rgoffset, numRows); err != nil {
			return err
		}
	}

	return nil
}

func printPageSubset(tbl *table.Table, name string, page parquet.Pages, start, num int) error {
	defer page.Close()

	for pg, err := page.ReadPage(); err != io.EOF; pg, err = page.ReadPage() {
		if err != nil {
			return err
		}

		if int(pg.NumRows()) < start {
			start -= int(pg.NumRows())
			continue
		}

		vr := pg.Values()
		values := make([]parquet.Value, pg.NumValues())
		_, err = vr.ReadValues(values)
		if err != nil && err != io.EOF {
			return err
		}

		end := start + num
		remainder := 0
		if end > int(pg.NumRows()) {
			end = int(pg.NumRows()) - start
			remainder = num - end
		}

		strs := make([]string, 0, len(values))
		for _, v := range values[start:end] {
			strs = append(strs, fmt.Sprintf("%v", v))
		}
		tbl.Row(append([]string{name}, strs...)...)

		if remainder <= 0 {
			break
		}
	}
	return nil
}
