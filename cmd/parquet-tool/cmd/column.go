package cmd

import (
	"fmt"
	"io"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

var columnCmd = &cobra.Command{
	Use:     "col",
	Example: "parquet-tool col <file.parquet> <column> <rowgroup>",
	Short:   "print out a column for a given row group in the file",
	Args:    cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		rg, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		return column(args[0], args[1], rg)
	},
}

func column(file string, col string, rg int) error {
	pf, closer, err := openParquetFile(file)
	if err != nil {
		return fmt.Errorf("failed to open file: ", err)
	}
	defer closer.Close()

	rowgroup := pf.RowGroups()[rg]
	fields := rowgroup.Schema().Fields()

	for i, f := range fields {
		if f.Name() == col {
			for j, chunk := range rowgroup.ColumnChunks() {
				if j%len(fields) == i {
					if err := printPage(chunk.Pages()); err != nil {
						return err
					}
				}
			}
			return nil
		}
	}

	fmt.Println("Column not found")
	return nil
}

func printPage(page parquet.Pages) error {
	defer page.Close()

	i := 0
	for pg, err := page.ReadPage(); err != io.EOF; pg, err = page.ReadPage() {
		if err != nil {
			return err
		}

		vr := pg.Values()
		values := make([]parquet.Value, pg.NumValues())
		_, err = vr.ReadValues(values)
		if err != nil && err != io.EOF {
			return err
		}

		fmt.Println("Values(", i, "):", values)
		i++
	}
	return nil
}
