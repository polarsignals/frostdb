package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

var (
	totalSize int
	count     int
)

var findCmd = &cobra.Command{
	Use:     "find",
	Example: "parquet-tool find timestamp=1698684986287 </path/to/file.parquet or directory>",
	Short:   "Find value(s) in a parquet file",
	Args:    cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return findAll(args[1], args[0])
	},
}

var HeaderStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(lipgloss.Color("#FAFAFA")).
	Background(lipgloss.Color("#7D56F4"))

var EvenRowStyle = lipgloss.NewStyle().
	Bold(false).
	Foreground(lipgloss.Color("#FAFAFA"))

var OddRowStyle = lipgloss.NewStyle().
	Bold(false).
	Foreground(lipgloss.Color("#a6a4a4"))

func parseColumnArg(columnArg string) (string, string, error) {
	splits := strings.Split(columnArg, "=")
	if len(splits) != 2 {
		return "", "", fmt.Errorf("invalid column argument: %s; expected format of <column>=<value>", columnArg)
	}

	return splits[0], splits[1], nil
}

func findAll(fileOrDir, column string) error {
	info, err := os.Stat(fileOrDir)
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
		Headers("FILE", "MIN", "MAX")
	defer func() {
		fmt.Println("Total Size of all column indexes: ", fmt.Sprint(totalSize))
		fmt.Println("Average Size of all column indexes: ", fmt.Sprint(totalSize/count))
	}()
	defer fmt.Println(t)

	if !info.IsDir() {
		return find(fileOrDir, column, t)
	}

	return filepath.WalkDir(fileOrDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		return find(path, column, t)
	})
}

func find(file, column string, t *table.Table) error {
	pf, err := openParquetFile(file)
	if err != nil {
		return err
	}

	for _, index := range pf.OffsetIndexes() {
		totalSize += int(unsafe.Sizeof(index))
	}
	for _, index := range pf.ColumnIndexes() {
		totalSize += int(unsafe.Sizeof(index))
	}
	count++

	// TODO: would be nice to support humand readable timestamps; and parse them into int64s
	column, _, err = parseColumnArg(column)
	if err != nil {
		return err
	}

	var min, max parquet.Value
	for _, rg := range pf.RowGroups() {
		schema := rg.Schema()
		for j, field := range schema.Fields() {
			if field.Name() != column {
				continue
			}

			// Check the min max values of each column
			index := rg.ColumnChunks()[j].ColumnIndex()
			for k := 0; k < index.NumPages(); k++ {
				if min.IsNull() {
					min = index.MinValue(k)
					max = index.MinValue(k)
					continue
				}

				if compare(index.MinValue(k), min) < 0 {
					min = index.MinValue(k)
				}

				if compare(index.MaxValue(k), max) > 0 {
					max = index.MaxValue(k)
				}
			}
		}
	}

	t.Row(file, fmt.Sprint(min), fmt.Sprint(max))

	return nil
}

func getValue(val string, kind parquet.Kind) (parquet.Value, error) {
	switch kind {
	case parquet.Int64:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return parquet.Value{}, err
		}

		return parquet.ValueOf(i), nil
	case parquet.Int96:
		fallthrough
	case parquet.Boolean:
		fallthrough
	case parquet.Int32:
		fallthrough
	case parquet.Float:
		fallthrough
	case parquet.Double:
		fallthrough
	case parquet.ByteArray:
		fallthrough
	case parquet.FixedLenByteArray:
		fallthrough
	default:
		return parquet.Value{}, fmt.Errorf("unsupported kind: %T", kind)
	}
}
