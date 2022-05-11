package arcticdb

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

// WriteBlock writes a block somewhere
func WriteBlock(block *TableBlock) error {
	ctx := context.Background()

	// Read all row groups
	rowGroups := []dynparquet.DynamicRowGroup{}
	err := block.RowGroupIterator(ctx, nil, &AlwaysTrueFilter{}, func(rg dynparquet.DynamicRowGroup) bool {
		rowGroups = append(rowGroups, rg)
		return true
	})
	if err != nil {
		return err
	}

	merged, err := block.table.config.schema.MergeDynamicRowGroups(rowGroups)
	if err != nil {
		return err
	}

	b := bytes.NewBuffer(nil)
	cols := merged.DynamicColumns()
	w, err := block.table.config.schema.NewWriter(b, cols)
	if err != nil {
		return err
	}

	rows := merged.Rows()
	n := 0
	for {
		row, err := rows.ReadRow(nil)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = w.WriteRow(row)
		if err != nil {
			return err
		}
		n++
	}

	err = w.Close()
	if err != nil {
		return err
	}

	// Write the serialized buffer to disk
	err = ioutil.WriteFile(filepath.Join("data", uuid.New().String()), b.Bytes(), 0666)
	if err != nil {
		return err
	}

	return nil
}

func ReadAllBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicRowGroup) bool) error {
	n := 0
	err := filepath.Walk("data", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() { // Skip reading directories
			return nil
		}

		pf, err := os.Open(path)
		if err != nil {
			return err
		}
		/* // TODO CLOSE?
		fmt.Println(path)
		defer func() {
			err := pf.Close()
			if err != nil {
				level.Error(logger).Log("msg", "failed to close block file", "err", err)
			}
		}()
		*/

		file, err := parquet.OpenFile(pf, info.Size())
		if err != nil {
			return err
		}

		// Get a reader from the file bytes
		buf, err := dynparquet.NewSerializedBuffer(file)
		if err != nil {
			return err
		}

		n++
		f := buf.ParquetFile()
		for i := 0; i < f.NumRowGroups(); i++ {
			rg := buf.DynamicRowGroup(i)
			var mayContainUsefulData bool
			mayContainUsefulData, err = filter.Eval(rg)
			if err != nil {
				return err
			}
			if mayContainUsefulData {
				continu := iterator(rg)
				if !continu {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	level.Info(logger).Log("msg", "read blocks", "n", n)
	return nil
}
