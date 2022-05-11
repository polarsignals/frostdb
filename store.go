package arcticdb

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/polarsignals/arcticdb/dynparquet"
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
