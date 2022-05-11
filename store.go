package arcticdb

import (
	"context"

	"github.com/polarsignals/arcticdb/dynparquet"
)

// WriteBlock writes a block somewhere
func WriteBlock(block *TableBlock) error {
	ctx := context.Background()

	// Read all row groups
	rowGroups := []dynparquet.DynamicRowGroup{}
	err := block.RowGroupIterator(ctx, nil, nil, func(rg dynparquet.DynamicRowGroup) bool {
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

	// TODO do something with the merged row group

	return nil
}
