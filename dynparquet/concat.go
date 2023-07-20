package dynparquet

import "github.com/parquet-go/parquet-go"

type concatenatedDynamicRowGroup struct {
	parquet.RowGroup
	dynamicColumns map[string][]string
	fields         []parquet.Field
}

func Concat(fields []parquet.Field, drg ...DynamicRowGroup) DynamicRowGroup {
	rg := make([]parquet.RowGroup, 0, len(drg))
	for _, d := range drg {
		rg = append(rg, d)
	}

	return &concatenatedDynamicRowGroup{
		RowGroup:       parquet.MultiRowGroup(rg...),
		dynamicColumns: drg[0].DynamicColumns(),
		fields:         fields,
	}
}

func (c *concatenatedDynamicRowGroup) String() string {
	return prettyRowGroup(c)
}

func (c *concatenatedDynamicRowGroup) DynamicColumns() map[string][]string {
	return c.dynamicColumns
}

func (c *concatenatedDynamicRowGroup) DynamicRows() DynamicRowReader {
	return newDynamicRowGroupReader(c, c.fields)
}
