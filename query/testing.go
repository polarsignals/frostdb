package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type FakeTableReader struct {
	Records       []arrow.Record
	FrostdbSchema *dynparquet.Schema
}

func (r *FakeTableReader) View(ctx context.Context, fn func(ctx context.Context, tx uint64) error) error {
	return fn(ctx, 0)
}

func (r *FakeTableReader) Iterator(
	ctx context.Context,
	_ uint64,
	_ memory.Allocator,
	callbacks []logicalplan.Callback,
	_ ...logicalplan.Option,
) error {
	if len(callbacks) == 0 {
		return errors.New("no callbacks provided")
	}

	for i, r := range r.Records {
		cb := callbacks[i%len(callbacks)]
		if err := cb(ctx, r); err != nil {
			return err
		}
	}

	return nil
}

func (r *FakeTableReader) SchemaIterator(
	_ context.Context,
	_ uint64,
	_ memory.Allocator,
	_ []logicalplan.Callback,
	_ ...logicalplan.Option,
) error {
	return errors.New("not implemented")
}

func (r *FakeTableReader) Schema() *dynparquet.Schema {
	return r.FrostdbSchema
}

type FakeTableProvider struct {
	Tables map[string]logicalplan.TableReader
}

func (f *FakeTableProvider) GetTable(name string) (logicalplan.TableReader, error) {
	if t, ok := f.Tables[name]; ok {
		return t, nil
	}

	return nil, fmt.Errorf("table %s not found", name)
}
