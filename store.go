package arcticdb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

// WriteBlock writes a block somewhere
func (block *TableBlock) WriteToDisk() error {
	data, err := block.Serialize()
	if err != nil {
		return err
	}
	// Write the serialized buffer to disk
	return block.table.blockFile.WriteRecord(data)
}

// FileDynamicRowGroup is a dynamic row group that is backed by a file object
type FileDynamicRowGroup struct {
	dynparquet.DynamicRowGroup
	io.Closer
}

// MemDynamicRowGroup is a row group that is in memory and nop the close function
type MemDynamicRowGroup struct {
	dynparquet.DynamicRowGroup
	io.Closer
}

func (MemDynamicRowGroup) Close() error {
	return nil
}

func (t *Table) IterateDiskBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicCloserRowGroup) bool) error {
	if t.blockFile == nil {
		return nil
	}

	it := t.blockFile.NewIterator()

	n := 0
	for it.HasNext() {
		nextRecord, err := it.NextRecord()
		if err != nil {
			return err
		}

		fmt.Println("iterating on data: ", len(nextRecord))

		reader := bytes.NewReader(nextRecord)
		file, err := parquet.OpenFile(reader, int64(len(nextRecord)))
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
				continu := iterator(&MemDynamicRowGroup{
					DynamicRowGroup: rg,
				})
				if !continu {
					return err
				}
			}
		}
	}
	level.Info(logger).Log("msg", "read blocks", "n", n)
	return nil
}
