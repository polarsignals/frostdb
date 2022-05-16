package arcticdb

import (
	"bytes"
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
	return block.table.blockFile.WriteBlock(block.timestamp, data)
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

func (t *Table) IterateDiskBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicCloserRowGroup) bool, lastBlockTimestamp int64) error {
	if t.blockFile == nil {
		return nil
	}

	it := t.blockFile.NewIterator()

	n := 0
	for it.HasNext() {
		timestamp, blockData, err := it.NextBlock()
		if err != nil {
			return err
		}

		if lastBlockTimestamp >= 0 && timestamp >= uint64(lastBlockTimestamp) {
			return nil
		}

		reader := bytes.NewReader(blockData)
		file, err := parquet.OpenFile(reader, int64(len(blockData)))
		if err != nil {
			return err
		}

		// Get a reader from the file bytes
		buf, err := dynparquet.NewSerializedBuffer(file)
		if err != nil {
			return err
		}

		n++
		for i := 0; i < buf.NumRowGroups(); i++ {
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
