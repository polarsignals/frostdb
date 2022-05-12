package arcticdb

import (
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
	data, err := block.Serialize()
	if err != nil {
		return err
	}
	// Write the serialized buffer to disk
	return ioutil.WriteFile(filepath.Join("data", uuid.New().String()), data, 0666)
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

func ReadAllBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicCloserRowGroup) bool) error {
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
				continu := iterator(&FileDynamicRowGroup{
					DynamicRowGroup: rg,
					Closer:          pf,
				})
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
