package arcticdb

import (
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

func writeToFile(data []byte, dir string, fileName string) error {
	tempFile, err := os.CreateTemp(dir, fileName)
	if err != nil {
		return err
	}

	if _, err := tempFile.Write(data); err != nil {
		return err
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	return os.Rename(tempFile.Name(), path.Join(dir, fileName))
}

func getBlockFileName(block *TableBlock) string {
	return strconv.FormatUint(block.timestamp, 10) + ".parquet"
}

func parseBlockFileName(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".parquet"), 0, 0)
}

// WriteBlock writes a block somewhere
func (block *TableBlock) WriteToDisk() (int, error) {
	data, err := block.Serialize()
	if err != nil {
		return -1, err
	}
	return len(data), writeToFile(data, block.table.StorePath(), getBlockFileName(block))
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

func (t *Table) IterateDiskBlocks(logger log.Logger, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicCloserRowGroup) bool, lastBlockTimestamp uint64) error {
	t.mtx.RLock()
	// make a copy of the blockFiles map
	blockFiles := make(map[uint64]*BlockFile)
	for timestamp, file := range t.blockFiles {
		blockFiles[timestamp] = file
	}
	t.mtx.RUnlock()

	n := 0
	for timestamp, file := range blockFiles {
		if timestamp >= lastBlockTimestamp {
			continue
		}

		file, err := parquet.OpenFile(file, int64(file.size))
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
