package frostdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/exp/slices"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
)

const (
	ParquetCompactionTXKey = "compaction_tx"
)

var ErrCompactionRecoveryFailed = fmt.Errorf("failed to recover compacted level")

type FileCompaction struct {
	dir string

	t *Table
}

func NewFileCompaction(t *Table, block ulid.ULID, lvl int) (*FileCompaction, error) {
	f := &FileCompaction{
		t: t,
	}

	if t.db.storagePath == "" { // No storage path provided, use temp files.
		return nil, errors.New("temp file LSM is not supported")
	}

	blockName := block.String()
	indexDir := filepath.Join(t.db.indexDir(), t.name, blockName, fmt.Sprintf("L%v", lvl))
	if err := os.MkdirAll(indexDir, dirPerms); err != nil {
		return nil, fmt.Errorf("failed to create index dir: %v", err)
	}
	f.dir = indexDir
	return f, nil
}

type accountingWriter struct {
	w io.Writer
	n int64
}

func (a *accountingWriter) Write(p []byte) (int, error) {
	n, err := a.w.Write(p)
	a.n += int64(n)
	return n, err
}

// writeRecordsToParquetFile will compact the given parts into a Parquet file written to the next level file.
func (f *FileCompaction) writeRecordsToParquetFile(compact []parts.Part, options ...parts.Option) ([]parts.Part, int64, int64, error) {
	if len(compact) == 0 {
		return nil, 0, 0, fmt.Errorf("no parts to compact")
	}

	tx := compact[0].TX()
	fileName := filepath.Join(f.dir, fmt.Sprintf("%v.parquet", tx))
	var preCompactionSize int64
	accountant := &accountingWriter{}
	if err := func() error {
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, filePerms)
		if err != nil {
			return err
		}
		defer file.Close()

		accountant.w = file
		preCompactionSize, err = f.t.compactParts(accountant, compact,
			parquet.KeyValueMetadata(
				ParquetCompactionTXKey, // Compacting up through this transaction.
				fmt.Sprintf("%v", compact[0].TX()),
			),
		) // compact into the next level
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return nil, 0, 0, err
	}

	file, err := os.Open(filepath.Join(f.dir, fmt.Sprintf("%v.parquet", tx)))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to open compacted parquet file: %v", err)
	}

	pf, err := parquet.OpenFile(file, accountant.n)
	if err != nil {
		return nil, 0, 0, err
	}

	buf, err := dynparquet.NewSerializedBuffer(pf)
	if err != nil {
		return nil, 0, 0, err
	}

	return []parts.Part{parts.NewParquetPart(tx, buf, append(options, parts.WithRelease(f.fileRelease(file)))...)}, preCompactionSize, accountant.n, nil
}

func (f *FileCompaction) fileRelease(file *os.File) func() {
	return func() {
		if err := file.Close(); err != nil {
			level.Error(f.t.logger).Log("msg", "failed to close compacted parquet file", "err", err, "file", file.Name())
		}
		if err := os.Remove(file.Name()); err != nil {
			level.Error(f.t.logger).Log("msg", "failed to remove compacted parquet file", "err", err, "file", file.Name())
		}
	}
}

func (f *FileCompaction) recover(options ...parts.Option) ([]parts.Part, error) {
	recovered := []parts.Part{}
	err := filepath.WalkDir(f.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Process the parquet parts.
		if filepath.Ext(path) == ".parquet" {
			info, err := d.Info()
			if err != nil {
				level.Error(f.t.logger).Log("msg", "failed to get file info", "err", err, "file", path)
				return nil
			}

			file, err := os.Open(path)
			if err != nil {
				level.Error(f.t.logger).Log("msg", "failed to open file", "err", err, "file", path)
				return nil
			}

			pf, err := parquet.OpenFile(file, info.Size())
			if err != nil {
				level.Error(f.t.logger).Log("msg", "failed to open parquet file", "err", err, "file", path)
				return nil
			}

			buf, err := dynparquet.NewSerializedBuffer(pf)
			if err != nil {
				level.Error(f.t.logger).Log("msg", "failed to create serialized buffer", "err", err, "file", path)
				return nil
			}

			txstr, ok := buf.ParquetFile().Lookup(ParquetCompactionTXKey)
			if !ok {
				level.Error(f.t.logger).Log("msg", "failed to find compaction_tx metadata", "file", path)
				return nil
			}

			tx, err := strconv.Atoi(txstr)
			if err != nil {
				level.Error(f.t.logger).Log("msg", "failed to parse compaction_tx metadata", "err", err, "file", path)
				return nil
			}

			recovered = append(recovered, parts.NewParquetPart(uint64(tx), buf, append(options, parts.WithRelease(f.fileRelease(file)))...))
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Reverse the recovered parts so they are in the correct order. We walk the filepath from lowest tx to highest, but we want to replay them in the opposite order.
	slices.Reverse(recovered)
	return recovered, nil
}
