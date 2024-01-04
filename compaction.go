package frostdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log/level"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
)

var ErrCompactionRecoveryFailed = fmt.Errorf("failed to recover compacted level")

type FileCompaction struct {
	t      *Table
	file   *os.File
	offset int64 // Writing offsets into the file
	ref    int64 // Number of references to file.
}

func (f *FileCompaction) Close() error {
	return f.file.Close()
}

func NewFileCompaction(t *Table, lvl int) *FileCompaction {
	f := &FileCompaction{
		t: t,
	}

	if t.db.storagePath == "" { // No storage path provided, use temp files.
		file, err := os.CreateTemp("", fmt.Sprintf("L%v-*.parquet", lvl))
		if err != nil {
			panic(err)
		}
		level.Info(t.logger).Log("msg", "created temp file for level", "level", lvl, "file", file.Name())
		f.file = file
		return f
	}

	indexDir := filepath.Join(t.db.indexDir(), t.name)
	if err := os.MkdirAll(indexDir, dirPerms); err != nil {
		panic(err)
	}

	file, err := os.OpenFile(filepath.Join(indexDir, fmt.Sprintf("L%v.parquet", lvl)), os.O_CREATE|os.O_RDWR, filePerms)
	if err != nil {
		panic(err)
	}
	f.file = file
	return f
}

// accountingWriter is a writer that accounts for the number of bytes written.
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

	accountant := &accountingWriter{w: f.file}
	preCompactionSize, err := f.t.compactParts(accountant, compact,
		parquet.KeyValueMetadata(
			"compaction_tx", // Compacting up through this transaction.
			fmt.Sprintf("%v", compact[0].TX()),
		),
	) // compact into the next level
	if err != nil {
		return nil, 0, 0, err
	}

	// Record the writing offset into the file.
	prevOffset := f.offset
	f.offset += accountant.n + 8
	f.ref++

	// Record the file size for recovery.
	size := make([]byte, 8)
	binary.LittleEndian.PutUint64(size, uint64(accountant.n))
	if n, err := f.file.Write(size); n != 8 {
		return nil, 0, 0, fmt.Errorf("failed to write size to file: %v", err)
	}

	pf, err := parquet.OpenFile(io.NewSectionReader(f.file, prevOffset, accountant.n), accountant.n)
	if err != nil {
		return nil, 0, 0, err
	}

	buf, err := dynparquet.NewSerializedBuffer(pf)
	if err != nil {
		return nil, 0, 0, err
	}

	return []parts.Part{parts.NewParquetPart(compact[0].TX(), buf, append(options, parts.WithRelease(f.release()))...)}, preCompactionSize, accountant.n, nil
}

// Truncate will truncate the file to 0 bytes. This is used when a compaction recovery fails.
func (f *FileCompaction) Truncate() error {
	return f.file.Truncate(0)
}

// release will account for all the Parts currently pointing to this file. Once the last one has been released it will truncate the file.
// release is not safe to call concurrently.
func (f *FileCompaction) release() func() {
	return func() {
		f.ref--
		if f.ref == 0 {
			level.Info(f.t.logger).Log("msg", "truncating file", "file", f.file.Name())
			if err := os.Truncate(f.file.Name(), 0); err != nil {
				panic(fmt.Errorf("failed to truncate the level file: %v", err))
			}

			f.offset = 0
			if _, err := f.file.Seek(0, io.SeekStart); err != nil {
				panic(fmt.Errorf("failed to seek the level file: %v", err))
			}
		}
	}
}

func (f *FileCompaction) recover(options ...parts.Option) ([]parts.Part, error) {
	recovered, err := func() ([]parts.Part, error) {
		info, err := os.Stat(f.file.Name())
		if err != nil {
			return nil, err
		}

		if info.Size() == 0 { // file was truncated, nothing to recover.
			return nil, nil
		}

		recovered := []parts.Part{}

		// Recover all parts from file.
		for offset := info.Size(); offset > 0; {
			offset -= 8
			size := make([]byte, 8)
			if n, err := f.file.ReadAt(size, offset); n != 8 {
				return nil, fmt.Errorf("failed to read size from file: %v", err)
			}
			parquetSize := int64(binary.LittleEndian.Uint64(size))
			offset -= parquetSize

			pf, err := parquet.OpenFile(io.NewSectionReader(f.file, offset, parquetSize), parquetSize)
			if err != nil {
				return nil, err
			}

			buf, err := dynparquet.NewSerializedBuffer(pf)
			if err != nil {
				return nil, err
			}

			txstr, ok := buf.ParquetFile().Lookup("compaction_tx")
			if !ok {
				return nil, fmt.Errorf("failed to find compaction_tx metadata")
			}

			tx, err := strconv.Atoi(txstr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse compaction_tx metadata: %v", err)
			}

			recovered = append(recovered, parts.NewParquetPart(uint64(tx), buf, append(options, parts.WithRelease(f.release()))...))
		}

		return recovered, nil
	}()
	if err != nil {
		level.Error(f.t.logger).Log("msg", "truncating file after failed recovery", "err", err, "file", f.file.Name())
		if err := os.Truncate(f.file.Name(), 0); err != nil {
			return nil, fmt.Errorf("failed to truncate the level file %s: %v", f.file.Name(), err)
		}

		return nil, ErrCompactionRecoveryFailed
	}

	return recovered, err
}
