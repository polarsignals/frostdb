package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
)

const (
	IndexFileExtension     = ".idx"
	ParquetCompactionTXKey = "compaction_tx"
	dirPerms               = os.FileMode(0o755)
	filePerms              = os.FileMode(0o640)
)

type Compaction func(w io.Writer, compact []parts.Part, options ...parquet.WriterOption) (int64, error)

type FileCompaction struct {
	// settings
	dir     string
	compact Compaction
	maxSize int64

	// internal data
	indexFiles []*os.File
	offset     int64 // Writing offsets into the file
	ref        int64 // Number of references to file.

	// Options
	logger log.Logger
}

func (f *FileCompaction) Close(cleanup bool) error {
	for _, file := range f.indexFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	if cleanup {
		if err := os.RemoveAll(f.dir); err != nil {
			return fmt.Errorf("failed to remove file: %v", err)
		}
	}
	return nil
}

func NewFileCompaction(dir string, maxSize int64, compact Compaction, logger log.Logger) (*FileCompaction, error) {
	f := &FileCompaction{
		dir:     dir,
		compact: compact,
		maxSize: maxSize,
		logger:  logger,
	}

	if err := os.MkdirAll(dir, dirPerms); err != nil {
		return nil, err
	}

	return f, nil
}

func (f *FileCompaction) MaxSize() int64 { return f.maxSize }

// Snapshot will create a new index file and all future compacitons are written to that index file.
func (f *FileCompaction) Snapshot(dir string) error {
	if len(f.indexFiles) > 0 && f.offset == 0 { // If we are at the beginning of the file, no need to create a new file.
		return nil
	}

	// First sync the current file if there is one.
	if len(f.indexFiles) > 0 {
		if err := f.Sync(); err != nil {
			return err
		}

		for _, file := range f.indexFiles {
			// Hard link the file into the snapshot directory.
			if err := os.Link(file.Name(), filepath.Join(dir, filepath.Base(file.Name()))); err != nil {
				return err
			}
		}
	}

	_, err := f.createIndexFile(len(f.indexFiles))
	return err
}

func (f *FileCompaction) createIndexFile(id int) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(f.dir, fmt.Sprintf("%020d%s", id, IndexFileExtension)), os.O_CREATE|os.O_RDWR, filePerms)
	if err != nil {
		return nil, err
	}

	f.offset = 0
	f.indexFiles = append(f.indexFiles, file)
	return file, nil
}

func (f *FileCompaction) openIndexFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR, filePerms)
	if err != nil {
		return nil, err
	}

	f.indexFiles = append(f.indexFiles, file)
	return file, nil
}

// file returns the currently active index file.
func (f *FileCompaction) file() *os.File {
	return f.indexFiles[len(f.indexFiles)-1]
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

// Compact will compact the given parts into a Parquet file written to the next level file.
func (f *FileCompaction) Compact(compact []parts.Part, options ...parts.Option) ([]parts.Part, int64, int64, error) {
	if len(compact) == 0 {
		return nil, 0, 0, fmt.Errorf("no parts to compact")
	}

	accountant := &accountingWriter{w: f.file()}
	preCompactionSize, err := f.compact(accountant, compact,
		parquet.KeyValueMetadata(
			ParquetCompactionTXKey, // Compacting up through this transaction.
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
	if n, err := f.file().Write(size); n != 8 {
		return nil, 0, 0, fmt.Errorf("failed to write size to file: %v", err)
	}

	// Sync file after writing.
	if err := f.Sync(); err != nil {
		return nil, 0, 0, fmt.Errorf("failed to sync file: %v", err)
	}

	pf, err := parquet.OpenFile(io.NewSectionReader(f.file(), prevOffset, accountant.n), accountant.n)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to open file after compaction: %w", err)
	}

	buf, err := dynparquet.NewSerializedBuffer(pf)
	if err != nil {
		return nil, 0, 0, err
	}

	return []parts.Part{parts.NewParquetPart(compact[0].TX(), buf, append(options, parts.WithRelease(f.release()))...)}, preCompactionSize, accountant.n, nil
}

// Truncate will truncate the file to 0 bytes. This is used when a compaction recovery fails.
func (f *FileCompaction) Truncate() error {
	return f.file().Truncate(0)
}

// release will account for all the Parts currently pointing to this file. Once the last one has been released it will truncate the file.
// release is not safe to call concurrently.
func (f *FileCompaction) release() func() {
	return func() {
		f.ref--
		if f.ref == 0 {
			for _, file := range f.indexFiles {
				if err := file.Close(); err != nil {
					level.Error(f.logger).Log("msg", "failed to close level file", "err", err)
				}
			}

			// Delete all the files in the directory level. And open a new file.
			if err := os.RemoveAll(f.dir); err != nil {
				level.Error(f.logger).Log("msg", "failed to remove level directory", "err", err)
			}

			if err := os.MkdirAll(f.dir, dirPerms); err != nil {
				level.Error(f.logger).Log("msg", "failed to create level directory", "err", err)
			}

			f.indexFiles = nil
			_, err := f.createIndexFile(len(f.indexFiles))
			if err != nil {
				level.Error(f.logger).Log("msg", "failed to create new level file", "err", err)
			}
		}
	}
}

// recovery the level from the given directory.
func (f *FileCompaction) recover(options ...parts.Option) ([]parts.Part, error) {
	defer func() {
		_, err := f.createIndexFile(len(f.indexFiles))
		if err != nil {
			level.Error(f.logger).Log("msg", "failed to create new level file", "err", err)
		}
	}()
	recovered := []parts.Part{}
	err := filepath.WalkDir(f.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if filepath.Ext(path) != IndexFileExtension {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info: %v", err)
		}

		if info.Size() == 0 { // file empty, nothing to recover.
			return nil
		}

		file, err := f.openIndexFile(path)
		if err != nil {
			return fmt.Errorf("failed to open file: %v", err)
		}

		// Recover all parts from file.
		if err := func() error {
			for offset := info.Size(); offset > 0; {
				offset -= 8
				size := make([]byte, 8)
				if n, err := file.ReadAt(size, offset); n != 8 {
					return fmt.Errorf("failed to read size from file: %v", err)
				}
				parquetSize := int64(binary.LittleEndian.Uint64(size))
				offset -= parquetSize

				pf, err := parquet.OpenFile(io.NewSectionReader(file, offset, parquetSize), parquetSize)
				if err != nil {
					return err
				}

				buf, err := dynparquet.NewSerializedBuffer(pf)
				if err != nil {
					return err
				}

				txstr, ok := buf.ParquetFile().Lookup(ParquetCompactionTXKey)
				if !ok {
					return fmt.Errorf("failed to find compaction_tx metadata")
				}

				tx, err := strconv.Atoi(txstr)
				if err != nil {
					return fmt.Errorf("failed to parse compaction_tx metadata: %v", err)
				}

				f.ref++
				recovered = append(recovered, parts.NewParquetPart(uint64(tx), buf, append(options, parts.WithRelease(f.release()))...))
			}

			return nil
		}(); err != nil {
			// If we failed to recover the file, remove it.
			if err := f.file().Close(); err != nil {
				level.Error(f.logger).Log("msg", "failed to close level file after failed recovery", "err", err)
			}
			f.indexFiles = f.indexFiles[:len(f.indexFiles)-1] // Remove the file from the list of files.
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return recovered, nil
}

// Sync calls Sync on the underlying file.
func (f *FileCompaction) Sync() error { return f.file().Sync() }

type inMemoryLevel struct {
	compact Compaction
	maxSize int64
}

func (l *inMemoryLevel) MaxSize() int64          { return l.maxSize }
func (l *inMemoryLevel) Snapshot(_ string) error { return nil }
func (l *inMemoryLevel) Compact(toCompact []parts.Part, options ...parts.Option) ([]parts.Part, int64, int64, error) {
	if len(toCompact) == 0 {
		return nil, 0, 0, fmt.Errorf("no parts to compact")
	}

	var b bytes.Buffer
	preCompactionSize, err := l.compact(&b, toCompact)
	if err != nil {
		return nil, 0, 0, err
	}

	buf, err := dynparquet.ReaderFromBytes(b.Bytes())
	if err != nil {
		return nil, 0, 0, err
	}

	postCompactionSize := int64(b.Len())
	return []parts.Part{parts.NewParquetPart(0, buf, options...)}, preCompactionSize, postCompactionSize, nil
}
