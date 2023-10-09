package frostdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/util"
	"github.com/go-kit/log/level"

	"github.com/polarsignals/frostdb/dynparquet"
	snapshotpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/snapshot/v1alpha1"
	tablepb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/table/v1alpha1"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/parts"
)

// This file implements writing and reading database snapshots from disk.
// The snapshot format at the time of writing is as follows:
// 4-byte magic "FDBS"
// <Table 1 Granule 1 Part 1>
// <Table 2 Granule 1 Part 1>
// <Table 2 Granule 1 Part 2>
// <Table 2 Granule 2 Part 1>
// ...
// Footer/File Metadata
// 4-byte length in bytes of footer/file metadata (little endian)
// 4-byte version number (little endian)
// 4-byte checksum (little endian)
// 4-byte magic "FDBS"
//
// Readers should start reading a snapshot by first verifying that the magic
// bytes are correct, followed by the version number to ensure that the snapshot
// was encoded using a version the reader supports. A version bump could, for
// example, add compression to the data bytes of the file.
// Refer to minVersionSupported/maxVersionSupported for more details.

const (
	snapshotMagic = "FDBS"
	dirPerms      = os.FileMode(0o755)
	filePerms     = os.FileMode(0o640)
	// When bumping the version number, please add a comment indicating the
	// reason for the bump. Note that the version should only be bumped if the
	// new version introduces backwards-incompatible changes. Note that protobuf
	// changes are backwards-compatible, this version number is only necessary
	// for the non-proto format (e.g. if compression is introduced).
	// Version 1: Initial snapshot version with checksum and version number.
	snapshotVersion = 1
	// minReadVersion is bumped when deprecating older versions. For example,
	// a reader of the new version can choose to still support reading older
	// versions, but will bump this constant to the minimum version it claims
	// to support.
	minReadVersion = snapshotVersion
)

type snapshotMetrics struct {
	snapshotsTotal            *prometheus.CounterVec
	snapshotFileSizeBytes     prometheus.Gauge
	snapshotDurationHistogram prometheus.Histogram
}

func newSnapshotMetrics(reg prometheus.Registerer) *snapshotMetrics {
	reg = prometheus.WrapRegistererWithPrefix("frostdb_snapshots_", reg)
	return &snapshotMetrics{
		snapshotsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "total",
			Help: "Total number of snapshots",
		},
			[]string{"success"},
		),
		snapshotFileSizeBytes: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "file_size_bytes",
			Help: "Size of snapshots in bytes",
		}),
		snapshotDurationHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "duration_seconds",
			Help:    "Duration of snapshots in seconds",
			Buckets: prometheus.ExponentialBucketsRange(1, 60, 5),
		}),
	}
}

// segmentName returns a 20-byte textual representation of a snapshot file name
// at a given txn used for lexical ordering.
func snapshotFileName(tx uint64) string {
	return fmt.Sprintf("%020d.fdbs", tx)
}

func getTxFromSnapshotFileName(fileName string) (uint64, error) {
	parsedTx, err := strconv.ParseUint(fileName[:20], 10, 64)
	if err != nil {
		return 0, err
	}
	return parsedTx, nil
}

// asyncSnapshot begins a new transaction and takes a snapshot of the
// database in a new goroutine at that txn. It returns whether a snapshot was
// started (i.e. no other snapshot was in progress). When the snapshot
// goroutine successfully completes a snapshot, onSuccess is called.
func (db *DB) asyncSnapshot(ctx context.Context, onSuccess func()) {
	db.snapshot(ctx, true, onSuccess)
}

func (db *DB) snapshot(ctx context.Context, async bool, onSuccess func()) {
	if !db.columnStore.enableWAL {
		return
	}
	if !db.snapshotInProgress.CompareAndSwap(false, true) {
		// Snapshot already in progress.
		level.Debug(db.logger).Log(
			"msg", "cannot start snapshot; snapshot already in progress",
		)
		return
	}

	tx, _, metadata, commit := db.begin()
	level.Debug(db.logger).Log(
		"msg", "starting a new snapshot",
		"tx", tx,
	)
	doSnapshot := func(writeSnapshot func(context.Context, io.Writer) error) {
		start := time.Now()
		defer db.snapshotInProgress.Store(false)
		defer commit()
		if db.columnStore.enableWAL {
			// Appending a snapshot record to the WAL is necessary,
			// since the WAL expects a 1:1 relationship between txn ids
			// and record indexes. This is done before the actual snapshot so
			// that a failure to snapshot still appends a record to the WAL,
			// avoiding a WAL deadlock.
			if err := db.wal.Log(
				tx,
				&walpb.Record{
					Entry: &walpb.Entry{
						EntryType: &walpb.Entry_Snapshot_{Snapshot: &walpb.Entry_Snapshot{Tx: tx}},
					},
					TxnMetadata: metadata,
				},
			); err != nil {
				level.Error(db.logger).Log(
					"msg", "failed to append snapshot record to WAL", "err", err,
				)
				return
			}
		}

		if err := db.snapshotAtTX(ctx, tx, writeSnapshot); err != nil {
			level.Error(db.logger).Log(
				"msg", "failed to snapshot database", "err", err,
			)
			return
		}
		level.Debug(db.logger).Log(
			"msg", "snapshot complete",
			"tx", tx,
			"duration", time.Since(start),
		)
		onSuccess()
	}

	if async {
		go doSnapshot(db.snapshotWriter(tx, metadata))
	} else {
		doSnapshot(db.offlineSnapshotWriter(tx, metadata))
	}
}

// snapshotAtTX takes a snapshot of the state of the database at transaction tx.
func (db *DB) snapshotAtTX(ctx context.Context, tx uint64, writeSnapshot func(context.Context, io.Writer) error) error {
	var fileSize int64
	start := time.Now()
	if err := func() error {
		snapshotsDir := db.snapshotsDir()
		if err := os.MkdirAll(snapshotsDir, dirPerms); err != nil {
			return err
		}

		fileName := filepath.Join(snapshotsDir, snapshotFileName(tx))
		f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, filePerms)
		if err != nil {
			return err
		}
		defer f.Close()

		if err := func() error {
			if err := writeSnapshot(ctx, f); err != nil {
				return err
			}
			if err := f.Sync(); err != nil {
				return err
			}
			info, err := f.Stat()
			if err != nil {
				return err
			}
			fileSize = info.Size()
			return nil
		}(); err != nil {
			err = fmt.Errorf("failed to write snapshot for tx %d: %w", tx, err)
			if removeErr := os.Remove(fileName); removeErr != nil {
				err = fmt.Errorf("%w: failed to remove snapshot file: %v", err, removeErr)
			}
			return err
		}
		return nil
	}(); err != nil {
		db.metrics.snapshotMetrics.snapshotsTotal.WithLabelValues("false").Inc()
		return err
	}
	db.metrics.snapshotMetrics.snapshotsTotal.WithLabelValues("true").Inc()
	if fileSize > 0 {
		db.metrics.snapshotMetrics.snapshotFileSizeBytes.Set(float64(fileSize))
	}
	db.metrics.snapshotMetrics.snapshotDurationHistogram.Observe(time.Since(start).Seconds())
	// TODO(asubiotto): If snapshot file sizes become too large, investigate
	// adding compression.
	return nil
}

// loadLatestSnapshot loads the latest snapshot (i.e. the snapshot with the
// highest txn) from the snapshots dir into the database.
func (db *DB) loadLatestSnapshot(ctx context.Context) (Txn, error) {
	return db.loadLatestSnapshotFromDir(ctx, db.snapshotsDir())
}

func (db *DB) loadLatestSnapshotFromDir(ctx context.Context, dir string) (Txn, error) {
	var (
		lastErr   error
		loadedTxn Txn
	)
	// No error should be returned from snapshotsDo.
	_ = db.snapshotsDo(ctx, dir, func(parsedTx uint64, entry os.DirEntry) (bool, error) {
		if err := func() error {
			f, err := os.Open(filepath.Join(dir, entry.Name()))
			if err != nil {
				return err
			}
			defer f.Close()
			info, err := entry.Info()
			if err != nil {
				return err
			}
			watermark, err := LoadSnapshot(ctx, db, parsedTx, f, info.Size(), false)
			if err != nil {
				return err
			}
			// Success.
			loadedTxn = watermark
			return nil
		}(); err != nil {
			err = fmt.Errorf("unable to read snapshot file %s: %w", entry.Name(), err)
			level.Debug(db.logger).Log(
				"msg", "error reading snapshot",
				"error", err,
			)
			lastErr = err
			return true, nil
		}
		return false, nil
	})
	if loadedTxn.TxnID != 0 {
		// Successfully loaded a snapshot.
		return loadedTxn, nil
	}

	errString := "no valid snapshots found"
	if lastErr != nil {
		return Txn{}, fmt.Errorf("%s: lastErr: %w", errString, lastErr)
	}
	return Txn{}, fmt.Errorf("%s", errString)
}

func LoadSnapshot(ctx context.Context, db *DB, tx uint64, r io.ReaderAt, size int64, truncateWAL bool) (Txn, error) {
	txnMetadata, err := loadSnapshot(ctx, db, r, size)
	if err != nil {
		return Txn{}, err
	}
	watermark := Txn{TxnID: tx, TxnMetadata: txnMetadata}
	var wal WAL
	if truncateWAL {
		wal = db.wal
	}
	db.resetToTxn(watermark, wal)
	return watermark, nil
}

func (db *DB) getLatestValidSnapshotTxn(ctx context.Context) (uint64, error) {
	dir := db.snapshotsDir()
	latestValidTxn := uint64(0)
	// No error should be returned from snapshotsDo.
	_ = db.snapshotsDo(ctx, dir, func(parsedTx uint64, entry os.DirEntry) (bool, error) {
		if err := func() error {
			f, err := os.Open(filepath.Join(dir, entry.Name()))
			if err != nil {
				return err
			}
			defer f.Close()
			info, err := entry.Info()
			if err != nil {
				return err
			}
			// readFooter validates the checksum.
			if _, err := readFooter(f, info.Size()); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			level.Debug(db.logger).Log(
				"msg", "error reading snapshot",
				"error", err,
			)
			// Continue to the next snapshot.
			return true, nil
		}
		// Valid snapshot found.
		latestValidTxn = parsedTx
		return false, nil
	})
	return latestValidTxn, nil
}

type offsetWriter struct {
	w               io.Writer
	runningChecksum hash.Hash32
	offset          int
}

func newChecksumWriter() hash.Hash32 {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}

func newOffsetWriter(w io.Writer) *offsetWriter {
	return &offsetWriter{
		w:               w,
		runningChecksum: newChecksumWriter(),
	}
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	if n, err := w.runningChecksum.Write(p); err != nil {
		return n, fmt.Errorf("error writing checksum: %w", err)
	}
	n, err := w.w.Write(p)
	w.offset += n
	return n, err
}

func (w *offsetWriter) checksum() uint32 {
	return w.runningChecksum.Sum32()
}

func (db *DB) snapshotWriter(tx uint64, txnMetadata []byte) func(context.Context, io.Writer) error {
	return func(ctx context.Context, w io.Writer) error {
		return WriteSnapshot(ctx, tx, txnMetadata, db, w, false)
	}
}

// offlineSnapshotWriter is used when a database is closing after all the tables have closed.
func (db *DB) offlineSnapshotWriter(tx uint64, txnMetadata []byte) func(context.Context, io.Writer) error {
	return func(ctx context.Context, w io.Writer) error {
		return WriteSnapshot(ctx, tx, txnMetadata, db, w, true)
	}
}

func WriteSnapshot(ctx context.Context, tx uint64, txnMetadata []byte, db *DB, w io.Writer, offline bool) error {
	offW := newOffsetWriter(w)
	w = offW
	var tables []*Table
	db.mtx.RLock()
	for _, t := range db.tables {
		tables = append(tables, t)
	}
	db.mtx.RUnlock()

	if _, err := w.Write([]byte(snapshotMagic)); err != nil {
		return err
	}

	metadata := &snapshotpb.FooterData{TxnMetadata: txnMetadata}
	for _, t := range tables {
		if err := func() error {
			// Obtain a write block to prevent racing with
			// compaction/persistence.
			var block *TableBlock
			if offline {
				block = t.ActiveBlock()
			} else {
				var done func()
				var err error
				block, done, err = t.ActiveWriteBlock()
				if err != nil {
					return err
				}
				defer done()
			}
			blockUlid, err := block.ulid.MarshalBinary()
			if err != nil {
				return err
			}
			tableMeta := &snapshotpb.Table{
				Name:   t.name,
				Config: t.config,
				ActiveBlock: &snapshotpb.Table_TableBlock{
					Ulid:   blockUlid,
					Size:   block.Size(),
					MinTx:  block.minTx,
					PrevTx: block.prevTx,
				},
			}

			var ascendErr error
			block.Index().Iterate(func(node *index.Node) bool {
				granuleMeta := &snapshotpb.Granule{}
				if node.Part() == nil {
					return true
				}
				p := node.Part()
				partMeta := &snapshotpb.Part{
					StartOffset:     int64(offW.offset),
					Tx:              p.TX(),
					CompactionLevel: uint64(p.CompactionLevel()),
				}
				if err := func() error {
					if err := ctx.Err(); err != nil {
						return err
					}
					schema := t.schema

					if record := p.Record(); record != nil {
						partMeta.Encoding = snapshotpb.Part_ENCODING_ARROW
						recordWriter := ipc.NewWriter(
							w,
							ipc.WithSchema(record.Schema()),
						)
						defer recordWriter.Close()
						return recordWriter.Write(record)
					}
					partMeta.Encoding = snapshotpb.Part_ENCODING_PARQUET

					buf, err := p.AsSerializedBuffer(schema)
					if err != nil {
						return err
					}

					f := buf.ParquetFile()
					if _, err := io.Copy(
						w, io.NewSectionReader(f, 0, f.Size()),
					); err != nil {
						return err
					}
					return nil
				}(); err != nil {
					ascendErr = err
					return false
				}
				partMeta.EndOffset = int64(offW.offset)
				granuleMeta.PartMetadata = append(granuleMeta.PartMetadata, partMeta)
				tableMeta.GranuleMetadata = append(tableMeta.GranuleMetadata, granuleMeta) // TODO: we have one part per granule now
				return true
			})
			metadata.TableMetadata = append(metadata.TableMetadata, tableMeta)
			return ascendErr
		}(); err != nil {
			return err
		}
	}
	footer, err := metadata.MarshalVT()
	if err != nil {
		return err
	}
	// Write footer + size.
	footer = binary.LittleEndian.AppendUint32(footer, uint32(len(footer)))
	if _, err := w.Write(footer); err != nil {
		return err
	}
	if _, err := w.Write(binary.LittleEndian.AppendUint32(nil, snapshotVersion)); err != nil {
		return err
	}
	if _, err := w.Write(binary.LittleEndian.AppendUint32(nil, offW.checksum())); err != nil {
		return err
	}
	if _, err := w.Write([]byte(snapshotMagic)); err != nil {
		return err
	}
	return nil
}

func readFooter(r io.ReaderAt, size int64) (*snapshotpb.FooterData, error) {
	buffer := make([]byte, 16)
	if _, err := r.ReadAt(buffer[:4], 0); err != nil {
		return nil, err
	}
	if string(buffer[:4]) != snapshotMagic {
		return nil, fmt.Errorf("invalid snapshot magic: %q", buffer[:4])
	}
	if _, err := r.ReadAt(buffer, size-int64(len(buffer))); err != nil {
		return nil, err
	}
	if string(buffer[12:]) != snapshotMagic {
		return nil, fmt.Errorf("invalid snapshot magic: %q", buffer[4:])
	}

	// The checksum does not include the last 8 bytes of the file, which is the
	// magic and the checksum. Create a section reader of all but the last 8
	// bytes to compute the checksum and validate it against the read checksum.
	checksum := binary.LittleEndian.Uint32(buffer[8:12])
	checksumWriter := newChecksumWriter()
	if _, err := io.Copy(checksumWriter, io.NewSectionReader(r, 0, size-8)); err != nil {
		return nil, fmt.Errorf("failed to compute checksum: %w", err)
	}
	if checksum != checksumWriter.Sum32() {
		return nil, fmt.Errorf(
			"snapshot file corrupt: invalid checksum: expected %x, got %x", checksum, checksumWriter.Sum32(),
		)
	}

	version := binary.LittleEndian.Uint32(buffer[4:8])
	if version > snapshotVersion {
		return nil, fmt.Errorf(
			"cannot read snapshot with version %d: max version supported: %d", version, snapshotVersion,
		)
	}
	if version < minReadVersion {
		return nil, fmt.Errorf(
			"cannot read snapshot with version %d: min version supported: %d", version, minReadVersion,
		)
	}

	footerSize := binary.LittleEndian.Uint32(buffer[:4])
	footerBytes := make([]byte, footerSize)
	if _, err := r.ReadAt(footerBytes, size-(int64(len(buffer))+int64(footerSize))); err != nil {
		return nil, err
	}
	footer := &snapshotpb.FooterData{}
	if err := footer.UnmarshalVT(footerBytes); err != nil {
		return nil, fmt.Errorf("could not unmarshal footer: %v", err)
	}
	return footer, nil
}

// loadSnapshot loads a snapshot from the given io.ReaderAt and returns the
// txnMetadata (if any) the snapshot was created with and an error if any
// occurred.
func loadSnapshot(ctx context.Context, db *DB, r io.ReaderAt, size int64) ([]byte, error) {
	footer, err := readFooter(r, size)
	if err != nil {
		return nil, err
	}

	for i, tableMeta := range footer.TableMetadata {
		if err := func() error {
			var schemaMsg proto.Message
			switch v := tableMeta.Config.Schema.(type) {
			case *tablepb.TableConfig_DeprecatedSchema:
				schemaMsg = v.DeprecatedSchema
			case *tablepb.TableConfig_SchemaV2:
				schemaMsg = v.SchemaV2
			default:
				return fmt.Errorf("unhandled schema type: %T", v)
			}

			options := []TableOption{
				WithRowGroupSize(int(tableMeta.Config.RowGroupSize)),
				WithBlockReaderLimit(int(tableMeta.Config.BlockReaderLimit)),
			}
			if tableMeta.Config.DisableWal {
				options = append(options, WithoutWAL())
			}
			tableConfig := NewTableConfig(
				schemaMsg,
				options...,
			)
			table, err := db.Table(tableMeta.Name, tableConfig)
			if err != nil {
				return err
			}

			var blockUlid ulid.ULID
			if err := blockUlid.UnmarshalBinary(tableMeta.ActiveBlock.Ulid); err != nil {
				return err
			}

			table.mtx.Lock()
			block := table.active
			block.mtx.Lock()
			block.ulid = blockUlid
			// Store the last snapshot size so a snapshot is not triggered right
			// after loading this snapshot.
			block.lastSnapshotSize.Store(tableMeta.ActiveBlock.Size)
			block.minTx = tableMeta.ActiveBlock.MinTx
			block.prevTx = tableMeta.ActiveBlock.PrevTx
			newIdx := block.Index()
			block.mtx.Unlock()
			table.mtx.Unlock()

			for _, granuleMeta := range tableMeta.GranuleMetadata {
				resultParts := make([]*parts.Part, 0, len(granuleMeta.PartMetadata))
				for _, partMeta := range granuleMeta.PartMetadata {
					if err := ctx.Err(); err != nil {
						return err
					}
					startOffset := partMeta.StartOffset
					endOffset := partMeta.EndOffset
					partBytes := make([]byte, endOffset-startOffset)
					if _, err := r.ReadAt(partBytes, startOffset); err != nil {
						return err
					}
					partOptions := parts.WithCompactionLevel(int(partMeta.CompactionLevel))
					switch partMeta.Encoding {
					case snapshotpb.Part_ENCODING_PARQUET:
						serBuf, err := dynparquet.ReaderFromBytes(partBytes)
						if err != nil {
							return err
						}
						resultParts = append(resultParts, parts.NewPart(partMeta.Tx, serBuf, partOptions))
					case snapshotpb.Part_ENCODING_ARROW:
						arrowReader, err := ipc.NewReader(bytes.NewReader(partBytes))
						if err != nil {
							return err
						}

						record, err := arrowReader.Read()
						if err != nil {
							return err
						}

						resultParts = append(resultParts, parts.NewArrowPart(partMeta.Tx, record, int(util.TotalRecordSize(record)), table.schema, partOptions))
					default:
						return fmt.Errorf("unknown part encoding: %s", partMeta.Encoding)
					}
				}

				for _, part := range resultParts {
					newIdx.InsertPart(index.SentinelType(part.CompactionLevel()), part)
				}
			}

			return nil
		}(); err != nil {
			db.mtx.Lock()
			for _, cleanupTable := range footer.TableMetadata[:i] {
				delete(db.tables, cleanupTable.Name)
			}
			db.mtx.Unlock()
			return nil, err
		}
	}

	return footer.TxnMetadata, nil
}

// cleanupSnapshotDir should be called with a tx at which the caller is certain
// a valid snapshot exists (e.g. the tx returned from
// getLatestValidSnapshotTxn). This method deletes all snapshots taken at any
// other transaction.
func (db *DB) cleanupSnapshotDir(ctx context.Context, tx uint64) error {
	dir := db.snapshotsDir()
	return db.snapshotsDo(ctx, dir, func(fileTx uint64, entry os.DirEntry) (bool, error) {
		if fileTx == tx {
			// Continue.
			return true, nil
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return false, err
		}
		return true, nil
	})
}

// snapshotsDo executes the given callback with the filename of each snapshot
// in dir in reverse lexicographical order (most recent snapshot first). If
// false or an error is returned by the callback, the iteration is aborted and
// the error returned.
func (db *DB) snapshotsDo(ctx context.Context, dir string, callback func(tx uint64, entry os.DirEntry) (bool, error)) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for i := len(files) - 1; i >= 0; i-- {
		entry := files[i]
		if ctx.Err() != nil {
			return ctx.Err()
		}
		name := entry.Name()
		if entry.IsDir() || len(name) < 20 {
			continue
		}
		parsedTx, err := getTxFromSnapshotFileName(name)
		if err != nil {
			continue
		}
		if ok, err := callback(parsedTx, entry); err != nil {
			return err
		} else if !ok {
			return nil
		}
	}
	return nil
}

func StoreSnapshot(ctx context.Context, tx uint64, db *DB, snapshot io.Reader) error {
	return db.snapshotAtTX(ctx, tx, func(ctx context.Context, w io.Writer) error {
		_, err := io.Copy(w, snapshot)
		return err
	})
}
