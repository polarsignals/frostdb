package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	internalWal "github.com/polarsignals/wal"
	"github.com/polarsignals/wal/types"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"

	"github.com/polarsignals/frostdb"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/samples"
	"github.com/polarsignals/frostdb/wal"
)

const (
	randomSeedKey = "POLARSIGNALS_RANDOM_SEED"
	numCommands   = 512
	dbName        = "test"
	tableName     = "test"
)

type command int

const (
	insert command = iota
	compact
	snapshot
	rotate
	restart
)

func (c command) String() string {
	switch c {
	case insert:
		return "insert"
	case compact:
		return "compact"
	case snapshot:
		return "snapshot"
	case rotate:
		return "rotate"
	case restart:
		return "restart"
	default:
		return "<unknown>"
	}
}

var commands = []command{insert, compact, snapshot, rotate, restart}

// probabilities are command probabilities. It is not strictly necessary that
// these sum to 1.
var probabilities = map[command]float64{
	insert:   0.75,
	compact:  0.25,
	snapshot: 0.1,
	rotate:   0.05,
	restart:  0.01,
}

var cumulativeProbabilities []float64

func init() {
	var sum float64
	for _, p := range probabilities {
		sum += p
		cumulativeProbabilities = append(cumulativeProbabilities, sum)
	}
}

func genCommand() command {
	f := rand.Float64()
	// Normalize f so it falls within a range.
	f *= cumulativeProbabilities[len(cumulativeProbabilities)-1]
	for i, p := range cumulativeProbabilities {
		if f < p {
			return commands[i]
		}
	}
	// Should never reach here unless rounding error, but return an insert.
	return insert
}

type int64checksum struct {
	sum        int64
	count      int64
	timestamps []int64
}

func (c *int64checksum) add(v int64) {
	c.sum ^= v
	c.count++
	c.timestamps = append(c.timestamps, v)
}

type tableProvider struct {
	table atomic.Pointer[frostdb.Table]
}

func (t *tableProvider) GetTable() *frostdb.Table {
	return t.table.Load()
}

func (t *tableProvider) Update(table *frostdb.Table) {
	t.table.Store(table)
}

// walTickProbability is the probability that a WAL tick will occur after a
// write, this simulates the passing of real time between WAL batch writes.
const walTickProbability = 0.25

// writerHelper is used concurrently to write to a table.
type writerHelper struct {
	logger       log.Logger
	tp           *tableProvider
	walTicker    *fakeTicker
	timestampSum struct {
		sync.Mutex
		int64checksum
	}
}

func genRandomLabels() map[string]string {
	var (
		labelKeys = []string{"label1", "label2", "label3"}
		labelVals = []string{"val1", "val2", "val3"}
	)
	labels := make(map[string]string)
	for i := 0; i < rand.Intn(len(labelKeys)); i++ {
		labels[labelKeys[i]] = labelVals[rand.Intn(len(labelVals))]
	}
	return labels
}

type fakeTicker struct {
	c chan time.Time
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.c
}

func (t *fakeTicker) Stop() {}

func (w *writerHelper) write(ctx context.Context) (uint64, error) {
	types := []string{"cpu", "memory", "block", "mutex"}

	// Note: UUID is not really interesting since it takes the current time,
	// which we control via the -faketime flag.
	genUUID, err := uuid.NewUUID()
	if err != nil {
		return 0, err
	}

	smpls := make([]samples.Sample, rand.Intn(16)+1)
	for i := range smpls {
		smpls[i] = samples.Sample{
			ExampleType: types[rand.Intn(len(types))],
			Labels:      genRandomLabels(),
			Stacktrace:  []uuid.UUID{genUUID},
			Timestamp:   rand.Int63(),
			Value:       rand.Int63(),
		}
	}

	record, err := samples.Samples(smpls).ToRecord()
	if err != nil {
		return 0, err
	}

	tx, err := w.tp.GetTable().InsertRecord(ctx, record)
	if err != nil {
		return 0, err
	}

	timestamps := make([]int64, 0, len(smpls))
	for i := range smpls {
		timestamps = append(timestamps, smpls[i].Timestamp)
		w.timestampSum.add(smpls[i].Timestamp)
	}

	if rand.Float64() < walTickProbability {
		// Nonblocking write to channel.
		select {
		case w.walTicker.c <- time.Now():
		default:
		}
	}
	level.Info(w.logger).Log("msg", "write complete", "txn", tx, "rows", len(smpls), "timestamps", fmt.Sprintf("%v", timestamps))
	return tx, nil
}

// testLogStore wraps a LogStore that the WAL uses to write records. The main
// purpose of this struct is to keep track of what data we can consider to be
// "committed". This is necessary because WAL writes are async, so a successful
// write doesn't necessarily mean that write is durable.
type testLogStore struct {
	internalWal.LogStore
	// acc does not need to be protected by a mutex since StoreLogs is called
	// synchronously.
	acc int64checksum
}

func (s *testLogStore) StoreLogs(logs []types.LogEntry) error {
	if err := s.LogStore.StoreLogs(logs); err != nil {
		return err
	}
	// Successful commit.
	for _, log := range logs {
		walRec := &walpb.Record{}
		if err := walRec.UnmarshalVT(log.Data); err != nil {
			return err
		}
		writeInfo, ok := walRec.Entry.EntryType.(*walpb.Entry_Write_)
		if !ok {
			continue
		}
		r, err := ipc.NewReader(bytes.NewReader(writeInfo.Write.Data))
		if err != nil {
			panic(fmt.Errorf("should not have error when decoding log entry: %w", err))
		}
		for r.Next() {
			rec := r.Record()
			idxSlice := rec.Schema().FieldIndices("timestamp")
			timestamps := rec.Column(idxSlice[0]).(*array.Int64)
			for i := 0; i < timestamps.Len(); i++ {
				s.acc.add(timestamps.Value(i))
			}
			rec.Release()
		}
		r.Release()
	}
	return nil
}

// canIgnoreError returns whether the given error can be ignored. Specifically,
// errors returned by operations when the database is closing are not a problem.
func canIgnoreError(err error) bool {
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "closed") ||
		strings.Contains(errMsg, "closing")
}

func newStore(
	storageDir string,
	logger log.Logger,
	objectStorage *frostdb.DefaultObjstoreBucket,
	newLogStoreWrapper func(internalWal.LogStore) internalWal.LogStore,
	fakeTicker wal.Ticker,
) (*frostdb.ColumnStore, error) {
	return frostdb.New(
		frostdb.WithStoragePath(storageDir),
		frostdb.WithLogger(logger),
		frostdb.WithWAL(),
		frostdb.WithManualBlockRotation(),
		frostdb.WithReadWriteStorage(objectStorage),
		frostdb.WithIndexConfig(
			[]*index.LevelConfig{
				{Level: index.L0, MaxSize: 1024 * 1024 * 15, Type: index.CompactionTypeParquetDisk},
				{Level: index.L1, MaxSize: 1024 * 1024 * 128, Type: index.CompactionTypeParquetDisk},
				{Level: index.L2, MaxSize: 1024 * 1024 * 512},
			},
		),
		// SnapshotTriggerSize enables snapshots. However, the size in question
		// is set to be very large in order to trigger snapshots manually.
		frostdb.WithSnapshotTriggerSize(math.MaxInt64),
		frostdb.WithTestingOptions(
			frostdb.WithTestingWalOptions(wal.WithTestingLogStoreWrapper(newLogStoreWrapper), wal.WithTestingLoopTicker(fakeTicker)),
		),
	)
}

type testOutput struct {
	t testing.TB
}

func (l *testOutput) Write(p []byte) (n int, err error) {
	l.t.Helper()
	l.t.Log(string(p))
	return len(p), nil
}

func newTestLogger(t testing.TB) log.Logger {
	t.Helper()
	logger := log.NewLogfmtLogger(log.NewSyncWriter(&testOutput{t: t}))
	logger = level.NewFilter(logger, level.AllowDebug())
	return logger
}

// TestDST runs deterministic simulation tests against FrostDB. For true
// determinism and reproducibility, this test needs to be run with
// POLARSIGNALS_RANDOM_SEED set, the modified go runtime found at
// github.com/asubiotto/go (for now), and GOOS=wasip1 GOARCH=wasm.
func TestDST(t *testing.T) {
	if os.Getenv(randomSeedKey) == "" {
		t.Skipf("%s not set, skipping deterministic simulation tests", randomSeedKey)
	}

	t.Log("Running DST using random seed:", os.Getenv(randomSeedKey))
	logger := newTestLogger(t)
	objectStorage := frostdb.NewDefaultObjstoreBucket(
		objstore.NewInMemBucket(), frostdb.StorageWithLogger(log.WithPrefix(logger, "component", "objstore")),
	)
	storageDir := t.TempDir()
	logStoreWrapper := &testLogStore{}
	walTicker := &fakeTicker{c: make(chan time.Time, 1)}
	storeID := 0
	c, err := newStore(
		storageDir, log.WithPrefix(logger, "storeID", storeID), objectStorage, func(logStore internalWal.LogStore) internalWal.LogStore {
			logStoreWrapper.LogStore = logStore
			return logStoreWrapper
		}, walTicker,
	)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	var db atomic.Pointer[frostdb.DB]
	{
		// Separate scope to avoid table pointer misuse.
		newDB, err := c.DB(ctx, dbName)
		require.NoError(t, err)
		db.Store(newDB)
	}

	tp := &tableProvider{}
	tableConfig := frostdb.NewTableConfig(samples.SampleDefinition())
	{
		// Separate scope to avoid table pointer misuse.
		table, err := db.Load().Table(tableName, tableConfig)
		require.NoError(t, err)
		tp.Update(table)
	}

	t.Log("DB initialized, starting commands")
	w := &writerHelper{logger: logger, tp: tp, walTicker: walTicker}
	writeAndWait := func() error {
		tx, err := w.write(ctx)
		if err != nil {
			return err
		}
		db.Load().Wait(tx)
		return nil
	}

	errg := &errgroup.Group{}
	errg.SetLimit(32)
	commandDistribution := make(map[command]int)

	ignoreGoroutinesAtStartOfTest := goleak.IgnoreCurrent()
	for i := 0; i < numCommands; i++ {
		cmd := genCommand()
		commandDistribution[cmd]++
		switch cmd {
		case insert:
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine
				// scheduling.
				// TODO(asubiotto): Figure out if we still need this.
				time.Sleep(1 * time.Millisecond)
				if _, err := w.write(ctx); err != nil && !canIgnoreError(err) {
					return fmt.Errorf("insert error: %s", err)
				}
				return nil
			})
		case compact:
			errg.Go(func() error {
				// TODO(asubiotto): Maybe we should offer a finer-grained way to
				// trigger leveled compaction.
				if err := tp.GetTable().EnsureCompaction(); err != nil && !errors.Is(err, io.EOF) {
					return fmt.Errorf("compaction error: %w", err)
				}
				return nil
			})
		case rotate:
			errg.Go(func() error {
				table := tp.GetTable()
				if err := table.RotateBlock(ctx, table.ActiveBlock(), false); err != nil {
					return fmt.Errorf("rotate error: %s", err)
				}
				return nil
			})
		case snapshot:
			errg.Go(func() error {
				if err := db.Load().Snapshot(ctx); err != nil {
					return fmt.Errorf("snapshot err: %w", err)
				}
				return nil
			})
		case restart:
			// This is a hack to ensure some goroutines are scheduled before
			// this restart.
			// TODO(asubiotto): Figure out if we still need this.
			time.Sleep(1 * time.Millisecond)
			// Graceful shutdown.
			require.NoError(t, c.Close())
			_ = errg.Wait()

			// Unfortunately frostdb doesn't have goroutine lifecycle management
			// and adding it could lead to subtle issues (e.g. on Close with
			// many DBs). Instead, this test simply verifies all goroutines
			// spawned up until this restart eventually exit after n retries.
			const maxRetries = 10
			for i := 0; i < maxRetries; i++ {
				if err := goleak.Find(ignoreGoroutinesAtStartOfTest); err == nil {
					break
				} else if i == maxRetries-1 {
					t.Fatalf("leaked goroutines found on Close: %v", err)
				} else {
					time.Sleep(1 * time.Millisecond)
				}
			}

			storeID++
			c, err = newStore(
				storageDir,
				log.WithPrefix(logger, "storeID", storeID), objectStorage, func(logStore internalWal.LogStore) internalWal.LogStore {
					logStoreWrapper.LogStore = logStore
					return logStoreWrapper
				}, walTicker,
			)
			require.NoError(t, err)
			newDB, err := c.DB(ctx, dbName)
			require.NoError(t, err)
			table, err := newDB.Table(tableName, tableConfig)
			require.NoError(t, err)
			db.Store(newDB)
			tp.Update(table)
			_, err = w.write(ctx)
			// This write should succeed.
			require.NoError(t, err)
		}
	}

	// Wait for all requests to complete.
	require.NoError(t, errg.Wait())

	t.Logf("All commands completed. Command distribution:\n%v\nIssuing write and waiting for high watermark.", commandDistribution)

	// Perform a write and wait for the high watermark to ensure all writes have
	// been committed.
	require.NoError(t, writeAndWait())

	t.Log("Verifying data integrity.")

	listFiles := func(dir string) string {
		de, err := os.ReadDir(filepath.Join(storageDir, "databases", dbName, dir))
		require.NoError(t, err)
		var files []string
		for _, e := range de {
			files = append(files, e.Name())
		}
		return strings.Join(files, " ")
	}
	t.Log("Index files:", listFiles(filepath.Join("index", tableName)))
	t.Log("snapshot files:", listFiles("snapshots"))
	t.Log("WAL files:", listFiles("wal"))

	timestampSum := &int64checksum{}
	readTimestamps := make(map[int64]int)
	expectedTimestamps := make(map[int64]struct{})
	for _, v := range w.timestampSum.timestamps {
		expectedTimestamps[v] = struct{}{}
	}
	require.NoError(
		t,
		query.NewEngine(
			memory.DefaultAllocator,
			db.Load().TableProvider(),
		).ScanTable(tableName).Execute(ctx, func(_ context.Context, r arrow.Record) error {
			idxSlice := r.Schema().FieldIndices("timestamp")
			require.Equal(t, 1, len(idxSlice))
			timestamps := r.Column(idxSlice[0]).(*array.Int64)
			for i := 0; i < timestamps.Len(); i++ {
				require.False(t, timestamps.IsNull(i))
				timestampSum.add(timestamps.Value(i))
				readTimestamps[timestamps.Value(i)]++
				delete(expectedTimestamps, timestamps.Value(i))
			}
			return nil
		}),
	)

	nonUnique := make([]int64, 0)
	for k, v := range readTimestamps {
		if v > 1 {
			nonUnique = append(nonUnique, k)
		}
	}

	if w.timestampSum.count != timestampSum.count {
		if w.timestampSum.count < timestampSum.count {
			// Duplicate data found.
			t.Fatalf(
				"too many rows read. wrote %d and found %d. timestamps found more than once: %v",
				w.timestampSum.count, timestampSum.count, nonUnique,
			)
		}

		// Missing rows. Double check it's not due to dropping WAL entries
		// (which is valid). If the missing writes were written to the WAL then
		// this is real data loss.
		// TODO(asubiotto): The verification code is a little brittle. It is
		//  possible that not all rows written are reflected in a read if a
		//  restart happened with WAL entries pending. We attempt to verify by
		//  subsequently comparing the number of rows read to number of rows
		//  committed to WAL.
		//  However, for some reason we seem to commit fewer entries than are
		//  read.
		//  The ColumnStore.Close call below should make this impossible since
		//  it should drain any pending WAL entries (and there are no writes in
		//  flight). This requires some more investigation and thought. For now,
		//  this check is fine.
		//  One option is to make sure no missing writes (i.e. any remaining
		//  timestamps in expectedTimestamps) are contained in
		//  logStoreWrapper.acc.timestamps. I'm worried this would hide real
		//  data loss since the rows written to the log store seem to be much
		//  less than expected.

		// Drain currently pending WAL entries by closing.
		require.NoError(t, c.Close())

		require.Equal(
			t,
			logStoreWrapper.acc.count,
			timestampSum.count,
			"number of rows mismatch, wrote %d, committed %d to WAL, and read %d\n"+
				"timestamps that were expected and not found: %v\n"+
				"timestamps that were encountered more than once: %v",
			w.timestampSum.count, logStoreWrapper.acc.count, timestampSum.count,
			expectedTimestamps, nonUnique,
		)
	}
	if w.timestampSum.sum != timestampSum.sum {
		require.Equal(
			t,
			logStoreWrapper.acc.sum,
			timestampSum.sum,
		)
	}
}
