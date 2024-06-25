package wal

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/wal"
	"github.com/polarsignals/wal/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
)

type ReplayHandlerFunc func(tx uint64, record *walpb.Record) error

type NopWAL struct{}

func (w *NopWAL) Close() error {
	return nil
}

func (w *NopWAL) Log(_ uint64, _ *walpb.Record) error {
	return nil
}

func (w *NopWAL) Replay(_ uint64, _ ReplayHandlerFunc) error {
	return nil
}

func (w *NopWAL) LogRecord(_ uint64, _ string, _ arrow.Record) error {
	return nil
}

func (w *NopWAL) Truncate(_ uint64) error {
	return nil
}

func (w *NopWAL) Reset(_ uint64) error {
	return nil
}

func (w *NopWAL) FirstIndex() (uint64, error) {
	return 0, nil
}

func (w *NopWAL) LastIndex() (uint64, error) {
	return 0, nil
}

type Metrics struct {
	FailedLogs            prometheus.Counter
	LastTruncationAt      prometheus.Gauge
	WalRepairs            prometheus.Counter
	WalRepairsLostRecords prometheus.Counter
	WalCloseTimeouts      prometheus.Counter
	WalQueueSize          prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{
		FailedLogs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "failed_logs_total",
			Help: "Number of failed WAL logs",
		}),
		LastTruncationAt: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "last_truncation_at",
			Help: "The last transaction the WAL was truncated to",
		}),
		WalRepairs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "repairs_total",
			Help: "The number of times the WAL had to be repaired (truncated) due to corrupt records",
		}),
		WalRepairsLostRecords: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "repairs_lost_records_total",
			Help: "The number of WAL records lost due to WAL repairs (truncations)",
		}),
		WalCloseTimeouts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "close_timeouts_total",
			Help: "The number of times the WAL failed to close due to a timeout",
		}),
		WalQueueSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "queue_size",
			Help: "The number of unprocessed requests in the WAL queue",
		}),
	}
}

const (
	dirPerms           = os.FileMode(0o750)
	progressLogTimeout = 10 * time.Second
)

type FileWAL struct {
	logger log.Logger
	path   string
	log    wal.LogStore

	metrics      *Metrics
	storeMetrics *wal.Metrics

	logRequestCh   chan *logRequest
	logRequestPool *sync.Pool
	arrowBufPool   *sync.Pool
	protected      struct {
		sync.Mutex
		queue logRequestQueue
		// truncateTx is set when the caller wishes to perform a truncation. The
		// WAL will keep on logging records up to and including this txn and
		// then perform a truncation. If another truncate call occurs in the
		// meantime, the highest txn will be used.
		truncateTx uint64
		// nextTx is the next expected txn. The FileWAL will only log a record
		// with this txn.
		nextTx uint64
	}

	// scratch memory reused to reduce allocations.
	scratch struct {
		walBatch []types.LogEntry
		reqBatch []*logRequest
	}

	// segmentSize indicates what the underlying WAL segment size is. This helps
	// the run goroutine size batches more or less appropriately.
	segmentSize int
	// lastBatchWrite is used to determine when to force a close of the WAL.
	lastBatchWrite time.Time

	cancel       func()
	shutdownCh   chan struct{}
	closeTimeout time.Duration

	// forceProcess is a channel to force processing of the pending log queue.
	forceProcess chan struct{}

	newLogStoreWrapper func(wal.LogStore) wal.LogStore
	ticker             Ticker
	testingDroppedLogs func([]types.LogEntry)
}

type logRequest struct {
	tx   uint64
	data []byte
	// onLog is set for callers to be notified when the data has been logged,
	// either successfully or not.
	onLog func(error)
}

// min-heap based priority queue to synchronize log requests to be in order of
// transactions.
type logRequestQueue []*logRequest

func (q logRequestQueue) Len() int           { return len(q) }
func (q logRequestQueue) Less(i, j int) bool { return q[i].tx < q[j].tx }
func (q logRequestQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

func (q *logRequestQueue) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*q = append(*q, x.(*logRequest))
}

func (q *logRequestQueue) Pop() any {
	old := *q
	n := len(old)
	x := old[n-1]
	// Remove this reference to a logRequest since the GC considers the popped
	// element still accessible otherwise. Since these are sync pooled, we want
	// to defer object lifetime management to the pool without interfering.
	old[n-1] = nil
	*q = old[0 : n-1]
	return x
}

type Option func(*FileWAL)

func WithTestingLogStoreWrapper(newLogStoreWrapper func(wal.LogStore) wal.LogStore) Option {
	return func(w *FileWAL) {
		w.newLogStoreWrapper = newLogStoreWrapper
	}
}

func WithMetrics(m *Metrics) Option {
	return func(w *FileWAL) {
		w.metrics = m
	}
}

func WithStoreMetrics(m *wal.Metrics) Option {
	return func(w *FileWAL) {
		w.storeMetrics = m
	}
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	*time.Ticker
}

func (t realTicker) C() <-chan time.Time {
	return t.Ticker.C
}

// WithTestingLoopTicker allows the caller to force processing of pending WAL
// entries by providing a custom ticker implementation.
func WithTestingLoopTicker(t Ticker) Option {
	return func(w *FileWAL) {
		w.ticker = t
	}
}

// WithTestingCallbackWithDroppedLogsOnClose is called when the WAL times out on
// close with all the entries that could not be written.
func WithTestingCallbackWithDroppedLogsOnClose(cb func([]types.LogEntry)) Option {
	return func(w *FileWAL) {
		w.testingDroppedLogs = cb
	}
}

func Open(
	logger log.Logger,
	path string,
	opts ...Option,
) (*FileWAL, error) {
	if err := os.MkdirAll(path, dirPerms); err != nil {
		return nil, err
	}

	segmentSize := wal.DefaultSegmentSize
	w := &FileWAL{
		logger:       logger,
		path:         path,
		logRequestCh: make(chan *logRequest),
		logRequestPool: &sync.Pool{
			New: func() any {
				return &logRequest{
					data: make([]byte, 1024),
				}
			},
		},
		arrowBufPool: &sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		},
		closeTimeout: 1 * time.Second,
		segmentSize:  segmentSize,
		shutdownCh:   make(chan struct{}),
		forceProcess: make(chan struct{}, 1),
	}

	for _, o := range opts {
		o(w)
	}

	logStore, err := wal.Open(path, wal.WithLogger(logger), wal.WithMetrics(w.storeMetrics), wal.WithSegmentSize(segmentSize))
	if err != nil {
		return nil, err
	}

	lastIndex, err := logStore.LastIndex()
	if err != nil {
		return nil, err
	}
	w.protected.nextTx = lastIndex + 1

	if w.newLogStoreWrapper != nil {
		w.log = w.newLogStoreWrapper(logStore)
	} else {
		w.log = logStore
	}
	if w.metrics == nil {
		w.metrics = newMetrics(prometheus.NewRegistry())
	}

	w.scratch.walBatch = make([]types.LogEntry, 0, 64)
	w.scratch.reqBatch = make([]*logRequest, 0, 64)

	return w, nil
}

func (w *FileWAL) run(ctx context.Context) {
	const defaultTickTime = 50 * time.Millisecond
	if w.ticker == nil {
		w.ticker = realTicker{Ticker: time.NewTicker(defaultTickTime)}
	}
	defer w.ticker.Stop()
	// lastQueueSize is only used on shutdown to reduce debug logging verbosity.
	lastQueueSize := 0
	w.lastBatchWrite = time.Now()

	for {
		select {
		case <-ctx.Done():
			// Need to drain the queue before we can shutdown, so
			// proactively try to process entries.
			w.process()

			w.protected.Lock()
			n := w.protected.queue.Len()
			w.protected.Unlock()
			if n > 0 {
				// Force the WAL to close after some a timeout.
				if w.closeTimeout > 0 && time.Since(w.lastBatchWrite) > w.closeTimeout {
					w.metrics.WalCloseTimeouts.Inc()
					level.Error(w.logger).Log(
						"msg", "WAL timed out attempting to close",
					)
					if w.testingDroppedLogs != nil {
						batch := make([]types.LogEntry, 0, n)
						w.protected.Lock()
						for _, v := range w.protected.queue {
							batch = append(batch, types.LogEntry{Index: v.tx, Data: v.data})
						}
						w.protected.Unlock()
						w.testingDroppedLogs(batch)
					}
					return
				}

				if n == lastQueueSize {
					// No progress made.
					time.Sleep(defaultTickTime)
					continue
				}

				lastQueueSize = n
				w.protected.Lock()
				minTx := w.protected.queue[0].tx
				w.protected.Unlock()
				lastIdx, err := w.log.LastIndex()
				logOpts := []any{
					"msg", "WAL received shutdown request; waiting for log request queue to drain",
					"queueSize", n,
					"minTx", minTx,
					"lastIndex", lastIdx,
				}
				if err != nil {
					logOpts = append(logOpts, "lastIndexErr", err)
				}
				level.Debug(w.logger).Log(logOpts...)
				continue
			}
			level.Debug(w.logger).Log("msg", "WAL shut down")
			return
		case <-w.ticker.C():
			w.process()
		case <-w.forceProcess:
			w.process()
		}
	}
}

func (w *FileWAL) releaseLogRequest(r *logRequest) {
	r.tx = 0
	r.data = r.data[:0]
	r.onLog = nil
	w.logRequestPool.Put(r)
}

func (w *FileWAL) process() {
	w.scratch.reqBatch = w.scratch.reqBatch[:0]

	w.protected.Lock()
	truncateTx := w.protected.truncateTx
	if truncateTx != 0 {
		// Reset truncateTx.
		w.protected.truncateTx = 0

		w.metrics.LastTruncationAt.Set(float64(truncateTx))
		level.Debug(w.logger).Log("msg", "truncating WAL", "tx", truncateTx)
		truncateErr := func() error {
			w.protected.Unlock()
			defer w.protected.Lock()
			return w.log.TruncateFront(truncateTx)
		}()

		if truncateErr != nil {
			level.Error(w.logger).Log("msg", "failed to truncate WAL", "tx", truncateTx, "err", truncateErr)
		} else {
			if truncateTx > w.protected.nextTx {
				// truncateTx is the new firstIndex of the WAL. If it is
				// greater than the next expected transaction, this was
				// a full WAL truncation/reset so both the first and
				// last index are now 0. The underlying WAL will allow a
				// record with any index to be written, however we only
				// want to allow the next index to be logged.
				w.protected.nextTx = truncateTx
				// Remove any records that have not yet been written and
				// are now below the nextTx.
				for w.protected.queue.Len() > 0 {
					if minTx := w.protected.queue[0].tx; minTx >= w.protected.nextTx {
						break
					}
					w.releaseLogRequest(heap.Pop(&w.protected.queue).(*logRequest))
					w.metrics.WalQueueSize.Sub(1)
				}
			}
			level.Debug(w.logger).Log("msg", "truncated WAL", "tx", truncateTx)
		}
	}

	batchSize := 0
	for w.protected.queue.Len() > 0 && batchSize < w.segmentSize {
		if minTx := w.protected.queue[0].tx; minTx != w.protected.nextTx {
			if minTx < w.protected.nextTx {
				// The next entry must be dropped otherwise progress
				// will never be made. Log a warning given this could
				// lead to missing data.
				level.Warn(w.logger).Log(
					"msg", "WAL cannot log a txn id that has already been seen; dropping entry",
					"expected", w.protected.nextTx,
					"found", minTx,
				)
				w.releaseLogRequest(heap.Pop(&w.protected.queue).(*logRequest))
				w.metrics.WalQueueSize.Sub(1)
				// Keep on going since there might be other transactions
				// below this one.
				continue
			}
			if sinceProgress := time.Since(w.lastBatchWrite); sinceProgress > progressLogTimeout {
				level.Info(w.logger).Log(
					"msg", "wal has not made progress",
					"since", sinceProgress,
					"next_expected_tx", w.protected.nextTx,
					"min_tx", minTx,
				)
			}
			// Next expected tx has not yet been seen.
			break
		}
		r := heap.Pop(&w.protected.queue).(*logRequest)
		w.metrics.WalQueueSize.Sub(1)
		w.scratch.reqBatch = append(w.scratch.reqBatch, r)
		batchSize += len(r.data)
		w.protected.nextTx++
	}
	w.protected.Unlock()

	if len(w.scratch.reqBatch) == 0 && truncateTx == 0 {
		// No records to log or truncations.
		return
	}

	w.scratch.walBatch = w.scratch.walBatch[:0]
	for _, r := range w.scratch.reqBatch {
		w.scratch.walBatch = append(w.scratch.walBatch, types.LogEntry{
			Index: r.tx,
			// No copy is needed here since the log request is only
			// released once these bytes are persisted.
			Data: r.data,
		})
	}

	if len(w.scratch.walBatch) > 0 {
		err := w.log.StoreLogs(w.scratch.walBatch)
		if err != nil {
			w.metrics.FailedLogs.Add(float64(len(w.scratch.reqBatch)))
			lastIndex, lastIndexErr := w.log.LastIndex()
			level.Error(w.logger).Log(
				"msg", "failed to write WAL batch",
				"err", err,
				"lastIndex", lastIndex,
				"lastIndexErr", lastIndexErr,
			)
		}
		for _, r := range w.scratch.reqBatch {
			if r.onLog != nil {
				r.onLog(err)
			}
		}
	}

	// Remove references to a logRequest since the GC considers the
	// popped element still accessible otherwise. Since these are sync
	// pooled, we want to defer object lifetime management to the pool
	// without interfering.
	for i := range w.scratch.walBatch {
		w.scratch.walBatch[i].Data = nil
	}
	for i, r := range w.scratch.reqBatch {
		w.scratch.reqBatch[i] = nil
		w.releaseLogRequest(r)
	}

	w.lastBatchWrite = time.Now()
}

// Truncate queues a truncation of the WAL at the given tx. Note that the
// truncation will be performed asynchronously. A nil error does not indicate
// a successful truncation.
func (w *FileWAL) Truncate(tx uint64) error {
	w.protected.Lock()
	defer w.protected.Unlock()
	if tx > w.protected.truncateTx {
		w.protected.truncateTx = tx
	}
	return nil
}

func (w *FileWAL) Reset(nextTx uint64) error {
	w.protected.Lock()
	defer w.protected.Unlock()
	// Drain any pending records.
	for w.protected.queue.Len() > 0 {
		_ = heap.Pop(&w.protected.queue)
	}
	// Set the next expected transaction.
	w.protected.nextTx = nextTx
	// This truncation will fully reset the underlying WAL. Any index can be
	// logged, but setting the nextTx above will ensure that only a record with
	// a matching txn will be accepted as the first record.
	return w.log.TruncateFront(math.MaxUint64)
}

func (w *FileWAL) Close() error {
	if w.cancel == nil { // wal was never started
		return nil
	}
	level.Debug(w.logger).Log("msg", "WAL received shutdown request; canceling run loop")
	w.cancel()
	<-w.shutdownCh
	return w.log.Close()
}

func (w *FileWAL) Log(tx uint64, record *walpb.Record) error {
	w.protected.Lock()
	nextTx := w.protected.nextTx
	w.protected.Unlock()
	if tx < nextTx {
		// Transaction should not be logged. This could happen if a truncation
		// has been issued simultaneously as logging a WAL record.
		level.Warn(w.logger).Log(
			"msg", "attempted to log txn below next expected txn",
			"tx", tx,
			"next_tx", nextTx,
		)
		return nil
	}

	synchronous := false
	size := record.SizeVT()
	if size != 0 {
		switch record.Entry.EntryType.(type) {
		case *walpb.Entry_NewTableBlock_:
			// NewTableBlock entries must be logged synchronously or we risk
			// losing data by allowing the active block to be overwritten before
			// we are sure that the previous table block's rotation event has
			// been durably persisted.
			synchronous = true
		default:
		}
	}

	r := w.logRequestPool.Get().(*logRequest)
	r.tx = tx
	var errCh chan error
	if synchronous {
		errCh = make(chan error, 1)
		r.onLog = func(err error) {
			errCh <- err
		}
	}
	if cap(r.data) < size {
		r.data = make([]byte, size)
	}
	r.data = r.data[:size]
	_, err := record.MarshalToSizedBufferVT(r.data)
	if err != nil {
		return err
	}

	w.protected.Lock()
	heap.Push(&w.protected.queue, r)
	w.metrics.WalQueueSize.Add(1)
	w.protected.Unlock()

	if !synchronous {
		return nil
	}
	w.forceProcess <- struct{}{}
	return <-errCh
}

func (w *FileWAL) getArrowBuf() *bytes.Buffer {
	return w.arrowBufPool.Get().(*bytes.Buffer)
}

func (w *FileWAL) putArrowBuf(b *bytes.Buffer) {
	b.Reset()
	w.arrowBufPool.Put(b)
}

func (w *FileWAL) writeRecord(buf *bytes.Buffer, record arrow.Record) error {
	writer := ipc.NewWriter(
		buf,
		ipc.WithSchema(record.Schema()),
	)
	defer writer.Close()

	return writer.Write(record)
}

func (w *FileWAL) LogRecord(tx uint64, table string, record arrow.Record) error {
	buf := w.getArrowBuf()
	defer w.putArrowBuf(buf)
	if err := w.writeRecord(buf, record); err != nil {
		return err
	}

	return w.Log(tx, &walpb.Record{
		Entry: &walpb.Entry{
			EntryType: &walpb.Entry_Write_{
				Write: &walpb.Entry_Write{
					Data:      buf.Bytes(),
					TableName: table,
					Arrow:     true,
				},
			},
		},
	})
}

func (w *FileWAL) FirstIndex() (uint64, error) {
	return w.log.FirstIndex()
}

func (w *FileWAL) LastIndex() (uint64, error) {
	return w.log.LastIndex()
}

func (w *FileWAL) Replay(tx uint64, handler ReplayHandlerFunc) (err error) {
	if handler == nil { // no handler provided
		return nil
	}

	logFirstIndex, err := w.log.FirstIndex()
	if err != nil {
		return fmt.Errorf("read first index: %w", err)
	}
	if tx == 0 || tx < logFirstIndex {
		tx = logFirstIndex
	}

	lastIndex, err := w.log.LastIndex()
	if err != nil {
		return fmt.Errorf("read last index: %w", err)
	}

	// FirstIndex and LastIndex returns zero when there is no WAL files.
	if tx == 0 || lastIndex == 0 {
		return nil
	}

	level.Debug(w.logger).Log("msg", "replaying WAL", "first_index", tx, "last_index", lastIndex)

	defer func() {
		// recover a panic of reading a transaction. Truncate the wal to the
		// last valid transaction.
		if r := recover(); r != nil {
			level.Error(w.logger).Log(
				"msg", "replaying WAL failed",
				"path", w.path,
				"first_index", logFirstIndex,
				"last_index", lastIndex,
				"offending_index", tx,
				"err", r,
			)
			if err = w.log.TruncateBack(tx - 1); err != nil {
				return
			}
			w.metrics.WalRepairs.Inc()
			w.metrics.WalRepairsLostRecords.Add(float64((lastIndex - tx) + 1))
		}
	}()

	var entry types.LogEntry
	for ; tx <= lastIndex; tx++ {
		level.Debug(w.logger).Log("msg", "replaying WAL record", "tx", tx)
		if err := w.log.GetLog(tx, &entry); err != nil {
			// Panic since this is most likely a corruption issue. The recover
			// call above will truncate the WAL to the last valid transaction.
			panic(fmt.Sprintf("read index %d: %v", tx, err))
		}

		record := &walpb.Record{}
		if err := record.UnmarshalVT(entry.Data); err != nil {
			// Panic since this is most likely a corruption issue. The recover
			// call above will truncate the WAL to the last valid transaction.
			panic(fmt.Sprintf("unmarshal WAL record: %v", err))
		}

		if err := handler(tx, record); err != nil {
			return fmt.Errorf("call replay handler: %w", err)
		}
	}

	return nil
}

func (w *FileWAL) RunAsync() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go func() {
		w.run(ctx)
		close(w.shutdownCh)
	}()
}
