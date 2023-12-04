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

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/ipc"
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

type fileWALMetrics struct {
	failedLogs            prometheus.Counter
	lastTruncationAt      prometheus.Gauge
	walRepairs            prometheus.Counter
	walRepairsLostRecords prometheus.Counter
	walCloseTimeouts      prometheus.Counter
	walQueueSize          prometheus.Gauge
}

const (
	dirPerms           = os.FileMode(0o750)
	progressLogTimeout = 10 * time.Second
)

type FileWAL struct {
	logger log.Logger
	path   string
	log    wal.LogStore

	metrics        *fileWALMetrics
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

	// segmentSize indicates what the underlying WAL segment size is. This helps
	// the run goroutine size batches more or less appropriately.
	segmentSize int
	// lastTimeProgressWasMade tracks just that and serves to log information in
	// case the WAL has not made progress in some time.
	lastTimeProgressWasMade time.Time

	cancel       func()
	shutdownCh   chan struct{}
	closeTimeout time.Duration
}

type logRequest struct {
	tx   uint64
	data []byte
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

func Open(
	logger log.Logger,
	reg prometheus.Registerer,
	path string,
) (*FileWAL, error) {
	if err := os.MkdirAll(path, dirPerms); err != nil {
		return nil, err
	}

	reg = prometheus.WrapRegistererWithPrefix("frostdb_wal_", reg)
	segmentSize := wal.DefaultSegmentSize
	logStore, err := wal.Open(path, wal.WithLogger(logger), wal.WithMetricsRegisterer(reg), wal.WithSegmentSize(segmentSize))
	if err != nil {
		return nil, err
	}

	lastIndex, err := logStore.LastIndex()
	if err != nil {
		return nil, err
	}

	w := &FileWAL{
		logger:       logger,
		path:         path,
		log:          logStore,
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
		metrics: &fileWALMetrics{
			failedLogs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "failed_logs_total",
				Help: "Number of failed WAL logs",
			}),
			lastTruncationAt: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
				Name: "last_truncation_at",
				Help: "The last transaction the WAL was truncated to",
			}),
			walRepairs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "repairs_total",
				Help: "The number of times the WAL had to be repaired (truncated) due to corrupt records",
			}),
			walRepairsLostRecords: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "repairs_lost_records_total",
				Help: "The number of WAL records lost due to WAL repairs (truncations)",
			}),
			walCloseTimeouts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "close_timeouts_total",
				Help: "The number of times the WAL failed to close due to a timeout",
			}),
			walQueueSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
				Name: "queue_size",
				Help: "The number of unprocessed requests in the WAL queue",
			}),
		},
		segmentSize: segmentSize,
		shutdownCh:  make(chan struct{}),
	}

	w.protected.nextTx = lastIndex + 1

	return w, nil
}

func (w *FileWAL) run(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	walBatch := make([]types.LogEntry, 0)
	batch := make([]*logRequest, 0, 128)
	// lastQueueSize is only used on shutdown to reduce debug logging verbosity.
	lastQueueSize := 0
	// lastBatchWrite is used to determine when to force a close of the WAL.
	lastBatchWrite := time.Now()

	for {
		select {
		case <-ctx.Done():
			w.protected.Lock()
			n := w.protected.queue.Len()
			w.protected.Unlock()
			if n > 0 {
				// force the WAL to close after some a timeout.
				if w.closeTimeout > 0 && time.Since(lastBatchWrite) > w.closeTimeout {
					w.metrics.walCloseTimeouts.Inc()
					level.Error(w.logger).Log(
						"msg", "WAL timed out attempting to close",
					)
					return
				}

				// Need to drain the queue before we can shutdown.
				if n == lastQueueSize {
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
		case <-ticker.C:
			batch := batch[:0]
			w.protected.Lock()
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
						w.logRequestPool.Put(heap.Pop(&w.protected.queue))
						w.metrics.walQueueSize.Sub(1)
						// Keep on going since there might be other transactions
						// below this one.
						continue
					}
					if sinceProgress := time.Since(w.lastTimeProgressWasMade); sinceProgress > progressLogTimeout {
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
				w.metrics.walQueueSize.Sub(1)
				batch = append(batch, r)
				batchSize += len(r.data)
				w.protected.nextTx++
			}
			w.lastTimeProgressWasMade = time.Now()
			// truncateTx will be non-zero if we either are about to log a
			// record with a txn past the txn to truncate, or we have logged one
			// in the past.
			truncateTx := uint64(0)
			if w.protected.truncateTx != 0 {
				truncateTx = w.protected.truncateTx
				w.protected.truncateTx = 0
			}
			w.protected.Unlock()
			if len(batch) == 0 && truncateTx == 0 {
				// No records to log or truncations.
				continue
			}

			walBatch = walBatch[:0]
			for _, r := range batch {
				walBatch = append(walBatch, types.LogEntry{
					Index: r.tx,
					// No copy is needed here since the log request is only
					// released once these bytes are persisted.
					Data: r.data,
				})
			}

			if len(walBatch) > 0 {
				if err := w.log.StoreLogs(walBatch); err != nil {
					w.metrics.failedLogs.Add(float64(len(batch)))
					lastIndex, lastIndexErr := w.log.LastIndex()
					level.Error(w.logger).Log(
						"msg", "failed to write WAL batch",
						"err", err,
						"lastIndex", lastIndex,
						"lastIndexErr", lastIndexErr,
					)
				}
			}

			if truncateTx != 0 {
				w.metrics.lastTruncationAt.Set(float64(truncateTx))
				level.Debug(w.logger).Log("msg", "truncating WAL", "tx", truncateTx)
				if err := w.log.TruncateFront(truncateTx); err != nil {
					level.Error(w.logger).Log("msg", "failed to truncate WAL", "tx", truncateTx, "err", err)
				} else {
					w.protected.Lock()
					if truncateTx > w.protected.nextTx {
						// truncateTx is the new firstIndex of the WAL. If it is
						// greater than the next expected transaction, this was
						// a full WAL truncation/reset so both the first and
						// last index are now 0. The underlying WAL will allow a
						// record with any index to be written, however we only
						// want to allow the next index to be logged.
						w.protected.nextTx = truncateTx + 1
						// Remove any records that have not yet been written and
						// are now below the nextTx.
						for w.protected.queue.Len() > 0 {
							if minTx := w.protected.queue[0].tx; minTx >= w.protected.nextTx {
								break
							}
							w.logRequestPool.Put(heap.Pop(&w.protected.queue))
							w.metrics.walQueueSize.Sub(1)
						}
					}
					w.protected.Unlock()
					level.Debug(w.logger).Log("msg", "truncated WAL", "tx", truncateTx)
				}
			}

			// Remove references to a logRequest since the GC considers the
			// popped element still accessible otherwise. Since these are sync
			// pooled, we want to defer object lifetime management to the pool
			// without interfering.
			for i := range walBatch {
				walBatch[i].Data = nil
			}

			for i, r := range batch {
				batch[i] = nil
				w.logRequestPool.Put(r)
			}

			lastBatchWrite = time.Now()
		}
	}
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
	r := w.logRequestPool.Get().(*logRequest)
	r.tx = tx
	size := record.SizeVT()
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
	w.metrics.walQueueSize.Add(1)
	w.protected.Unlock()

	return nil
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
	buf := w.getArrowBuf()
	defer w.putArrowBuf(buf)
	if err := w.writeRecord(buf, record); err != nil {
		return err
	}

	walrecord := &walpb.Record{
		Entry: &walpb.Entry{
			EntryType: &walpb.Entry_Write_{
				Write: &walpb.Entry_Write{
					Data:      buf.Bytes(),
					TableName: table,
					Arrow:     true,
				},
			},
		},
	}

	r := w.logRequestPool.Get().(*logRequest)
	r.tx = tx
	size := walrecord.SizeVT()
	if cap(r.data) < size {
		r.data = make([]byte, size)
	}
	r.data = r.data[:size]
	_, err := walrecord.MarshalToSizedBufferVT(r.data)
	if err != nil {
		return err
	}

	w.protected.Lock()
	heap.Push(&w.protected.queue, r)
	w.metrics.walQueueSize.Add(1)
	w.protected.Unlock()

	return nil
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
			w.metrics.walRepairs.Inc()
			w.metrics.walRepairsLostRecords.Add(float64((lastIndex - tx) + 1))
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
