package wal

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/wal"

	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
)

type NopWAL struct{}

func (w *NopWAL) Close() error {
	return nil
}

func (w *NopWAL) Log(tx uint64, record *walpb.Record) error {
	return nil
}

func (w *NopWAL) LogRecord(tx uint64, table string, record arrow.Record) error {
	return nil
}

func (w *NopWAL) Replay(handler func(tx uint64, record *walpb.Record) error) error {
	return nil
}

func (w *NopWAL) Truncate(tx uint64) error {
	return nil
}

func (w *NopWAL) FirstIndex() (uint64, error) {
	return 0, nil
}

type fileWALMetrics struct {
	recordsLogged        prometheus.Counter
	failedLogs           prometheus.Counter
	lastTruncationAt     prometheus.Gauge
	walTruncations       prometheus.Counter
	walTruncationsFailed prometheus.Counter
}

type FileWAL struct {
	logger log.Logger
	path   string
	log    *wal.Log

	nextTx uint64
	txmtx  *sync.Mutex

	metrics        *fileWALMetrics
	logRequestCh   chan *logRequest
	queue          *logRequestQueue
	logRequestPool *sync.Pool
	arrowBufPool   *sync.Pool
	mtx            *sync.Mutex

	cancel     func()
	shutdownCh chan struct{}
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
	*q = old[0 : n-1]
	return x
}

func Open(
	logger log.Logger,
	reg prometheus.Registerer,
	path string,
) (*FileWAL, error) {
	log, err := wal.Open(path, wal.DefaultOptions)
	if err != nil {
		if !errors.Is(err, wal.ErrCorrupt) {
			return nil, err
		}
		// Try to repair the corrupt WAL
		if err := tryRepairWAL(path); err != nil {
			return nil, fmt.Errorf("failed to repair corrupt WAL: %w", err)
		}
		log, err = wal.Open(path, wal.DefaultOptions)
		if err != nil {
			return nil, err
		}
	}

	w := &FileWAL{
		logger:       logger,
		path:         path,
		log:          log,
		nextTx:       1,
		txmtx:        &sync.Mutex{},
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
		mtx:   &sync.Mutex{},
		queue: &logRequestQueue{},
		metrics: &fileWALMetrics{
			recordsLogged: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "wal_records_logged_total",
				Help: "Number of records logged to WAL",
			}),
			failedLogs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "wal_failed_logs_total",
				Help: "Number of failed WAL logs",
			}),
			lastTruncationAt: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
				Name: "last_truncation_at",
				Help: "The last transaction the WAL was truncated to",
			}),
			walTruncations: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "wal_truncations_total",
				Help: "The number of WAL truncations",
			}),
			walTruncationsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "wal_truncations_failed_total",
				Help: "The number of WAL truncations",
			}),
		},
		shutdownCh: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go func() {
		w.run(ctx)
		close(w.shutdownCh)
	}()

	return w, nil
}

// tryRepairWAL operates on a corrupt WAL directory by removing the last file
// in the directory. Corruption can occur when the recorded size of an entry in
// the file does not correspond to its actual size. A better option would be to
// read the last WAL file and remove the corrupted entry, but this is good
// enough for now.
func tryRepairWAL(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	path = absPath

	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	if err := os.Remove(filepath.Join(absPath, entries[len(entries)-1].Name())); err != nil {
		return err
	}
	return nil
}

func (w *FileWAL) run(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	walBatch := &wal.Batch{}
	batch := make([]*logRequest, 0, 128) // random number is random
	for {
		select {
		case <-ctx.Done():
			w.mtx.Lock()
			n := w.queue.Len()
			w.mtx.Unlock()
			if n > 0 {
				// Need to drain the queue before we can shutdown.
				continue
			}
			return
		case <-ticker.C:
			w.txmtx.Lock()
			nextTx := w.nextTx
			w.txmtx.Unlock()
			batch := batch[:0]
			w.mtx.Lock()
			for w.queue.Len() > 0 {
				if minTx := (*w.queue)[0].tx; minTx != nextTx {
					if minTx < nextTx {
						// The next entry must be dropped otherwise progress
						// will never be made. Log a warning given this could
						// lead to missing data.
						level.Warn(w.logger).Log(
							"msg", "WAL cannot log a txn id that has already been seen; dropping entry",
							"expected", nextTx,
							"found", minTx,
						)
						_ = heap.Pop(w.queue)
					}
					break
				}
				r := heap.Pop(w.queue).(*logRequest)
				batch = append(batch, r)
				nextTx++
			}
			w.mtx.Unlock()
			if len(batch) == 0 {
				continue
			}

			walBatch.Clear()
			for _, r := range batch {
				walBatch.Write(r.tx, r.data)
			}

			err := w.log.WriteBatch(walBatch)
			if err != nil {
				w.metrics.failedLogs.Add(float64(len(batch)))
				lastIndex, lastIndexErr := w.log.LastIndex()
				level.Error(w.logger).Log(
					"msg", "failed to write WAL batch",
					"err", err,
					// Sprintf is used here because the logging package does not
					// support logging arbitrary values.
					"batch", fmt.Sprintf("%v", walBatch),
					"lastIndex", lastIndex,
					"lastIndexErr", lastIndexErr,
				)
			} else {
				w.metrics.recordsLogged.Add(float64(len(batch)))
			}

			for _, r := range batch {
				w.logRequestPool.Put(r)
			}

			w.txmtx.Lock()
			w.nextTx = nextTx
			w.txmtx.Unlock()
		}
	}
}

func (w *FileWAL) Truncate(tx uint64) error {
	w.metrics.lastTruncationAt.Set(float64(tx))
	w.metrics.walTruncations.Inc()

	level.Debug(w.logger).Log("msg", "truncating WAL", "tx", tx)
	err := w.log.TruncateFront(tx)
	if err != nil {
		level.Error(w.logger).Log("msg", "failed to truncate WAL", "tx", tx, "err", err)
		w.metrics.walTruncationsFailed.Inc()
		return err
	}
	level.Debug(w.logger).Log("msg", "truncated WAL", "tx", tx)

	return nil
}

func (w *FileWAL) Close() error {
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

	w.mtx.Lock()
	heap.Push(w.queue, r)
	w.mtx.Unlock()

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

	w.mtx.Lock()
	heap.Push(w.queue, r)
	w.mtx.Unlock()

	return nil
}

func (w *FileWAL) FirstIndex() (uint64, error) {
	return w.log.FirstIndex()
}

func (w *FileWAL) LastIndex() (uint64, error) {
	return w.log.LastIndex()
}

func (w *FileWAL) Replay(handler func(tx uint64, record *walpb.Record) error) error {
	firstIndex, err := w.log.FirstIndex()
	if err != nil {
		return fmt.Errorf("read first index: %w", err)
	}

	lastIndex, err := w.log.LastIndex()
	if err != nil {
		return fmt.Errorf("read last index: %w", err)
	}

	// FirstIndex and LastIndex returns zero when there is no WAL files.
	if firstIndex == 0 || lastIndex == 0 {
		return nil
	}

	level.Debug(w.logger).Log("msg", "replaying WAL", "first_index", firstIndex, "last_index", lastIndex)

	defer func() {
		// recover the panic to print more context. Exit afterwards regardless.
		if err := recover(); err != nil {
			level.Error(w.logger).Log("msg", "replaying WAL failed", "path", w.path, "first_index", firstIndex, "last_index", lastIndex, "err", err)
			os.Exit(2)
		}
	}()

	for tx := firstIndex; tx <= lastIndex; tx++ {
		level.Debug(w.logger).Log("msg", "replaying WAL record", "tx", tx)
		data, err := w.log.Read(tx)
		if err != nil {
			return fmt.Errorf("read index %d: %w", tx, err)
		}

		record := &walpb.Record{}
		if err := record.UnmarshalVT(data); err != nil {
			return fmt.Errorf("unmarshal WAL record: %w", err)
		}

		if err := handler(tx, record); err != nil {
			return fmt.Errorf("call replay handler: %w", err)
		}
	}

	w.txmtx.Lock()
	w.nextTx = lastIndex + 1
	w.txmtx.Unlock()
	return nil
}
