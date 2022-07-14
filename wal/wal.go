package wal

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/wal"
	"go.uber.org/atomic"

	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
)

type NopWAL struct{}

func (w *NopWAL) Close() error {
	return nil
}

func (w *NopWAL) Log(tx uint64, record *walpb.Record) error {
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
	logger  log.Logger
	path    string
	log     *wal.Log
	nextTx  *atomic.Uint64
	metrics *fileWALMetrics
}

func Open(
	logger log.Logger,
	reg prometheus.Registerer,
	path string,
) (*FileWAL, error) {
	log, err := wal.Open(path, wal.DefaultOptions)
	if err != nil {
		return nil, err
	}

	return &FileWAL{
		logger: logger,
		path:   path,
		log:    log,
		nextTx: atomic.NewUint64(1),
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
	}, nil
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
	return w.log.Close()
}

func (w *FileWAL) waitUntilTxTurn(tx uint64) {
	for w.nextTx.Load() != tx {
	}
}

func (w *FileWAL) Log(tx uint64, record *walpb.Record) error {
	w.metrics.recordsLogged.Inc()
	var err error
	defer func() {
		if err != nil {
			w.metrics.failedLogs.Inc()
		}
	}()

	var data []byte
	data, err = record.MarshalVT()
	if err != nil {
		return err
	}

	w.waitUntilTxTurn(tx)

	err = w.log.Write(tx, data)
	if err != nil {
		return err
	}

	w.nextTx.Inc()
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
		return err
	}

	lastIndex, err := w.log.LastIndex()
	if err != nil {
		return err
	}

	level.Debug(w.logger).Log("msg", "replaying WAL", "first_index", firstIndex, "last_index", lastIndex)

	for tx := firstIndex; tx <= lastIndex; tx++ {
		level.Debug(w.logger).Log("msg", "replaying WAL record", "tx", tx)
		data, err := w.log.Read(tx)
		if err != nil {
			return err
		}

		record := &walpb.Record{}
		if err := record.UnmarshalVT(data); err != nil {
			return err
		}

		if err := handler(tx, record); err != nil {
			return err
		}
	}

	w.nextTx.Store(lastIndex + 1)
	return nil
}
