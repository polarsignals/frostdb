package frostdb

import (
	"github.com/polarsignals/wal"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	filewal "github.com/polarsignals/frostdb/wal"

	"github.com/polarsignals/frostdb/index"
)

var (
	descTxHighWatermark = prometheus.NewDesc(
		"frostdb_tx_high_watermark",
		"The highest transaction number that has been released to be read",
		[]string{"db"}, nil,
	)
	descActiveBlockSize = prometheus.NewDesc(
		"frostdb_table_active_block_size",
		"Size of the active table block in bytes.",
		[]string{"db", "table"}, nil,
	)
)

// collector is a custom prometheus collector that exports metrics from live
// databases and tables.
type collector struct {
	s *ColumnStore
}

var _ prometheus.Collector = (*collector)(nil)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTxHighWatermark
	ch <- descActiveBlockSize
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	for _, dbName := range c.s.DBs() {
		db, err := c.s.GetDB(dbName)
		if err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(descTxHighWatermark, prometheus.GaugeValue, float64(db.HighWatermark()), dbName)
		for _, tableName := range db.TableNames() {
			table, err := db.GetTable(tableName)
			if err != nil {
				continue
			}
			activeBlock := table.ActiveBlock()
			if activeBlock == nil {
				continue
			}
			ch <- prometheus.MustNewConstMetric(descActiveBlockSize, prometheus.GaugeValue, float64(activeBlock.Size()), dbName, tableName)
		}
	}
}

// globalMetrics defines the store-level metrics registered at instantiation.
// Most metrics are not directly accessed, and instead provided to components
// at instantiation time with preset labels.
type globalMetrics struct {
	shutdownDuration  prometheus.Histogram
	shutdownStarted   prometheus.Counter
	shutdownCompleted prometheus.Counter
	dbMetrics         struct {
		snapshotMetrics struct {
			snapshotsTotal            *prometheus.CounterVec
			snapshotFileSizeBytes     *prometheus.GaugeVec
			snapshotDurationHistogram *prometheus.HistogramVec
		}
		walMetrics struct {
			bytesWritten          *prometheus.CounterVec
			entriesWritten        *prometheus.CounterVec
			appends               *prometheus.CounterVec
			entryBytesRead        *prometheus.CounterVec
			entriesRead           *prometheus.CounterVec
			segmentRotations      *prometheus.CounterVec
			entriesTruncated      *prometheus.CounterVec
			truncations           *prometheus.CounterVec
			lastSegmentAgeSeconds *prometheus.GaugeVec
		}
		fileWalMetrics struct {
			failedLogs            *prometheus.CounterVec
			lastTruncationAt      *prometheus.GaugeVec
			walRepairs            *prometheus.CounterVec
			walRepairsLostRecords *prometheus.CounterVec
			walCloseTimeouts      *prometheus.CounterVec
			walQueueSize          *prometheus.GaugeVec
		}
	}
	tableMetrics struct {
		blockPersisted       *prometheus.CounterVec
		blockRotated         *prometheus.CounterVec
		rowsInserted         *prometheus.CounterVec
		rowBytesInserted     *prometheus.CounterVec
		zeroRowsInserted     *prometheus.CounterVec
		rowInsertSize        *prometheus.HistogramVec
		lastCompletedBlockTx *prometheus.GaugeVec
		numParts             *prometheus.GaugeVec
		indexMetrics         struct {
			compactions        *prometheus.CounterVec
			levelSize          *prometheus.GaugeVec
			compactionDuration *prometheus.HistogramVec
		}
	}
}

func makeLabelsForDBMetric(extraLabels ...string) []string {
	return append([]string{"db"}, extraLabels...)
}

func makeLabelsForTablesMetrics(extraLabels ...string) []string {
	return append([]string{"db", "table"}, extraLabels...)
}

func makeAndRegisterGlobalMetrics(unwrappedReg prometheus.Registerer) globalMetrics {
	m := globalMetrics{
		shutdownDuration: promauto.With(unwrappedReg).NewHistogram(prometheus.HistogramOpts{
			Name: "frostdb_shutdown_duration",
			Help: "time it takes for the columnarstore to complete a full shutdown.",
		}),
		shutdownStarted: promauto.With(unwrappedReg).NewCounter(prometheus.CounterOpts{
			Name: "frostdb_shutdown_started",
			Help: "Indicates a shutdown of the columnarstore has started.",
		}),
		shutdownCompleted: promauto.With(unwrappedReg).NewCounter(prometheus.CounterOpts{
			Name: "frostdb_shutdown_completed",
			Help: "Indicates a shutdown of the columnarstore has completed.",
		}),
	}

	// DB metrics.
	{
		// Snapshot metrics.
		{
			reg := prometheus.WrapRegistererWithPrefix("frostdb_snapshot_", unwrappedReg)
			m.dbMetrics.snapshotMetrics.snapshotsTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "total",
				Help: "Total number of snapshots",
			}, makeLabelsForDBMetric("success"))
			m.dbMetrics.snapshotMetrics.snapshotFileSizeBytes = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
				Name: "file_size_bytes",
				Help: "Size of snapshots in bytes",
			}, makeLabelsForDBMetric())
			m.dbMetrics.snapshotMetrics.snapshotDurationHistogram = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
				Name:    "duration_seconds",
				Help:    "Duration of snapshots in seconds",
				Buckets: prometheus.ExponentialBucketsRange(1, 60, 5),
			}, makeLabelsForDBMetric())
		}
		// WAL metrics.
		{
			reg := prometheus.WrapRegistererWithPrefix("frostdb_wal_", unwrappedReg)
			m.dbMetrics.walMetrics.bytesWritten = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "entry_bytes_written",
				Help: "entry_bytes_written counts the bytes of log entry after encoding." +
					" Actual bytes written to disk might be slightly higher as it" +
					" includes headers and index entries.",
			}, makeLabelsForDBMetric())
			m.dbMetrics.walMetrics.entriesWritten = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "entries_written",
				Help: "entries_written counts the number of entries written.",
			}, makeLabelsForDBMetric())
			m.dbMetrics.walMetrics.appends = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "appends",
				Help: "appends counts the number of calls to StoreLog(s) i.e." +
					" number of batches of entries appended.",
			}, makeLabelsForDBMetric())
			m.dbMetrics.walMetrics.entryBytesRead = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "entry_bytes_read",
				Help: "entry_bytes_read counts the bytes of log entry read from" +
					" segments before decoding. actual bytes read from disk might be higher" +
					" as it includes headers and index entries and possible secondary reads" +
					" for large entries that don't fit in buffers.",
			}, makeLabelsForDBMetric())
			m.dbMetrics.walMetrics.entriesRead = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "entries_read",
				Help: "entries_read counts the number of calls to get_log.",
			}, makeLabelsForDBMetric())
			m.dbMetrics.walMetrics.segmentRotations = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "segment_rotations",
				Help: "segment_rotations counts how many times we move to a new segment file.",
			}, makeLabelsForDBMetric())
			m.dbMetrics.walMetrics.entriesTruncated = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "entries_truncated_total",
				Help: "entries_truncated counts how many log entries have been truncated" +
					" from the front or back.",
			}, makeLabelsForDBMetric("type"))
			m.dbMetrics.walMetrics.truncations = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "truncations_total",
				Help: "truncations is the number of truncate calls categorized by whether" +
					" the call was successful or not.",
			}, makeLabelsForDBMetric("type", "success"))
			m.dbMetrics.walMetrics.lastSegmentAgeSeconds = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
				Name: "last_segment_age_seconds",
				Help: "last_segment_age_seconds is a gauge that is set each time we" +
					" rotate a segment and describes the number of seconds between when" +
					" that segment file was first created and when it was sealed. this" +
					" gives a rough estimate how quickly writes are filling the disk.",
			}, makeLabelsForDBMetric())
		}
		// FileWAL metrics.
		{
			reg := prometheus.WrapRegistererWithPrefix("frostdb_wal_", unwrappedReg)
			m.dbMetrics.fileWalMetrics.failedLogs = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "failed_logs_total",
				Help: "Number of failed WAL logs",
			}, makeLabelsForDBMetric())
			m.dbMetrics.fileWalMetrics.lastTruncationAt = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
				Name: "last_truncation_at",
				Help: "The last transaction the WAL was truncated to",
			}, makeLabelsForDBMetric())
			m.dbMetrics.fileWalMetrics.walRepairs = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "repairs_total",
				Help: "The number of times the WAL had to be repaired (truncated) due to corrupt records",
			}, makeLabelsForDBMetric())
			m.dbMetrics.fileWalMetrics.walRepairsLostRecords = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "repairs_lost_records_total",
				Help: "The number of WAL records lost due to WAL repairs (truncations)",
			}, makeLabelsForDBMetric())
			m.dbMetrics.fileWalMetrics.walCloseTimeouts = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "close_timeouts_total",
				Help: "The number of times the WAL failed to close due to a timeout",
			}, makeLabelsForDBMetric())
			m.dbMetrics.fileWalMetrics.walQueueSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
				Name: "queue_size",
				Help: "The number of unprocessed requests in the WAL queue",
			}, makeLabelsForDBMetric())
		}
	}

	// Table metrics.
	{
		reg := prometheus.WrapRegistererWithPrefix("frostdb_table_", unwrappedReg)
		m.tableMetrics.blockPersisted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "blocks_persisted_total",
			Help: "Number of table blocks that have been persisted.",
		}, makeLabelsForTablesMetrics())
		m.tableMetrics.blockRotated = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "blocks_rotated_total",
			Help: "Number of table blocks that have been rotated.",
		}, makeLabelsForTablesMetrics())
		m.tableMetrics.rowsInserted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "rows_inserted_total",
			Help: "Number of rows inserted into table.",
		}, makeLabelsForTablesMetrics())
		m.tableMetrics.rowBytesInserted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "row_bytes_inserted_total",
			Help: "Number of bytes inserted into table.",
		}, makeLabelsForTablesMetrics())
		m.tableMetrics.zeroRowsInserted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "zero_rows_inserted_total",
			Help: "Number of times it was attempted to insert zero rows into the table.",
		}, makeLabelsForTablesMetrics())
		m.tableMetrics.rowInsertSize = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "row_insert_size",
			Help:    "Size of batch inserts into table.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		}, makeLabelsForTablesMetrics())
		m.tableMetrics.lastCompletedBlockTx = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "last_completed_block_tx",
			Help: "Last completed block transaction.",
		}, makeLabelsForTablesMetrics())
		m.tableMetrics.numParts = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "num_parts",
			Help: "Number of parts currently active.",
		}, makeLabelsForTablesMetrics())

		// LSM metrics.
		{
			reg := prometheus.WrapRegistererWithPrefix("frostdb_lsm_", unwrappedReg)
			m.tableMetrics.indexMetrics.compactions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "compactions_total",
				Help: "The total number of compactions that have occurred.",
			}, makeLabelsForTablesMetrics("level"))

			m.tableMetrics.indexMetrics.levelSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
				Name: "level_size_bytes",
				Help: "The size of the level in bytes.",
			}, makeLabelsForTablesMetrics("level"))

			m.tableMetrics.indexMetrics.compactionDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
				Name:                        "compaction_total_duration_seconds",
				Help:                        "Total compaction duration",
				NativeHistogramBucketFactor: 1.1,
			}, makeLabelsForTablesMetrics())
		}
	}
	return m
}

type snapshotMetrics struct {
	snapshotsTotal            *prometheus.CounterVec
	snapshotFileSizeBytes     prometheus.Gauge
	snapshotDurationHistogram prometheus.Observer
}

func (m globalMetrics) snapshotMetricsForDB(dbName string) snapshotMetrics {
	return snapshotMetrics{
		snapshotsTotal:            m.dbMetrics.snapshotMetrics.snapshotsTotal.MustCurryWith(prometheus.Labels{"db": dbName}),
		snapshotFileSizeBytes:     m.dbMetrics.snapshotMetrics.snapshotFileSizeBytes.WithLabelValues(dbName),
		snapshotDurationHistogram: m.dbMetrics.snapshotMetrics.snapshotDurationHistogram.WithLabelValues(dbName),
	}
}

type tableMetricsProvider struct {
	dbName string
	m      globalMetrics
}

type tableMetrics struct {
	blockPersisted       prometheus.Counter
	blockRotated         prometheus.Counter
	rowsInserted         prometheus.Counter
	rowBytesInserted     prometheus.Counter
	zeroRowsInserted     prometheus.Counter
	rowInsertSize        prometheus.Observer
	lastCompletedBlockTx prometheus.Gauge
	numParts             prometheus.Gauge

	indexMetrics index.LSMMetrics
}

func (p tableMetricsProvider) metricsForTable(tableName string) tableMetrics {
	return tableMetrics{
		blockPersisted:       p.m.tableMetrics.blockPersisted.WithLabelValues(p.dbName, tableName),
		blockRotated:         p.m.tableMetrics.blockRotated.WithLabelValues(p.dbName, tableName),
		rowsInserted:         p.m.tableMetrics.rowsInserted.WithLabelValues(p.dbName, tableName),
		rowBytesInserted:     p.m.tableMetrics.rowBytesInserted.WithLabelValues(p.dbName, tableName),
		zeroRowsInserted:     p.m.tableMetrics.zeroRowsInserted.WithLabelValues(p.dbName, tableName),
		rowInsertSize:        p.m.tableMetrics.rowInsertSize.WithLabelValues(p.dbName, tableName),
		lastCompletedBlockTx: p.m.tableMetrics.lastCompletedBlockTx.WithLabelValues(p.dbName, tableName),
		numParts:             p.m.tableMetrics.numParts.WithLabelValues(p.dbName, tableName),
		indexMetrics: index.LSMMetrics{
			Compactions:        p.m.tableMetrics.indexMetrics.compactions.MustCurryWith(prometheus.Labels{"db": p.dbName, "table": tableName}),
			LevelSize:          p.m.tableMetrics.indexMetrics.levelSize.MustCurryWith(prometheus.Labels{"db": p.dbName, "table": tableName}),
			CompactionDuration: p.m.tableMetrics.indexMetrics.compactionDuration.WithLabelValues(p.dbName, tableName),
		},
	}
}

func (m globalMetrics) metricsForWAL(dbName string) *wal.Metrics {
	return &wal.Metrics{
		BytesWritten:          m.dbMetrics.walMetrics.bytesWritten.WithLabelValues(dbName),
		EntriesWritten:        m.dbMetrics.walMetrics.entriesWritten.WithLabelValues(dbName),
		Appends:               m.dbMetrics.walMetrics.appends.WithLabelValues(dbName),
		EntryBytesRead:        m.dbMetrics.walMetrics.entryBytesRead.WithLabelValues(dbName),
		EntriesRead:           m.dbMetrics.walMetrics.entriesRead.WithLabelValues(dbName),
		SegmentRotations:      m.dbMetrics.walMetrics.segmentRotations.WithLabelValues(dbName),
		EntriesTruncated:      m.dbMetrics.walMetrics.entriesTruncated.MustCurryWith(prometheus.Labels{"db": dbName}),
		Truncations:           m.dbMetrics.walMetrics.truncations.MustCurryWith(prometheus.Labels{"db": dbName}),
		LastSegmentAgeSeconds: m.dbMetrics.walMetrics.lastSegmentAgeSeconds.WithLabelValues(dbName),
	}
}

func (m globalMetrics) metricsForFileWAL(dbName string) *filewal.Metrics {
	return &filewal.Metrics{
		FailedLogs:            m.dbMetrics.fileWalMetrics.failedLogs.WithLabelValues(dbName),
		LastTruncationAt:      m.dbMetrics.fileWalMetrics.lastTruncationAt.WithLabelValues(dbName),
		WalRepairs:            m.dbMetrics.fileWalMetrics.walRepairs.WithLabelValues(dbName),
		WalRepairsLostRecords: m.dbMetrics.fileWalMetrics.walRepairsLostRecords.WithLabelValues(dbName),
		WalCloseTimeouts:      m.dbMetrics.fileWalMetrics.walCloseTimeouts.WithLabelValues(dbName),
		WalQueueSize:          m.dbMetrics.fileWalMetrics.walQueueSize.WithLabelValues(dbName),
	}
}
