package frostdb

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore/errutil"
)

type CompactionOption func(*CompactionConfig)

type CompactionConfig struct {
	concurrency          int
	interval             time.Duration
	l1ToGranuleSizeRatio float64
}

// NewCompactionConfig creates a new compaction config with the given options.
// If none are specified, a default compaction config is created.
func NewCompactionConfig(options ...CompactionOption) *CompactionConfig {
	c := &CompactionConfig{
		concurrency: runtime.GOMAXPROCS(0),
		interval:    100 * time.Millisecond,
		// 0.5 was chosen so that a level1 part takes up around half the space
		// in a full granule, allowing for level0 parts to grow up to the
		// remaining space before compaction. The higher this ratio, the less
		// new data is compacted and the more compactions occur. However, a
		// lower ratio implies leaving some parquet compression size savings on
		// the table.
		l1ToGranuleSizeRatio: 0.5,
	}
	for _, o := range options {
		o(c)
	}
	return c
}

// WithConcurrency specifies the number of concurrent goroutines compacting data
// for each database.
func WithConcurrency(concurrency int) CompactionOption {
	return func(c *CompactionConfig) {
		c.concurrency = concurrency
	}
}

// WithInterval specifies the compaction sweep interval.
func WithInterval(i time.Duration) CompactionOption {
	return func(c *CompactionConfig) {
		c.interval = i
	}
}

// WithL1ToGranuleSizeRatio sets the target level1 part size relative to the
// granule size. The closer this value is to 1, the more compacted data becomes
// with an expected rise in memory and CPU usage.
func WithL1ToGranuleSizeRatio(r float64) CompactionOption {
	return func(c *CompactionConfig) {
		c.l1ToGranuleSizeRatio = r
	}
}

type compactorPool struct {
	db *DB

	cfg    *CompactionConfig
	wg     sync.WaitGroup
	cancel context.CancelFunc
	paused atomic.Bool
}

func newCompactorPool(db *DB, cfg *CompactionConfig) *compactorPool {
	return &compactorPool{
		db:  db,
		cfg: cfg,
	}
}

func (c *compactorPool) start() {
	ctx, cancelFn := context.WithCancel(context.Background())
	c.cancel = cancelFn
	for i := 0; i < c.cfg.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.compactLoop(ctx)
		}()
	}
}

func (c *compactorPool) pause() {
	c.paused.Store(true)
}

func (c *compactorPool) resume() {
	c.paused.Store(false)
}

func (c *compactorPool) stop() {
	if c.cancel == nil {
		// Pool was not started.
		return
	}
	c.cancel()
	c.wg.Wait()
}

func (c *compactorPool) compactLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.cfg.interval):
			if c.paused.Load() {
				continue
			}
			c.db.mtx.RLock()
			tablesToCompact := make([]*Table, 0, len(c.db.tables))
			for _, table := range c.db.tables {
				tablesToCompact = append(tablesToCompact, table)
			}
			c.db.mtx.RUnlock()

			for _, table := range tablesToCompact {
				block, done, err := table.ActiveWriteBlock() // obtain a write block to prevent compaction and persistence from running at the same time
				if err != nil {
					continue // errors are for tables closing
				}
				if err := block.compact(c.cfg); err != nil {
					level.Warn(c.db.logger).Log("msg", "compaction failed", "err", err)
				}
				done()
			}
		}
	}
}

func (t *TableBlock) compact(cfg *CompactionConfig) error {
	maxL0 := int64(1024 * 1024 * 10)  // 10MB
	maxL1 := int64(1024 * 1024 * 100) // 100MB // TODO THOR make these settings
	var compactionErrors errutil.MultiError
	for i := 0; i < len(t.index.sizes); i++ {
		switch i {
		case int(L0):
			if size := atomic.LoadInt64(&t.index.sizes[i]); size > maxL0 {
				if err := t.index.merge(L0, t.table.Schema(), nil); err != nil {
					compactionErrors.Add(err)
				}
			}
		case int(L1):
			if size := atomic.LoadInt64(&t.index.sizes[i]); size > maxL1 {
				if err := t.index.merge(L1, t.table.Schema(), nil); err != nil {
					compactionErrors.Add(err)
				}
			}
		default:
		}
	}
	return compactionErrors.Err()
}

// compactionStats is a helper struct to collect metrics during compaction.
type compactionStats struct {
	level0SizeBefore        int64
	level0CountBefore       int
	level0NumRowsBefore     int64
	level1SizeBefore        int64
	level1CountBefore       int
	level1NumRowsBefore     int64
	numPartsOverlap         int
	uncompletedTxnPartCount int64
	level1SizeAfter         uint64
	level1CountAfter        int
	splits                  int
	totalDuration           time.Duration
}

func (s compactionStats) recordAndLog(m *compactionMetrics, l log.Logger) {
	m.level0SizeBefore.Observe(float64(s.level0SizeBefore))
	m.level0CountBefore.Observe(float64(s.level0CountBefore))
	m.level1SizeBefore.Observe(float64(s.level1SizeBefore))
	m.level1CountBefore.Observe(float64(s.level1CountBefore))
	m.numPartsOverlap.Observe(float64(s.numPartsOverlap))
	m.uncompletedTxnPartCount.Observe(float64(s.uncompletedTxnPartCount))
	m.level1SizeAfter.Observe(float64(s.level1SizeAfter))
	m.level1CountAfter.Observe(float64(s.level1CountAfter))
	m.splits.Observe(float64(s.splits))
	m.totalDuration.Observe(float64(s.totalDuration.Seconds()))

	level.Debug(l).Log(
		"msg", "compaction complete",
		"duration", s.totalDuration,
		"l0Before", fmt.Sprintf(
			"[sz=%s,cnt=%d]", humanize.IBytes(uint64(s.level0SizeBefore)), s.level0CountBefore,
		),
		"l1Before", fmt.Sprintf(
			"[sz=%s,cnt=%d]", humanize.IBytes(uint64(s.level1SizeBefore)), s.level1CountBefore,
		),
		"l1After", fmt.Sprintf(
			"[sz=%s,cnt=%d]", humanize.IBytes(uint64(s.level1SizeAfter)), s.level1CountAfter,
		),
		"overlaps", s.numPartsOverlap,
		"splits", s.splits,
	)
}

// compactionMetrics are metrics recorded on each successful compaction.
type compactionMetrics struct {
	level0SizeBefore        prometheus.Histogram
	level0CountBefore       prometheus.Histogram
	level1SizeBefore        prometheus.Histogram
	level1CountBefore       prometheus.Histogram
	numPartsOverlap         prometheus.Histogram
	uncompletedTxnPartCount prometheus.Histogram
	level1SizeAfter         prometheus.Histogram
	level1CountAfter        prometheus.Histogram
	splits                  prometheus.Histogram
	totalDuration           prometheus.Histogram
}

func newCompactionMetrics(reg prometheus.Registerer, granuleSize float64) *compactionMetrics {
	const (
		twoKiB = 2 << 10
		// metricResolution is the number of buckets in a histogram for sizes
		// and times.
		metricResolution = 25
	)
	minSize := float64(twoKiB)
	if granuleSize < minSize {
		// This should only happen in tests.
		minSize = 1
		granuleSize = twoKiB
	}
	sizeBuckets := prometheus.ExponentialBucketsRange(minSize, granuleSize, metricResolution)
	timeBuckets := prometheus.ExponentialBuckets(0.5, 2, metricResolution)
	countBuckets := prometheus.ExponentialBuckets(1, 2, 10)
	return &compactionMetrics{
		level0SizeBefore: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_level_0_size_before",
			Help:    "Total level 0 size when beginning compaction",
			Buckets: sizeBuckets,
		}),
		level0CountBefore: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_level_0_count_before",
			Help:    "Number of level 0 parts when beginning compaction",
			Buckets: countBuckets,
		}),
		level1SizeBefore: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_level_1_size_before",
			Help:    "Total level 1 size when beginning compaction",
			Buckets: sizeBuckets,
		}),
		level1CountBefore: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_level_1_count_before",
			Help:    "Number of level 0 parts when beginning compaction",
			Buckets: countBuckets,
		}),
		numPartsOverlap: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_num_parts_overlap",
			Help:    "Number of level 1 parts that overlapped with level 0 parts",
			Buckets: countBuckets,
		}),
		uncompletedTxnPartCount: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_uncompleted_txn_part_count",
			Help:    "Number of parts with uncompleted txns that could not be compacted",
			Buckets: countBuckets,
		}),
		level1SizeAfter: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_level_1_size_after",
			Help:    "Total level 1 size after compaction",
			Buckets: sizeBuckets,
		}),
		level1CountAfter: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_level_1_count_after",
			Help:    "Number of level 1 parts after compaction",
			Buckets: countBuckets,
		}),
		splits: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_splits",
			Help:    "Number of granule splits",
			Buckets: countBuckets,
		}),
		totalDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "frostdb_compaction_total_duration_seconds",
			Help:    "Total compaction duration",
			Buckets: timeBuckets,
		}),
	}
}
