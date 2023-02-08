package frostdb

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore/errutil"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
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
			c.db.mtx.RLock()
			tablesToCompact := make([]*Table, 0, len(c.db.tables))
			for _, table := range c.db.tables {
				tablesToCompact = append(tablesToCompact, table)
			}
			c.db.mtx.RUnlock()

			for _, table := range tablesToCompact {
				if err := table.ActiveBlock().compact(c.cfg); err != nil {
					level.Warn(c.db.logger).Log("msg", "compaction failed", "err", err)
				}
			}
		}
	}
}

func (t *TableBlock) compact(cfg *CompactionConfig) error {
	var compactionErrors errutil.MultiError
	index := t.Index()
	index.Ascend(func(i btree.Item) bool {
		granuleToCompact := i.(*Granule)
		if granuleToCompact.metadata.size.Load() < uint64(t.table.db.columnStore.granuleSizeBytes) {
			// Skip granule since its size is under the target size.
			return true
		}
		if !granuleToCompact.metadata.pruned.CompareAndSwap(0, 1) {
			// Someone else claimed this granule compaction.
			return true
		}

		defer t.table.metrics.compactions.Inc()
		if successful, err := t.compactGranule(granuleToCompact, cfg); !successful || err != nil {
			t.abortCompaction(granuleToCompact)
			if err != nil {
				compactionErrors.Add(err)
			}
		}
		return true
	})
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
			Name:    "compaction_level_0_size_before",
			Help:    "Total level 0 size when beginning compaction",
			Buckets: sizeBuckets,
		}),
		level0CountBefore: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_level_0_count_before",
			Help:    "Number of level 0 parts when beginning compaction",
			Buckets: countBuckets,
		}),
		level1SizeBefore: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_level_1_size_before",
			Help:    "Total level 1 size when beginning compaction",
			Buckets: sizeBuckets,
		}),
		level1CountBefore: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_level_1_count_before",
			Help:    "Number of level 0 parts when beginning compaction",
			Buckets: countBuckets,
		}),
		numPartsOverlap: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_num_parts_overlap",
			Help:    "Number of level 1 parts that overlapped with level 0 parts",
			Buckets: countBuckets,
		}),
		uncompletedTxnPartCount: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_uncompleted_txn_part_count",
			Help:    "Number of parts with uncompleted txns that could not be compacted",
			Buckets: countBuckets,
		}),
		level1SizeAfter: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_level_1_size_after",
			Help:    "Total level 1 size after compaction",
			Buckets: sizeBuckets,
		}),
		level1CountAfter: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_level_1_count_after",
			Help:    "Number of level 1 parts after compaction",
			Buckets: countBuckets,
		}),
		splits: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_splits",
			Help:    "Number of granule splits",
			Buckets: countBuckets,
		}),
		totalDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "compaction_total_duration_seconds",
			Help:    "Total compaction duration",
			Buckets: timeBuckets,
		}),
	}
}

// compactGranule is the "meat" of granule compaction. It returns whether the
// compaction succeeded or not as well as an error (there are cases in which
// false, nil is returned).
// The goal of this method is to optimize both memory usage and scan speed.
// Memory usage is optimized by merging multiple row groups into fewer row
// groups, since the more data there is in a row group, the higher the benefits
// of things like parquet compression (this is applied at the row group level).
// Optimizing for memory usage is a tradeoff, since the more data there is in a
// row group, the more sequential reads need to be done to find data that
// queries want. This is why we aim to have a granule maximum size, since
// searching for granules that satisfy query filters is an
// O(log(number of granules)) operation, so the more granules we have, the less
// O(n) work we have to do to search for row groups that satisfy query filters.
// The compaction logic is as follows:
//
//  1. Before any compaction has happened, granules contain a variable number of
//     parts (basically an encapsulation of a parquet file). These parts contain
//     a variable number of variable-length row groups. The row group size in
//     this case is the buffer size that was used by the client on the
//     corresponding insert call. The compaction strategy in this simple case is
//     to rewrite the row groups into multiple parts that contain optimally
//     sized row groups (set by the user using the WithRowGroupSize table
//     option). The new part sizes aim to be
//     l1ToGranuleSizeRatio*granuleSize bytes large (specified in the config).
//     The resulting parts are marked as compacted using compactionLevel1.
//
//  2. As more data is added to a granule compacted in point 1, the new data
//     will again be stored in a suboptimal multi-part format with multiple
//     variable length row groups. The same compaction as point 1 is performed
//     but only on the new data as long as it does not overlap with any level1
//     data (otherwise the overlapping parts are merged into new level1 parts).
//     If the sum of the part sizes is above the target granule size, a split is
//     performed, which simply means that the parts will be assigned to new
//     granules.
//
// Some things to be aware of:
//   - The current compaction strategy assumes there is no benefit to having
//     muliple parts over a single part as long as the row groups are of equal
//     length.
//   - Currently, our compaction strategy is based on a sweep on a timed
//     interval. This is OK for now, but ideally we would track metrics that
//     are proxies for memory size/scan speed that would trigger compactions.
//     For example, the ratio of uncompacted bytes to total bytes. Or performing
//     split operations based on load.
//
// A note on ordering guarantees:
// FrostDB guarantees ordering amongst granules. This means that if there are
// two granules in the index, all rows in granule A are less than granule B.
// However, within a granule, rows are only ordered at a part level since parts
// are created when a client appends rows and clients are not required to
// append in order. The current compaction strategy ensures that ordering
// guarantees between granules are upheld. Additionally, level1 parts will never
// overlap.
func (t *TableBlock) compactGranule(granule *Granule, cfg *CompactionConfig) (bool, error) {
	// Use the latest watermark as the tx id.
	tx := t.table.db.tx.Load()

	start := time.Now()
	level0Parts, level1Parts, partsWithUncompletedTxns, stats, err := collectPartsForCompaction(
		// Start compaction by adding a sentinel node to its parts list.
		tx, granule.parts.Sentinel(parts.Compacting),
	)
	if err != nil {
		return false, err
	}

	if len(level0Parts) == 0 && len(level1Parts) == 0 {
		// Edge case in which only parts with uncompleted txns are found and
		// they are already bigger than the max size. A future compaction cycle
		// will take care of this, so return that the compaction was not
		// successful
		return false, nil
	}

	partsBefore := len(level0Parts) + len(level1Parts)
	if len(level0Parts) > 0 {
		var err error
		level1Parts, err = compactLevel0IntoLevel1(
			t, tx, level0Parts, level1Parts, cfg.l1ToGranuleSizeRatio, &stats,
		)
		if err != nil {
			return false, err
		}
		stats.level1CountAfter += len(level1Parts)
	}

	// This sort is done to simplify the splitting of non-overlapping parts
	// amongst new granules. There might be something more clever we could do
	// here if we maintain stronger guarantees about the part ordering given
	// that level1 parts should already be sorted when collected at the start
	// of this method (since this is the only place level1 parts get added to
	// granules). However, it seems like new parts are prepended so the sort
	// order is inverted. Another option is to recursively split using
	// addPartToGranule, which would be cheap in the general case.
	sorter := parts.NewPartSorter(t.table.config.schema, level1Parts)
	sort.Sort(sorter)
	if sorter.Err() != nil {
		return false, fmt.Errorf("error sorting level1: %w", sorter.Err())
	}

	newParts, err := divideLevel1PartsForGranule(
		t,
		tx,
		level1Parts,
		// Set a maximum size of the l1 ratio to give "breathing room" to new
		// level0 parts. Otherwise, compactions will be initiated more often
		// and work on less data. This ends up creating granules half filled
		// up with a level1 part.
		int64(float64(t.table.db.columnStore.granuleSizeBytes)*cfg.l1ToGranuleSizeRatio),
	)
	if err != nil {
		return false, err
	}
	stats.splits = len(newParts) - 1

	newGranules := make([]*Granule, 0, len(newParts))
	partsAfter := 0
	for _, parts := range newParts {
		partsAfter += len(parts)
		newGranule, err := NewGranule(t.table.config, parts...)
		if err != nil {
			if err != nil {
				return false, fmt.Errorf("failed to create new granule: %w", err)
			}
		}
		t.table.metrics.granulesCreated.Inc()
		newGranules = append(newGranules, newGranule)
	}

	// Size calculation is done before adding the ignored parts to the new
	// granules.
	for _, g := range newGranules {
		stats.level1SizeAfter += g.metadata.size.Load()
	}

	// Add remaining parts onto new granules.
	for _, p := range partsWithUncompletedTxns {
		if err := addPartToGranule(newGranules, p); err != nil {
			return false, fmt.Errorf("add parts to granules: %w", err)
		}
	}

	// We disable compaction for new granules before allowing new inserts to be
	// propagated to them.
	for _, childGranule := range newGranules {
		childGranule.metadata.pruned.Store(1)
	}

	// We restore the possibility to trigger compaction after we exit the
	// function.
	defer func() {
		for _, childGranule := range newGranules {
			childGranule.metadata.pruned.Store(0)
		}
	}()

	// Set the newGranules pointer, so new writes will propogate into these new
	// granules.
	granule.newGranules = newGranules

	// Mark compaction complete in the granule; this will cause new writes to
	// start using the newGranules pointer. Iterate over the resulting part
	// list to copy any new parts that were added while we were compacting.
	var addPartErr error
	granule.parts.Sentinel(parts.Compacted).Iterate(func(p *parts.Part) bool {
		if err := addPartToGranule(newGranules, p); err != nil {
			addPartErr = err
			return false
		}
		return true
	})
	if addPartErr != nil {
		return false, fmt.Errorf("add part to granules: %w", addPartErr)
	}

	for {
		index := t.Index()
		t.mtx.Lock()
		newIdx := index.Clone() // TODO(THOR): we can't clone concurrently
		t.mtx.Unlock()

		if newIdx.Delete(granule) == nil {
			level.Error(t.logger).Log("msg", "failed to delete granule during split")
			return false, fmt.Errorf("failed to delete granule")
		}

		for _, g := range newGranules {
			if dupe := newIdx.ReplaceOrInsert(g); dupe != nil {
				level.Error(t.logger).Log("duplicate insert performed")
			}
		}

		// Point to the new index.
		if t.index.CompareAndSwap(index, newIdx) {
			sizeDiff := int64(stats.level1SizeAfter) - (stats.level0SizeBefore + stats.level1SizeBefore)
			t.size.Add(sizeDiff)

			t.table.metrics.numParts.Add(float64(int(stats.level1CountAfter) - partsBefore))
			break
		}
	}
	stats.totalDuration = time.Since(start)
	stats.recordAndLog(t.table.metrics.compactionMetrics, t.logger)
	// Release all records in L0 parts
	for _, p := range level0Parts {
		if r := p.Record(); r != nil {
			r.Release()
		}
	}
	return true, nil
}

// compactLevel0IntoLevel1 compacts the given level0Parts into level1Parts by
// merging them with overlapping level1 parts and producing parts that are up
// to the TableBlock's maximum granule size. These parts are guaranteed to be
// non-overlapping.
// The size limit is a best effort as there is currently no easy way to
// determine the size of a parquet file while writing row groups to it.
func compactLevel0IntoLevel1(
	t *TableBlock,
	tx uint64,
	level0Parts,
	level1Parts []*parts.Part,
	l1ToGranuleSizeRatio float64,
	stats *compactionStats,
) ([]*parts.Part, error) {
	// Verify whether the level0 parts overlap with level1 parts. If they do,
	// we need to merge the overlapping parts. Not merging these parts could
	// cause them to be split to different granules, rendering the index
	// useless.
	nonOverlapping := make([]*parts.Part, 0, len(level1Parts))
	partsToCompact := level0Parts
	// size and numRows are used to estimate the number of bytes per row so that
	// we can provide a maximum number of rows to write in writeRowGroups which
	// will more or less keep the compacted part under the maximum granule size.
	size := stats.level1SizeBefore
	numRows := stats.level1NumRowsBefore
	if len(level1Parts) == 0 {
		// If no level1 parts exist, then we estimate the parquet rows to
		// write based on level0.
		size = stats.level0SizeBefore
		numRows = stats.level0NumRowsBefore
	}
	for _, p1 := range level1Parts {
		overlapped := false
		for _, p0 := range level0Parts {
			if overlaps, err := p0.OverlapsWith(t.table.config.schema, p1); err != nil {
				return nil, err
			} else if overlaps {
				stats.numPartsOverlap++
				partsToCompact = append(partsToCompact, p1)
				overlapped = true
				break
			}
		}
		if !overlapped {
			nonOverlapping = append(nonOverlapping, p1)
		}
	}

	bufs := make([]dynparquet.DynamicRowGroup, 0, len(level0Parts))
	for _, p := range partsToCompact {
		buf, err := p.AsSerializedBuffer(t.table.config.schema)
		if err != nil {
			return nil, err
		}

		// All the row groups in a part are wrapped in a single row group given
		// that all rows are sorted within a part. This reduces the number of
		// cursors open when merging the row groups.
		bufs = append(bufs, buf.MultiDynamicRowGroup())
	}

	cursor := 0
	merged, err := t.table.config.schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		return nil, err
	}

	estimatedBytesPerRow := float64(size) / float64(numRows)
	estimatedRowsPerPart := int(
		math.Ceil(
			(float64(t.table.db.columnStore.granuleSizeBytes) * l1ToGranuleSizeRatio) /
				estimatedBytesPerRow,
		),
	)
	if err := func() error {
		rows := merged.Rows()
		defer rows.Close()

		for {
			var mergedBytes bytes.Buffer
			n, err := t.writeRows(&mergedBytes, rows, merged.DynamicColumns(), estimatedRowsPerPart)
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
			cursor += n
			serBuf, err := dynparquet.ReaderFromBytes(mergedBytes.Bytes())
			if err != nil {
				return err
			}
			compactedPart := parts.NewPart(tx, serBuf, parts.WithCompactionLevel(parts.CompactionLevel1))
			nonOverlapping = append(nonOverlapping, compactedPart)
		}
		return nil
	}(); err != nil {
		return nil, fmt.Errorf("failed level0->level1 compaction: %w", err)
	}

	return nonOverlapping, nil
}

func collectPartsForCompaction(tx uint64, list *parts.List) (
	level0Parts, level1Parts, partsWithUncompletedTxns []*parts.Part,
	stats compactionStats, err error,
) {
	list.Iterate(func(p *parts.Part) bool {
		if p.TX() > tx {
			if !p.HasTombstone() {
				partsWithUncompletedTxns = append(partsWithUncompletedTxns, p)
				stats.uncompletedTxnPartCount++
			}
			// Parts with a tombstone are dropped.
			return true
		}

		switch cl := p.CompactionLevel(); cl {
		case parts.CompactionLevel0:
			level0Parts = append(level0Parts, p)
			stats.level0CountBefore++
			stats.level0SizeBefore += p.Size()
			stats.level0NumRowsBefore += p.NumRows()
		case parts.CompactionLevel1:
			level1Parts = append(level1Parts, p)
			stats.level1CountBefore++
			stats.level1SizeBefore += p.Size()
			stats.level1NumRowsBefore += p.NumRows()
		default:
			err = fmt.Errorf("unexpected part compaction level %d", cl)
			return false
		}
		return true
	})
	return
}

// divideLevel1PartsForGranule returns a two-dimensional slice of parts where
// each element is a slice of parts that all together have a sum of sizes less
// than maxSize.
// The global sort order of the input parts is maintained.
func divideLevel1PartsForGranule(t *TableBlock, tx uint64, level1 []*parts.Part, maxSize int64) ([][]*parts.Part, error) {
	var totalSize int64
	sizes := make([]int64, len(level1))
	for i, p := range level1 {
		size := p.Size()
		totalSize += size
		sizes[i] = size
	}
	if totalSize <= maxSize {
		// No splits needed.
		return [][]*parts.Part{level1}, nil
	}

	// We want to maximize the size of each split slice, so we follow a greedy
	// approach. Note that in practice because we create level1 parts that are
	// around half the size of a granule (level1ToGranuleSizeRatio), if a
	// granule is split, it will be split into two granules that are half filled
	// up with a level1 part.
	var (
		runningSize int64
		newParts    [][]*parts.Part
	)
	for i, size := range sizes {
		runningSize += size
		if runningSize > maxSize {
			// Close the current subslice and create a new one.
			newParts = append(newParts, []*parts.Part{level1[i]})
			runningSize = size
			continue
		}
		if i == 0 {
			newParts = append(newParts, []*parts.Part{level1[i]})
		} else {
			newParts[len(newParts)-1] = append(newParts[len(newParts)-1], level1[i])
		}
	}
	return newParts, nil
}
