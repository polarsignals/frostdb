package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/util"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
	"github.com/polarsignals/frostdb/query/expr"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type CompactionType int

const (
	CompactionTypeUnknown CompactionType = iota

	// CompactionTypeParquetDisk is a compaction type that will compact the parts into a Parquet file on disk.
	CompactionTypeParquetDisk

	// CompactionTypeParquetMemory is a compaction type that will compact the parts into a Parquet file in memory.
	CompactionTypeParquetMemory
)

// LSM is a log-structured merge-tree like index. It is implemented as a single linked list of parts.
//
// Arrow records are always added to the L0 list. When a list reaches it's configured max size it is compacted
// calling the levels Compact function and is then added as a new part to the next level.
//
// [L0]->[record]->[record]->[L1]->[record/parquet]->[record/parquet] etc.
type LSM struct {
	sync.RWMutex
	compacting   sync.Mutex
	compactionWg sync.WaitGroup

	schema *dynparquet.Schema

	dir           string
	maxTXRecoverd []uint64
	levels        []Level
	partList      *Node
	sizes         []atomic.Int64

	// Options
	logger    log.Logger
	metrics   *LSMMetrics
	watermark func() uint64
}

// LSMMetrics are the metrics for an LSM index.
type LSMMetrics struct {
	Compactions        *prometheus.CounterVec
	LevelSize          *prometheus.GaugeVec
	CompactionDuration prometheus.Histogram
}

// LevelConfig is the configuration for a level in the LSM.
// The MaxSize is the maximum size of the level in bytes before it triggers a compaction into the next level.
type LevelConfig struct {
	Level   SentinelType
	MaxSize int64
	Type    CompactionType
	Compact Compaction
}

type Level interface {
	Compact(parts []parts.Part, options ...parts.Option) ([]parts.Part, int64, int64, error)
	Snapshot(parts []parts.Part, writer func(parts.Part) error, dir string) error
	MaxSize() int64
	Reset()
}

type LSMOption func(*LSM)

func LSMWithLogger(logger log.Logger) LSMOption {
	return func(l *LSM) {
		l.logger = logger
	}
}

func LSMWithMetrics(metrics *LSMMetrics) LSMOption {
	return func(l *LSM) {
		l.metrics = metrics
	}
}

func NewLSMMetrics(reg prometheus.Registerer) *LSMMetrics {
	return &LSMMetrics{
		Compactions: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "frostdb_lsm_compactions_total",
			Help: "The total number of compactions that have occurred.",
		}, []string{"level"}),

		LevelSize: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "frostdb_lsm_level_size_bytes",
			Help: "The size of the level in bytes.",
		}, []string{"level"}),

		CompactionDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "frostdb_lsm_compaction_total_duration_seconds",
			Help:                        "Total compaction duration",
			NativeHistogramBucketFactor: 1.1,
		}),
	}
}

// NewLSM returns an LSM-like index of len(levels) levels.
// wait is a function that will block until the given transaction has been committed; this is used only during compaction to ensure
// that all the tx in the level up to the compaction tx have been committed before compacting.
func NewLSM(dir string, schema *dynparquet.Schema, levels []*LevelConfig, watermark func() uint64, options ...LSMOption) (*LSM, error) {
	if err := validateLevels(levels); err != nil {
		return nil, err
	}

	lsm := &LSM{
		schema:        schema,
		dir:           dir,
		maxTXRecoverd: make([]uint64, len(levels)),
		partList:      NewList(L0),
		sizes:         make([]atomic.Int64, len(levels)),
		compacting:    sync.Mutex{},
		logger:        log.NewNopLogger(),
		watermark:     watermark,
	}

	for _, opt := range options {
		opt(lsm)
	}

	// Configure the LSM levels.
	settings, recovered, err := configureLSMLevels(dir, levels, lsm.logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, part := range recovered {
			part.Release()
		}
	}()
	lsm.levels = settings

	// Reverse iterate (due to prepend) to create the chain of sentinel nodes.
	for i := len(levels) - 1; i > 0; i-- {
		lsm.partList.Sentinel(levels[i].Level)
	}

	if lsm.metrics == nil {
		lsm.metrics = NewLSMMetrics(prometheus.NewRegistry())
	} else {
		for _, lvl := range levels {
			lsm.metrics.LevelSize.WithLabelValues(lvl.Level.String()).Set(0)
		}
	}

	// Replay the recovered parts
	for _, part := range recovered {
		lsm.InsertPart(part)
	}

	return lsm, nil
}

func (l *LSM) Close() error {
	l.compacting.Lock()
	defer l.compacting.Unlock()
	l.Lock()
	defer l.Unlock()

	// Release all the parts to free up any underlying resources.
	l.partList.Iterate(func(node *Node) bool {
		if node.part != nil {
			node.part.Release()
		}
		return true
	})

	// Reset the levels to ensure that none of the parts are still being referenced.
	for i := range l.levels {
		if l.levels[i] != nil {
			l.levels[i].Reset()
		}
	}

	// Remove the index directory
	if err := os.RemoveAll(l.dir); err != nil {
		return fmt.Errorf("remove lsm dir: %w", err)
	}

	return nil
}

// configureLSMLevels will configure the LSM levels. It will recover the levels from disk and return the recovered parts.
func configureLSMLevels(dir string, levels []*LevelConfig, logger log.Logger) ([]Level, []parts.Part, error) {
	settings := make([]Level, len(levels))
	recovered := []parts.Part{}

	// Recover in reverse order so that the highest level is recovered first.
	// This allows us to throw away parts that were compacted into a higher level but for some reason weren't successfully removed.
	for i := len(levels) - 1; i >= 0; i-- {
		lvl := levels[i]
		switch lvl.Type {
		case CompactionTypeParquetMemory:
			settings[i] = &inMemoryLevel{
				maxSize: lvl.MaxSize,
				compact: lvl.Compact,
			}
		case CompactionTypeParquetDisk:
			fileCompaction, err := NewFileCompaction(filepath.Join(dir, fmt.Sprintf("L%v", i+1)), lvl.MaxSize, lvl.Compact, logger) // TODO: it would be nice to not need to inject the compact function here.
			if err != nil {
				return nil, nil, err
			}
			parts, err := fileCompaction.recover(parts.WithCompactionLevel(i + 1))
			if err != nil {
				return nil, nil, fmt.Errorf("failed to recover level %v parts: %w", i+1, err)
			}
			recovered = append(recovered, parts...)
			settings[i] = fileCompaction
		default:
			if i != len(levels)-1 { // Compaction type should not be set for last level
				panic(fmt.Sprintf("unknown compaction type: %v", lvl.Type))
			}
		}
	}

	return settings, recovered, nil
}

// Size returns the total size of the index in bytes.
func (l *LSM) Size() int64 {
	var size int64
	for i := range l.sizes {
		size += l.sizes[i].Load()
	}
	return size
}

// LevelSize returns the size of a specific level in bytes.
func (l *LSM) LevelSize(t SentinelType) int64 {
	return l.sizes[t].Load()
}

// Snapshot creates a snapshot of the index at the given transaction. It will call the writer function with the parts in the index that are in-memory.
func (l *LSM) Snapshot(tx uint64, writer func(parts.Part) error, dir string) error {
	l.compacting.Lock()
	defer l.compacting.Unlock()

	var (
		snapshotList []parts.Part
		iterError    error
	)
	var snapshotLevel SentinelType
	l.partList.Iterate(func(node *Node) bool {
		if node.part == nil {
			if node.sentinel == L0 { // First node in the list will be L0
				snapshotLevel = L0
				return true
			}

			switch snapshotLevel {
			case L0: // L0 is always in-memory
				for _, part := range snapshotList {
					if err := writer(part); err != nil {
						iterError = err
						return false
					}
				}
			default:
				lvl := l.levels[snapshotLevel-1]
				lvldir := filepath.Join(dir, fmt.Sprintf("%v", snapshotLevel))
				if err := lvl.Snapshot(snapshotList, writer, lvldir); err != nil {
					iterError = err
					return false
				}
			}

			snapshotLevel = node.sentinel
			snapshotList = nil
			return true
		}

		if node.part.TX() <= tx {
			snapshotList = append(snapshotList, node.part)
		}
		return true
	})
	if iterError != nil {
		return iterError
	}

	lvl := l.levels[snapshotLevel-1]
	lvldir := filepath.Join(dir, fmt.Sprintf("%v", snapshotLevel))
	return lvl.Snapshot(snapshotList, writer, lvldir)
}

func validateLevels(levels []*LevelConfig) error {
	for i, l := range levels {
		if int(l.Level) != i {
			return fmt.Errorf("level %d is not in order", l.Level)
		}

		switch i {
		case len(levels) - 1:
			if l.Compact != nil {
				return fmt.Errorf("level %d is the last level and should not have a compact function", l.Level)
			}
		default:
			if l.Compact == nil {
				return fmt.Errorf("level %d is not the last level and should have a compact function", l.Level)
			}
		}
	}

	return nil
}

func (l *LSM) MaxLevel() SentinelType {
	return SentinelType(len(l.levels) - 1)
}

func (l *LSM) Add(tx uint64, record arrow.Record) {
	record.Retain()
	size := util.TotalRecordSize(record)
	l.partList.Insert(parts.NewArrowPart(tx, record, uint64(size), l.schema, parts.WithCompactionLevel(int(L0))))
	l0 := l.sizes[L0].Add(int64(size))
	l.metrics.LevelSize.WithLabelValues(L0.String()).Set(float64(l0))
	if l0 >= l.levels[L0].MaxSize() {
		if l.compacting.TryLock() {
			l.compactionWg.Add(1)
			go func() {
				defer l.compacting.Unlock()
				defer l.compactionWg.Done()
				_ = l.compact(false)
			}()
		}
	}
}

func (l *LSM) WaitForPendingCompactions() {
	l.compactionWg.Wait()
}

// InsertPart inserts a part into the LSM tree. It will be inserted into the correct level. It does not check if the insert should cause a compaction.
// This should only be used during snapshot recovery. It will drop the insert on the floor if the part is older than a part in the next level of the LSM. This indicates
// that this part is already accounted for in the next level vis compaction.
func (l *LSM) InsertPart(part parts.Part) {
	level := SentinelType(part.CompactionLevel())
	// Check the next levels if there is one to see if this part should be inserted.
	if level != l.MaxLevel() {
		for i := level + 1; i < l.MaxLevel()+1; i++ {
			if part.TX() <= l.maxTXRecoverd[i] {
				return
			}
		}
	}

	// Retain the part
	part.Retain()

	if tx := part.TX(); tx > l.maxTXRecoverd[level] {
		l.maxTXRecoverd[level] = tx
	}

	// Insert the part into the correct level, but do not do this if parts with newer TXs have already been inserted.
	l.findLevel(level).Insert(part)
	size := l.sizes[level].Add(int64(part.Size()))
	l.metrics.LevelSize.WithLabelValues(level.String()).Set(float64(size))
}

func (l *LSM) String() string {
	s := ""
	for i := range l.sizes {
		s += fmt.Sprintf("L%v: %d ", i, l.sizes[i].Load())
	}
	s += "\n"
	s += l.partList.String()
	return s
}

func (l *LSM) Prefixes(_ context.Context, _ string) ([]string, error) {
	return []string{}, nil
}

func (l *LSM) Iterate(iter func(node *Node) bool) {
	l.RLock()
	defer l.RUnlock()
	l.partList.Iterate(iter)
}

func (l *LSM) Scan(ctx context.Context, _ string, _ *dynparquet.Schema, filter logicalplan.Expr, tx uint64, callback func(context.Context, any) error) error {
	l.RLock()
	defer l.RUnlock()

	booleanFilter, err := expr.BooleanExpr(filter)
	if err != nil {
		return fmt.Errorf("boolean expr: %w", err)
	}

	var iterError error
	l.partList.Iterate(func(node *Node) bool {
		if node.part == nil { // encountered a sentinel node; continue on
			return true
		}

		if node.part.TX() > tx { // skip parts that are newer than this transaction
			return true
		}

		if r := node.part.Record(); r != nil {
			r.Retain()
			if err := callback(ctx, r); err != nil {
				iterError = err
				return false
			}
			return true
		}

		buf, err := node.part.AsSerializedBuffer(nil)
		if err != nil {
			iterError = err
			return false
		}

		for i := 0; i < buf.NumRowGroups(); i++ {
			rg := buf.DynamicRowGroup(i)
			mayContainUsefulData, err := booleanFilter.Eval(rg)
			if err != nil {
				iterError = err
				return false
			}

			if mayContainUsefulData {
				node.part.Retain() // Create another reference to this part
				if err := callback(ctx, &releaseableRowGroup{DynamicRowGroup: rg, release: node.part.Release}); err != nil {
					iterError = err
					return false
				}
			}
		}
		return true
	})
	return iterError
}

type releaseableRowGroup struct {
	dynparquet.DynamicRowGroup
	release func()
}

func (r *releaseableRowGroup) Release() {
	r.release()
}

type ReleaseableRowGroup interface {
	dynparquet.DynamicRowGroup
	Release()
}

// TODO: this should be changed to just retain the sentinel nodes in the lsm struct to do an O(1) lookup.
func (l *LSM) findLevel(level SentinelType) *Node {
	var list *Node
	l.partList.Iterate(func(node *Node) bool {
		if node.part == nil && node.sentinel == level {
			list = node
			return false
		}
		return true
	})

	return list
}

// findNode returns the node that points to node.
func (l *LSM) findNode(node *Node) *Node {
	var list *Node
	l.partList.Iterate(func(n *Node) bool {
		if n.next.Load() == node {
			list = n
			return false
		}
		return true
	})

	return list
}

// EnsureCompaction forces a compaction of all levels, regardless of whether the
// levels are below the target size.
func (l *LSM) EnsureCompaction() error {
	l.compacting.Lock()
	defer l.compacting.Unlock()
	return l.compact(true /* ignoreSizes */)
}

// Rotate will write all parts in the LSM into the external writer. No changes are made to the LSM.
func (l *LSM) Rotate(externalWriter func([]parts.Part) (parts.Part, int64, int64, error)) error {
	l.compacting.Lock()
	defer l.compacting.Unlock()
	start := time.Now()
	defer func() {
		l.metrics.CompactionDuration.Observe(time.Since(start).Seconds())
	}()

	// Write all the parts to the external writer
	compact := []parts.Part{}
	l.partList.Iterate(func(node *Node) bool {
		if node.part == nil {
			return true
		}

		compact = append(compact, node.part)
		return true
	})

	_, _, _, err := externalWriter(compact)
	return err
}

// Merge will merge the given level into an arrow record for the next level using the configured Compact function for the given level.
// If this is the max level of the LSM an external writer must be provided to write the merged part elsewhere.
func (l *LSM) merge(level SentinelType) error {
	if int(level) > len(l.levels) {
		return fmt.Errorf("level %d does not exist", level)
	}
	if int(level) == len(l.levels)-1 {
		return fmt.Errorf("cannot merge the last level")
	}
	l.metrics.Compactions.WithLabelValues(level.String()).Inc()

	compact := l.findLevel(level)

	// Find a transaction that is <= the current watermark.
	// This ensures a contiguous sorted list of transactions.
	if level == L0 {
		compact = compact.next.Load()
		if compact == nil || compact.part == nil {
			return nil // nothing to compact
		}

		// Find the first part that is <= the watermark and reset the compact list to that part.
		wm := l.watermark()
		compact.Iterate(func(node *Node) bool {
			if node.part != nil && node.sentinel != L0 {
				return false
			}
			if node.part.TX() <= wm {
				compact = node
				return false
			}
			return true
		})
	}

	nodeList := []*Node{}
	var next *Node
	var iterErr error
	compact.Iterate(func(node *Node) bool {
		if node.part == nil { // sentinel encountered
			switch {
			case node.sentinel == level: // the sentinel for the beginning of the list
				return true
			case node.sentinel == level+1:
				next = node.next.Load() // skip the sentinel to combine the lists
			default:
				next = node
			}
			return false
		}

		nodeList = append(nodeList, node)
		return true
	})
	if iterErr != nil {
		return iterErr
	}

	if len(nodeList) == 0 {
		return nil
	}

	var size int64
	var compactedSize int64
	var compacted []parts.Part
	var err error
	mergeList := make([]parts.Part, 0, len(nodeList))
	for _, node := range nodeList {
		mergeList = append(mergeList, node.part)
	}
	s := &Node{
		sentinel: level + 1,
	}
	compacted, size, compactedSize, err = l.levels[level].Compact(mergeList, parts.WithCompactionLevel(int(level)+1))
	if err != nil {
		return err
	}

	// Create new list for the compacted parts.
	compactedList := &Node{
		part: compacted[0],
	}
	node := compactedList
	for _, p := range compacted[1:] {
		node.next.Store(&Node{
			part: p,
		})
		node = node.next.Load()
	}
	s.next.Store(compactedList)
	if next != nil {
		node.next.Store(next)
	}
	l.sizes[level+1].Add(int64(compactedSize))
	l.metrics.LevelSize.WithLabelValues(SentinelType(level + 1).String()).Set(float64(l.sizes[level+1].Load()))

	// Replace the compacted list with the new list
	// find the node that points to the first node in our compacted list.
	node = l.findNode(nodeList[0])
	for !node.next.CompareAndSwap(nodeList[0], s) {
		// This can happen at most once in the scenario where a new part is added to the L0 list while we are trying to replace it.
		node = l.findNode(nodeList[0])
	}
	l.sizes[level].Add(-int64(size))
	l.metrics.LevelSize.WithLabelValues(level.String()).Set(float64(l.sizes[level].Load()))

	// release the old parts
	l.Lock()
	for _, part := range mergeList {
		part.Release()
	}
	l.Unlock()

	// Reset the level that was just compacted
	if level != L0 {
		l.levels[level-1].Reset()
	}

	return nil
}

// compact is a cascading compaction routine. It will start at the lowest level and compact until the next level is either the max level or the next level does not exceed the max size.
// compact can not be run concurrently.
func (l *LSM) compact(ignoreSizes bool) error {
	start := time.Now()
	defer func() {
		l.metrics.CompactionDuration.Observe(time.Since(start).Seconds())
	}()

	for i := 0; i < len(l.levels)-1; i++ {
		if ignoreSizes || l.sizes[i].Load() >= l.levels[i].MaxSize() {
			if err := l.merge(SentinelType(i)); err != nil {
				level.Error(l.logger).Log("msg", "failed to merge level", "level", i, "err", err)
				return err
			}
		}
	}

	return nil
}
