package frostdb

import (
	"context"
	"fmt"
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
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// LSM is a log-structured merge-tree like index. It is implemented as a single linked list of parts.
//
// Arrow records are always added to the L0 list. When a list reaches it's configured max size it is compacted
// calling the levels Compact function and is then added as a new part to the next level.
//
// [L0]->[record]->[record]->[L1]->[record/parquet]->[record/parquet] etc.
type LSM struct {
	sync.RWMutex
	compacting *atomic.Bool

	prefix  string
	levels  *Node
	sizes   []atomic.Int64
	configs []*LevelConfig

	logger  log.Logger
	metrics *LSMMetrics
}

// LSMMetrics are the metrics for an LSM index.
type LSMMetrics struct {
	reg                prometheus.Registerer
	Compactions        *prometheus.CounterVec
	LevelSize          *prometheus.GaugeVec
	CompactionDuration prometheus.Histogram
}

// LevelConfig is the configuration for a level in the LSM tree.
// The Level is the sentinel node that represents the level.
// The MaxSize is the maximum size in bytes that the level can reach before it triggers compaction into the next level.
// The Compact function is called when the level reaches it's max size. NOTE: that this is not yet implemtened and the database will rotate the full block as normal.
type LevelConfig struct {
	Level   SentinelType
	MaxSize int64
	Compact func([]*parts.Part) (*parts.Part, int64, int64, error)
}

type LSMOption func(*LSM)

func LSMWithRegistry(reg prometheus.Registerer) LSMOption {
	return func(l *LSM) {
		l.metrics.reg = reg
	}
}

func LSMWithLogger(logger log.Logger) LSMOption {
	return func(l *LSM) {
		l.logger = logger
	}
}

// NewLSM returns an LSM-like index of len(levels) levels.
func NewLSM(prefix string, levels []*LevelConfig, options ...LSMOption) (*LSM, error) {
	if err := validateLevels(levels); err != nil {
		return nil, err
	}

	lsm := &LSM{
		prefix:     prefix,
		levels:     NewList(L0),
		sizes:      make([]atomic.Int64, len(levels)),
		configs:    levels,
		compacting: &atomic.Bool{},
		logger:     log.NewNopLogger(),
		metrics: &LSMMetrics{
			reg: prometheus.NewRegistry(),
		},
	}

	for _, opt := range options {
		opt(lsm)
	}

	for i := len(levels) - 1; i > 0; i-- {
		lsm.levels.Sentinel(levels[i].Level)
	}

	lsm.metrics.Compactions = promauto.With(lsm.metrics.reg).NewCounterVec(prometheus.CounterOpts{
		Name: "frostdb_lsm_compactions_total",
		Help: "The total number of compactions that have occurred.",
	}, []string{"level"})

	lsm.metrics.LevelSize = promauto.With(lsm.metrics.reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "frostdb_lsm_level_size_bytes",
		Help: "The size of the level in bytes.",
	}, []string{"level"})

	lsm.metrics.CompactionDuration = promauto.With(lsm.metrics.reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "frostdb_lsm_compaction_total_duration_seconds",
		Help:    "Total compaction duration",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 25),
	})

	return lsm, nil
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
	return SentinelType(len(l.configs) - 1)
}

func (l *LSM) Add(tx uint64, record arrow.Record) {
	record.Retain()
	size := util.TotalRecordSize(record)
	l.levels.Prepend(parts.NewArrowPart(tx, record, int(size), nil))
	l0 := l.sizes[L0].Add(int64(size))
	l.metrics.LevelSize.WithLabelValues(L0.String()).Set(float64(l0))
	if l0 >= l.configs[L0].MaxSize {
		if l.compacting.CompareAndSwap(false, true) {
			go l.compact()
		}
	}
}

// InsertPart inserts a part into the LSM tree. It will be inserted into the correct level. It does not check if the insert should cause a compaction.
// This should only be used during snapshot recovery.
func (l *LSM) InsertPart(level SentinelType, part *parts.Part) {
	l.findLevel(level).Prepend(part)
}

func (l *LSM) String() string {
	return l.levels.String()
}

func (l *LSM) Prefixes(ctx context.Context, prefix string) ([]string, error) {
	return []string{l.prefix}, nil
}

func (l *LSM) Scan(ctx context.Context, _ string, _ *dynparquet.Schema, filter logicalplan.Expr, tx uint64, callback func(context.Context, any) error) error {
	l.RLock()
	defer l.RUnlock()

	booleanFilter, err := BooleanExpr(filter)
	if err != nil {
		return fmt.Errorf("boolean expr: %w", err)
	}
	var iterError error
	l.levels.Iterate(func(node *Node) bool {
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
				if err := callback(ctx, rg); err != nil {
					iterError = err
					return false
				}
			}
		}
		return true
	})
	return iterError
}

// TODO: this should be changed to just retain the sentinel nodes in the lsm struct to do an O(1) lookup.
func (l *LSM) findLevel(level SentinelType) *Node {
	var list *Node
	l.levels.Iterate(func(node *Node) bool {
		if node.part == nil && node.sentinel == level {
			list = node
			return false
		}
		return true
	})

	return list
}

// Merge will merge the given level into an arrow record for the next level using the configured Compact function for the given level.
// If this is the max level of the LSM an external writer must be provided to write the merged part elsewhere.
func (l *LSM) merge(level SentinelType, externalWriter func([]*parts.Part) (*parts.Part, int64, int64, error)) error {
	if int(level) > len(l.configs) {
		return fmt.Errorf("level %d does not exist", level)
	}
	if int(level) == len(l.configs)-1 && externalWriter == nil {
		return fmt.Errorf("cannot merge the last level without an external writer")
	}
	l.metrics.Compactions.WithLabelValues(level.String()).Inc()

	mergeList := []*parts.Part{}
	var next *Node
	compact := l.findLevel(level)
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

		mergeList = append(mergeList, node.part)
		return true
	})
	if iterErr != nil {
		return iterErr
	}

	if len(mergeList) == 0 {
		return nil
	}

	var size int64
	var compactedSize int64
	var compacted *parts.Part
	var err error
	s := &Node{
		next:     &atomic.Pointer[Node]{},
		sentinel: level + 1,
	}
	if externalWriter != nil {
		_, size, _, err = externalWriter(mergeList)
		if err != nil {
			return err
		}
		// Drop compacted files from list
		if next != nil {
			s.next.Store(next)
		}
	} else {
		compacted, size, compactedSize, err = l.configs[level].Compact(mergeList)
		if err != nil {
			return err
		}

		// Create new list for the compacted part.
		node := &Node{
			next: &atomic.Pointer[Node]{},
			part: compacted,
		}
		s.next.Store(node)
		if next != nil {
			node.next.Store(next)
		}
		l.sizes[level+1].Add(int64(compactedSize))
		l.metrics.LevelSize.WithLabelValues(SentinelType(level + 1).String()).Set(float64(l.sizes[level+1].Load()))
	}

	// Replace the compacted list with the new list
	compact.next.Store(s)
	l.sizes[level].Add(-int64(size))
	l.metrics.LevelSize.WithLabelValues(level.String()).Set(float64(l.sizes[level].Load()))

	// release the old parts
	l.Lock()
	defer l.Unlock()
	for _, part := range mergeList {
		if r := part.Record(); r != nil {
			r.Release()
		}
	}

	return nil
}

// compact is a cascading compaction routine. It will start at the lowest level and compact until the next level is either the max level or the next level does not exceed the max size.
// compact can not be run concurrently.
func (l *LSM) compact() {
	defer l.compacting.Store(false)
	start := time.Now()
	defer func() {
		l.metrics.CompactionDuration.Observe(time.Since(start).Seconds())
	}()

	for i := 0; i < len(l.configs)-1; i++ {
		if l.sizes[i].Load() >= l.configs[i].MaxSize {
			if err := l.merge(SentinelType(i), nil); err != nil {
				level.Error(l.logger).Log("msg", "failed to merge level", "level", i, "err", err)
				return
			}
		}
	}
}
