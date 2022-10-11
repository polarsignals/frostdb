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

	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/thanos-io/objstore/errutil"

	"github.com/polarsignals/frostdb/dynparquet"
)

type compactorPool struct {
	db *DB

	concurrency int
	interval    time.Duration
	wg          sync.WaitGroup
	cancel      context.CancelFunc
}

var (
	defaultConcurrency   = runtime.GOMAXPROCS(0)
	defaultSweepInterval = 100 * time.Millisecond
)

func newCompactorPool(db *DB) *compactorPool {
	concurrency := defaultConcurrency
	if override := db.columnStore.compactionOptions.concurrency; override != 0 {
		concurrency = override
	}

	sweepInterval := defaultSweepInterval
	if override := db.columnStore.compactionOptions.sweepInterval; override != 0 {
		sweepInterval = override
	}
	return &compactorPool{
		db:          db,
		concurrency: concurrency,
		interval:    sweepInterval,
	}
}

func (c *compactorPool) start() {
	ctx, cancelFn := context.WithCancel(context.Background())
	c.cancel = cancelFn
	for i := 0; i < c.concurrency; i++ {
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
		case <-time.After(c.interval):
			c.db.mtx.RLock()
			tablesToCompact := make([]*Table, 0, len(c.db.tables))
			for _, table := range c.db.tables {
				tablesToCompact = append(tablesToCompact, table)
			}
			c.db.mtx.RUnlock()

			for _, table := range tablesToCompact {
				if err := table.ActiveBlock().compact(); err != nil {
					level.Warn(c.db.logger).Log("msg", "compaction failed", "err", err)
				}
			}
		}
	}
}

func (t *TableBlock) compact() error {
	var compactionErrors errutil.MultiError
	t.Index().Ascend(func(i btree.Item) bool {
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
		if successful, err := t.compactGranule(granuleToCompact); !successful || err != nil {
			t.abortCompaction(granuleToCompact)
			if err != nil {
				compactionErrors.Add(err)
			}
		}
		return true
	})
	return compactionErrors.Err()
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
//     level1ToGranuleSizeRatio*granuleSize bytes large. The resulting parts are
//     marked as compacted using compactionLevel1.
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
func (t *TableBlock) compactGranule(granule *Granule) (bool, error) {
	// Use the latest watermark as the tx id.
	tx := t.table.db.tx.Load()

	level0Parts, level1Parts, partsWithUncompletedTxns, sizeBefore, err := collectPartsForCompaction(
		// Start compaction by adding a sentinel node to its parts list.
		tx, granule.parts.Sentinel(Compacting),
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
		level1Parts, err = compactLevel0IntoLevel1(t, tx, level0Parts, level1Parts)
		if err != nil {
			return false, err
		}
	}

	// This sort is done to simplify the splitting of non-overlapping parts
	// amongst new granules. There might be something more clever we could do
	// here if we maintain stronger guarantees about the part ordering given
	// that level1 parts should already be sorted when collected at the start
	// of this method (since this is the only place level1 parts get added to
	// granules). However, it seems like new parts are prepended so the sort
	// order is inverted. Another option is to recursively split using
	// addPartToGranule, which would be cheap in the general case.
	sorter := &partSorter{schema: t.table.config.schema, parts: level1Parts}
	sort.Sort(sorter)
	if sorter.err != nil {
		return false, fmt.Errorf("error sorting level1: %w", sorter.err)
	}

	newParts, err := divideLevel1PartsForGranule(
		t,
		tx,
		level1Parts,
		// Set a maximum size of the l1 ratio to give "breathing room" to new
		// level0 parts. Otherwise, compactions will be initiated more often
		// and work on less data. This ends up creating granules half filled
		// up with a level1 part.
		int64(float64(t.table.db.columnStore.granuleSizeBytes)*level1ToGranuleTargetSizeRatio),
	)
	if err != nil {
		return false, err
	}

	newGranules := make([]*Granule, 0, len(newParts))
	partsAfter := 0
	for _, parts := range newParts {
		partsAfter += len(parts)
		newGranule, err := NewGranule(
			t.table.metrics.granulesCreated, t.table.config, nil, /* firstPart */
		)
		if err != nil {
			if err != nil {
				return false, fmt.Errorf("failed to create new granule: %w", err)
			}
		}
		for _, p := range parts {
			if _, err := newGranule.AddPart(p); err != nil {
				return false, fmt.Errorf("failed to add level1 part to granule: %w", err)
			}
		}
		newGranules = append(newGranules, newGranule)
	}

	// Size calculation is done before adding the ignored parts to the new
	// granules.
	var sizeAfter uint64
	for _, g := range newGranules {
		sizeAfter += g.metadata.size.Load()
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
	granule.parts.Sentinel(Compacted).Iterate(func(p *Part) bool {
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
		curIndex := t.Index()
		t.mtx.Lock()
		index := curIndex.Clone() // TODO(THOR): we can't clone concurrently
		t.mtx.Unlock()

		if index.Delete(granule) == nil {
			level.Error(t.logger).Log("msg", "failed to delete granule during split")
			continue
		}

		for _, g := range newGranules {
			if dupe := index.ReplaceOrInsert(g); dupe != nil {
				level.Error(t.logger).Log("duplicate insert performed")
			}
		}

		// Point to the new index.
		if t.index.CompareAndSwap(curIndex, index) {
			sizeDiff := int64(sizeAfter) - sizeBefore
			t.size.Add(sizeDiff)

			t.table.metrics.numParts.Add(float64(partsAfter - partsBefore))
			break
		}
	}
	return true, nil
}

// level1ToGranuleTargetSizeRatio*granuleSize is the target size for new level1
// parts.
// 0.5 was chosen so that a level1 part takes up around half the space in a full
// granule, allowing for level0 parts to grow up to the remaining space before
// compaction. The higher this ratio, the less data is compacted and the more
// compactions occur. However, a lower ratio implies leaving some parquet
// compression size savings on the table.
const level1ToGranuleTargetSizeRatio = 0.5

// compactLevel0IntoLevel1 compacts the given level0Parts into level1Parts by
// merging them with overlapping level1 parts and producing parts that are up
// to the TableBlock's maximum granule size. These parts are guaranteed to be
// non-overlapping.
// The size limit is a best effort as there is currently no easy way to
// determine the size of a parquet file while writing row groups to it.
func compactLevel0IntoLevel1(
	t *TableBlock, tx uint64, level0Parts, level1Parts []*Part,
) ([]*Part, error) {
	// Verify whether the level0 parts overlap with level1 parts. If they do,
	// we need to merge the overlapping parts. Not merging these parts could
	// cause them to be split to different granules, rendering the index
	// useless.
	nonOverlapping := make([]*Part, 0, len(level1Parts))
	partsToCompact := level0Parts
	// size and numRows are used to estimate the number of bytes per row so that
	// we can provide a maximum number of rows to write in writeRowGroups which
	// will more or less keep the compacted part under the maximum granule size.
	var size, numRows int64
	if len(level1Parts) == 0 {
		for _, p0 := range level0Parts {
			// If no level1 parts exist, then we estimate the parquet rows to
			// write based on level0.
			size += p0.Buf.ParquetFile().Size()
			numRows += p0.Buf.ParquetFile().NumRows()
		}
	}
	for _, p1 := range level1Parts {
		size += p1.Buf.ParquetFile().Size()
		numRows += p1.Buf.ParquetFile().NumRows()
		overlapped := false
		for _, p0 := range level0Parts {
			if overlaps, err := p0.overlapsWith(t.table.config.schema, p1); err != nil {
				return nil, err
			} else if overlaps {
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
		numRowGroups := p.Buf.NumRowGroups()
		for i := 0; i < numRowGroups; i++ {
			bufs = append(bufs, p.Buf.DynamicRowGroup(i))
		}
	}

	cursor := 0
	merged, err := t.table.config.schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		return nil, err
	}

	estimatedBytesPerRow := float64(size) / float64(numRows)
	estimatedRowsPerPart := int(
		math.Ceil(
			(float64(t.table.db.columnStore.granuleSizeBytes) * level1ToGranuleTargetSizeRatio) /
				estimatedBytesPerRow,
		),
	)
	if err := func() error {
		rows := merged.Rows()
		defer rows.Close()

		for {
			var mergedBytes bytes.Buffer
			if err := rows.SeekToRow(int64(cursor)); err != nil {
				return err
			}
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
			compactedPart := NewPart(tx, serBuf)
			compactedPart.compactionLevel = compactionLevel1
			nonOverlapping = append(nonOverlapping, compactedPart)
		}
		return nil
	}(); err != nil {
		return nil, fmt.Errorf("failed level0->level1 compaction: %w", err)
	}

	return nonOverlapping, nil
}

func collectPartsForCompaction(tx uint64, parts *PartList) (
	level0Parts, level1Parts, partsWithUncompletedTxns []*Part,
	size int64, err error,
) {
	parts.Iterate(func(p *Part) bool {
		if p.tx > tx {
			if !p.hasTombstone() {
				partsWithUncompletedTxns = append(partsWithUncompletedTxns, p)
			}
			// Parts with a tombstone are dropped.
			return true
		}

		switch p.compactionLevel {
		case compactionLevel0:
			level0Parts = append(level0Parts, p)
		case compactionLevel1:
			level1Parts = append(level1Parts, p)
		default:
			err = fmt.Errorf(
				"unexpected part compaction level %d", p.compactionLevel,
			)
			return false
		}
		size += p.Buf.ParquetFile().Size()
		return true
	})
	return
}

// divideLevel1PartsForGranule returns a two-dimensional slice of parts where
// each element is a slice of parts that all together have a sum of sizes less
// than maxSize.
// The global sort order of the input parts is maintained.
func divideLevel1PartsForGranule(t *TableBlock, tx uint64, parts []*Part, maxSize int64) ([][]*Part, error) {
	var totalSize int64
	sizes := make([]int64, len(parts))
	for i, p := range parts {
		size := p.Buf.ParquetFile().Size()
		totalSize += size
		sizes[i] = size
	}
	if totalSize <= maxSize {
		// No splits needed.
		return [][]*Part{parts}, nil
	}

	// We want to maximize the size of each split slice, so we follow a greedy
	// approach. Note that in practice because we create level1 parts that are
	// around half the size of a granule (level1ToGranuleSizeRatio), if a
	// granule is split, it will be split into two granules that are half filled
	// up with a level1 part.
	var (
		runningSize int64
		newParts    [][]*Part
	)
	for i, size := range sizes {
		runningSize += size
		if runningSize > maxSize {
			// Close the current subslice and create a new one.
			newParts = append(newParts, []*Part{parts[i]})
			runningSize = size
			continue
		}
		if i == 0 {
			newParts = append(newParts, []*Part{parts[i]})
		} else {
			newParts[len(newParts)-1] = append(newParts[len(newParts)-1], parts[i])
		}
	}
	return newParts, nil
}
