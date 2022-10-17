package frostdb

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/thanos-io/objstore/errutil"
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
		if err := t.compactGranule(granuleToCompact); err != nil {
			t.abort(granuleToCompact)
			compactionErrors.Add(err)
		}
		return true
	})
	return compactionErrors.Err()
}
