package physicalplan

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/apache/arrow/go/v14/arrow"
)

// Synchronizer is used to combine the results of multiple parallel streams
// into a single stream concurrent stream. It also forms a barrier on the
// finishers, by waiting to call next plan's finish until all previous parallel
// stages have finished.
type Synchronizer struct {
	next    PhysicalPlan
	buffer  chan *syncCmd
	err     atomic.Value
	running atomic.Int64
	open    atomic.Int64
}

type syncCmd struct {
	typ  syncCmdType
	r    arrow.Record
	done chan struct{}
}

type syncCmdType byte

const (
	syncCallback syncCmdType = iota
	syncFinish
	syncClose
)

func Synchronize(ctx context.Context, concurrency int) *Synchronizer {
	s := &Synchronizer{
		buffer: make(chan *syncCmd, concurrency),
	}
	s.running.Add(int64(concurrency))
	s.open.Add(int64(concurrency))
	go s.run(ctx)
	return s
}

func (m *Synchronizer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			m.err.Store(ctx.Err())
			return
		case cmd := <-m.buffer:
			switch cmd.typ {
			case syncCallback:
				err := m.next.Callback(ctx, cmd.r)
				cmd.r.Release()
				if err != nil {
					// TODO:(gernest) handle this ?
					// There is no clear way to give feedback on error.
					//  - Halt everything on first sight of error ?
					//  - Log the error and continue?
					_ = err
				}
			case syncFinish:
				err := m.next.Finish(ctx)
				if err != nil {
					m.err.Store(err)
				}
				cmd.done <- struct{}{}
			case syncClose:
				m.next.Close()
				cmd.done <- struct{}{}
				close(m.buffer)
				return
			}
		}
	}
}

func (m *Synchronizer) hasErr() error {
	if err := m.err.Load(); err != nil {
		return err.(error)
	}
	return nil
}

func (m *Synchronizer) Callback(ctx context.Context, r arrow.Record) error {
	if err := m.hasErr(); err != nil {
		return err
	}
	r.Retain()
	m.buffer <- &syncCmd{typ: syncCallback, r: r}
	return nil
}

func (m *Synchronizer) Finish(ctx context.Context) error {
	running := m.running.Add(-1)
	if running < 0 {
		return errors.New("too many Synchronizer Finish calls")
	}
	if running > 0 {
		return nil
	}
	cmd := &syncCmd{typ: syncFinish, done: make(chan struct{})}
	m.buffer <- cmd
	<-cmd.done
	return nil
}

func (m *Synchronizer) SetNext(next PhysicalPlan) {
	m.next = next
}

func (m *Synchronizer) SetNextPlan(nextPlan PhysicalPlan) {
	m.next = nextPlan
}

func (m *Synchronizer) Draw() *Diagram {
	return &Diagram{Details: "Synchronizer", Child: m.next.Draw()}
}

func (m *Synchronizer) Close() {
	open := m.open.Add(-1)
	if open < 0 {
		panic("too many Synchronizer Close calls")
	}
	if open > 0 {
		return
	}
	cmd := &syncCmd{typ: syncClose, done: make(chan struct{})}
	m.buffer <- cmd
	<-cmd.done
}
