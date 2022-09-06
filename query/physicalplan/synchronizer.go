package physicalplan

import (
	"context"

	"github.com/apache/arrow/go/v8/arrow"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

type Synchronizer struct {
	next      PhysicalPlan
	callbacks chan synchronizerPayload

	errgr *errgroup.Group
	done  *atomic.Bool
}

type synchronizerPayload struct {
	ctx context.Context
	r   arrow.Record
}

func Synchronize(ctx context.Context, next PhysicalPlan, concurrency int) *Synchronizer {
	s := &Synchronizer{
		next: next,
		// buffer up to 4 calls per concurrent plan
		callbacks: make(chan synchronizerPayload, concurrency*4),
		done:      atomic.NewBool(false),
	}

	s.errgr, ctx = errgroup.WithContext(ctx)
	s.errgr.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case payload, ok := <-s.callbacks:
				if !ok {
					return nil // We're done here, hopefully Finish was called.
				}

				if err := next.Callback(payload.ctx, payload.r); err != nil {
					payload.r.Release()
					return err
				}
				payload.r.Release()
			}
		}
	})

	return s
}

func (s *Synchronizer) Callback(ctx context.Context, r arrow.Record) error {
	r.Retain()
	s.callbacks <- synchronizerPayload{ctx, r}
	return nil
}

func (s *Synchronizer) Finish(ctx context.Context) error {
	if err := s.next.Finish(ctx); err != nil {
		return err
	}

	if s.done.Load() {
		return nil
	}

	s.done.Store(true)
	// TODO: We might need channels per plan. Basically fan-in.
	close(s.callbacks)
	if err := s.errgr.Wait(); err != nil {
		return err
	}
	return nil
}

func (s *Synchronizer) SetNext(next PhysicalPlan) {
	// Not necessary as the next callback is passed into the Synchronize constructor.
}

func (s *Synchronizer) Draw() *Diagram {
	return &Diagram{}
}
