package physicalplan_concurrent

import (
	"context"
	"time"
)

type PhysicalOperator interface {
	Callbacks() []func(ctx context.Context, data int) error
	SetCallbacks([]func(ctx context.Context, data int) error)
}

func Build(callback func(ctx context.Context, data int) error) *OutputPlan {
	const concurrency = 4
	callbacks := make([]func(ctx context.Context, data int) error, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		callbacks = append(callbacks, func(ctx context.Context, data int) error {
			return callback(ctx, data)
		})
	}

	prev := callbacks
	operators := []PhysicalOperator{
		&Sleeper{},
		&Squarer{},
	}
	for _, op := range operators {
		op.SetCallbacks(prev)
		prev = op.Callbacks()
	}

	return &OutputPlan{
		scan: &Scan{
			callbacks: prev,
		},
	}
}

type OutputPlan struct {
	scan      *Scan
	callbacks []func(ctx context.Context, data int) error
}

func (p *OutputPlan) Execute(ctx context.Context) error {
	return p.scan.Execute(ctx)
}

type Scan struct {
	callbacks []func(ctx context.Context, data int) error
}

func (s *Scan) Execute(ctx context.Context) error {
	data := make(chan int, len(s.callbacks))

	ctx, cancel := context.WithCancel(ctx)
	for i, callback := range s.callbacks {
		callback := callback
		// This will be an errgroup
		go func(i int) {
			for {
				select {
				case <-ctx.Done():
					return
				case d := <-data:
					if err := callback(ctx, d); err != nil {
						return
					}
				}
			}
		}(i)
	}

	for i := 0; i < 50; i++ {
		data <- i
	}
	close(data)
	cancel()

	return nil
}

type Squarer struct {
	callbacks []func(ctx context.Context, data int) error
}

func (s *Squarer) Callbacks() []func(ctx context.Context, data int) error {
	return s.callbacks
}

func (s *Squarer) SetCallbacks(nextCallbacks []func(ctx context.Context, data int) error) {
	for _, next := range nextCallbacks {
		s.callbacks = append(s.callbacks, func(ctx context.Context, data int) error {
			return next(ctx, data*data)
		})
	}
}

type Sleeper struct {
	callbacks []func(ctx context.Context, data int) error
}

func (s *Sleeper) SetCallbacks(nextCallbacks []func(ctx context.Context, data int) error) {
	for _, next := range nextCallbacks {
		s.callbacks = append(s.callbacks, func(ctx context.Context, data int) error {
			time.Sleep(time.Duration(data) * time.Millisecond)
			return next(ctx, data)
		})
	}
}

func (s *Sleeper) Callbacks() []func(ctx context.Context, data int) error {
	return s.callbacks
}
