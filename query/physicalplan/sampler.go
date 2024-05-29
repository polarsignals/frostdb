package physicalplan

import (
	"context"
	"fmt"
	"math"
	"math/rand"

	"github.com/apache/arrow/go/v16/arrow"
)

type ReservoirSampler struct {
	next PhysicalPlan

	// size is the max number of rows in the reservoir
	size int64

	// reservoir is the set of records that have been sampled. They may vary in schema due to dynamic columns.
	reservoir []arrow.Record

	// currentSize is the number of rows in the reservoir in all records.
	currentSize int64

	w float64 // w is the probability of keeping a record
	n int64   // n is the number of rows that have been sampled thus far
}

// NewReservoirSampler will create a new ReservoirSampler operator that will sample up to size rows of all records seen by Callback.
func NewReservoirSampler(size int64) *ReservoirSampler {
	return &ReservoirSampler{
		size:      size,
		reservoir: []arrow.Record{},
		w:         math.Exp(math.Log(rand.Float64()) / float64(size)),
	}
}

func (s *ReservoirSampler) SetNext(p PhysicalPlan) {
	s.next = p
}

func (s *ReservoirSampler) Draw() *Diagram {
	var child *Diagram
	if s.next != nil {
		child = s.next.Draw()
	}
	details := fmt.Sprintf("Reservoir Sampler (%v)", s.size)
	return &Diagram{Details: details, Child: child}
}

func (s *ReservoirSampler) Close() {
	for _, r := range s.reservoir {
		r.Release()
	}
	s.next.Close()
}

// Callback collects all the records to sample.
func (s *ReservoirSampler) Callback(ctx context.Context, r arrow.Record) error {
	r = s.fill(r)
	if r == nil {
		return nil
	}

	// Sample the record
	s.sample(r)
	return nil
}

// fill will fill the reservoir with the first size records
func (s *ReservoirSampler) fill(r arrow.Record) arrow.Record {
	if s.currentSize == s.size {
		return r
	}

	if s.currentSize+r.NumRows() <= s.size { // The record fits in the reservoir
		for i := int64(0); i < r.NumRows(); i++ { // For simplicity of implementation the reservoir is by row; This is probably not optimal
			s.reservoir = append(s.reservoir, r.NewSlice(i, i+1))
		}
		s.currentSize += r.NumRows()
		s.n += r.NumRows()
		return nil
	}

	// The record partially fits in the reservoir
	for i := int64(0); i < s.size-s.currentSize; i++ {
		s.reservoir = append(s.reservoir, r.NewSlice(i, i+1))
	}
	r = r.NewSlice(s.size-s.currentSize, r.NumRows())
	s.currentSize = s.size
	s.n = s.size
	return r
}

// sample implements the reservoir sampling algorithm found https://en.wikipedia.org/wiki/Reservoir_sampling.
func (s *ReservoirSampler) sample(r arrow.Record) {
	n := s.n + r.NumRows()
	for i := float64(s.n); i < float64(n); {
		i += math.Floor(math.Log(rand.Float64())/math.Log(1-s.w)) + 1
		if i <= float64(n) {
			// replace a random item of the reservoir with row i
			s.reservoir[rand.Intn(int(s.size))] = r.NewSlice(int64(i)-s.n-1, int64(i)-s.n)
			s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
		}
	}
	s.n = n
}

// Finish sends all the records in the reservoir to the next operator.
func (s *ReservoirSampler) Finish(ctx context.Context) error {
	// Send all the records in the reservoir to the next operator
	for _, r := range s.reservoir {
		if err := s.next.Callback(ctx, r); err != nil {
			return err
		}
	}

	return s.next.Finish(ctx)
}
