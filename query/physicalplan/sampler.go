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
	reservoir []sample

	w float64 // w is the probability of keeping a record
	n int64   // n is the number of rows that have been sampled thus far
	i float64 // i is the current row number being sampled
}

// NewReservoirSampler will create a new ReservoirSampler operator that will sample up to size rows of all records seen by Callback.
func NewReservoirSampler(size int64) *ReservoirSampler {
	return &ReservoirSampler{
		size: size,
		w:    math.Exp(math.Log(rand.Float64()) / float64(size)),
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
		r.r.Release()
	}
	s.next.Close()
}

// Callback collects all the records to sample.
func (s *ReservoirSampler) Callback(_ context.Context, r arrow.Record) error {
	r = s.fill(r)
	if r == nil { // The record fit in the reservoir
		return nil
	}
	if s.n == s.size { // The reservoir just filled up. Slice the reservoir to the correct size so we can easily perform row replacement
		s.sliceReservoir()
	}

	// Sample the record
	s.sample(r)
	return nil
}

// fill will fill the reservoir with the first size records.
func (s *ReservoirSampler) fill(r arrow.Record) arrow.Record {
	if s.n >= s.size {
		return r
	}

	if s.n+r.NumRows() <= s.size { // The record fits in the reservoir
		s.reservoir = append(s.reservoir, sample{r: r, i: -1}) // -1 means the record is not sampled; use the entire record
		r.Retain()
		s.n += r.NumRows()
		return nil
	}

	// The record partially fits in the reservoir
	s.reservoir = append(s.reservoir, sample{r: r.NewSlice(0, s.size-s.n), i: -1})
	r = r.NewSlice(s.size-s.n, r.NumRows())
	s.n = s.size
	return r
}

func (s *ReservoirSampler) sliceReservoir() {
	newReservoir := make([]sample, 0, s.size)
	for _, r := range s.reservoir {
		for j := int64(0); j < r.r.NumRows(); j++ {
			newReservoir = append(newReservoir, sample{r: r.r, i: j})
		}
	}
	s.reservoir = newReservoir
}

// sample implements the reservoir sampling algorithm found https://en.wikipedia.org/wiki/Reservoir_sampling.
func (s *ReservoirSampler) sample(r arrow.Record) {
	n := s.n + r.NumRows()
	if s.i == 0 {
		s.i = float64(s.n) - 1
	} else if s.i < float64(n) {
		s.replace(rand.Intn(int(s.size)), sample{r: r, i: int64(s.i) - s.n})
		s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
	}

	for s.i < float64(n) {
		s.i += math.Floor(math.Log(rand.Float64())/math.Log(1-s.w)) + 1
		if s.i < float64(n) {
			// replace a random item of the reservoir with row i
			s.replace(rand.Intn(int(s.size)), sample{r: r, i: int64(s.i) - s.n})
			s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
		}
	}
	s.n = n
}

// Finish sends all the records in the reservoir to the next operator.
func (s *ReservoirSampler) Finish(ctx context.Context) error {
	// Send all the records in the reservoir to the next operator
	for _, r := range s.reservoir {
		if r.i == -1 {
			if err := s.next.Callback(ctx, r.r); err != nil {
				return err
			}
			continue
		}

		record := r.r.NewSlice(r.i, r.i+1)
		defer record.Release()
		if err := s.next.Callback(ctx, record); err != nil {
			return err
		}
	}

	return s.next.Finish(ctx)
}

// replace will replace the row at index i with the row in the record r at index j.
func (s *ReservoirSampler) replace(i int, newRow sample) {
	s.reservoir[i].r.Release()
	s.reservoir[i] = newRow
	newRow.r.Retain()
}

type sample struct {
	r arrow.Record
	i int64
}
