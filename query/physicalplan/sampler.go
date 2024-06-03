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

	w float64 // w is the probability of keeping a record
	n int64   // n is the number of rows that have been sampled thus far
	i float64 // i is the current row number being sampled
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
func (s *ReservoirSampler) Callback(_ context.Context, r arrow.Record) error {
	r = s.fill(r)
	if r == nil { // The record fit in the reservoir
		return nil
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
		s.reservoir = append(s.reservoir, r)
		r.Retain()
		s.n += r.NumRows()
		return nil
	}

	// The record partially fits in the reservoir
	s.reservoir = append(s.reservoir, r.NewSlice(0, s.size-s.n))
	r = r.NewSlice(s.size-s.n, r.NumRows())
	s.n = s.size
	return r
}

// sample implements the reservoir sampling algorithm found https://en.wikipedia.org/wiki/Reservoir_sampling.
func (s *ReservoirSampler) sample(r arrow.Record) {
	n := s.n + r.NumRows()
	if s.i == 0 {
		s.i = float64(s.n) - 1
	} else if s.i < float64(n) {
		s.replace(rand.Intn(int(s.size)), r.NewSlice(int64(s.i)-s.n, int64(s.i)-s.n+1))
		s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
	}

	for s.i < float64(n) {
		s.i += math.Floor(math.Log(rand.Float64())/math.Log(1-s.w)) + 1
		if s.i < float64(n) {
			// replace a random item of the reservoir with row i
			s.replace(rand.Intn(int(s.size)), r.NewSlice(int64(s.i)-s.n, int64(s.i)-s.n+1))
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

// replace will replace the row at index i with the row in the record r at index j.
func (s *ReservoirSampler) replace(i int, newRow arrow.Record) {
	// find the record in the reservoir that contains the row at index i
	rows := int64(0)
	for k, record := range s.reservoir {
		if int64(i) < rows+record.NumRows() { // Row i is contained in this record
			// Edge case where we're replacing a record that is a single row
			if record.NumRows() == 1 {
				s.reservoir[k].Release()
				s.reservoir[k] = newRow
				return
			}
			front := record.NewSlice(0, int64(i)-rows)
			back := record.NewSlice(int64(i)-rows+1, record.NumRows())
			s.reservoir[k].Release() // Release the old reference to the entire record
			s.reservoir = append(s.reservoir[:k], append([]arrow.Record{front, newRow, back}, s.reservoir[k+1:]...)...)
			return
		}
		rows += record.NumRows()
	}
}
