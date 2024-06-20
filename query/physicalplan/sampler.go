package physicalplan

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/arrow/util"

	"github.com/polarsignals/frostdb/pqarrow/builder"
)

type ReservoirSampler struct {
	next      PhysicalPlan
	allocator memory.Allocator

	// size is the max number of rows in the reservoir
	size int64
	// sizeInBytes is the total number of bytes that are held by records in the reservoir. This includes
	// rows that are not sampled but are being held onto because of a reference to the record that is still in the reservoir.
	sizeInBytes int64
	// sizeLimit is the number of bytes that sizeInBytes is allowed to get to before the reservoir is materialized. This is to prevent the reservoir from growing too large.
	sizeLimit int64

	// reservoir is the set of records that have been sampled. They may vary in schema due to dynamic columns.
	reservoir []sample

	w float64 // w is the probability of keeping a record
	n int64   // n is the number of rows that have been sampled thus far
	i float64 // i is the current row number being sampled
}

type sample struct {
	// i is the index of the row in the record that is being sampled. If i is -1, the entire record is being sampled.
	i int64
	// ref to the record that is being sampled
	ref *referencedRecord
}

type referencedRecord struct {
	// Record is a pointer to the record that is being sampled
	arrow.Record
	// size is the size of the record in bytes
	size int64
	// ref is the number of references to the record. When ref is 0, the record can be released.
	ref int64
}

func (s *referencedRecord) Release() int64 {
	defer s.Record.Release()
	s.ref--
	if s.ref == 0 {
		return s.size
	}

	return 0
}

func (s *referencedRecord) Retain() int64 {
	defer s.Record.Retain()
	s.ref++
	if s.ref == 1 {
		return s.size
	}
	return 0
}

// NewReservoirSampler will create a new ReservoirSampler operator that will sample up to size rows of all records seen by Callback.
func NewReservoirSampler(size, limit int64, allocator memory.Allocator) *ReservoirSampler {
	return &ReservoirSampler{
		size:      size,
		sizeLimit: limit,
		w:         math.Exp(math.Log(rand.Float64()) / float64(size)),
		allocator: allocator,
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
		s.sizeInBytes -= r.ref.Release()
	}
	s.next.Close()
}

// Callback collects all the records to sample.
func (s *ReservoirSampler) Callback(_ context.Context, r arrow.Record) error {
	var ref *referencedRecord
	r, ref = s.fill(r)
	if r == nil { // The record fit in the reservoir
		return nil
	}
	if s.n == s.size { // The reservoir just filled up. Slice the reservoir to the correct size so we can easily perform row replacement
		s.sliceReservoir()
	}

	// Sample the record
	s.sample(r, ref)
	if s.sizeInBytes >= s.sizeLimit {
		if err := s.materialize(s.allocator); err != nil {
			return err
		}
	}
	return nil
}

func refPtr(r arrow.Record) *referencedRecord {
	return &referencedRecord{Record: r, size: util.TotalRecordSize(r)}
}

// fill will fill the reservoir with the first size records.
func (s *ReservoirSampler) fill(r arrow.Record) (arrow.Record, *referencedRecord) {
	if s.n >= s.size {
		return r, refPtr(r)
	}

	if s.n+r.NumRows() <= s.size { // The record fits in the reservoir
		smpl := sample{
			i:   -1,
			ref: refPtr(r),
		}
		s.reservoir = append(s.reservoir, smpl)
		s.sizeInBytes += smpl.ref.Retain()
		s.n += r.NumRows()
		return nil, nil
	}

	// The record partially fits in the reservoir
	ref := refPtr(r)
	smpl := sample{
		i:   -1,
		ref: refPtr(r.NewSlice(0, s.size-s.n)),
	}
	s.reservoir = append(s.reservoir, smpl)
	s.sizeInBytes += smpl.ref.Retain()
	r = r.NewSlice(s.size-s.n, r.NumRows())
	s.n = s.size
	return r, ref
}

func (s *ReservoirSampler) sliceReservoir() {
	newReservoir := make([]sample, 0, s.size)
	for _, r := range s.reservoir {
		ref := refPtr(r.ref.Record)
		for j := int64(0); j < r.ref.Record.NumRows(); j++ {
			smpl := sample{
				i:   j,
				ref: ref,
			}
			newReservoir = append(newReservoir, smpl)
			s.sizeInBytes += smpl.ref.Retain()
		}
		s.sizeInBytes -= r.ref.Release()
	}
	s.reservoir = newReservoir
}

// sample implements the reservoir sampling algorithm found https://en.wikipedia.org/wiki/Reservoir_sampling.
func (s *ReservoirSampler) sample(r arrow.Record, ref *referencedRecord) {
	n := s.n + r.NumRows()
	if s.i == 0 {
		s.i = float64(s.n) - 1
	} else if s.i < float64(n) {
		s.replace(rand.Intn(int(s.size)), sample{i: int64(s.i) - s.n, ref: ref})
		s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
	}

	for s.i < float64(n) {
		s.i += math.Floor(math.Log(rand.Float64())/math.Log(1-s.w)) + 1
		if s.i < float64(n) {
			// replace a random item of the reservoir with row i
			s.replace(rand.Intn(int(s.size)), sample{i: int64(s.i) - s.n, ref: ref})
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
			if err := s.next.Callback(ctx, r.ref.Record); err != nil {
				return err
			}
			continue
		}

		record := r.ref.Record.NewSlice(r.i, r.i+1)
		defer record.Release()
		if err := s.next.Callback(ctx, record); err != nil {
			return err
		}
	}

	return s.next.Finish(ctx)
}

// replace will replace the row at index i with the row in the record r at index j.
func (s *ReservoirSampler) replace(i int, newRow sample) {
	s.sizeInBytes -= s.reservoir[i].ref.Release()
	s.reservoir[i] = newRow
	s.sizeInBytes += newRow.ref.Retain()
}

// materialize will build a new record from the reservoir to release the underlying records.
func (s *ReservoirSampler) materialize(allocator memory.Allocator) error {
	// Build the unified schema for the records
	schema := s.reservoir[0].ref.Schema()
	fields := schema.Fields()
	added := map[string]struct{}{}
	for i := 1; i < len(s.reservoir); i++ {
		for j := 0; j < s.reservoir[i].ref.Schema().NumFields(); j++ {
			newField := s.reservoir[i].ref.Schema().Field(j).Name
			if _, ok := added[newField]; !ok && !schema.HasField(s.reservoir[i].ref.Schema().Field(j).Name) {
				fields = append(fields, s.reservoir[i].ref.Schema().Field(j))
				added[newField] = struct{}{}
			}
		}
	}

	// Sort the fields alphabetically
	slices.SortFunc(fields, func(i, j arrow.Field) int {
		switch {
		case i.Name < j.Name:
			return -1
		case i.Name > j.Name:
			return 1
		default:
			return 0
		}
	})

	// Merge all the records slices
	schema = arrow.NewSchema(fields, nil)
	bldr := array.NewRecordBuilder(allocator, schema)
	defer bldr.Release()

	for _, r := range s.reservoir {
		for i, f := range bldr.Fields() { // TODO handle disparate schemas
			// Check if this record has this field
			if !r.ref.Schema().HasField(schema.Field(i).Name) {
				if err := builder.AppendValue(f, nil, -1); err != nil {
					return err
				}
			} else {
				if err := builder.AppendValue(f, r.ref.Column(i), int(r.i)); err != nil {
					return err
				}
			}
		}
	}

	// Clear the reservoir
	for _, r := range s.reservoir {
		s.sizeInBytes -= r.ref.Release()
	}
	// Set the record to be the new reservoir
	smpl := sample{i: -1, ref: refPtr(bldr.NewRecord())}
	s.sizeInBytes += smpl.ref.Retain()
	smpl.ref.Record.Release() // Release this here because of the retain in the previous line.
	s.reservoir = []sample{smpl}

	// reslice the reservoir for easy row replacement
	s.sliceReservoir()
	return nil
}
