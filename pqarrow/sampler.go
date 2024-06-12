package pqarrow

import (
	"context"
	"math"
	"math/rand"
	"slices"
	"unsafe"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
)

type Sampler struct {
	size      int64
	reservoir []*Sample
	schema    *dynparquet.Schema

	w float64
	n int64
	i float64

	// options
	filter physicalplan.BooleanExpression
}

type Sample struct {
	parquet.RowGroup
	rows *physicalplan.Bitmap

	sampled bool
	i       int64
}

func (s *Sample) NumRows() int64 {
	if s.rows == nil {
		return s.RowGroup.NumRows()
	}

	return int64(s.rows.GetCardinality())
}

type SamplerOption func(*Sampler)

func WithSamplerFilter(filter physicalplan.BooleanExpression) SamplerOption {
	return func(s *Sampler) {
		s.filter = filter
	}
}

func NewSampler(k int64, schema *dynparquet.Schema, options ...SamplerOption) *Sampler {
	s := &Sampler{
		size:   k,
		schema: schema,
		w:      math.Exp(math.Log(rand.Float64()) / float64(k)),
	}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func (s *Sampler) Sample(rg parquet.RowGroup) error {
	sample := &Sample{RowGroup: rg, rows: nil}
	if s.filter != nil {
		bm, _, err := s.filter.EvalParquet(rg, nil)
		if err != nil {
			return err
		}

		sample.rows = bm
	}

	sample = s.fill(sample)
	if sample == nil {
		return nil
	}
	if s.n == s.size { // The reservoir is full; Slice up the reservoir into single row entries. This will make replacement easier.
		s.sliceReservoir()
	}

	// Sample the rowgroup
	s.sample(sample)
	return nil
}

func (s *Sampler) fill(rg *Sample) *Sample {
	if rg == nil {
		return nil
	}

	if rg.NumRows() == 0 {
		return nil
	}

	if s.n+rg.NumRows() <= s.size { // All rows fit in the reservoir
		s.reservoir = append(s.reservoir, rg)
		s.n += rg.NumRows()
		return nil
	}

	// TODO the reservoir partially fits
	remaining := s.size - s.n
	rows := rg.rows.ToArray()
	bm := physicalplan.NewBitmap()
	bm.AddMany(rows[:remaining])
	s.reservoir = append(s.reservoir, &Sample{RowGroup: rg.RowGroup, rows: bm})

	toSample := physicalplan.NewBitmap()
	toSample.AddMany(rows[remaining:])
	return &Sample{RowGroup: rg.RowGroup, rows: toSample}
}

func (s *Sampler) sliceReservoir() {
	newReservoir := make([]*Sample, 0, s.size)
	for _, sample := range s.reservoir {
		rows := sample.rows.ToArray()
		for _, row := range rows {
			newReservoir = append(newReservoir, &Sample{RowGroup: sample.RowGroup, rows: sample.rows, sampled: true, i: int64(row)})
		}
	}
	s.reservoir = newReservoir
}

func (s *Sampler) sample(rg *Sample) {
	n := s.n + rg.NumRows()
	if s.i == 0 {
		s.i = float64(s.n) - 1
	} else if s.i < float64(n) {
		s.replace(rand.Intn(int(s.size)), &Sample{RowGroup: rg.RowGroup, rows: rg.rows, sampled: true, i: int64(s.i) - s.n})
		s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
	}

	for s.i < float64(n) {
		s.i += math.Floor(math.Log(rand.Float64())/math.Log(1-s.w)) + 1
		if s.i < float64(n) {
			// replace a random item of the reservoir with row i
			s.replace(rand.Intn(int(s.size)), &Sample{RowGroup: rg.RowGroup, rows: rg.rows, sampled: true, i: int64(s.i) - s.n})
			s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
		}
	}
	s.n = n
}

func (s *Sampler) replace(j int, r *Sample) {
	s.reservoir[j] = r
}

func (s *Sampler) Convert(ctx context.Context, pool memory.Allocator, iterOpts logicalplan.IterOptions) (arrow.Record, error) {
	s.merge()

	converter := NewParquetConverter(pool, iterOpts)
	defer converter.Close()

	for _, sample := range s.reservoir {
		var indices *physicalplan.Bitmap
		switch {
		case sample.sampled:
			indices = physicalplan.NewBitmap()
			indices.Add(uint32(sample.i))
		default:
			indices = sample.rows
		}

		if err := converter.Convert(ctx, sample, s.schema, indices); err != nil {
			return nil, err
		}
	}

	return converter.NewRecord(), nil
}

// Merge merges samples that contain the same underlying rowgroup into a single sample.
func (s *Sampler) merge() {
	slices.SortFunc(s.reservoir, func(i, j *Sample) int {
		switch {
		case uintptr(unsafe.Pointer(&i.RowGroup)) < uintptr(unsafe.Pointer(&j.RowGroup)):
			return -1
		case uintptr(unsafe.Pointer(&i.RowGroup)) > uintptr(unsafe.Pointer(&j.RowGroup)):
			return 1
		default:
			return 0
		}
	})

	// Since we sorted the slice by underlying rowgroup, we can perform a single scan and merge only adjacent samples.
	newReservoir := make([]*Sample, 0, len(s.reservoir))
	current := s.reservoir[0]
	for i := 1; i < len(s.reservoir); i++ {
		if !current.sampled {
			newReservoir = append(newReservoir, current)
			current = s.reservoir[i]
			continue
		}

		if uintptr(unsafe.Pointer(&current.RowGroup)) == uintptr(unsafe.Pointer(&s.reservoir[i].RowGroup)) {
			current.rows.AddMany(s.reservoir[i].rows.ToArray())
			continue
		} else {
			newReservoir = append(newReservoir, current)
			current = s.reservoir[i]
		}
	}

	newReservoir = append(newReservoir, current)
	s.reservoir = newReservoir
}
