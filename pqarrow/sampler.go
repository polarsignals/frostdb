package pqarrow

import (
	"context"
	"math"
	"math/rand"
	"slices"

	"github.com/RoaringBitmap/roaring"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
)

type Sampler struct {
	rowGroupID int64 // monotonically increasing row group ID; used for merging samples of the same row group
	size       int64
	reservoir  []Sample
	schema     *dynparquet.Schema

	w float64
	n int64
	i float64

	// options
	filter    physicalplan.BooleanExpression
	pool      memory.Allocator
	converter *ParquetConverter
}

type Sample struct {
	id   int64
	rg   parquet.RowGroup
	rows *physicalplan.Bitmap

	sampled bool
	i       int64
}

func (s Sample) NumRows() int64 {
	if s.rows == nil {
		return s.rg.NumRows()
	}

	return int64(s.rows.GetCardinality())
}

type SamplerOption func(*Sampler)

func WithSamplerPool(pool memory.Allocator) SamplerOption {
	return func(s *Sampler) {
		s.pool = pool
	}
}

func WithSamplerFilter(filter physicalplan.BooleanExpression) SamplerOption {
	return func(s *Sampler) {
		s.filter = filter
	}
}

func WithSamplerConverter(converter *ParquetConverter) SamplerOption {
	return func(s *Sampler) {
		s.converter = converter
	}
}

func NewSampler(k int64, schema *dynparquet.Schema, options ...SamplerOption) *Sampler {
	s := &Sampler{
		size:   k,
		schema: schema,
		w:      math.Exp(math.Log(rand.Float64()) / float64(k)),
		pool:   memory.NewGoAllocator(),
	}
	for _, opt := range options {
		opt(s)
	}
	return s
}

// NumRows is a noop
func (s *Sampler) NumRows() int { return 0 }

// Reset is a noop
func (s *Sampler) Reset() {}

func (s *Sampler) Convert(ctx context.Context, rg parquet.RowGroup, _ *dynparquet.Schema, _ *roaring.Bitmap) error {
	sample := Sample{
		rg: rg,
		id: s.rowGroupID,
	}
	s.rowGroupID++
	if s.filter != nil {
		bm, _, err := s.filter.EvalParquet(rg, nil)
		if err != nil {
			return err
		}

		sample.rows = bm
	}

	var ok bool
	sample, ok = s.fill(sample)
	if !ok {
		return nil
	}
	if s.n == s.size { // The reservoir is full; Slice up the reservoir into single row entries. This will make replacement easier.
		s.sliceReservoir()
	}

	// Sample the rowgroup
	s.sample(sample)
	return nil
}

func (s *Sampler) fill(rg Sample) (Sample, bool) {
	if rg.NumRows() == 0 {
		return Sample{}, false
	}

	if s.n+rg.NumRows() <= s.size { // All rows fit in the reservoir
		s.reservoir = append(s.reservoir, rg)
		s.n += rg.NumRows()
		return Sample{}, false
	}

	remaining := s.size - s.n
	rows := rg.rows.ToArray()
	bm := physicalplan.NewBitmap()
	bm.AddMany(rows[:remaining])
	s.reservoir = append(s.reservoir, Sample{rg: rg.rg, rows: bm})
	s.n = s.size

	toSample := physicalplan.NewBitmap()
	toSample.AddMany(rows[remaining:])
	return Sample{rg: rg.rg, rows: toSample}, true
}

func (s *Sampler) sliceReservoir() {
	newReservoir := make([]Sample, 0, s.size)
	for _, sample := range s.reservoir {
		rows := sample.rows.ToArray()
		for _, row := range rows {
			newReservoir = append(newReservoir, Sample{rg: sample.rg, rows: sample.rows, sampled: true, i: int64(row)})
		}
	}
	s.reservoir = newReservoir
}

func (s *Sampler) sample(rg Sample) {
	indices := rg.rows.ToArray()
	n := s.n + rg.NumRows()
	if s.i == 0 {
		s.i = float64(s.n) - 1
	} else if s.i < float64(n) {
		index := indices[int64(s.i)-s.n]
		s.replace(rand.Intn(int(s.size)), Sample{rg: rg.rg, rows: rg.rows, sampled: true, i: int64(index)})
		s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
	}

	for s.i < float64(n) {
		s.i += math.Floor(math.Log(rand.Float64())/math.Log(1-s.w)) + 1
		if s.i < float64(n) {
			// replace a random item of the reservoir with row i
			index := indices[int64(s.i)-s.n]
			s.replace(rand.Intn(int(s.size)), Sample{rg: rg.rg, rows: rg.rows, sampled: true, i: int64(index)})
			s.w = s.w * math.Exp(math.Log(rand.Float64())/float64(s.size))
		}
	}
	s.n = n
}

func (s *Sampler) replace(j int, r Sample) {
	s.reservoir[j].rg = r.rg
	s.reservoir[j].rows = r.rows
	s.reservoir[j].sampled = r.sampled
	s.reservoir[j].i = r.i
}

func (s *Sampler) NewRecord() (arrow.Record, error) {
	if len(s.reservoir) == 0 {
		return nil, nil
	}
	s.merge()

	if s.converter == nil {
		s.converter = NewParquetConverter(s.pool, logicalplan.IterOptions{})
		defer s.converter.Close()
	}

	for _, sample := range s.reservoir {
		if err := s.converter.Convert(context.TODO(), sample.rg, s.schema, sample.rows); err != nil {
			return nil, err
		}
	}

	return s.converter.NewRecord()
}

// Merge merges samples that contain the same underlying rowgroup into a single sample.
func (s *Sampler) merge() {
	slices.SortFunc(s.reservoir, func(i, j Sample) int {
		switch {
		case i.id < j.id:
			return -1
		case i.id > j.id:
			return 1
		default:
			return 0
		}
	})

	// Since we sorted the slice by underlying rowgroup, we can perform a single scan and merge only adjacent samples.
	newReservoir := make([]Sample, 0, len(s.reservoir))
	current := s.reservoir[0]
	if current.sampled {
		current.rows = physicalplan.NewBitmap()
		current.rows.Add(uint32(current.i))
	}
	for i := 1; i < len(s.reservoir); i++ {
		if !current.sampled {
			newReservoir = append(newReservoir, current)
			current = s.reservoir[i]
			if current.sampled {
				current.rows = physicalplan.NewBitmap()
				current.rows.Add(uint32(current.i))
			}
			continue
		}

		if current.id == s.reservoir[i].id {
			current.rows.Add(uint32(s.reservoir[i].i))
			continue
		}

		newReservoir = append(newReservoir, current)
		current = s.reservoir[i]
		if current.sampled {
			current.rows = physicalplan.NewBitmap()
			current.rows.Add(uint32(current.i))
		}
	}

	newReservoir = append(newReservoir, current)
	s.reservoir = newReservoir

	total := 0
	for _, sample := range s.reservoir {
		total += int(sample.rows.GetCardinality())
	}
}
