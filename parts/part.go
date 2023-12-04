package parts

import (
	"sort"

	"github.com/apache/arrow/go/v14/arrow"

	"github.com/polarsignals/frostdb/dynparquet"
)

type Part interface {
	// Record returns the Arrow record for the part. If the part is not an Arrow
	// record part, nil is returned.
	Record() arrow.Record
	SerializeBuffer(schema *dynparquet.Schema, w dynparquet.ParquetWriter) error
	AsSerializedBuffer(schema *dynparquet.Schema) (*dynparquet.SerializedBuffer, error)
	NumRows() int64
	Size() int64
	CompactionLevel() int
	TX() uint64
	Least() (*dynparquet.DynamicRow, error)
	Most() (*dynparquet.DynamicRow, error)
	OverlapsWith(schema *dynparquet.Schema, otherPart Part) (bool, error)
}

type basePart struct {
	tx              uint64
	compactionLevel int
	minRow          *dynparquet.DynamicRow
	maxRow          *dynparquet.DynamicRow
}

func (p *basePart) CompactionLevel() int {
	return p.compactionLevel
}

func (p *basePart) TX() uint64 { return p.tx }

type Option func(*basePart)

func WithCompactionLevel(level int) Option {
	return func(p *basePart) {
		p.compactionLevel = level
	}
}

type PartSorter struct {
	schema *dynparquet.Schema
	parts  []Part
	err    error
}

func NewPartSorter(schema *dynparquet.Schema, parts []Part) *PartSorter {
	return &PartSorter{
		schema: schema,
		parts:  parts,
	}
}

func (p *PartSorter) Len() int {
	return len(p.parts)
}

func (p *PartSorter) Less(i, j int) bool {
	a, err := p.parts[i].Least()
	if err != nil {
		p.err = err
		return false
	}
	b, err := p.parts[j].Least()
	if err != nil {
		p.err = err
		return false
	}
	return p.schema.RowLessThan(a, b)
}

func (p *PartSorter) Swap(i, j int) {
	p.parts[i], p.parts[j] = p.parts[j], p.parts[i]
}

func (p *PartSorter) Err() error {
	return p.err
}

// FindMaximumNonOverlappingSet removes the minimum number of parts from the
// given slice in order to return the maximum non-overlapping set of parts.
// The function returns the non-overlapping parts first and any overlapping
// parts second. The parts returned are in sorted order according to their Least
// row.
func FindMaximumNonOverlappingSet(schema *dynparquet.Schema, parts []Part) ([]Part, []Part, error) {
	if len(parts) < 2 {
		return parts, nil, nil
	}
	sorter := NewPartSorter(schema, parts)
	sort.Sort(sorter)
	if sorter.Err() != nil {
		return nil, nil, sorter.Err()
	}

	// Parts are now sorted according to their Least row.
	prev := 0
	prevEnd, err := parts[0].Most()
	if err != nil {
		return nil, nil, err
	}
	nonOverlapping := make([]Part, 0, len(parts))
	overlapping := make([]Part, 0, len(parts))
	var missing Part
	for i := 1; i < len(parts); i++ {
		start, err := parts[i].Least()
		if err != nil {
			return nil, nil, err
		}
		curEnd, err := parts[i].Most()
		if err != nil {
			return nil, nil, err
		}
		if schema.Cmp(prevEnd, start) <= 0 {
			// No overlap, append the previous part and update end for the next
			// iteration.
			nonOverlapping = append(nonOverlapping, parts[prev])
			prevEnd = curEnd
			prev = i
			continue
		}

		// This part overlaps with the previous part. Remove the part with
		// the highest end row.
		if schema.Cmp(prevEnd, curEnd) >= 0 {
			overlapping = append(overlapping, parts[prev])
			prevEnd = curEnd
			prev = i
		} else {
			// The current part must be removed. Don't update prevEnd or prev,
			// this will be used in the next iteration and must stay the same.
			overlapping = append(overlapping, parts[i])

			if i == len(parts)-1 { // This is the last iteration mark this one as missing
				missing = parts[prev]
			}
		}
	}
	if len(overlapping) == 0 || overlapping[len(overlapping)-1] != parts[len(parts)-1] {
		// The last part either did not overlap with its previous part, or
		// overlapped but had a smaller end row than its previous part (so the
		// previous part is in the overlapping slice). The last part must be
		// appended to nonOverlapping.
		nonOverlapping = append(nonOverlapping, parts[len(parts)-1])
	} else if missing != nil {
		overlapping = append(overlapping, missing)
	}
	return nonOverlapping, overlapping, nil
}
