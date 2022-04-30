package arcticdb

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/google/btree"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/parquet-go"
	"go.uber.org/atomic"

	"github.com/polarsignals/arcticdb/dynparquet"
)

type Granule struct {
	metadata GranuleMetadata

	parts       *PartList
	tableConfig *TableConfig

	granulesCreated prometheus.Counter

	// newGranules are the granules that were created after a split
	newGranules []*Granule
}

// GranuleMetadata is the metadata for a granule.
type GranuleMetadata struct {
	// least is the row that exists within the Granule that is the least.
	// This is used for quick insertion into the btree, without requiring an iterator
	least *atomic.UnsafePointer

	// min contains the minimum value found for each column in the granule. It is used during iteration to validate if the granule contains interesting data
	minlock sync.RWMutex
	min     map[string]*parquet.Value
	// max contains the maximum value found for each column in the granule. It is used during iteration to validate if the granule contains interesting data
	maxlock sync.RWMutex
	max     map[string]*parquet.Value

	// card is the raw commited, and uncommited cardinality of the granule. It is used as a suggestion for potential compaction
	card *atomic.Uint64

	// pruned indicates if this Granule is longer found in the index
	pruned *atomic.Uint64
}

func NewGranule(granulesCreated prometheus.Counter, tableConfig *TableConfig, firstPart *Part) (*Granule, error) {
	g := &Granule{
		granulesCreated: granulesCreated,
		parts:           NewPartList(nil, 0, None),
		tableConfig:     tableConfig,

		metadata: GranuleMetadata{
			min:    map[string]*parquet.Value{},
			max:    map[string]*parquet.Value{},
			least:  atomic.NewUnsafePointer(nil),
			card:   atomic.NewUint64(0),
			pruned: atomic.NewUint64(0),
		},
	}

	// Find the least column
	if firstPart != nil {
		g.metadata.card = atomic.NewUint64(uint64(firstPart.Buf.NumRows()))
		g.parts.Prepend(firstPart)
		// Since we assume a part is sorted, we need only to look at the first row in each Part
		row, err := firstPart.Buf.DynamicRowGroup(0).DynamicRows().ReadRow(nil)
		if err != nil {
			return nil, err
		}
		g.metadata.least.Store(unsafe.Pointer(row))

		// Set the minmaxes on the new granule
		if err := g.minmaxes(firstPart); err != nil {
			return nil, err
		}
	}

	granulesCreated.Inc()
	return g, nil
}

// AddPart returns the new cardinality of the Granule.
func (g *Granule) AddPart(p *Part) (uint64, error) {
	rows := p.Buf.NumRows()
	if rows == 0 {
		return g.metadata.card.Load(), nil
	}
	node := g.parts.Prepend(p)

	newcard := g.metadata.card.Add(uint64(p.Buf.NumRows()))
	r, err := p.Buf.DynamicRowGroup(0).DynamicRows().ReadRow(nil)
	if err != nil {
		return 0, err
	}

	for {
		least := g.metadata.least.Load()
		if least == nil || g.tableConfig.schema.RowLessThan(r, (*dynparquet.DynamicRow)(least)) {
			if g.metadata.least.CAS(least, unsafe.Pointer(r)) {
				break
			}
		} else {
			break
		}
	}

	// Set the minmaxes for the granule
	if err := g.minmaxes(p); err != nil {
		return 0, err
	}

	// If the prepend returned that we're adding to the compacted list; then we need to propogate the Part to the new granules
	if node.sentinel == Compacted {
		err := addPartToGranule(g.newGranules, p)
		if err != nil {
			return 0, err
		}
	}

	return newcard, nil
}

// split a granule into n sized granules. With the last granule containing the remainder.
// Returns the granules in order.
// This assumes the Granule has had its parts merged into a single part.
func (g *Granule) split(tx uint64, n int) ([]*Granule, error) {
	// Get the first part in the granule's part list.
	var p *Part
	g.parts.Iterate(func(part *Part) bool {
		// Since all parts are already merged into one, this iterator will only
		// iterate over the one and only part.
		p = part
		return false
	})
	// How many granules we'll need to build
	count := int(p.Buf.NumRows()) / n

	// Build all the new granules
	granules := make([]*Granule, 0, count)

	// TODO: Buffers should be able to efficiently slice themselves.
	var (
		row parquet.Row
		b   *bytes.Buffer
		w   *parquet.Writer
		err error
	)
	b = bytes.NewBuffer(nil)
	w, err = g.tableConfig.schema.NewWriter(b, p.Buf.DynamicColumns())
	if err != nil {
		return nil, ErrCreateSchemaWriter{err}
	}
	rowsWritten := 0

	f := p.Buf.ParquetFile()
	rowGroups := f.NumRowGroups()
	for i := 0; i < rowGroups; i++ {
		rows := f.RowGroup(i).Rows()
		for {
			row, err = rows.ReadRow(row[:0])
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, ErrReadRow{err}
			}

			err = w.WriteRow(row)
			if err != nil {
				return nil, ErrWriteRow{err}
			}
			rowsWritten++

			if rowsWritten == n && len(granules) != count-1 { // If we have n rows, and aren't on the last granule, create the n-sized granule
				err = w.Close()
				if err != nil {
					return nil, fmt.Errorf("close writer: %w", err)
				}
				r, err := dynparquet.ReaderFromBytes(b.Bytes())
				if err != nil {
					return nil, fmt.Errorf("create reader: %w", err)
				}
				gran, err := NewGranule(g.granulesCreated, g.tableConfig, NewPart(tx, r))
				if err != nil {
					return nil, fmt.Errorf("new granule failed: %w", err)
				}
				granules = append(granules, gran)
				b = bytes.NewBuffer(nil)
				w, err = g.tableConfig.schema.NewWriter(b, p.Buf.DynamicColumns())
				if err != nil {
					return nil, ErrCreateSchemaWriter{err}
				}
				rowsWritten = 0
			}
		}
	}

	if rowsWritten > 0 {
		// Save the remaining Granule
		err = w.Close()
		if err != nil {
			return nil, fmt.Errorf("close last writer: %w", err)
		}
		r, err := dynparquet.ReaderFromBytes(b.Bytes())
		if err != nil {
			return nil, fmt.Errorf("create last reader: %w", err)
		}
		gran, err := NewGranule(g.granulesCreated, g.tableConfig, NewPart(tx, r))
		if err != nil {
			return nil, fmt.Errorf("new granule failed: %w", err)
		}
		granules = append(granules, gran)
	}

	return granules, nil
}

// PartBuffersForTx returns the PartBuffers for the given transaction constraints.
func (g *Granule) PartBuffersForTx(watermark uint64, iterator func(*dynparquet.SerializedBuffer) bool) {
	g.parts.Iterate(func(p *Part) bool {
		// Don't iterate over parts from an uncompleted transaction
		if p.tx > watermark {
			return true
		}

		return iterator(p.Buf)
	})
}

// Less implements the btree.Item interface.
func (g *Granule) Less(than btree.Item) bool {
	return g.tableConfig.schema.RowLessThan(g.Least(), than.(*Granule).Least())
}

// Least returns the least row in a Granule.
func (g *Granule) Least() *dynparquet.DynamicRow {
	return (*dynparquet.DynamicRow)(g.metadata.least.Load())
}

// minmaxes finds the mins and maxes of every column in a part.
func (g *Granule) minmaxes(p *Part) error {
	f := p.Buf.ParquetFile()
	numRowGroups := f.NumRowGroups()
	for i := 0; i < numRowGroups; i++ {
		rowGroup := f.RowGroup(i)

		numColumns := rowGroup.NumColumns()
		for j := 0; j < numColumns; j++ {
			columnChunk := rowGroup.Column(j)

			pages := columnChunk.Pages()
			for {
				p, err := pages.ReadPage()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				page := p.Values()
				values := make([]parquet.Value, p.NumValues())
				n, err := page.ReadValues(values)
				if err != nil && err != io.EOF {
					return err
				}
				if n == 0 {
					break
				}

				// Check for min
				min := findMin(values)
				g.metadata.minlock.RLock()
				val := g.metadata.min[rowGroup.Schema().Fields()[columnChunk.Column()].Name()]
				g.metadata.minlock.RUnlock()
				if val == nil || Compare(*val, *min) == 1 {
					if !min.IsNull() {
						g.metadata.minlock.Lock() // Check again after acquiring the write lock
						if val := g.metadata.min[rowGroup.Schema().Fields()[columnChunk.Column()].Name()]; val == nil || Compare(*val, *min) == 1 {
							g.metadata.min[rowGroup.Schema().Fields()[columnChunk.Column()].Name()] = min
						}
						g.metadata.minlock.Unlock()
					}
				}

				// Check for max
				max := findMax(values)
				g.metadata.maxlock.RLock()
				val = g.metadata.max[rowGroup.Schema().Fields()[columnChunk.Column()].Name()]
				g.metadata.maxlock.RUnlock()
				if val == nil || Compare(*val, *max) == -1 {
					if !max.IsNull() {
						g.metadata.maxlock.Lock() // Check again after acquiring the write lock
						if val := g.metadata.max[rowGroup.Schema().Fields()[columnChunk.Column()].Name()]; val == nil || Compare(*val, *max) == -1 {
							g.metadata.max[rowGroup.Schema().Fields()[columnChunk.Column()].Name()] = max
						}
						g.metadata.maxlock.Unlock()
					}
				}
			}
		}
	}

	return nil
}

func find(minmax int, values []parquet.Value) *parquet.Value {
	if len(values) == 0 {
		return nil
	}

	val := values[0]
	for i := 1; i < len(values); i++ {
		if Compare(val, values[i]) != minmax {
			val = values[i]
		}
	}

	return &val
}

func findMax(values []parquet.Value) *parquet.Value {
	return find(1, values)
}

func findMin(values []parquet.Value) *parquet.Value {
	return find(-1, values)
}

func Compare(v1, v2 parquet.Value) int {
	if v1.Kind() != v2.Kind() {
		return -2
	}
	switch v1.Kind() {
	case parquet.Int32:
		switch {
		case v1.Int32() > v2.Int32():
			return 1
		case v1.Int32() < v2.Int32():
			return -1
		}
	case parquet.Int64:
		switch {
		case v1.Int64() > v2.Int64():
			return 1
		case v1.Int64() < v2.Int64():
			return -1
		}
	case parquet.Float:
		switch {
		case v1.Float() > v2.Float():
			return 1
		case v1.Float() < v2.Float():
			return -1
		}
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return bytes.Compare(v1.ByteArray(), v2.ByteArray())
	case -1: // null
		return 0
	}

	return 0
}
