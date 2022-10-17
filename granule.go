package frostdb

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/google/btree"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
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
	// This is used for quick insertion into the btree, without requiring an
	// iterator.
	least atomic.Pointer[dynparquet.DynamicRow]

	// size is the raw committed, and uncommitted size of the granule. It is
	// used as a suggestion for potential compaction.
	size *atomic.Uint64

	// pruned indicates if this Granule is longer found in the index.
	pruned *atomic.Uint64
}

func NewGranule(granulesCreated prometheus.Counter, tableConfig *TableConfig, firstPart *Part) (*Granule, error) {
	g := &Granule{
		granulesCreated: granulesCreated,
		parts:           NewPartList(&atomic.Pointer[Node]{}, 0, None),
		tableConfig:     tableConfig,

		metadata: GranuleMetadata{
			least:  atomic.Pointer[dynparquet.DynamicRow]{},
			size:   &atomic.Uint64{},
			pruned: &atomic.Uint64{},
		},
	}

	// Find the "smallest" row
	if firstPart != nil {
		g.metadata.size.Store(uint64(firstPart.Buf.ParquetFile().Size()))
		g.parts.Prepend(firstPart)
		least, err := firstPart.Least()
		if err != nil {
			return nil, err
		}
		g.metadata.least.Store(least)
	}

	granulesCreated.Inc()
	return g, nil
}

func (g *Granule) addPart(p *Part, r *dynparquet.DynamicRow) (uint64, error) {
	rows := p.Buf.NumRows()
	if rows == 0 {
		return g.metadata.size.Load(), nil
	}
	node := g.parts.Prepend(p)

	newSize := g.metadata.size.Add(uint64(p.Buf.ParquetFile().Size()))

	for {
		least := g.metadata.least.Load()
		if least == nil || g.tableConfig.schema.RowLessThan(r, (*dynparquet.DynamicRow)(least)) {
			if g.metadata.least.CompareAndSwap(least, r) {
				break
			}
		} else {
			break
		}
	}

	// If the prepend returned that we're adding to the compacted list; then we need to propogate the Part to the new granules
	if node.sentinel == Compacted {
		err := addPartToGranule(g.newGranules, p)
		if err != nil {
			return 0, err
		}
	}

	return newSize, nil
}

// AddPart returns the new size of the Granule.
func (g *Granule) AddPart(p *Part) (uint64, error) {
	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	reader := p.Buf.DynamicRowGroup(0).DynamicRows()
	n, err := reader.ReadRows(rowBuf)
	if err != nil {
		return 0, fmt.Errorf("read first row of part: %w", err)
	}
	if n != 1 {
		return 0, fmt.Errorf("expected to read exactly 1 row, but read %d", n)
	}
	r := rowBuf.GetCopy(0)
	if err := reader.Close(); err != nil {
		return 0, err
	}

	return g.addPart(p, r)
}

// split a granule into n granules. With the last granule containing the remainder.
// Returns the granules in order.
// This assumes the Granule has had its parts merged into a single part.
func (g *Granule) split(tx uint64, count int) ([]*Granule, int64, error) {
	// Get the first part in the granule's part list.
	var p *Part
	g.parts.Iterate(func(part *Part) bool {
		// Since all parts are already merged into one, this iterator will only
		// iterate over the one and only part.
		p = part
		return false
	})

	// Build all the new granules
	granules := make([]*Granule, 0, count)

	// TODO: Buffers should be able to efficiently slice themselves.
	var (
		rowBuf = make([]parquet.Row, 1)
		b      *bytes.Buffer
		w      *dynparquet.PooledWriter
	)
	b = bytes.NewBuffer(nil)
	w, err := g.tableConfig.schema.GetWriter(b, p.Buf.DynamicColumns())
	if err != nil {
		return nil, 0, ErrCreateSchemaWriter{err}
	}

	rowsWritten := 0
	totalSize := int64(0)
	n := int(p.Buf.NumRows()) / count

	f := p.Buf.ParquetFile()
	for _, rowGroup := range f.RowGroups() {
		rows := rowGroup.Rows()
		for {
			_, err = rows.ReadRows(rowBuf)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, 0, ErrReadRow{err}
			}

			_, err = w.WriteRows(rowBuf)
			if err != nil {
				return nil, 0, ErrWriteRow{err}
			}
			rowsWritten++

			if rowsWritten == n && len(granules) != count-1 { // If we have n rows, and aren't on the last granule create a new granule
				err = w.Close()
				if err != nil {
					return nil, 0, fmt.Errorf("close writer: %w", err)
				}
				r, err := dynparquet.ReaderFromBytes(b.Bytes())
				if err != nil {
					return nil, 0, fmt.Errorf("create reader: %w", err)
				}
				totalSize += r.ParquetFile().Size()
				gran, err := NewGranule(g.granulesCreated, g.tableConfig, NewPart(tx, r))
				if err != nil {
					return nil, 0, fmt.Errorf("new granule failed: %w", err)
				}
				granules = append(granules, gran)
				b = bytes.NewBuffer(nil)
				g.tableConfig.schema.PutWriter(w)
				w, err = g.tableConfig.schema.GetWriter(b, p.Buf.DynamicColumns())
				if err != nil {
					return nil, 0, ErrCreateSchemaWriter{err}
				}
				rowsWritten = 0
			}
		}
		if err := rows.Close(); err != nil {
			return nil, 0, err
		}
	}

	if rowsWritten > 0 {
		// Save the remaining Granule
		err = w.Close()
		if err != nil {
			return nil, 0, fmt.Errorf("close last writer: %w", err)
		}
		g.tableConfig.schema.PutWriter(w)

		r, err := dynparquet.ReaderFromBytes(b.Bytes())
		if err != nil {
			return nil, 0, fmt.Errorf("create last reader: %w", err)
		}
		totalSize += r.ParquetFile().Size()
		gran, err := NewGranule(g.granulesCreated, g.tableConfig, NewPart(tx, r))
		if err != nil {
			return nil, 0, fmt.Errorf("new granule failed: %w", err)
		}
		granules = append(granules, gran)
	}

	return granules, totalSize, nil
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
