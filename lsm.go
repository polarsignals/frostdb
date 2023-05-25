package frostdb

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/segmentio/parquet-go"
)

// LSM is a log-structured merge-tree. However this tree keeps both L0 and L1 in memory in Arrow format.
// L2 is flushed to storage in Parquet format.
type LSM struct {
	prefix string
	levels *List

	// TODO support the final level of parquet files in remote storage?
}

func NewLSM(prefix string, maxLevel SentinelType) *LSM {
	lsm := &LSM{
		prefix: prefix,
		levels: NewList(&atomic.Pointer[Node]{}),
	}

	for i := maxLevel; i > 0; i-- {
		lsm.levels.Sentinel(i)
	}

	return lsm
}

func (l *LSM) Add(tx uint64, record arrow.Record) {
	record.Retain()
	l.levels.Prepend(parts.NewArrowPart(tx, record, 0, nil)) // TODO size and schema...
}

func (l *LSM) String() string {
	return l.levels.String()
}

func (l *LSM) Prefixes(ctx context.Context, prefix string) ([]string, error) {
	return []string{l.prefix}, nil
}

func (l *LSM) Scan(ctx context.Context, _ string, schema *dynparquet.Schema, _ logicalplan.Expr, _ uint64, callback func(context.Context, any) error) error {
	l.levels.Iterate(func(node *Node) bool {
		if node.part == nil { // encountered a sentinel node; continue on
			return true
		}

		if r := node.part.Record(); r != nil {
			r.Retain()
			if err := callback(ctx, r); err != nil {
				return false
			}
			return true
		}

		buf, err := node.part.AsSerializedBuffer(nil)
		if err != nil {
			panic("programming error")
		}
		if err := callback(ctx, buf); err != nil {
			return false
		}
		return true
	})
	return nil
}

func (l *LSM) findLevel(level SentinelType) *List {
	var list *List
	l.levels.Iterate(func(node *Node) bool {
		if node.part == nil && node.sentinel == level {
			list = NewList(node.next)
			return false
		}
		return true
	})

	return list
}

// merge will merge the given level into an arrow record for the next level.
func (l *LSM) merge(level SentinelType, schema *dynparquet.Schema) error {
	bufs := []dynparquet.DynamicRowGroup{}
	var next *Node
	var compact *List
	switch level {
	case L0: // special case because L0 never has a sentinel node and is always at the front of the list
		compact = l.levels.Sentinel(level + 1)
		var iterErr error
		sentinelFound := false
		compact.Iterate(func(node *Node) bool {
			if node.part == nil { // sentinel encountered
				if node.sentinel == level+1 { // either the sentinel for the beginning of the list or the end of the list
					if sentinelFound {
						next = node.next.Load() // skip the sentinel to combine the lists
						return false
					}
					sentinelFound = true
					return true
				} else {
					next = node
					return false
				}
			}

			buf, err := node.part.AsSerializedBuffer(schema)
			if err != nil {
				iterErr = err
				return false
			}

			bufs = append(bufs, buf.MultiDynamicRowGroup())
			return true
		})
		if iterErr != nil {
			return iterErr
		}
	default:
		compact = l.findLevel(level)
		var iterErr error
		compact.Iterate(func(node *Node) bool {
			if node.part == nil { // sentinel encountered
				if node.sentinel == level+1 { // either the sentinel for the beginning of the list or the end of the list
					next = node.next.Load() // skip the sentinel to combine the lists
				} else {
					next = node
				}
				return false
			}

			buf, err := node.part.AsSerializedBuffer(schema)
			if err != nil {
				iterErr = err
				return false
			}

			bufs = append(bufs, buf.MultiDynamicRowGroup())
			return true
		})
		if iterErr != nil {
			return iterErr
		}
	}

	if len(bufs) == 0 {
		return nil
	}

	merged, err := schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		return err
	}

	b := &bytes.Buffer{}
	err = func() error {
		w, err := schema.GetWriter(b, merged.DynamicColumns())
		if err != nil {
			return err
		}

		p := &parquetRowWriter{
			w:            w,
			schema:       schema,
			rowsBuf:      make([]parquet.Row, 1024),
			rowGroupSize: 4096,
		}
		defer p.close()

		rows := merged.Rows()
		defer rows.Close()
		if _, err := p.writeRows(rows); err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		return err
	}

	buf, err := dynparquet.ReaderFromBytes(b.Bytes())
	if err != nil {
		return err
	}

	// Create new list
	node := &Node{
		next: &atomic.Pointer[Node]{},
		part: parts.NewPart(0, buf),
	}
	s := &Node{
		next:     &atomic.Pointer[Node]{},
		sentinel: level + 1,
	}
	s.next.Store(node)
	if next != nil {
		node.next.Store(next)
	}
	compact.head.Store(s)
	return nil
}
