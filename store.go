package frostdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// Persist uploads the block to the underlying bucket.
func (t *TableBlock) Persist() error {
	if t.table.db.bucket == nil {
		return nil
	}

	r, w := io.Pipe()
	var err error
	go func() {
		defer w.Close()
		err = t.Serialize(w)
	}()
	defer r.Close()

	fileName := filepath.Join(t.table.name, t.ulid.String(), "data.parquet")
	if err := t.table.db.bucket.Upload(context.Background(), fileName, r); err != nil {
		return fmt.Errorf("failed to upload block %v", err)
	}

	if err != nil {
		return fmt.Errorf("failed to serialize block: %v", err)
	}
	return nil
}

// BlockFilterFunc takes a block ULID and returns true if this block can be ignored during this query
type BlockFilterFunc func(ulid.ULID, objstore.ObjectAttributes) bool

// A BlockFilter is a logical construction of multiple block filters
type BlockFilter struct {
	BlockFilterFunc
	input *BlockFilter
}

// Filter will recursively call input filters until one returns true or the final filter is reached
func (b *BlockFilter) Filter(block ulid.ULID, info objstore.ObjectAttributes) bool {
	if b.BlockFilterFunc == nil {
		return false
	}

	if b.BlockFilterFunc(block, info) {
		return true
	}

	return b.input.Filter(block, info)
}

// LastBlockTimestamp adds a LastBlockTimestamp filter to the block filter
// TODO better description
func (b *BlockFilter) LastBlockTimestamp(lastBlockTimestamp uint64) *BlockFilter {
	return &BlockFilter{
		BlockFilterFunc: func(block ulid.ULID, _ objstore.ObjectAttributes) bool {
			return lastBlockTimestamp != 0 && block.Time() >= lastBlockTimestamp
		},
		input: b,
	}
}

// TODO
func (b *BlockFilter) TimestampFilter(timestampCol string, filter logicalplan.Expr) *BlockFilter {
	if timestampCol == "" {
		return b
	}

	return &BlockFilter{
		BlockFilterFunc: func(block ulid.ULID, info objstore.ObjectAttributes) bool {
			ok, err := compareTimestamp(timestampCol, block.Time(), uint64(info.LastModified.UnixMilli()), filter)
			if err != nil {
				// TODO have a logger maybe?
			}
			return !ok
		},
		input: b,
	}
}

type BlockFilterIf interface {
	Filter(ulid.ULID, objstore.ObjectAttributes) bool
}

func (t *Table) IterateBucketBlocks(ctx context.Context, logger log.Logger, blockFilter BlockFilterIf, filter TrueNegativeFilter, iterator func(rg dynparquet.DynamicRowGroup) bool) error {
	if t.db.bucket == nil || t.db.ignoreStorageOnQuery {
		return nil
	}

	n := 0
	err := t.db.bucket.Iter(ctx, t.name, func(blockDir string) error {
		blockUlid, err := ulid.Parse(filepath.Base(blockDir))
		if err != nil {
			return err
		}

		blockName := filepath.Join(blockDir, "data.parquet")
		attribs, err := t.db.bucket.Attributes(ctx, blockName)
		if err != nil {
			return err
		}

		// NOTE: we probably could infer the timestamp width of a block if we just compare it to the next blocks ulid thus avoiding the attributes call
		if blockFilter.Filter(blockUlid, attribs) {
			return nil
		}

		b := &BucketReaderAt{
			name:   blockName,
			ctx:    ctx,
			Bucket: t.db.bucket,
		}

		file, err := parquet.OpenFile(b, attribs.Size)
		if err != nil {
			return err
		}

		// Get a reader from the file bytes
		buf, err := dynparquet.NewSerializedBuffer(file)
		if err != nil {
			return err
		}

		n++
		for i := 0; i < buf.NumRowGroups(); i++ {
			rg := buf.DynamicRowGroup(i)
			var mayContainUsefulData bool
			mayContainUsefulData, err = filter.Eval(rg)
			if err != nil {
				return err
			}
			if mayContainUsefulData {
				if continu := iterator(rg); !continu {
					return err
				}
			}
		}
		return nil
	})
	level.Debug(logger).Log("msg", "read blocks", "n", n)
	return err
}

// BucketReaderAt is an objstore.Bucket wrapper that supports the io.ReaderAt interface.
type BucketReaderAt struct {
	name string
	ctx  context.Context
	objstore.Bucket
}

// ReadAt implements the io.ReaderAt interface.
func (b *BucketReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := b.GetRange(b.ctx, b.name, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer func() {
		err = rc.Close()
	}()

	return rc.Read(p)
}

// compareTimestamp returns true if the block contains timestamps covered in expr
func compareTimestamp(col string, start, end uint64, expr logicalplan.Expr) (bool, error) {
	fmt.Println("compareTimestamp: ", col)
	if expr == nil {
		return false, nil
	}

	parseExpr := func(e *logicalplan.BinaryExpr) (uint64, error) {
		var leftColumnRef *ColumnRef
		e.Left.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.Column:
				leftColumnRef = &ColumnRef{
					ColumnName: e.ColumnName,
				}
				return false
			}
			return true
		}))
		if leftColumnRef == nil {
			return 0, errors.New("left side of binary expression must be a column")
		}

		// Not the timestamp column; don't care about it.
		if leftColumnRef.ColumnName != col {
			return 0, nil // TODO maybe return an error?
		}

		var (
			rightValue parquet.Value
			err        error
		)
		e.Right.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.LiteralExpr:
				rightValue, err = pqarrow.ArrowScalarToParquetValue(e.Value)
				return false
			}
			return true
		}))
		if err != nil {
			return 0, err
		}

		return rightValue.Uint64(), nil
	}

	switch e := expr.(type) {
	case *logicalplan.BinaryExpr:
		switch e.Op {
		case logicalplan.OpLtEq:
			v, err := parseExpr(e)
			if err != nil {
				return false, err
			}

			return start <= v, nil
		case logicalplan.OpLt:
			v, err := parseExpr(e)
			if err != nil {
				return false, err
			}

			return start < v, nil
		case logicalplan.OpGt:
			v, err := parseExpr(e)
			if err != nil {
				return false, err
			}

			return end > v, nil
		case logicalplan.OpGtEq:
			v, err := parseExpr(e)
			if err != nil {
				return false, err
			}

			return end >= v, nil
		case logicalplan.OpEq:
			fmt.Println("compareTimestamp OpEq")

			v, err := parseExpr(e)
			if err != nil {
				return false, err
			}

			return v >= start && v <= end, nil

		case logicalplan.OpAnd:
			left, err := compareTimestamp(col, start, end, e.Left)
			if err != nil {
				return false, err
			}

			right, err := compareTimestamp(col, start, end, e.Right)
			if err != nil {
				return false, err
			}

			return left && right, nil
		case logicalplan.OpOr:
			left, err := compareTimestamp(col, start, end, e.Left)
			if err != nil {
				return false, err
			}

			right, err := compareTimestamp(col, start, end, e.Right)
			if err != nil {
				return false, err
			}

			return left || right, nil
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported expr %T", e)
	}
}
