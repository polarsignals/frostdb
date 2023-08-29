package physicalplan

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// OrderedSynchronizer implements synchronizing ordered input from multiple
// goroutines. The strategy used is that any input that calls Callback must wait
// for all the other inputs to call Callback, since an ordered result cannot
// be produced until all inputs have pushed data. Another strategy would be to
// store the pushed records, but that requires fully copying all the data for
// safety.
type OrderedSynchronizer struct {
	pool         memory.Allocator
	orderByExprs []logicalplan.Expr
	orderByCols  []int

	sync struct {
		mtx        sync.Mutex
		lastSchema *arrow.Schema
		data       []arrow.Record
		// inputsWaiting is an integer that keeps track of the number of inputs
		// waiting on the wait channel. It cannot be an atomic because it
		// sometimes needs to be compared to inputsRunning.
		inputsWaiting int
		// inputsRunning is an integer that keeps track of the number of inputs
		// that have not called Finish yet.
		inputsRunning int
	}
	wait chan struct{}
	next PhysicalPlan
}

func NewOrderedSynchronizer(pool memory.Allocator, inputs int, orderByExprs []logicalplan.Expr) *OrderedSynchronizer {
	o := &OrderedSynchronizer{
		pool:         pool,
		orderByExprs: orderByExprs,
		wait:         make(chan struct{}),
	}
	o.sync.inputsRunning = inputs
	return o
}

func (o *OrderedSynchronizer) Close() {
	o.next.Close()
}

func (o *OrderedSynchronizer) Callback(ctx context.Context, r arrow.Record) error {
	o.sync.mtx.Lock()
	o.sync.data = append(o.sync.data, r)
	o.sync.inputsWaiting++
	inputsWaiting := o.sync.inputsWaiting
	inputsRunning := o.sync.inputsRunning
	o.sync.mtx.Unlock()

	if inputsWaiting != inputsRunning {
		select {
		case <-o.wait:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	o.sync.mtx.Lock()
	defer o.sync.mtx.Unlock()
	o.sync.inputsWaiting--
	// This is the last input to call Callback, merge the records.
	mergedRecord, err := o.mergeRecordsLocked()
	if err != nil {
		return err
	}

	// Note that we hold the mutex while calling Callback because we want to
	// ensure that Callback is called in an ordered fashion since we could race
	// with a call to Callback in Finish.
	return o.next.Callback(ctx, mergedRecord)
}

func (o *OrderedSynchronizer) Finish(ctx context.Context) error {
	o.sync.mtx.Lock()
	defer o.sync.mtx.Unlock()
	o.sync.inputsRunning--
	running := o.sync.inputsRunning
	if running > 0 && running == o.sync.inputsWaiting {
		// All other goroutines are currently waiting to be woken up. We need to
		// merge the records.
		mergedRecord, err := o.mergeRecordsLocked()
		if err != nil {
			return err
		}
		return o.next.Callback(ctx, mergedRecord)
	}
	if running < 0 {
		return errors.New("too many OrderedSynchronizer Finish calls")
	}
	if running > 0 {
		return nil
	}
	return o.next.Finish(ctx)
}

// mergeRecordsLocked must be called while holding o.sync.mtx. It merges the
// records found in o.sync.data and unblocks all the inputs waiting on o.wait.
func (o *OrderedSynchronizer) mergeRecordsLocked() (arrow.Record, error) {
	if err := o.ensureSameSchema(o.sync.data); err != nil {
		return nil, err
	}
	mergedRecord, err := arrowutils.MergeRecords(o.pool, o.sync.data, o.orderByCols)
	if err != nil {
		return nil, err
	}
	// Now that the records have been merged, we can wake up the other input
	// goroutines. Since we have exactly o.sync.inputsWaiting waiting on the
	// channel, send the corresponding number of messages. Since we are also
	// holding the records mutex during this broadcast, fast inputs won't be
	// able to re-enter Callback until the mutex is released, so won't
	// mistakenly read another input's signal.
	for i := 0; i < o.sync.inputsWaiting; i++ {
		o.wait <- struct{}{}
	}
	// Reset inputsWaiting.
	o.sync.inputsWaiting = 0
	o.sync.data = o.sync.data[:0]
	return mergedRecord, nil
}

// ensureSameSchema ensures that all the records have the same schema. In cases
// where the schema is not equal, virtual null columns are inserted in the
// records with the missing column. When we have static schemas in the execution
// engine, steps like these should be unnecessary.
func (o *OrderedSynchronizer) ensureSameSchema(records []arrow.Record) error {
	var needSchemaRecalculation bool
	for i := range records {
		if !records[i].Schema().Equal(o.sync.lastSchema) {
			needSchemaRecalculation = true
			break
		}
	}
	if !needSchemaRecalculation {
		return nil
	}

	orderCols := make([]map[string]arrow.Field, len(o.orderByExprs))
	leftoverCols := make(map[string]arrow.Field)
	for i, orderCol := range o.orderByExprs {
		orderCols[i] = make(map[string]arrow.Field)
		for _, r := range records {
			for _, field := range r.Schema().Fields() {
				if ok := orderCol.MatchColumn(field.Name); ok {
					orderCols[i][field.Name] = field
				} else {
					leftoverCols[field.Name] = field
				}
			}
		}
	}

	newFields := make([]arrow.Field, 0, len(orderCols))
	for _, colsFound := range orderCols {
		if len(colsFound) == 0 {
			// An expected order by field is missing from the records, this
			// field will just be considered to be null.
			continue
		}

		if len(colsFound) == 1 {
			for _, field := range colsFound {
				newFields = append(newFields, field)
			}
			continue
		}
		// These columns are dynamic columns and should be merged to follow
		// the physical sort order.
		colNames := make([]string, 0, len(colsFound))
		for name := range colsFound {
			colNames = append(colNames, name)
		}
		// MergeDeduplicatedDynCols will return the dynamic column names in
		// the order that they sort physically.
		for _, name := range dynparquet.MergeDeduplicatedDynCols(colNames) {
			newFields = append(newFields, colsFound[name])
		}
	}

	o.orderByCols = o.orderByCols[:0]
	for i := range newFields {
		o.orderByCols = append(o.orderByCols, i)
	}

	for _, field := range leftoverCols {
		newFields = append(newFields, field)
	}

	// This is the schema that all records must respect in order to be merged.
	schema := arrow.NewSchema(newFields, nil)

	for i := range records {
		otherSchema := records[i].Schema()
		if schema.Equal(records[i].Schema()) {
			continue
		}

		var columns []arrow.Array
		for _, field := range schema.Fields() {
			if otherFields := otherSchema.FieldIndices(field.Name); otherFields != nil {
				if len(otherFields) > 1 {
					fieldsFound, _ := otherSchema.FieldsByName(field.Name)
					return fmt.Errorf(
						"found multiple fields %v for name %s",
						fieldsFound,
						field.Name,
					)
				}
				columns = append(columns, records[i].Column(otherFields[0]))
			} else {
				// Note that this VirtualNullArray will be read from, but the
				// merged output will be a physical null array, so there is no
				// virtual->physical conversion necessary before we return data.
				columns = append(columns, arrowutils.MakeVirtualNullArray(field.Type, int(records[i].NumRows())))
			}
		}

		records[i] = array.NewRecord(schema, columns, records[i].NumRows())
	}
	o.sync.lastSchema = schema
	return nil
}

func (o *OrderedSynchronizer) SetNext(next PhysicalPlan) {
	o.next = next
}

func (o *OrderedSynchronizer) Draw() *Diagram {
	return &Diagram{Details: "OrderedSynchronizer", Child: o.next.Draw()}
}
