package physicalplan

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

// ConcurrentHashAggregate is the part of the physical plan that combines many
// aggregation (which may have been produced in parallel) results into a single
// result. It then adds the result to a record and emits records of multiple
// results batched together.
type ConcurrentHashAggregate struct {
	pool     memory.Allocator
	buffer   []arrow.Record
	nextPlan PhysicalPlan
	aggFunc  AggregationFunction
	// emitRecordSize is the size of the records of aggregations that will be
	// passed to the next callback
	emitRecordSize int
}

func ConcurrentAggregate(
	pool memory.Allocator,
	aggFunc AggregationFunction,
) *ConcurrentHashAggregate {
	return &ConcurrentHashAggregate{
		pool:           pool,
		buffer:         make([]arrow.Record, 0),
		aggFunc:        aggFunc,
		emitRecordSize: 1024, // TODO magic number
	}
}

func (a *ConcurrentHashAggregate) SetNextPlan(nextPlan PhysicalPlan) {
	a.nextPlan = nextPlan
}

func (a *ConcurrentHashAggregate) Callback(record arrow.Record) error {
	a.buffer = append(a.buffer, record)
	return nil
}

func (a *ConcurrentHashAggregate) Finish() error {
	combined := a.combineRecords(a.buffer)
	for _, record := range combined {
		err := a.nextPlan.Callback(record)
		if err != nil {
			return err
		}
	}

	return a.nextPlan.Finish()
}

func (a *ConcurrentHashAggregate) combineRecords(records []arrow.Record) []arrow.Record {
	schema := getFinalSchema(records)
	results := make([]arrow.Record, 0)

	// create a list of builders for each column in the data set (based on the
	// schema of the first record, as all should have the same schema)
	var numRows int
	var resultBuilders []array.Builder
	resetBuilders := func() {
		numRows = 0
		resultBuilders = make([]array.Builder, 0)
		for _, field := range schema.Fields() {
			resultBuilders = append(resultBuilders, array.NewBuilder(a.pool, field.Type))
		}
	}
	resetBuilders()

	buildRecord := func() arrow.Record {
		// combine our result builders into the list of columns
		cols := make([]arrow.Array, 0)
		for _, builder := range resultBuilders {
			cols = append(cols, builder.NewArray())
		}
		return array.NewRecord(schema, cols, int64(numRows))
	}

	// traverse the merge tree and for each path (which is a tuple in our result
	// set), add the values into each of the result builders
	mergeTree := a.buildMergeTree(records, schema)
	a.traverseAndAggregate(mergeTree, make([]interface{}, 0), func(pathTuple []interface{}, array2 arrow.Array) {
		numRows++
		for i, val := range pathTuple {
			appendArrayVal(resultBuilders[i], val)
		}

		// aggregate the results from each parallel exeuction
		aggArray, _ := a.aggFunc.Aggregate(a.pool, []arrow.Array{array2})
		aggResultBuilder := resultBuilders[len(resultBuilders)-1]
		appendArrayVal(aggResultBuilder, getArrayVal(aggArray, 0))

		// when we reach the trheshold for length of the record, build and add to results
		if numRows >= a.emitRecordSize {
			results = append(results, buildRecord())
			resetBuilders()
		}
	})

	// build the final record
	if numRows > 0 {
		results = append(results, buildRecord())
	}

	return results
}

var nilTreeNode = struct{}{}

// buildMergeTree creates a tree where each level of the tree is a column in
// the result set, and the leaf nodes are arrays values to be aggregated. For
// example the records:
//
// record1:
// col1    ["a", "b"]
// col2    ["c", "d"]
// sum(c3) [ 1,   2 ]
//
// record2:
// col1    ["a", "b"]
// col2    ["c", "f"]
// sum(c3) [ 1,   3 ]
//
//   root
//  /    \
// "a"   "b"__
//  |     |   \
// "c"   "d"  "f"
//  |     |    |
// [1,1] [2]  [3].
//
func (a *ConcurrentHashAggregate) buildMergeTree(records []arrow.Record, schema *arrow.Schema) map[interface{}]interface{} {
	mergeTree := make(map[interface{}]interface{})
	// for each record ...
	for _, record := range records {
		// for each row ...
		for i := int64(0); i < record.NumRows(); i++ {
			currTree := mergeTree
			for fieldIndex, field := range schema.Fields() {
				col := columnForName(field.Name, record)
				var key interface{}
				if col != nil {
					key = getArrayVal(col, int(i))
				}
				if key == nil {
					key = &nilTreeNode
				}

				if fieldIndex < len(schema.Fields())-2 {
					// here we're extending the tree
					if _, ok := currTree[key]; !ok {
						currTree[key] = make(map[interface{}]interface{})
					}
					currTree = currTree[key].(map[interface{}]interface{})
				} else {
					// here we're adding the leaf
					aggCol := columnForName(schema.Fields()[fieldIndex+1].Name, record)
					if _, ok := currTree[key]; !ok {
						currTree[key] = array.NewBuilder(a.pool, aggCol.DataType())
					}
					arrayList := currTree[key].(array.Builder)
					appendArrayVal(arrayList, getArrayVal(aggCol, int(i)))
					break
				}
			}
		}
	}
	return mergeTree
}

// traverseAndAggregate traverses the merge tree DFS and when it reaches a leaf
// node calls callback.
func (a *ConcurrentHashAggregate) traverseAndAggregate(
	mergeTree map[interface{}]interface{},
	pathStack []interface{},
	callback func([]interface{}, arrow.Array),
) {
	for key := range mergeTree {
		// push path element to stack
		if key == &nilTreeNode {
			pathStack = append(pathStack, nil)
		} else {
			pathStack = append(pathStack, key)
		}

		nextTree, ok := mergeTree[key].(map[interface{}]interface{})
		if ok {
			a.traverseAndAggregate(nextTree, pathStack, callback)
		} else {
			arrayBuilder := mergeTree[key].(array.Builder)
			callback(pathStack, arrayBuilder.NewArray())
		}
		pathStack = pathStack[0 : len(pathStack)-1] // pop
	}
}

// // getFinalSchema gets the schema that wil be used to merge all records. It's
// // possible that some aggregates might return results that are missing fields
// // if we're grouping by dynamic fields. This returns a schema where all fields
// // in any result record are present.
func getFinalSchema(records []arrow.Record) *arrow.Schema {
	if len(records) == 0 {
		// cannot investigate the records to determine what should be the schema,
		// so we return ane empty schema
		return arrow.NewSchema([]arrow.Field{}, nil)
	}

	// it assumes that the last field in the every result record is the field
	// being aggregated
	aggField := records[0].Schema().Fields()[len(records[0].Schema().Fields())-1]

	fields := make(map[string]arrow.DataType)
	for _, record := range records {
		for _, field := range record.Schema().Fields() {
			if field.Name == aggField.Name {
				continue
			}
			if _, ok := fields[field.Name]; !ok {
				fields[field.Name] = field.Type
			}
		}
	}

	schemaFields := make([]arrow.Field, 0)
	for name, dataType := range fields {
		schemaFields = append(schemaFields, arrow.Field{Name: name, Type: dataType})
	}
	schemaFields = append(schemaFields, aggField)
	return arrow.NewSchema(schemaFields, nil)
}

// columnForName returns the column from the record for the field with the name.
func columnForName(name string, record arrow.Record) arrow.Array {
	for columnIndex, field := range record.Schema().Fields() {
		if field.Name == name {
			return record.Column(columnIndex)
		}
	}
	return nil
}

// getArrayVal is a helper method of getting the value at some column out of the
// arrow array.
func getArrayVal(col arrow.Array, i int) interface{} {
	bin, ok := col.(*array.Binary)
	if ok {
		return bin.ValueString(i)
	}

	num, isNum := col.(*array.Int64)
	if isNum {
		return num.Value(i)
	}
	return nil
}

// appendArrayVal is a helper function for appending the value into the arrow array.
func appendArrayVal(arrayBuilder array.Builder, val interface{}) {
	if val == nil {
		arrayBuilder.AppendNull()
		return
	}

	if bin, ok := arrayBuilder.(*array.BinaryBuilder); ok {
		bin.AppendString(val.(string))
		return
	}

	if num, ok := arrayBuilder.(*array.Int64Builder); ok {
		num.Append(val.(int64))
		return
	}
}
