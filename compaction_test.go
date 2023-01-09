package frostdb

import (
	"context"
	"io"
	"testing"

	"github.com/google/btree"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// insertSamples is a helper function to insert a deterministic sample with a
// given timestamp. Note that rows inserted should be sorted by timestamp since
// it is a sorting column.
func insertSamples(ctx context.Context, t *testing.T, table *Table, timestamps ...int64) uint64 {
	t.Helper()
	samples := make([]any, 0, len(timestamps))
	for _, ts := range timestamps {
		samples = append(samples, dynparquet.Sample{
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
			},
			Timestamp: ts,
		})
	}
	tx, err := table.Write(ctx, samples...)
	require.NoError(t, err)
	return tx
}

// insertSampleRecords is the same helper function as insertSamples but it inserts arrow records instead.
func insertSampleRecords(ctx context.Context, t *testing.T, table *Table, timestamps ...int64) uint64 {
	t.Helper()
	var samples dynparquet.Samples
	samples = make([]dynparquet.Sample, 0, len(timestamps))
	for _, ts := range timestamps {
		samples = append(samples, dynparquet.Sample{
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
			},
			Timestamp: ts,
		})
	}

	ps, err := table.Schema().DynamicParquetSchema(map[string][]string{
		"labels": {"label1"},
	})
	require.NoError(t, err)

	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps, logicalplan.IterOptions{})
	require.NoError(t, err)

	ar, err := samples.ToRecord(sc)
	require.NoError(t, err)

	tx, err := table.InsertRecord(ctx, ar)
	require.NoError(t, err)
	return tx
}

func TestCompaction(t *testing.T) {
	// expectedPart specifies the expected part data the test should verify.
	type expectedPart struct {
		compactionLevel parts.CompactionLevel
		numRowGroups    int
		numRows         int
		// data is the expected data. Only the timestamps are verified in this
		// test for simplicity.
		data []int64
	}
	// expectedGranule specifies the expected granule data the test should
	// verify.
	type expectedGranule struct {
		parts []expectedPart
	}

	const (
		// comactCommand indicates a compaction should be performed. Note that
		// compactions need to be specified explicitly.
		compactCommand = -1
		// recordGranuleSizeCommand records the granule size at that moment.
		recordGranuleSizeCommand = -2
		// setRecordedGranuleSizeCommand sets the granule size to the size that
		// was recorded when recordGranuleSizeCommand was executed.
		setRecordedGranuleSizeCommand = -3
		// acc accumulates the following inserts into a buffer.
		acc = -4
		// flushAcc inserts the accumulated data into the table. Useful to
		// create row groups larger than one row.
		flushAcc = -5
	)

	testCases := []struct {
		name string
		// rgSize is the desired row group size. If unspecified, will default to
		// 2 rows.
		rgSize int
		// inserts are the timestamps to insert at. Negative int64s are
		// interpreted as commands to do something, see the const declaration
		// above.
		inserts  []int64
		expected []expectedGranule
	}{
		{
			name: "SimpleLevel0ToLevel1",
			// Insert three rows.
			inserts: []int64{1, 2, 3, compactCommand},
			// Expect compaction into a single part with level1 compaction.
			expected: []expectedGranule{
				{
					[]expectedPart{
						{
							numRowGroups:    2,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{1, 2, 3},
						},
					},
				},
			},
		},
		{
			// This test is the same as above, but inserts a couple more rows,
			// expecting a second part with compaction level 0 to be created.
			name: "AddLevel0ToLevel1",
			// Insert three rows.
			inserts: []int64{1, 2, 3, compactCommand, 4},
			expected: []expectedGranule{
				{
					[]expectedPart{
						{
							numRowGroups:    1,
							compactionLevel: parts.CompactionLevel0,
							data:            []int64{4},
						},
						{
							numRowGroups:    2,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{1, 2, 3},
						},
					},
				},
			},
		},
		{
			// This test is the same as above, but adds a command to set the
			// granule size artificially low followed by a compaction. This
			// should trigger a split: i.e. the new part is reassigned to a new
			// granule.
			name: "Split",
			inserts: []int64{
				1, 2, 3,
				compactCommand, recordGranuleSizeCommand,
				4, 5,
				setRecordedGranuleSizeCommand, compactCommand,
			},
			expected: []expectedGranule{
				{
					[]expectedPart{
						{
							numRowGroups:    2,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{1, 2, 3},
						},
					},
				},
				{
					[]expectedPart{
						{
							numRowGroups:    1,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{4, 5},
						},
					},
				},
			},
		},
		{
			// SimpleOverlap tests out of order inserts after a compaction. The
			// new inserts should be merged into the existing part, because
			// otherwise a new granule could be created with overlapping data,
			// rendering the index useless.
			name:    "SimpleOverlap",
			inserts: []int64{1, 2, 3, compactCommand, 2, compactCommand},
			expected: []expectedGranule{
				{
					[]expectedPart{
						{
							numRowGroups:    2,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{1, 2, 2, 3},
						},
					},
				},
			},
		},
		{
			name: "MultiOverlap",
			inserts: []int64{
				1, 3, // Don't insert 2 to test non-equality overlapping.
				compactCommand,
				4, 5, 6,
				compactCommand,
				7, 8, 9,
				compactCommand,
				5, 2, 1,
				compactCommand,
			},
			expected: []expectedGranule{
				{
					[]expectedPart{
						{
							numRowGroups:    2,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{7, 8, 9},
						},
						{
							numRowGroups:    4,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{1, 1, 2, 3, 4, 5, 5, 6},
						},
					},
				},
			},
		},
		{
			name: "SingleL0MultipleL1Overlaps",
			inserts: []int64{
				1, 3,
				compactCommand,
				4, 5, 6,
				compactCommand,
				acc, 2, 4, flushAcc,
				compactCommand,
			},
			expected: []expectedGranule{
				{
					[]expectedPart{
						{
							numRowGroups:    4,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{1, 2, 3, 4, 4, 5, 6},
						},
					},
				},
			},
		},
		{
			name: "MultipleL0ToL1",
			inserts: []int64{
				acc, 1, 2, 3, flushAcc,
				acc, 4, 5, 6, flushAcc,
				recordGranuleSizeCommand,
				acc, 7, 8, 9, flushAcc,
				setRecordedGranuleSizeCommand,
				compactCommand,
			},
			expected: []expectedGranule{
				{
					[]expectedPart{
						{
							numRowGroups:    2,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{1, 2, 3, 4},
						},
					},
				},
				{
					[]expectedPart{
						{
							numRowGroups:    2,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{5, 6, 7, 8},
						},
					},
				},
				{
					[]expectedPart{
						{
							numRowGroups:    1,
							compactionLevel: parts.CompactionLevel1,
							data:            []int64{9},
						},
					},
				},
			},
		},
	}

	numParts := func(g *Granule) int {
		numparts := 0
		g.parts.Iterate(func(p *parts.Part) bool {
			numparts++
			return true
		})
		return numparts
	}

	for _, tc := range testCases {
		f := func(asArrow bool) func(t *testing.T) {
			return func(t *testing.T) {
				c, table := basicTable(t)
				defer c.Close()
				// Disable interval compaction for tests. These are triggered
				// manually.
				table.db.compactorPool.stop()
				table.db.compactorPool = nil

				table.config.rowGroupSize = 2
				if tc.rgSize != 0 {
					table.config.rowGroupSize = tc.rgSize
				}

				var (
					numInserts          int
					accumulating        bool
					accBuf              []int64
					lastTx              uint64
					recordedGranuleSize uint64
				)
				for _, v := range tc.inserts {
					switch v {
					case compactCommand:
						table.db.Wait(lastTx)
						require.Equal(
							t,
							1,
							table.active.Index().Len(),
							"tests assume only a single granule as input",
						)
						success, err := table.active.compactGranule(
							(table.active.Index().Min()).(*Granule),
							table.db.columnStore.compactionConfig,
						)
						require.True(t, success)
						require.NoError(t, err)
					case recordGranuleSizeCommand:
						table.db.Wait(lastTx)
						require.Equal(
							t,
							1,
							table.active.Index().Len(),
							"tests assume only a single granule as input",
						)
						recordedGranuleSize = (table.active.Index().Min()).(*Granule).metadata.size.Load()
					case setRecordedGranuleSizeCommand:
						table.db.columnStore.granuleSizeBytes = int64(recordedGranuleSize)
					case acc:
						accumulating = true
					case flushAcc:
						accumulating = false
						switch asArrow {
						case true:
							lastTx = insertSampleRecords(context.Background(), t, table, accBuf...)
						default:
							lastTx = insertSamples(context.Background(), t, table, accBuf...)
						}
						accBuf = accBuf[:0]
						numInserts++
					default:
						if accumulating {
							accBuf = append(accBuf, v)
							continue
						}
						switch asArrow {
						case true:
							lastTx = insertSampleRecords(context.Background(), t, table, v)
						default:
							lastTx = insertSamples(context.Background(), t, table, v)
						}
						numInserts++
					}
				}

				require.Equal(t, len(tc.expected), table.active.Index().Len())
				i := 0
				table.active.Index().Ascend(func(item btree.Item) bool {
					g := item.(*Granule)
					expected := tc.expected[i]
					require.Equal(t, len(expected.parts), numParts(g))

					j := 0
					g.parts.Iterate(func(p *parts.Part) bool {
						expectedPart := expected.parts[j]
						if expectedPart.numRowGroups == 0 {
							require.Equal(t, int64(expectedPart.numRows), p.Record().NumRows())
						} else {
							buf, err := p.AsSerializedBuffer(table.Schema())
							require.NoError(t, err)
							rgs := buf.ParquetFile().RowGroups()
							require.Equal(t, expectedPart.numRowGroups, len(rgs))
							require.Equal(t, expectedPart.compactionLevel, p.CompactionLevel())
							rowsRead := make([]parquet.Row, 0)
							for _, rg := range rgs {
								func() {
									rows := rg.Rows()
									defer rows.Close()

									for {
										rowBuf := make([]parquet.Row, 1)
										n, err := rows.ReadRows(rowBuf)
										if err != nil && err != io.EOF {
											require.NoError(t, err)
										}
										if n > 0 {
											rowsRead = append(rowsRead, rowBuf...)
										}

										if err == io.EOF {
											break
										}
									}
								}()
							}
							require.Equal(
								t,
								len(expectedPart.data),
								len(rowsRead),
								"different number of rows read for granule %d part %d",
								i,
								j,
							)

							// This is a bit of a hack. If the check below fails
							// unexpectedly after a change to the default schema, think
							// about a more robust search of the timestamp column index.
							const timestampColumnIdx = 3
							for k, expectedTimestamp := range expectedPart.data {
								require.Equal(t, rowsRead[k][timestampColumnIdx].Int64(), expectedTimestamp)
							}
						}

						j++
						return true
					})
					i++
					return true
				})
			}
		}
		t.Run(tc.name+"-parquet", f(false))
		t.Run(tc.name+"-arrow", f(true))
	}
}
