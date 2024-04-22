package parts

import (
	"testing"

	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/internal/records"
	"github.com/polarsignals/frostdb/pqarrow"
)

func TestFindMaximumNonOverlappingSet(t *testing.T) {
	testSchema, err := dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "test_schema",
		Columns: []*schemapb.Column{{
			Name: "ints",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_INT64,
				Encoding: schemapb.StorageLayout_ENCODING_PLAIN_UNSPECIFIED,
			},
		}},
		SortingColumns: []*schemapb.SortingColumn{{Name: "ints", Direction: schemapb.SortingColumn_DIRECTION_ASCENDING}},
	})
	require.NoError(t, err)

	type rng struct {
		start int64
		end   int64
	}
	type dataModel struct {
		Ints int64 `frostdb:",asc,plain"`
	}
	for _, tc := range []struct {
		name                   string
		ranges                 []rng
		expectedNonOverlapping []rng
		expectedOverlapping    []rng
	}{
		{
			name:                   "SinglePart",
			ranges:                 []rng{{1, 2}},
			expectedNonOverlapping: []rng{{1, 2}},
		},
		{
			name:                   "RemoveFirst",
			ranges:                 []rng{{1, 4}, {1, 2}},
			expectedNonOverlapping: []rng{{1, 2}},
			expectedOverlapping:    []rng{{1, 4}},
		},
		{
			name:                   "TwoNonOverlapping",
			ranges:                 []rng{{1, 2}, {3, 4}},
			expectedNonOverlapping: []rng{{1, 2}, {3, 4}},
		},
		{
			name:                   "OneOverlap",
			ranges:                 []rng{{1, 2}, {4, 7}, {3, 8}},
			expectedNonOverlapping: []rng{{1, 2}, {4, 7}},
			expectedOverlapping:    []rng{{3, 8}},
		},
		{
			name:                   "ChooseMinimumNumber",
			ranges:                 []rng{{1, 2}, {4, 10}, {4, 5}, {6, 7}},
			expectedNonOverlapping: []rng{{1, 2}, {4, 5}, {6, 7}},
			expectedOverlapping:    []rng{{4, 10}},
		},
		{
			// ReuseCursor makes sure that when dropping a range, its boundaries
			// are not reused. This is a regression test (which is why it's so
			// specific).
			name:                   "ReuseCursor",
			ranges:                 []rng{{1, 3}, {2, 4}, {4, 5}, {6, 7}},
			expectedNonOverlapping: []rng{{1, 3}, {4, 5}, {6, 7}},
			expectedOverlapping:    []rng{{2, 4}},
		},
		{
			name:                   "OnlyTwoOverlap",
			ranges:                 []rng{{1, 3}, {2, 4}},
			expectedNonOverlapping: []rng{},
			expectedOverlapping:    []rng{{2, 4}, {1, 3}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parts := make([]Part, len(tc.ranges))
			b := records.NewBuild[dataModel](memory.NewGoAllocator())
			schema, err := dynparquet.SchemaFromDefinition(b.Schema("test_schema"))
			require.NoError(t, err)
			defer b.Release()
			for i := range parts {
				start := dataModel{Ints: tc.ranges[i].start}
				end := dataModel{Ints: tc.ranges[i].end}
				require.Nil(t, b.Append(start, end))
				r := b.NewRecord()
				serBuf, err := pqarrow.SerializeRecord(r, schema)
				require.NoError(t, err)
				r.Release()
				require.NoError(t, err)
				parts[i] = NewParquetPart(0, serBuf)
			}
			nonOverlapping, overlapping, err := FindMaximumNonOverlappingSet(testSchema, parts)
			require.NoError(t, err)

			verify := func(t *testing.T, expected []rng, actual []Part) {
				t.Helper()
				require.Len(t, actual, len(expected))
				for i := range actual {
					start, err := actual[i].Least()
					require.NoError(t, err)
					end, err := actual[i].Most()
					require.NoError(t, err)
					require.Equal(t, expected[i].start, start.Row[0].Int64())
					require.Equal(t, expected[i].end, end.Row[0].Int64())
				}
			}
			verify(t, tc.expectedNonOverlapping, nonOverlapping)
			verify(t, tc.expectedOverlapping, overlapping)
		})
	}
}
