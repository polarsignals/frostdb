package logictest

import (
	"context"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb"
)

const testdataDirectory = "testdata"

// TestLogic runs all the datadriven tests in the testdata directory. Refer to
// the RunCmd method of the Runner struct for more information on the expected
// syntax of these tests. If this test fails but the results look the same, it
// might be because the test returns tab-separated expected results and your
// IDE inserts tabs instead of spaces. Just run this test with the -rewrite flag
// to rewrite expected results.
// TODO(asubiotto): Add metamorphic testing to logic tests. The idea is to
// randomly generate variables that should have no effect on output (e.g. row
// group split points, granule split size).
func TestLogic(t *testing.T) {
	ctx := context.Background()

	t.Parallel()
	datadriven.Walk(t, testdataDirectory, func(t *testing.T, path string) {
		columnStore, err := frostdb.New()
		require.NoError(t, err)
		defer columnStore.Close()
		db, err := columnStore.DB(ctx, "test")
		require.NoError(t, err)
		r := NewRunner(db)
		datadriven.RunTest(t, path, func(t *testing.T, c *datadriven.TestData) string {
			return r.RunCmd(ctx, c)
		})
	})
}
