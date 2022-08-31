package physicalplan

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/stretchr/testify/require"
)

func TestNoRaceAndSingleFinish(t *testing.T) {
	numCbCalls := 0
	numFinCalls := 0

	finMtx := sync.Mutex{}

	nextPlan := mockPhysicalPlan{
		// testing if the callback really is run synchronously ...
		// ensure that we get the correct count, if the callback is not sync'd it
		// will be wrong (and/or the test will fail if the suite is run with the
		// -race flag)
		callback: func(context.Context, arrow.Record) error {
			numCbCalls++
			return nil
		},
		finish: func(context.Context) error {
			finMtx.Lock()
			defer finMtx.Unlock()
			numFinCalls++
			return nil
		},
	}

	merge := Merge()
	merge.SetNext(&nextPlan)

	recChan := make(chan arrow.Record)
	similateMergeCaller := func() {
		merge.wg.Add(1)
		for rec := range recChan {
			err := merge.Callback(context.TODO(), rec)
			require.Nil(t, err)
		}
		err := merge.Finish(context.TODO())
		require.Nil(t, err)
	}
	go similateMergeCaller()
	go similateMergeCaller()
	for i := 0; i < 10000; i++ {
		recChan <- nil
	}
	// give goroutines a time to finish
	time.Sleep(50 * time.Millisecond)

	// expect it doesn't call the finisher until everything is finished
	require.Equal(t, 0, numFinCalls)

	// expect it only calls the finisher once
	close(recChan)
	// give it an opportunity call if it's going to
	time.Sleep(50 * time.Millisecond)
	finMtx.Lock()
	require.Equal(t, 1, numFinCalls)
	finMtx.Unlock()

	// expect the number of calls to the callback is correct
	require.Equal(t, 10000, numCbCalls)
}
