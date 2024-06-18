package frostdb

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestDuplicateRegistration(t *testing.T) {
	// Create a reusableRegistry
	promReg := prometheus.NewRegistry()
	reg := newReusableRegistry(promReg)

	// Create a test collector.
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test",
		Help: "test",
	})

	// Register the collector.
	reg.MustRegister(c)

	c.Inc()
	c.Inc()

	checkCounter := func(expValue float64) {
		mf, err := promReg.Gather()
		require.NoError(t, err)
		require.Len(t, mf, 1)
		require.Len(t, mf[0].Metric, 1)
		require.Equal(t, expValue, *mf[0].Metric[0].Counter.Value)
	}
	checkCounter(2)

	// New collector
	c = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test",
		Help: "test",
	})
	// Registering the same collector on the vanilla registry should panic.
	require.Panics(t, func() {
		promReg.MustRegister(c)
	}, "should panic when registering the same collector twice")

	// Registering the same collector on the reusable registry should not panic.
	reg.MustRegister(c)

	c.Inc()
	// Counter should be reset and show 1.
	checkCounter(1)
}
