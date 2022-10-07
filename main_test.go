package frostdb

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs all the tests in this package with a goroutine leak detector.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
