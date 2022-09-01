package physicalplan_concurrent

import (
	"context"
	"testing"
)

func TestPlan(t *testing.T) {
	// Simulates concurrent access in the app
	seen := map[int]struct{}{}

	_ = Build(func(ctx context.Context, data int) error {
		t.Log(data)
		seen[data] = struct{}{}
		return nil
	}).Execute(context.Background())
}
