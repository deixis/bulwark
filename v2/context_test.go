package bulwark_test

import (
	"context"
	"testing"

	bulwark "github.com/deixis/bulwark/v2"
)

func TestPriorityContext(t *testing.T) {
	ctx := context.Background()
	defaultPriority := bulwark.Medium
	got := bulwark.PriorityFromContext(ctx, defaultPriority)
	if got != defaultPriority {
		t.Errorf("PriorityFromContext(ctx) = %v; want %v", got, defaultPriority)
	}

	priority := bulwark.High
	ctx = bulwark.WithPriority(ctx, priority)
	got = bulwark.PriorityFromContext(ctx, defaultPriority)
	if got != priority {
		t.Errorf("PriorityFromContext(ctx) = %v; want %v", got, priority)
	}
}
