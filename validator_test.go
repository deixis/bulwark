package bulwark

import (
	"context"
	"testing"
)

// TestAssertValidPriority verifies that AssertValidPriority passes valid
// priorities through unchanged and panics for any priority outside [0, priorities).
// This validator is intended for development environments where misuse of the
// library should be caught immediately rather than silently corrected.
func TestAssertValidPriority(t *testing.T) {
	t.Run("valid priority passes through", func(t *testing.T) {
		for _, p := range []Priority{High, Important, Medium, Low} {
			got, err := AssertValidPriority(p, StandardPriorities)
			if err != nil {
				t.Errorf("priority %d: unexpected error: %v", p, err)
			}
			if got != p {
				t.Errorf("priority %d: expected %d, got %d", p, p, got)
			}
		}
	})

	t.Run("negative priority panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic, got none")
			}
		}()
		AssertValidPriority(-1, StandardPriorities)
	})

	t.Run("out of range priority panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic, got none")
			}
		}()
		AssertValidPriority(Priority(StandardPriorities), StandardPriorities)
	})
}

// TestClampInvalidPriority verifies that ClampInvalidPriority passes valid
// priorities through unchanged and clamps any out-of-range priority — including
// negative values — to the lowest valid priority (priorities-1). Clamping to
// the lowest rather than the nearest boundary ensures that invalid or malicious
// input is never silently promoted to a higher-importance tier.
func TestClampInvalidPriority(t *testing.T) {
	t.Run("valid priority passes through", func(t *testing.T) {
		for _, p := range []Priority{High, Important, Medium, Low} {
			got, err := ClampInvalidPriority(p, StandardPriorities)
			if err != nil {
				t.Errorf("priority %d: unexpected error: %v", p, err)
			}
			if got != p {
				t.Errorf("priority %d: expected %d, got %d", p, p, got)
			}
		}
	})

	t.Run("negative priority clamped to lowest", func(t *testing.T) {
		got, err := ClampInvalidPriority(-1, StandardPriorities)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != Priority(StandardPriorities-1) {
			t.Errorf("expected %d, got %d", StandardPriorities-1, got)
		}
	})

	t.Run("out of range priority adjusted to lowest", func(t *testing.T) {
		got, err := ClampInvalidPriority(Priority(StandardPriorities), StandardPriorities)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != Priority(StandardPriorities-1) {
			t.Errorf("expected %d, got %d", StandardPriorities-1, got)
		}
	})
}

// TestRejectInvalidPriority verifies that RejectInvalidPriority passes valid
// priorities through unchanged and returns an error for any priority outside
// [0, priorities). This validator is suited for APIs where the caller is
// responsible for providing a valid priority and must handle the error.
func TestRejectInvalidPriority(t *testing.T) {
	t.Run("valid priority passes through", func(t *testing.T) {
		for _, p := range []Priority{High, Important, Medium, Low} {
			got, err := RejectInvalidPriority(p, StandardPriorities)
			if err != nil {
				t.Errorf("priority %d: unexpected error: %v", p, err)
			}
			if got != p {
				t.Errorf("priority %d: expected %d, got %d", p, p, got)
			}
		}
	})

	t.Run("negative priority returns error", func(t *testing.T) {
		_, err := RejectInvalidPriority(-1, StandardPriorities)
		if err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("out of range priority returns error", func(t *testing.T) {
		_, err := RejectInvalidPriority(Priority(StandardPriorities), StandardPriorities)
		if err == nil {
			t.Error("expected error, got nil")
		}
	})
}

// TestWithPriorityValidator verifies that WithPriorityValidator wires a custom
// validator into the throttle so that it is called on every request. It also
// confirms that the default behaviour (no option provided) uses
// ClampInvalidPriority: out-of-range priorities are clamped rather than
// rejected, so the throttled function is still invoked.
func TestWithPriorityValidator(t *testing.T) {
	t.Run("custom validator is applied", func(t *testing.T) {
		called := false
		throttle := NewAdaptiveThrottle(StandardPriorities,
			WithPriorityValidator(func(p Priority, priorities int) (Priority, error) {
				called = true
				return p, nil
			}),
		)
		throttle.Throttle(context.Background(), High, func(_ context.Context) error {
			return nil
		})
		if !called {
			t.Error("expected custom validator to be called")
		}
	})

	t.Run("RejectInvalidPriority used as validator rejects invalid priority", func(t *testing.T) {
		throttle := NewAdaptiveThrottle(StandardPriorities,
			WithPriorityValidator(RejectInvalidPriority),
		)
		err := throttle.Throttle(context.Background(), Priority(StandardPriorities), func(_ context.Context) error {
			return nil
		})
		if err == nil {
			t.Error("expected error for out-of-range priority, got nil")
		}
	})

	t.Run("default validator adjusts invalid priority", func(t *testing.T) {
		throttle := NewAdaptiveThrottle(StandardPriorities)
		called := false
		err := throttle.Throttle(context.Background(), Priority(StandardPriorities), func(_ context.Context) error {
			called = true
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("expected throttled function to be called after priority adjustment")
		}
	})
}
