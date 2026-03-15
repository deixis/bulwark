package bulwark

import "context"

type priorityKey struct{}

var activePriorityKey = priorityKey{}

// PriorityFromContext returns the `Priority` attached to the context.
// If no priority is attached, it returns the default priority.
//
// It is good practive to attach a global priority to requests, so all throttles
// can adapt their behaviour accordingly.
func PriorityFromContext(ctx context.Context, defaultPriority Priority) Priority {
	if p, ok := ctx.Value(activePriorityKey).(Priority); ok {
		return p
	}

	return defaultPriority
}

// WithPriority attaches the given `Priority` to the context.
// It is good practice to call the adaptive throttle this way:
//
//	`bulwark.WithAdaptiveThrottle(at, bulwark.PriorityFromContext(ctx, priority), f)`
//
// Then requests should have a priority attached to them, so all throttles can
// adapt their behaviour accordingly.
func WithPriority(ctx context.Context, priority Priority) context.Context {
	return context.WithValue(ctx, activePriorityKey, priority)
}
