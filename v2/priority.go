package bulwark

// StandardPriorities is the number of priority levels that are available.
// This value should be used when creating a new AdaptiveThrottle when the
// default Priority constants are used.
//
//		throttler := bulwark.NewAdaptiveThrottle(bulwark.StandardPriorities)
//		_, err := bulwark.Throttle(ctx, throttler, bulwark.High, throttledFn)
//		if err != nil {
//			// handle the error
//		}
const StandardPriorities = 4

// Priority determines the importance of a request in ascending order.
// e.g. priority 0 is more important than priority 1.
//
// When a system reaches its capacity, it will sort requests by their priority
// and process them. Lower-priority requests can either be delayed or dropped.
type Priority int8

// These are pre-defined priority levels that can be used, but any int value
// can be used as a priority.
const (
	// Use High when for requests that are critical to the overall experience.
	High Priority = 0
	// Use Important for requests that are important, but not critical.
	Important Priority = 1
	// Use Medium for noncritical requests where an elevated latency or
	// failure rate would not significantly impact the experience.
	Medium Priority = 2
	// Use Low for trivial requests and good for any system that can retry
	// later when the system has spare capacity.
	Low Priority = 3
)
