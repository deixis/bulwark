package bulwark

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/deixis/faults"
)

const (
	// K is the default accept multiplier, which is used to determine the number
	// of requests that are allowed to reach the backend.
	//
	// A value of 2 means that the throttle will allow twice as many requests to
	// actually reach the backend as it believes will succeed.
	K = 2
	// MinRPS is the minimum number of requests per second that the adaptive
	// throttle will allow (approximately) through to the upstream, even if every
	// request is failing.
	MinRPS = 1
)

// ErrClientSideRejection is the default error returned when the throttle
// rejects a request on the client side without forwarding it to the backend.
// Use WithClientSideRejectionError to override this per instance.
var ErrClientSideRejection = errors.New("bulwark: client-side rejection")

// DefaultRejectedErrorFunc is the default function used to classify errors as
// rejections. It treats Unavailable and ResourceExhausted errors as rejections.
// Use WithRejectedErrorFunc to override this per instance.
func DefaultRejectedErrorFunc(err error) bool {
	return faults.IsUnavailable(err) || faults.IsResourceExhausted(err)
}

// AdaptiveThrottle is used in a client to throttle requests to a backend as it becomes unhealthy to
// help it recover from overload more quickly. Because backends must expend resources to reject
// requests over their capacity it is vital for clients to ease off on sending load when they are
// in trouble, lest the backend spend all of its resources on rejecting requests and have none left
// over to actually serve any.
//
// The adaptive throttle works by tracking the success rate of requests over some time interval
// (usually a minute or so), and randomly rejecting requests without sending them to avoid sending
// too much more than the rate that are expected to actually be successful. Some slop is included,
// because even if the backend is serving zero requests successfully, we do need to occasionally
// send it requests to learn when it becomes healthy again.
//
// More on adaptive throttles in https://sre.google/sre-book/handling-overload/
type AdaptiveThrottle struct {
	m sync.Mutex

	k            float64
	minPerWindow float64

	priorities int
	requests   []windowedCounter
	accepts    []windowedCounter
	validate   func(p Priority, priorities int) (Priority, error)

	isRejectedError          func(error) bool
	clientSideRejectionError error
	now                      func() time.Time
	rng                      *rand.Rand
}

// NewAdaptiveThrottle returns an AdaptiveThrottle.
//
// priorities is the number of priorities that the throttle will accept. Giving a priority outside
// of `[0, priorities)` will panic.
func NewAdaptiveThrottle(priorities int, options ...AdaptiveThrottleOption) *AdaptiveThrottle {
	if priorities <= 0 {
		panic("bulwark: priorities must be greater than 0")
	}

	opts := adaptiveThrottleOptions{
		d:                        time.Minute,
		k:                        K,
		minRate:                  MinRPS,
		isRejectedError:          DefaultRejectedErrorFunc,
		clientSideRejectionError: ErrClientSideRejection,
		now:                      time.Now,
	}
	for _, option := range options {
		option.f(&opts)
	}

	now := opts.now()
	requests := make([]windowedCounter, priorities)
	accepts := make([]windowedCounter, priorities)
	for i := range requests {
		requests[i] = newWindowedCounter(now, opts.d/10, 10)
		accepts[i] = newWindowedCounter(now, opts.d/10, 10)
	}

	validate := opts.validate
	if validate == nil {
		validate = ClampInvalidPriority
	}

	var rng *rand.Rand
	if opts.randSource != nil {
		rng = rand.New(opts.randSource)
	}

	return &AdaptiveThrottle{
		k:                        opts.k,
		priorities:               priorities,
		requests:                 requests,
		accepts:                  accepts,
		minPerWindow:             opts.minRate * opts.d.Seconds(),
		validate:                 validate,
		isRejectedError:          opts.isRejectedError,
		clientSideRejectionError: opts.clientSideRejectionError,
		now:                      opts.now,
		rng:                      rng,
	}
}

// Throttle sends a request to the backend when the adaptive throttle allows it.
// The request is throttled based on the priority of the request.
//
// The default priority is used when the given `ctx` does not have a priority set.
// The `ctx` can set the priority using `WithPriority`.
//
// When `throttledFn` returns an error, the error is considered as a rejection
// when `WithRejectedErrorFunc` returns true or when the error is wrapped in a
// `RejectedError`.
//
// If there are enough rejections within a given time window, further calls to
// `Throttle` may begin returning `ErrClientSideRejection` immediately
// without invoking `throttledFn`. Lower-priority requests are preferred to be
// rejected first.
func (t *AdaptiveThrottle) Throttle(
	ctx context.Context, defaultPriority Priority, fn throttledFn, fallbackFn ...fallbackFn,
) error {
	var fb []fallbackArgsFn[struct{}]
	if len(fallbackFn) > 0 {
		f := fallbackFn[0]
		fb = []fallbackArgsFn[struct{}]{
			func(ctx context.Context, err error, local bool) (struct{}, error) {
				return struct{}{}, f(ctx, err, local)
			},
		}
	}
	_, err := Throttle(ctx, t, defaultPriority, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, fb...)
	return err
}

// rejectionProbability returns the probability that a request of the given
// priority will be rejected. The result is clamped to the range [0, 1].
//
// It uses the formula from https://sre.google/sre-book/handling-overload/ to
// calculate the probability that a request will be rejected. The formula is:
//
//	clamp(0, (requests - k * accepts) / (requests + minPerWindow), 1)
//
// Where:
//   - requests is the number of requests of the given priority in the last d time window.
//   - accepts is the number of requests of the given priority that were accepted in the last d time
//     window.
//   - k is the ratio of the measured success rate and the rate that the throttle will admit.
//   - minPerWindow is the minimum number of requests per second that the adaptive throttle will allow
//     (approximately) through to the upstream, even if every request is failing.
func (t *AdaptiveThrottle) rejectionProbability(p Priority, now time.Time) float64 {
	t.m.Lock()
	requests := float64(t.requests[int(p)].get(now))
	accepts := float64(t.accepts[int(p)].get(now))
	for i := range int(p) {
		// Also count non-accepted requests for every higher priority as
		// non-accepted for this priority.
		requests += float64(t.requests[i].get(now) - t.accepts[i].get(now))
	}
	t.m.Unlock()

	return clamp(0, (requests-t.k*accepts)/(requests+t.minPerWindow), 1)
}

// accept records that a request of the given priority was accepted.
func (t *AdaptiveThrottle) accept(p Priority, now time.Time) {
	t.m.Lock()
	t.requests[int(p)].add(now, 1)
	t.accepts[int(p)].add(now, 1)
	t.m.Unlock()
}

// reject records that a request of the given priority was rejected.
func (t *AdaptiveThrottle) reject(p Priority, now time.Time) {
	t.m.Lock()
	t.requests[int(p)].add(now, 1)
	t.m.Unlock()
}

func (t *AdaptiveThrottle) randFloat64() float64 {
	if t.rng != nil {
		return t.rng.Float64()
	}
	return rand.Float64()
}

// Additional options for the AdaptiveThrottle type. These options do not frequently need to be
// tuned as the defaults work in a majority of cases.
type AdaptiveThrottleOption struct {
	f func(*adaptiveThrottleOptions)
}

type adaptiveThrottleOptions struct {
	k                        float64
	minRate                  float64
	d                        time.Duration
	validate                 func(p Priority, priorities int) (Priority, error)
	isRejectedError          func(error) bool
	clientSideRejectionError error
	now                      func() time.Time
	randSource               rand.Source
}

// WithAdaptiveThrottleRatio sets the ratio of the measured success rate and the rate that the throttle
// will admit. For example, when k is 2 the throttle will allow twice as many requests to actually
// reach the backend as it believes will succeed. Higher values of k mean that the throttle will
// react more slowly when a backend becomes unhealthy, but react more quickly when it becomes
// healthy again, and will allow more load to an unhealthy backend. k=2 is usually a good place to
// start, but backends that serve "cheap" requests (e.g. in-memory caches) may need a lower value.
func WithAdaptiveThrottleRatio(k float64) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.k = k
	}}
}

// WithAdaptiveThrottleMinimumRate sets the minimum number of requests per second that the adaptive
// throttle will allow (approximately) through to the upstream, even if every request is failing.
// This is important because this is how the adaptive throttle 'learns' when the upstream becomes
// healthy again.
func WithAdaptiveThrottleMinimumRate(x float64) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.minRate = x
	}}
}

// WithAdaptiveThrottleWindow sets the time window over which the throttle remembers requests for use in
// figuring out the success rate.
func WithAdaptiveThrottleWindow(d time.Duration) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.d = d
	}}
}

// WithPriorityValidator sets the function that validates input priority values.
//
// The function should return the validated priority value. If the priority is
// invalid, the function should return an error.
func WithPriorityValidator(fn func(p Priority, priorities int) (Priority, error)) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.validate = func(p Priority, priorities int) (Priority, error) {
			p, err := fn(p, priorities)
			if err != nil {
				return p, err
			}

			// Safeguard in case the validator returns an out-of-range priority
			// without an error. Clamp rather than panic to stay consistent with
			// the default behaviour.
			return ClampInvalidPriority(p, priorities)
		}
	}}
}

// WithRejectedErrorFunc sets the per-instance function that determines whether
// an error returned by the throttled function should be counted as a rejection.
// Defaults to DefaultRejectedErrorFunc.
func WithRejectedErrorFunc(fn func(error) bool) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.isRejectedError = fn
	}}
}

// WithClientSideRejectionError sets the per-instance error returned when the
// throttle rejects a request on the client side without forwarding it to the
// backend. Defaults to ErrClientSideRejection.
func WithClientSideRejectionError(err error) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.clientSideRejectionError = err
	}}
}

// WithNow sets the per-instance time source. This is primarily useful in tests
// to control the clock without affecting other AdaptiveThrottle instances.
func WithNow(fn func() time.Time) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.now = fn
	}}
}

// WithRandomSource sets the per-instance random source used to sample the
// rejection probability. This is primarily useful in tests to produce
// deterministic behaviour: a source that always returns 0 will shed every
// request whose rejection probability is greater than zero, and a source that
// always returns math.MaxUint64 will never shed.
//
// The provided source must be safe for concurrent use if the AdaptiveThrottle
// is used concurrently. rand.NewPCG and rand.NewChaCha8 are not concurrent-safe
// by default.
func WithRandomSource(src rand.Source) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.randSource = src
	}}
}

// Throttle executes throttledFn through the given AdaptiveThrottle and returns
// the result. It is the generic counterpart to AdaptiveThrottle.Throttle for
// functions that return a value.
//
// The default priority is used when the given `ctx` does not have a priority set.
// The `ctx` can set the priority using `WithPriority`.
func Throttle[T any](
	ctx context.Context,
	at *AdaptiveThrottle,
	defaultPriority Priority,
	throttledFn throttledArgsFn[T],
	fallbackFn ...fallbackArgsFn[T],
) (res T, err error) {
	priority, err := at.validate(PriorityFromContext(ctx, defaultPriority), at.priorities)
	if err != nil {
		return res, err
	}

	now := at.now()
	rejectionProbability := at.rejectionProbability(priority, now)
	if at.randFloat64() < rejectionProbability {
		// As Bulwark starts rejecting requests, requests will continue to exceed
		// accepts. While it may seem counterintuitive, given that locally rejected
		// requests aren't actually propagated, this is the preferred behavior. As the
		// rate at which the application attempts requests to Bulwark grows
		// (relative to the rate at which the backend accepts them), we want to
		// increase the probability of dropping new requests.
		at.reject(priority, now)
		var zero T

		if len(fallbackFn) > 0 {
			return fallbackFn[0](ctx, at.clientSideRejectionError, true)
		}

		return zero, at.clientSideRejectionError
	}

	res, err = throttledFn(ctx)

	now = at.now()
	switch {
	case err == nil:
		at.accept(priority, now)
	case errors.Is(err, errRejected{}):
		// Unwrap error to return the original error to the caller.
		if re, ok := errors.AsType[errRejected](err); ok {
			err = re.inner
		}

		fallthrough
	case at.isRejectedError(err):
		at.reject(priority, now)
	default:
		at.accept(priority, now)
	}

	if err != nil && len(fallbackFn) > 0 {
		return fallbackFn[0](ctx, err, false)
	}

	return res, err
}

// RejectedError wraps an error to indicate that the error should be considered
// for the throttling.
//
// Any error that indicates that the backend is unhealthy should be wrapped with
// `RejectedError`. But other errors, such as bad requests, authentication failures,
// pre-condition failures, etc., should not be wrapped with `RejectedError`.
func RejectedError(err error) error { return errRejected{inner: err} }

type errRejected struct{ inner error }

func (err errRejected) Error() string { return err.inner.Error() }
func (err errRejected) Unwrap() error { return err.inner }
func (err errRejected) Is(target error) bool {
	_, ok := target.(errRejected)

	return ok
}


func clamp(lo, x, hi float64) float64 { return max(lo, min(x, hi)) }

type (
	throttledFn            func(ctx context.Context) error
	fallbackFn             func(ctx context.Context, err error, local bool) error
	throttledArgsFn[T any] func(ctx context.Context) (T, error)
	fallbackArgsFn[T any]  func(ctx context.Context, err error, local bool) (T, error)
)
