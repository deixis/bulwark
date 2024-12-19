package bulwark

import (
	"context"
	"errors"
	"math/rand"
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
		d:       time.Minute,
		k:       K,
		minRate: MinRPS,
	}
	for _, option := range options {
		option.f(&opts)
	}

	now := Now()
	requests := make([]windowedCounter, priorities)
	accepts := make([]windowedCounter, priorities)
	for i := range requests {
		requests[i] = newWindowedCounter(now, opts.d/10, 10)
		accepts[i] = newWindowedCounter(now, opts.d/10, 10)
	}

	return &AdaptiveThrottle{
		k:            opts.k,
		priorities:   priorities,
		requests:     requests,
		accepts:      accepts,
		minPerWindow: opts.minRate * opts.d.Seconds(),
		validate:     OnInvalidPriorityAdjust,
	}
}

// Throttle sends a request to the backend when the adaptive throttle allows it.
// The request is throttled based on the priority of the request.
//
// The default priority is used when the given `ctx` does not have a priority set.
// The `ctx` can set the priority using `WithPriority`.
//
// When `throttledFn` returns an error, the error is considered as a rejection
// when `isErrorAccepted` returns false or when the error is wrapped in a
// `RejectedError`.
//
// If there are enough rejections within a given time window, further calls to
// `Throttle` may begin returning `ClientSideRejectionError` immediately
// without invoking `throttledFn`. Lower-priority requests are preferred to be
// rejected first.
func (t *AdaptiveThrottle) Throttle(
	ctx context.Context, defaultPriority Priority, fn throttledFn, fallbackFn ...fallbackFn,
) error {
	priority, err := t.validate(PriorityFromContext(ctx, defaultPriority), t.priorities)
	if err != nil {
		return err
	}

	now := Now()
	rejectionProbability := t.rejectionProbability(priority, now)
	if rand.Float64() < rejectionProbability {
		// As Bulwark starts rejecting requests, requests will continue to exceed
		// accepts. While it may seem counterintuitive, given that locally rejected
		// requests aren't actually propagated, this is the preferred behavior. As the
		// rate at which the application attempts requests to Bulwark grows
		// (relative to the rate at which the backend accepts them), we want to
		// increase the probability of dropping new requests.
		t.reject(priority, now)

		if len(fallbackFn) > 0 {
			return fallbackFn[0](ctx, ClientSideRejectionError, true)
		}

		return ClientSideRejectionError
	}

	err = fn(ctx)

	now = Now()
	switch {
	case err == nil:
		t.accept(priority, now)
	case errors.Is(err, errRejected{}):
		// Unwrap error to return the original error to the caller
		err = err.(errRejected).inner

		fallthrough
	case IsRejectedError(err):
		t.reject(priority, now)
	default:
		t.accept(priority, now)
	}

	if err != nil && len(fallbackFn) > 0 {
		return fallbackFn[0](ctx, err, false)
	}

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
	for i := 0; i < int(p); i++ {
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

// Additional options for the AdaptiveThrottle type. These options do not frequently need to be
// tuned as the defaults work in a majority of cases.
type AdaptiveThrottleOption struct {
	f func(*adaptiveThrottleOptions)
}

type adaptiveThrottleOptions struct {
	k               float64
	minRate         float64
	d               time.Duration
	isErrorAccepted func(err error) bool
	validate        func(p Priority, priorities int) (Priority, error)
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

// Deprecated: Wrap errors with RejectedError instead and use the global DefaultRejectedErrors.
//
// WithAcceptedErrors sets the function that determines whether an error should
// be considered for the throttling. When the call to fn returns true, the error
// is not counted towards the throttling.
func WithAcceptedErrors(fn func(err error) bool) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.isErrorAccepted = fn
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

			// This is a safeguard in case the validator function does not return a
			// valid priority. It is better to panic with this functions, because
			// the message is more informative.
			return OnInvalidPriorityPanic(p, priorities)
		}
	}}
}

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

	now := Now()
	rejectionProbability := at.rejectionProbability(priority, now)
	if rand.Float64() < rejectionProbability {
		// As Bulwark starts rejecting requests, requests will continue to exceed
		// accepts. While it may seem counterintuitive, given that locally rejected
		// requests aren't actually propagated, this is the preferred behavior. As the
		// rate at which the application attempts requests to Bulwark grows
		// (relative to the rate at which the backend accepts them), we want to
		// increase the probability of dropping new requests.
		at.reject(priority, now)
		var zero T

		if len(fallbackFn) > 0 {
			return fallbackFn[0](ctx, ClientSideRejectionError, true)
		}

		return zero, ClientSideRejectionError
	}

	res, err = throttledFn(ctx)

	now = Now()
	switch {
	case err == nil:
		at.accept(priority, now)
	case errors.Is(err, errRejected{}):
		// Unwrap error to return the original error to the caller
		err = err.(errRejected).inner

		fallthrough
	case IsRejectedError(err):
		at.reject(priority, now)
	default:
		at.accept(priority, now)
	}

	if err != nil && len(fallbackFn) > 0 {
		return fallbackFn[0](ctx, err, false)
	}

	return res, err
}

// WithAdaptiveThrottle is used to send a request to a backend using the given AdaptiveThrottle for
// client-rejections.
//
// If f returns an error, at considers this to be a rejection unless it is wrapped with
// AcceptedError(). If there are enough rejections within a given time window, further calls to
// WithAdaptiveThrottle may begin returning ErrClientRejection immediately without invoking f. The
// rate at which this happens depends on the error rate of f.
//
// WithAdaptiveThrottle will prefer to reject lower-priority requests if it can.
func WithAdaptiveThrottle[T any](
	at *AdaptiveThrottle,
	priority Priority,
	throttledFn func() (T, error),
) (res T, err error) {
	priority, err = at.validate(priority, at.priorities)
	if err != nil {
		return res, err
	}

	now := Now()
	rejectionProbability := at.rejectionProbability(priority, now)
	if rand.Float64() < rejectionProbability {
		// As Bulwark starts rejecting requests, requests will continue to exceed
		// accepts. While it may seem counterintuitive, given that locally rejected
		// requests aren't actually propagated, this is the preferred behavior. As the
		// rate at which the application attempts requests to Bulwark grows
		// (relative to the rate at which the backend accepts them), we want to
		// increase the probability of dropping new requests.
		at.reject(priority, now)
		var zero T

		return zero, ClientSideRejectionError
	}

	res, err = throttledFn()

	now = Now()
	switch {
	case err == nil:
		at.accept(priority, now)
	case errors.Is(err, errRejected{}):
		at.reject(priority, now)

		// Unwrap error to return the original error to the caller
		err = err.(errRejected).inner
	case IsRejectedError(err):
		at.reject(priority, now)
	default:
		at.accept(priority, now)
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

// clamp clamps x to the range [min, max].
func clamp(min, x, max float64) float64 {
	if x < min {
		return min
	}
	if x > max {
		return max
	}

	return x
}

type (
	throttledFn            func(ctx context.Context) error
	fallbackFn             func(ctx context.Context, err error, local bool) error
	throttledArgsFn[T any] func(ctx context.Context) (T, error)
	fallbackArgsFn[T any]  func(ctx context.Context, err error, local bool) (T, error)
)

var (
	// Deprecated: Override `IsRejectedError` instead and/or wrap errors with `RejectedError`.
	//
	// DefaultAcceptedErrors is the default function used to determine whether
	// an error should be considered for the throttling.
	DefaultAcceptedErrors = func(err error) bool {
		return errors.Is(err, context.Canceled) ||
			faults.IsUnauthenticated(err) ||
			faults.IsPermissionDenied(err) ||
			faults.IsBad(err) ||
			faults.IsAborted(err) ||
			faults.IsNotFound(err) ||
			faults.IsFailedPrecondition(err) ||
			faults.IsUnimplemented(err)
	}
	// DefaultRejectedError is the default function used to determine whether
	// an error should be considered for the throttling.
	DefaultRejectedError = func(err error) bool {
		return faults.IsUnavailable(err) ||
			faults.IsResourceExhausted(err)
	}
	// Deprecated: Use `ClientSideRejectionError` instead.
	//
	// DefaultClientSideRejectionError is the default error returned when the
	// client rejects the request due to the adaptive throttle.
	DefaultClientSideRejectionError = faults.Unavailable(time.Second)
	// ClientSideRejectionError is the error returned when the client rejects the
	// request due to the adaptive throttle.
	ClientSideRejectionError = DefaultClientSideRejectionError
	// IsRejectedError is a global function that determines whether an error
	// should be considered for the throttling. Any error that indicates that the
	// backend is unhealthy should be considered for the throttling.
	//
	// This function can be overridden to customise the behaviour of Bulwark.
	// For example, it is possible to use a whitelist of errors that should be
	// accepted and reject the rest.
	IsRejectedError = DefaultRejectedError
	// Now returns the current time. It is a variable to allow tests to override
	// the current time.
	Now = time.Now
)
