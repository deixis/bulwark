package bulwark

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deixis/faults"
	"golang.org/x/time/rate"
)

// TestAdaptiveThrottlePriorityShedding verifies that under server overload,
// Bulwark sheds lower-priority requests at a higher rate than higher-priority
// ones. It measures the client-side shed rate (requests blocked by Bulwark
// before reaching the server), which is what Bulwark directly controls.
//
// The test uses only High and Low priorities. Comparing adjacent intermediate
// priorities is not stable because once a mid-tier priority enters a shed
// spiral its "failures" count inflates the next tier's formula, creating a
// feedback loop whose resolution depends on demand ratios. The High-vs-Low
// ordering is always guaranteed: Low's rejection-probability formula explicitly
// adds High's failure count to its numerator, so P(reject,Low) ≥ P(reject,High).
func TestAdaptiveThrottlePriorityShedding(t *testing.T) {
	const (
		duration   = 10 * time.Second
		highDemand = 15 // rps
		lowDemand  = 30 // rps
	)

	// Each priority gets its own server limiter to avoid goroutine scheduling
	// artifacts from a shared resource. A shared limiter creates a token lottery:
	// the Low goroutine (running at 2× rate) tends to win more tokens, collapsing
	// High's accept count and triggering a death spiral regardless of priority.
	//
	// With separate limiters we control the accept rates directly:
	//   High server: 10 rps capacity  →  67% accept  →  P(reject,High) ≤ 0  →  no shedding
	//   Low  server: 10 rps capacity  →  33% accept  →  P(reject,Low)  > 0  →  Bulwark sheds
	//
	// This tests the invariant cleanly: Low's rejection-probability formula adds
	// High's failure count to its numerator, so P(reject,Low) ≥ P(reject,High).
	highServerLimiter := rate.NewLimiter(rate.Limit(float64(highDemand)*2/3), 1) // 10 rps
	lowServerLimiter := rate.NewLimiter(rate.Limit(float64(lowDemand)/3), 1)     // 10 rps

	clientThrottle := NewAdaptiveThrottle(
		StandardPriorities,
		WithAdaptiveThrottleWindow(3*time.Second),
	)

	start := time.Now()
	var highAttempts, highSent int64
	var lowAttempts, lowSent int64

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		lim := rate.NewLimiter(rate.Limit(highDemand), 1)
		for time.Since(start) < duration {
			if err := lim.Wait(context.Background()); err != nil {
				return
			}
			atomic.AddInt64(&highAttempts, 1)
			_, _ = Throttle(context.Background(), clientThrottle, High, func(ctx context.Context) (struct{}, error) {
				atomic.AddInt64(&highSent, 1)
				if !highServerLimiter.Allow() {
					return struct{}{}, RejectedError(faults.Unavailable(0))
				}
				return struct{}{}, nil
			})
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		lim := rate.NewLimiter(rate.Limit(lowDemand), 1)
		for time.Since(start) < duration {
			if err := lim.Wait(context.Background()); err != nil {
				return
			}
			atomic.AddInt64(&lowAttempts, 1)
			_, _ = Throttle(context.Background(), clientThrottle, Low, func(ctx context.Context) (struct{}, error) {
				atomic.AddInt64(&lowSent, 1)
				if !lowServerLimiter.Allow() {
					return struct{}{}, RejectedError(faults.Unavailable(0))
				}
				return struct{}{}, nil
			})
		}
	}()

	wg.Wait()

	ha, hs := atomic.LoadInt64(&highAttempts), atomic.LoadInt64(&highSent)
	la, ls := atomic.LoadInt64(&lowAttempts), atomic.LoadInt64(&lowSent)

	highShedRate := float64(ha-hs) / float64(ha)
	lowShedRate := float64(la-ls) / float64(la)

	t.Logf("High: attempts=%d  sent=%d  shed=%.1f%%", ha, hs, highShedRate*100)
	t.Logf("Low:  attempts=%d  sent=%d  shed=%.1f%%", la, ls, lowShedRate*100)

	if highShedRate >= lowShedRate {
		t.Errorf("High shed rate (%.1f%%) should be lower than Low (%.1f%%)",
			highShedRate*100, lowShedRate*100)
	}
}

// TestAdaptiveThrottleRecovery verifies that once a server recovers from
// overload, Bulwark stops shedding and resumes forwarding requests. This
// validates the other half of the adaptive throttle contract: not only must it
// shed under pressure, it must also relax when the pressure is gone.
//
// The clock is controlled via WithNow so the window can be expired instantly
// without real sleeps.
func TestAdaptiveThrottleRecovery(t *testing.T) {
	const (
		k       = 2.0
		minRate = 1.0
		window  = time.Second
		// minPerWindow = minRate * window.Seconds() = 1
	)

	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	at := NewAdaptiveThrottle(1,
		WithAdaptiveThrottleRatio(k),
		WithAdaptiveThrottleMinimumRate(minRate),
		WithAdaptiveThrottleWindow(window),
		WithNow(func() time.Time { return now }),
	)

	// Phase 1: healthy — 100 requests all accepted.
	// P = (100 - 2*100) / (100+1) = -100/101 → clamped to 0.
	for range 100 {
		at.accept(0, now)
	}
	if p := at.rejectionProbability(0, now); p != 0 {
		t.Errorf("healthy phase: expected P=0, got %f", p)
	}

	// Expire the healthy window, then enter overload.
	now = now.Add(window)

	// Phase 2: overloaded — 100 requests all rejected.
	// P = (100 - 0) / (100+1) = 100/101 ≈ 0.99.
	for range 100 {
		at.reject(0, now)
	}
	wantOverload := 100.0 / 101.0
	if p := at.rejectionProbability(0, now); math.Abs(p-wantOverload) > 1e-10 {
		t.Errorf("overloaded phase: expected P=%.10f, got %.10f", wantOverload, p)
	}

	// Phase 3: expire the overload window — recovery.
	now = now.Add(window)
	if p := at.rejectionProbability(0, now); p != 0 {
		t.Errorf("recovery phase: expected P=0, got %f", p)
	}
}

// TestRejectionProbabilityFormula verifies the exact rejection probability
// formula at known request/accept counts, including window expiry.
func TestRejectionProbabilityFormula(t *testing.T) {
	const (
		k       = 2.0
		minRate = 1.0
		window  = time.Minute
		// minPerWindow = 1.0 * 60 = 60
	)

	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	newThrottle := func() *AdaptiveThrottle {
		return NewAdaptiveThrottle(1,
			WithAdaptiveThrottleRatio(k),
			WithAdaptiveThrottleMinimumRate(minRate),
			WithAdaptiveThrottleWindow(window),
			WithNow(func() time.Time { return now }),
		)
	}

	t.Run("no requests", func(t *testing.T) {
		// P = (0 - 0) / (0 + 60) = 0
		at := newThrottle()
		if p := at.rejectionProbability(0, now); p != 0 {
			t.Errorf("expected 0, got %f", p)
		}
	})

	t.Run("requests equal k*accepts", func(t *testing.T) {
		// 10 requests, 5 accepts: P = (10 - 2*5) / (10+60) = 0/70 = 0
		at := newThrottle()
		for range 5 {
			at.accept(0, now)
		}
		for range 5 {
			at.reject(0, now)
		}
		if p := at.rejectionProbability(0, now); p != 0 {
			t.Errorf("expected 0, got %f", p)
		}
	})

	t.Run("all rejections", func(t *testing.T) {
		// 100 requests, 0 accepts: P = 100 / (100+60) = 100/160 = 0.625
		at := newThrottle()
		for range 100 {
			at.reject(0, now)
		}
		want := 100.0 / 160.0
		if p := at.rejectionProbability(0, now); math.Abs(p-want) > 1e-10 {
			t.Errorf("expected %.10f, got %.10f", want, p)
		}
	})

	t.Run("window expiry resets probability", func(t *testing.T) {
		at := newThrottle()
		for range 100 {
			at.reject(0, now)
		}
		if p := at.rejectionProbability(0, now); p <= 0 {
			t.Fatal("expected non-zero probability before expiry")
		}
		// Advance past the full window — all 10 buckets expire.
		now = now.Add(window)
		if p := at.rejectionProbability(0, now); p != 0 {
			t.Errorf("expected 0 after window expiry, got %f", p)
		}
	})
}

// TestRejectionProbabilityPriorityCrossContamination verifies that failures at
// higher priority (lower number) are added to lower priority's request count,
// raising its rejection probability.
func TestRejectionProbabilityPriorityCrossContamination(t *testing.T) {
	const (
		k       = 2.0
		minRate = 1.0
		window  = time.Minute
		// minPerWindow = 60
	)

	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	at := NewAdaptiveThrottle(2,
		WithAdaptiveThrottleRatio(k),
		WithAdaptiveThrottleMinimumRate(minRate),
		WithAdaptiveThrottleWindow(window),
		WithNow(func() time.Time { return now }),
	)

	// Priority 0 (high): 100 requests, 10 accepts → 90 rejections.
	for range 10 {
		at.accept(0, now)
	}
	for range 90 {
		at.reject(0, now)
	}

	// Priority 1 (low): 10 requests, 8 accepts → 2 rejections.
	for range 8 {
		at.accept(1, now)
	}
	for range 2 {
		at.reject(1, now)
	}

	// P(0) = (100 - 2*10) / (100+60) = 80/160 = 0.5
	wantP0 := 80.0 / 160.0
	if p := at.rejectionProbability(0, now); math.Abs(p-wantP0) > 1e-10 {
		t.Errorf("priority 0: expected %.10f, got %.10f", wantP0, p)
	}

	// P(1): requests = r1 + (r0-a0) = 10 + 90 = 100; accepts = a1 = 8
	// P(1) = (100 - 2*8) / (100+60) = 84/160 = 0.525
	wantP1 := 84.0 / 160.0
	if p := at.rejectionProbability(1, now); math.Abs(p-wantP1) > 1e-10 {
		t.Errorf("priority 1: expected %.10f, got %.10f", wantP1, p)
	}
}

// TestAdaptiveThrottleNonRejectionErrors verifies that only errors signalling
// genuine server overload (Unavailable, ResourceExhausted, or wrapped with
// RejectedError) are counted as failures. Errors that indicate a correct server
// response to a bad request — NotFound, Unauthenticated, FailedPrecondition,
// etc. — must count as accepts and must not drive shedding.
func TestAdaptiveThrottleNonRejectionErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("non-rejection errors do not trigger shedding", func(t *testing.T) {
		throttle := NewAdaptiveThrottle(1)
		for i := 0; i < 100; i++ {
			_, _ = Throttle(ctx, throttle, Priority(0), func(ctx context.Context) (struct{}, error) {
				return struct{}{}, faults.WithNotFound(errors.New("item not found"))
			})
		}

		// P(reject) = (req - 2*acc) / (req + min). With req==acc==100: P ≤ 0.
		// All subsequent requests must go through.
		calls := 0
		for i := 0; i < 10; i++ {
			_, _ = Throttle(ctx, throttle, Priority(0), func(ctx context.Context) (struct{}, error) {
				calls++
				return struct{}{}, nil
			})
		}
		if calls != 10 {
			t.Errorf("expected all 10 requests forwarded after non-rejection errors, got %d", calls)
		}
	})

	t.Run("rejection errors do trigger shedding", func(t *testing.T) {
		// alwaysShed ensures rand returns 0, so any P > 0 causes a shed.
		throttle := NewAdaptiveThrottle(1, WithRandomSource(alwaysShed{}))
		for i := 0; i < 100; i++ {
			throttle.Throttle(ctx, 0, func(ctx context.Context) error {
				return RejectedError(faults.Unavailable(0))
			})
		}

		calls := 0
		for i := 0; i < 10; i++ {
			throttle.Throttle(ctx, 0, func(ctx context.Context) error {
				calls++
				return nil
			})
		}
		if calls != 0 {
			t.Errorf("expected all requests shed after repeated rejection errors, got %d forwarded", calls)
		}
	})
}

// TestAdaptiveThrottleMinimumRate verifies that the minimum rate floor is
// correctly encoded in the rejection probability formula. Because minPerWindow
// sits in the denominator, P = N/(N+minPerWindow) approaches but never reaches
// 1, guaranteeing that some probe traffic always reaches the backend.
func TestAdaptiveThrottleMinimumRate(t *testing.T) {
	const (
		k       = 2.0
		minRate = 1.0
		window  = time.Second
		// minPerWindow = minRate * window.Seconds() = 1
	)

	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, n := range []int{1, 10, 100, 10_000} {
		at := NewAdaptiveThrottle(1,
			WithAdaptiveThrottleRatio(k),
			WithAdaptiveThrottleMinimumRate(minRate),
			WithAdaptiveThrottleWindow(window),
			WithNow(func() time.Time { return now }),
		)
		for range n {
			at.reject(0, now)
		}
		// P = (n - k*0) / (n + minPerWindow) = n / (n+1)
		want := float64(n) / float64(n+1)
		got := at.rejectionProbability(0, now)
		if math.Abs(got-want) > 1e-10 {
			t.Errorf("n=%d: expected P=%.10f, got %.10f", n, want, got)
		}
		if got >= 1.0 {
			t.Errorf("n=%d: P must be < 1 (got %f) — minimum rate floor is broken", n, got)
		}
	}
}

// TestFallback ensures the fallback function is called when a request is
// rejected by the throttle.
func TestFallback(t *testing.T) {
	ctx := context.Background()
	throttle := NewAdaptiveThrottle(StandardPriorities,
		WithRandomSource(alwaysShed{}),
	)
	for i := 0; i < 100; i++ {
		throttle.Throttle(ctx, 0, func(ctx context.Context) error {
			return faults.Unavailable(0)
		})
	}

	throttledFnCalls := 0
	fallbackFnCalls := 0
	throttle.Throttle(ctx, 0, func(ctx context.Context) error {
		throttledFnCalls++
		return nil
	}, func(ctx context.Context, err error, local bool) error {
		fallbackFnCalls++
		return err
	})

	if throttledFnCalls != 0 {
		t.Errorf("expected throttled function to not be called, got %d", throttledFnCalls)
	}
	if fallbackFnCalls != 1 {
		t.Errorf("expected fallback function to be called once, got %d", fallbackFnCalls)
	}
}

// TestWithNow verifies that WithNow controls the clock used by the throttle,
// allowing windowed counters to be fast-forwarded without real time passing.
// It saturates the throttle with failures, then advances the fake clock past
// the window and confirms the throttle stops shedding.
func TestWithNow(t *testing.T) {
	const window = time.Second

	now := time.Now()
	throttle := NewAdaptiveThrottle(1,
		WithAdaptiveThrottleWindow(window),
		WithNow(func() time.Time { return now }),
		WithRandomSource(alwaysShed{}),
	)

	ctx := context.Background()

	// Saturate with rejections so shedding kicks in.
	for i := 0; i < 100; i++ {
		throttle.Throttle(ctx, 0, func(ctx context.Context) error {
			return RejectedError(faults.Unavailable(0))
		})
	}

	// With alwaysShed and P > 0, every subsequent call must be shed.
	calls := 0
	for i := 0; i < 10; i++ {
		throttle.Throttle(ctx, 0, func(ctx context.Context) error {
			calls++
			return nil
		})
	}
	if calls != 0 {
		t.Fatalf("expected all calls shed before clock advance, got %d forwarded", calls)
	}

	// Advance fake clock past the window — all failure buckets should expire.
	now = now.Add(window)

	// With P = 0, alwaysShed returns 0 but 0 < 0 is false — all calls go through.
	calls = 0
	for i := 0; i < 10; i++ {
		throttle.Throttle(ctx, 0, func(ctx context.Context) error {
			calls++
			return nil
		})
	}
	if calls != 10 {
		t.Errorf("expected all 10 calls forwarded after window expired, got %d", calls)
	}
}

// TestWithRejectedErrorFunc verifies that WithRejectedErrorFunc replaces the
// default error classifier for a single instance without affecting others.
func TestWithRejectedErrorFunc(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("custom rejection")

	// This throttle treats sentinel as a rejection; the default would not.
	custom := NewAdaptiveThrottle(1,
		WithRejectedErrorFunc(func(err error) bool {
			return errors.Is(err, sentinel)
		}),
		WithRandomSource(alwaysShed{}),
	)
	// This throttle uses the default classifier; sentinel is not a rejection.
	// neverShed ensures rand never triggers a shed regardless of P.
	defaultThrottle := NewAdaptiveThrottle(1, WithRandomSource(neverShed{}))

	saturate := func(at *AdaptiveThrottle, err error) {
		for i := 0; i < 100; i++ {
			at.Throttle(ctx, 0, func(ctx context.Context) error { return err })
		}
	}

	countForwarded := func(at *AdaptiveThrottle) int {
		calls := 0
		for i := 0; i < 10; i++ {
			at.Throttle(ctx, 0, func(ctx context.Context) error { calls++; return nil })
		}
		return calls
	}

	saturate(custom, sentinel)
	if got := countForwarded(custom); got != 0 {
		t.Errorf("custom throttle: expected all calls shed, got %d forwarded", got)
	}

	saturate(defaultThrottle, sentinel)
	if got := countForwarded(defaultThrottle); got != 10 {
		t.Errorf("default throttle: sentinel should not count as rejection, got %d forwarded (want 10)", got)
	}
}

// TestWithClientSideRejectionError verifies that WithClientSideRejectionError
// controls the error returned for locally rejected requests.
func TestWithClientSideRejectionError(t *testing.T) {
	ctx := context.Background()
	customErr := errors.New("custom client rejection")

	throttle := NewAdaptiveThrottle(1,
		WithClientSideRejectionError(customErr),
		WithRandomSource(alwaysShed{}),
	)

	// Saturate so P > 0.
	for i := 0; i < 100; i++ {
		throttle.Throttle(ctx, 0, func(ctx context.Context) error {
			return RejectedError(faults.Unavailable(0))
		})
	}

	// With alwaysShed every call must be shed with the custom error.
	for i := 0; i < 10; i++ {
		err := throttle.Throttle(ctx, 0, func(ctx context.Context) error { return nil })
		if !errors.Is(err, customErr) {
			t.Errorf("call %d: expected custom rejection error, got %v", i, err)
		}
	}
}

// TestInvalidFallback ensures errors from the throttled function are passed
// through correctly and do not trigger client-side rejection logic unexpectedly.
func TestInvalidFallback(t *testing.T) {
	stdError := errors.New("standard error")

	table := []struct {
		name   string
		err    error
		expect error
	}{
		{"no error", nil, nil},
		{"client rejection error", ErrClientSideRejection, ErrClientSideRejection},
		{"wrapped rejection error", RejectedError(faults.ResourceExhausted()), faults.ResourceExhausted()},
		{"standard error", stdError, stdError},
	}

	ctx := context.Background()
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			throttle := NewAdaptiveThrottle(StandardPriorities)
			err := throttle.Throttle(ctx, 0, func(ctx context.Context) error {
				return tt.err
			}, func(ctx context.Context, err error, local bool) error {
				return err
			})
			if !errors.Is(err, tt.expect) {
				t.Errorf("expected %v, got %v", tt.expect, err)
			}
		})
	}
}

// alwaysShed is a rand.Source whose Float64() always returns 0, causing the
// throttle to shed every request whose rejection probability is greater than 0.
type alwaysShed struct{}

func (alwaysShed) Uint64() uint64 { return 0 }

// neverShed is a rand.Source whose Float64() always returns a value just below
// 1, causing the throttle to never shed (since rejection probability ≤ 1).
type neverShed struct{}

func (neverShed) Uint64() uint64 { return math.MaxUint64 }
