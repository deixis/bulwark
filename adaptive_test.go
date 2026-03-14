package bulwark

import (
	"context"
	"errors"
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
			_, _ = WithAdaptiveThrottle(clientThrottle, High, func() (struct{}, error) {
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
			_, _ = WithAdaptiveThrottle(clientThrottle, Low, func() (struct{}, error) {
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
func TestAdaptiveThrottleRecovery(t *testing.T) {
	const (
		window   = 1 * time.Second
		demand   = 20
		supply   = demand * 2  // healthy: capacity well above demand
		overload = demand / 10 // sick: capacity well below demand
	)

	var serverHealthy atomic.Bool
	serverHealthy.Store(true)

	healthyLimiter := rate.NewLimiter(rate.Limit(supply), 1)
	sickLimiter := rate.NewLimiter(rate.Limit(overload), 1)

	throttle := NewAdaptiveThrottle(1, WithAdaptiveThrottleWindow(window))
	clientLimiter := rate.NewLimiter(rate.Limit(demand), 1)

	measureShedRate := func(d time.Duration) float64 {
		var attempts, sent int64
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			if err := clientLimiter.Wait(context.Background()); err != nil {
				return 0
			}
			attempts++
			_, _ = WithAdaptiveThrottle(throttle, Priority(0), func() (struct{}, error) {
				sent++
				lim := healthyLimiter
				if !serverHealthy.Load() {
					lim = sickLimiter
				}
				if !lim.Allow() {
					return struct{}{}, RejectedError(faults.Unavailable(0))
				}
				return struct{}{}, nil
			})
		}
		if attempts == 0 {
			return 0
		}
		return float64(attempts-sent) / float64(attempts)
	}

	// Phase 1: healthy baseline — no shedding expected.
	shedRate := measureShedRate(3 * time.Second)
	t.Logf("healthy:    shed=%.1f%%", shedRate*100)
	if shedRate > 0.05 {
		t.Errorf("healthy phase shed rate %.1f%% should be near 0%%", shedRate*100)
	}

	// Phase 2: overloaded — shedding must kick in.
	serverHealthy.Store(false)
	shedRate = measureShedRate(3 * time.Second)
	t.Logf("overloaded: shed=%.1f%%", shedRate*100)
	if shedRate < 0.3 {
		t.Errorf("overloaded phase shed rate %.1f%% should be significant", shedRate*100)
	}

	// Phase 3: recovery — after the failure window expires, shedding must cease.
	// Sleep for 2× the window so the windowed counters are fully cleared before
	// we start measuring; otherwise the first window of recovery still contains
	// the failures from phase 2.
	serverHealthy.Store(true)
	time.Sleep(2 * window)
	shedRate = measureShedRate(3 * time.Second)
	t.Logf("recovery:   shed=%.1f%%", shedRate*100)
	if shedRate > 0.05 {
		t.Errorf("recovery phase shed rate %.1f%% should be near 0%%", shedRate*100)
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
			_, _ = WithAdaptiveThrottle(throttle, Priority(0), func() (struct{}, error) {
				return struct{}{}, faults.WithNotFound(errors.New("item not found"))
			})
		}

		// P(reject) = (req - 2*acc) / (req + min). With req==acc==100: P ≤ 0.
		// All subsequent requests must go through.
		calls := 0
		for i := 0; i < 10; i++ {
			_, _ = WithAdaptiveThrottle(throttle, Priority(0), func() (struct{}, error) {
				calls++
				return struct{}{}, nil
			})
		}
		if calls != 10 {
			t.Errorf("expected all 10 requests forwarded after non-rejection errors, got %d", calls)
		}
	})

	t.Run("rejection errors do trigger shedding", func(t *testing.T) {
		throttle := NewAdaptiveThrottle(1)
		for i := 0; i < 100; i++ {
			throttle.Throttle(ctx, 0, func(ctx context.Context) error {
				return RejectedError(faults.Unavailable(0))
			})
		}

		// P(reject) = req / (req + min) ≈ 100/160 ≈ 62%. With 10 attempts,
		// P(all forwarded) = 0.38^10 < 0.01% — effectively impossible.
		calls := 0
		for i := 0; i < 10; i++ {
			throttle.Throttle(ctx, 0, func(ctx context.Context) error {
				calls++
				return nil
			})
		}
		if calls == 10 {
			t.Error("expected shedding after repeated rejection errors, but all 10 calls were forwarded")
		}
	})
}

// TestAdaptiveThrottleMinimumRate verifies that even under total server failure
// Bulwark still forwards approximately MinRPS requests per second. This probe
// traffic is essential: without it the throttle could never detect that a
// failed server has recovered.
func TestAdaptiveThrottleMinimumRate(t *testing.T) {
	const (
		window  = 1 * time.Second
		minRate = 1.0
		demand  = 20
		testDur = 5 * time.Second
	)

	throttle := NewAdaptiveThrottle(1,
		WithAdaptiveThrottleWindow(window),
		WithAdaptiveThrottleMinimumRate(minRate),
	)
	clientLimiter := rate.NewLimiter(rate.Limit(demand), 1)

	var attempts, sent int64
	start := time.Now()
	for time.Since(start) < testDur {
		if err := clientLimiter.Wait(context.Background()); err != nil {
			break
		}
		attempts++
		_, _ = WithAdaptiveThrottle(throttle, Priority(0), func() (struct{}, error) {
			sent++
			return struct{}{}, RejectedError(faults.Unavailable(0))
		})
	}

	elapsed := time.Since(start).Seconds()
	forwardedPerSec := float64(sent) / elapsed
	shedRate := float64(attempts-sent) / float64(attempts)

	t.Logf("attempts=%d  sent=%d  forwarded=%.2f/sec  shed=%.1f%%",
		attempts, sent, forwardedPerSec, shedRate*100)

	// The throttle must be actively shedding under total server failure.
	if shedRate < 0.5 {
		t.Errorf("shed rate %.1f%% is too low — throttle should shed most requests under total failure", shedRate*100)
	}
	// But the MinRPS floor must keep probe traffic flowing to enable recovery
	// detection. With minRate=1 and demand=20, steady-state forwarding rate is
	// demand*(minPerWindow/(demand+minPerWindow)) ≈ 20*(1/21) ≈ 0.95/sec.
	// Use a conservative lower bound of 0.4/sec (2 requests over 5s).
	if forwardedPerSec < 0.4 {
		t.Errorf("forwarded %.2f/sec is below MinRPS floor — probe traffic must reach the server", forwardedPerSec)
	}
}

// TestFallback ensures the fallback function is called when a request is
// rejected by the throttle.
func TestFallback(t *testing.T) {
	ctx := context.Background()
	// Short window keeps minPerWindow = 1 * 0.1 = 0.1, so after 100 rejections:
	// P = 100 / (100 + 0.1) = 99.9%. Without this, the default 60s window gives
	// P = 100/160 = 62.5% — not deterministic enough for a single-shot assertion.
	throttle := NewAdaptiveThrottle(StandardPriorities,
		WithAdaptiveThrottleRatio(1),
		WithAdaptiveThrottleWindow(100*time.Millisecond),
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
		{"client rejection error", DefaultClientSideRejectionError, DefaultClientSideRejectionError},
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
