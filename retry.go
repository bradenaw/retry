// Package retry provides exponential backoff.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"time"
)

// Option affects the behavior of Retry.
type Option struct {
	apply func(*options)
}

type options struct {
	minDelay  time.Duration
	maxDelay  time.Duration
	attempts  int
	retryable func(error) bool
	log       func(error, int, time.Duration)
	clock     clock
}

// The minimum delay between attempts.
func MinDelay(d time.Duration) Option {
	return Option{func(o *options) { o.minDelay = d }}
}

// The maximum delay between attempts.
func MaxDelay(d time.Duration) Option {
	return Option{func(o *options) { o.maxDelay = d }}
}

// The maximum number of times to call f before returning the error. Attempts(1) means try just once
// and do not retry.
func Attempts(x int) Option {
	return Option{func(o *options) { o.attempts = x }}
}

// Indefinitely is shorthand for Attempts(math.MaxInt).
func Indefinitely() Option { return Attempts(math.MaxInt) }

// If f returns false for an error observed during Retry, it is returned immediately rather than
// retried.
func Retryable(f func(error) bool) Option {
	return Option{func(o *options) { o.retryable = f }}
}

// Called before sleeping on each retry with the last error that occurred, the attempt index, and
// the delay before the next attempt.
func Log(f func(error, int, time.Duration)) Option {
	return Option{func(o *options) { o.log = f }}
}

func withClock(clock clock) Option {
	return Option{func(o *options) { o.clock = clock }}
}

// Retry calls f, retrying with exponential backoff on errors. After the first error, will wait
// MinDelay before retrying. Each successive retry waits for twice as long, up to MaxDelay. f might
// be failing because an upstream system is overloaded and retries may exacerbate that problem,
// doubling the delay means that even uncoordinated clients will eventually estimate the available
// capacity in the upstream.
//
// Delays are jittered by +/-50% to avoid thundering herds. That is, the configured MinDelay is the
// _average_ first delay, and the actual minimum delay is half of the configured. Likewise for
// MaxDelay, the configured is the average max delay, but the actual maximum delay is 1.5x this
// value.
//
// Attempts that are very old are forgotten with regard to picking the next delay because they are
// not likely to still be relevant to the health of the upstream system. This is to make Retry
// usable for retrying very long-running operations, for example keeping a long-lived stream alive.
// The age is set so that if f is consistently failing once per MaxDelay, the next delay will be
// MaxDelay, but any attempts older than the minimum needed to achieve that are forgotten.
func Retry[T any](
	ctx context.Context,
	f func(ctx context.Context) (T, error),
	opts ...Option,
) (T, error) {
	var zero T

	o := options{
		minDelay:  20 * time.Millisecond,
		maxDelay:  10 * time.Second,
		attempts:  5,
		retryable: alwaysRetry,
		clock:     realClock{},
	}
	for _, option := range opts {
		option.apply(&o)
	}
	if o.minDelay < 0 {
		return zero, errNegativeMinDelay
	}
	if o.minDelay == 0 {
		return zero, errZeroMinDelay
	}
	if o.maxDelay < o.minDelay {
		return zero, errMaxDelayBelowMin
	}
	if o.attempts <= 0 {
		return zero, errNoAttempts
	}

	var attempts []time.Time
	// This is the number of attempts needed to reach maxDelay. We can discard any more attempts
	// than this, because they won't matter to our delay calculation.
	attemptsSize := int(math.Ceil(math.Log2(o.maxDelay.Seconds()) - math.Log2(o.minDelay.Seconds())))
	// This is the age we'd have to keep so that if f was consistently failing every maxDelay, we'd
	// still wait maxDelay next time.
	attemptsMaxAge := saturatingMul(o.maxDelay, attemptsSize)

	var lastErr error
	start := o.clock.Now()
	i := 0
	for {
		t, err := f(ctx)
		if err == nil {
			return t, nil
		}
		if ctx.Err() != nil {
			if lastErr != nil {
				return zero, lastErr
			}
			return zero, ctx.Err()
		}
		if !o.retryable(err) {
			return zero, fmt.Errorf("not retryable: %w", err)
		}
		lastErr = err

		now := o.clock.Now()
		for len(attempts) > attemptsSize ||
			(len(attempts) > 0 && now.Sub(attempts[0]) > attemptsMaxAge) {

			attempts = attempts[1:]
		}

		i++
		if i < 0 {
			i = math.MaxInt
		}
		if o.attempts != math.MaxInt && i == o.attempts {
			break
		}

		avgDelay := saturatingShift(o.minDelay, len(attempts))
		if avgDelay > o.maxDelay {
			avgDelay = o.maxDelay
		}
		jitter := time.Duration(rand.Int63n(int64(avgDelay)))
		delay := saturatingAdd(avgDelay/2, jitter)

		if o.log != nil {
			o.log(err, i-1, delay)
		}

		ok := o.clock.SleepContext(ctx, delay)
		if !ok {
			return zero, lastErr
		}

		attempts = append(attempts, now)

	}
	return zero, fmt.Errorf(
		"gave up after %d attempts over %s: %w",
		i,
		time.Since(start),
		lastErr,
	)
}

const maxDuration = time.Duration(math.MaxInt64)

func saturatingShift(d time.Duration, s int) time.Duration {
	// >= because duration is signed
	if s >= bits.LeadingZeros64(uint64(d)) {
		return maxDuration
	}
	return d << s
}

func saturatingMul(d time.Duration, x int) time.Duration {
	dx := time.Duration(x)
	r := d * dx
	if dx != 0 && r/dx != d {
		return maxDuration
	}
	return r
}

func saturatingAdd(a, b time.Duration) time.Duration {
	if math.MaxInt64-int64(a) < int64(b) {
		return maxDuration
	}
	return a + b
}

func alwaysRetry(error) bool { return true }

func ilog2(x uint64) int {
	return 64 - bits.LeadingZeros64(x-1)
}

type clock interface {
	Now() time.Time
	SleepContext(ctx context.Context, d time.Duration) bool
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) SleepContext(ctx context.Context, d time.Duration) bool {
	deadline, ok := ctx.Deadline()
	if ok && time.Until(deadline) < d {
		return false
	}
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return false
	case <-t.C:
		return true
	}
}

var (
	errNoAttempts       = errors.New("retry.Attempts(0) means don't even try once")
	errNegativeMinDelay = errors.New("negative MinDelay is nonsense")
	errZeroMinDelay     = errors.New("zero MinDelay implies no exponential backoff, you probably want at least a couple of milliseconds")
	errMaxDelayBelowMin = errors.New("MaxDelay < MinDelay")
)
