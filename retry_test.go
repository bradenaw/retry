package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

type testClock struct {
	now time.Time
}

func (c *testClock) Now() time.Time {
	return c.now
}

// Slightly silly, but since it isn't ever used concurrently, just advance the clock and keep going.
func (c *testClock) SleepContext(ctx context.Context, d time.Duration) bool {
	c.now = c.now.Add(d)
	return true
}

func TestRetryBasic(t *testing.T) {
	for i := 0; i < 1000; i++ { // do it a couple of times because jitters are random
		start := time.Now()
		c := &testClock{now: start}
		v, err := Retry(
			context.Background(),
			failNTimes(4),
			withClock(c),
			MinDelay(10*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("expected no error, got %s", err)
		}
		if v != 5 {
			t.Fatalf("expected success on try 5, got %d", v)
		}

		elapsed := c.now.Sub(start)

		avgTime := (10 + 20 + 40 + 80) * time.Millisecond
		minTime := avgTime / 2
		maxTime := avgTime * 3 / 2

		if elapsed < minTime || elapsed > maxTime {
			t.Fatalf(
				"expected sleep time to be in [%s, %s], was actually %s",
				minTime,
				maxTime,
				elapsed,
			)
		}
	}
}

func TestDecay(t *testing.T) {
	for j := 0; j < 1000; j++ { // do it a couple of times because jitters are random
		start := time.Now()
		c := &testClock{now: start}
		i := 0

		// attempt      0   1   2   3   4   5   6   7
		// avgDelay     10  20  40  80  160 320 640 800

		var delays []time.Duration
		v, err := Retry(
			context.Background(),
			func(ctx context.Context) (int, error) {
				defer func() { i++ }()

				if i >= 21 {
					return 21, nil
				}
				if i >= 20 {
					c.SleepContext(ctx, 7*800*3/2*time.Millisecond)
				}
				return 0, errors.New("fail")
			},
			withClock(c),
			Log(func(_ error, _ int, d time.Duration) {
				delays = append(delays, d)
			}),
			MinDelay(10*time.Millisecond),
			MaxDelay(800*time.Millisecond),
			Indefinitely(),
		)
		if err != nil {
			t.Fatalf("expected nil err, got %s", err)
		}
		if v != 21 {
			t.Fatalf("expected 21 return, got %d", v)
		}
		if len(delays) != 21 {
			t.Fatalf("expected 21 delays, got %d", len(delays))
		}

		t.Logf("delays %v", delays)
		t.Logf("delays[0:8] %v", delays[0:8])
		t.Logf("delays[8:20] %v", delays[8:20])
		t.Logf("delays[20] %v", delays[20])

		requireInJitter(t, delays[0], 10*time.Millisecond)
		requireInJitter(t, delays[1], 20*time.Millisecond)
		requireInJitter(t, delays[2], 40*time.Millisecond)
		requireInJitter(t, delays[3], 80*time.Millisecond)
		requireInJitter(t, delays[4], 160*time.Millisecond)
		requireInJitter(t, delays[5], 320*time.Millisecond)
		requireInJitter(t, delays[6], 640*time.Millisecond)
		requireInJitter(t, delays[7], 800*time.Millisecond)
		for i := 7; i < 20; i++ {
			requireInJitter(t, delays[i], 800*time.Millisecond)
		}
		requireInJitter(t, delays[20], 10*time.Millisecond)
	}
}

func requireInJitter(t *testing.T, d time.Duration, exp time.Duration) {
	t.Helper()
	minExp := exp / 2
	maxExp := exp * 3 / 2

	if d < minExp || d > maxExp {
		t.Fatalf(
			"expected duration to be in [%s, %s], was actually %s",
			minExp,
			maxExp,
			d,
		)
	}
}

// fails n times and then succeeds
func failNTimes(n int) func(ctx context.Context) (int, error) {
	i := 0
	return func(ctx context.Context) (int, error) {
		if i < n {
			i++
			return 0, errors.New("fails the first 5 times")
		}
		i++
		return i, nil
	}
}
