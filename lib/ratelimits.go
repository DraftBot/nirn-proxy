package lib

import (
	"context"
	"github.com/sirupsen/logrus"
	"math"
	"sync"
	"time"
)

// isClose checks if two durations are within `diff` seconds of difference
func isClose(a, b time.Duration, absTol float64) bool {
	return math.Abs(a.Seconds()-b.Seconds()) <= absTol
}

func calculateSlidingWindow(remaining, limit int64, resetAt, resetAfter float64) (time.Duration, time.Time) {
	// slidePeriod = resetAfter / (limit - remaining)
	slidePeriod := time.Duration((resetAfter/float64(limit-remaining))*1_000) * time.Millisecond

	// increaseAt = (resetAt - resetAfter) + slidePeriod
	resetAtTime := time.Unix(0, int64(resetAt*1_000_000_000))
	resetAfterDuration := time.Duration(resetAfter*1_000) * time.Millisecond
	increaseAt := resetAtTime.Add(-resetAfterDuration).Add(slidePeriod)

	return slidePeriod, increaseAt
}

// BucketRateLimit is a sliding window ratelimit implementation
type BucketRateLimit struct {
	userID     string
	path       string
	bucket     string
	lock       sync.Mutex
	remaining  int64
	limit      int64
	period     time.Duration
	increaseAt time.Time
	outOfSync  bool
}

func NewBucketRatelimit(remaining, limit int64, resetAt, resetAfter float64, bucket, path, userID string) *BucketRateLimit {
	if remaining == limit {
		// If we somehow get this case, then we cannot create a ratelimit from the info
		return nil
	}

	slidePeriod, increaseAt := calculateSlidingWindow(remaining, limit, resetAt, resetAfter)

	return &BucketRateLimit{
		bucket:     bucket,
		path:       path,
		userID:     userID,
		remaining:  remaining,
		limit:      limit,
		period:     slidePeriod,
		increaseAt: increaseAt,
	}
}

// Note: this MUST be called from a locked state
func (b *BucketRateLimit) isRatelimited(now time.Time) bool {
	// If we are out of sync, we shouldn't slide the window along, as we will be off due to
	// network latency.
	// The second part of this 'if' is to account for some cases where there can be a race
	// condition and we receive rate limit updates out of order, and we cannot update `outOfSync`
	if (now.After(b.increaseAt) || now.Equal(b.increaseAt)) && (!b.outOfSync || now.Sub(b.increaseAt) > b.period) {
		// We can slide the window along
		gain := int64(math.Floor((now.Sub(b.increaseAt).Seconds())/b.period.Seconds())) + 1
		nowRemaining := b.remaining + gain

		b.remaining = min(nowRemaining, b.limit)

		if b.remaining == b.limit {
			// When a ratelimit resets, we will fall out of sync from the remote, so
			// we want to prevent future sliding
			b.increaseAt = now.Add(b.period)
			b.outOfSync = true
		} else {
			b.increaseAt = b.increaseAt.Add(b.period * time.Duration(gain))
		}
	}

	return b.remaining <= 0
}

// Acquire will request a slot from the ratelimit and sleep until there is one available
func (b *BucketRateLimit) Acquire(ctx context.Context) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	for {
		now := time.Now()
		if !b.isRatelimited(now) {
			break
		}

		sleepDuration := b.increaseAt.Sub(now)
		if sleepDuration > 0 {
			logger.WithFields(logrus.Fields{
				"bucket":        b.bucket,
				"path":          b.path,
				"user":          b.userID,
				"sleepDuration": sleepDuration,
			}).Debug("backing off to avoid hitting ratelimits")

			// FIXME: This doesn't work and idk why
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleepDuration):
			}
		}
	}

	b.remaining--
	return nil
}

func (b *BucketRateLimit) Update(remaining, limit int64, resetAt, resetAfter float64) {
	if remaining == limit {
		// This should never happen, but just in case
		return
	}

	slidePeriod, increaseAt := calculateSlidingWindow(remaining, limit, resetAt, resetAfter)

	if increaseAt.Before(b.increaseAt) {
		// Old ratelimit information, ignore
		return
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	if b.limit != limit {
		if b.limit > limit {
			logger.WithFields(logrus.Fields{
				"bucket":   b.bucket,
				"path":     b.path,
				"user":     b.userID,
				"newLimit": limit,
				"oldLimit": b.limit,
			}).Warn("Bucket decreased its limit. It is possible you will see a small increase in 429s")
		}

		b.limit = limit
	}

	// We want to update the slide period only, and only if:
	//   1. The bucket is out of sync (ie, we reset the full window)
	//   2. We receive the first usage of the bucket, which will always have the most accurate slide period
	//   3. The slide periods differ too much. This is helpful if we diverged too much from the real one
	//      due to network latency, of if the bucket randomly changed
	//      Note: 0.3 and 0.5 are chosen arbitrarily after some testing
	if b.outOfSync || remaining == limit-1 || !isClose(slidePeriod, b.period, 0.3) {
		if !isClose(slidePeriod, b.period, 0.5) {
			logger.WithFields(logrus.Fields{
				"bucket":         b.bucket,
				"path":           b.path,
				"user":           b.userID,
				"newSlidePeriod": slidePeriod,
				"oldSlidePeriod": b.period,
			}).Warn("Bucket greatly increased its slide period. It is possible you will see a small increase in 429s")
		}

		b.outOfSync = false
		b.period = slidePeriod

		if increaseAt.After(b.increaseAt) {
			// We only want to change this if we are lacking behind, as that can lead to 429s
			b.increaseAt = increaseAt
		}
	}
}
