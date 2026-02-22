package client

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"tidbcloud-insight/pkg/backoff"
	"tidbcloud-insight/pkg/logger"
)

type RateLimiterConfig struct {
	MaxRequestsPerSecond     int
	MinInterval              time.Duration
	ConsecutiveFailThreshold int
	BackoffInitial           time.Duration
	BackoffMax               time.Duration
	GetConcurrencyFunc       func() int
	ProgressInterval         time.Duration
	OnBackoffCallback        func()
}

type RateLimiter struct {
	mu sync.Mutex

	maxRequestsPerSecond     int
	minInterval              time.Duration
	consecutiveFailThreshold int
	getConcurrencyFunc       func() int
	progressInterval         time.Duration
	onBackoffCallback        func()

	totalRequests   int64
	successRequests int64
	failedRequests  int64
	lastRequestTime time.Time

	backoffStrategy        *backoff.Strategy
	backoffRetry           *backoff.Retry
	inBackoff              bool
	backoffUntil           time.Time
	backoffStartTime       time.Time
	currentBackoffInterval time.Duration
	consecutiveFails       int

	recentRequests []time.Time
	windowSize     time.Duration
	rand           *rand.Rand

	cond           *sync.Cond
	activeRequests int
}

func DefaultRateLimiterConfig() RateLimiterConfig {
	return RateLimiterConfig{
		MaxRequestsPerSecond:     10,
		MinInterval:              100 * time.Millisecond,
		ConsecutiveFailThreshold: 3,
		BackoffInitial:           1 * time.Second,
		BackoffMax:               5 * time.Minute,
	}
}

func NewRateLimiter(cfg RateLimiterConfig) *RateLimiter {
	if cfg.MaxRequestsPerSecond <= 0 {
		cfg.MaxRequestsPerSecond = 10
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 100 * time.Millisecond
	}
	if cfg.ConsecutiveFailThreshold <= 0 {
		cfg.ConsecutiveFailThreshold = 3
	}
	if cfg.BackoffInitial <= 0 {
		cfg.BackoffInitial = 1 * time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 5 * time.Minute
	}

	backoffStrategy := backoff.NewStrategy(
		backoff.WithInitialInterval(cfg.BackoffInitial),
		backoff.WithMaxInterval(cfg.BackoffMax),
		backoff.WithMultiplier(2.0),
		backoff.WithRandomizationFactor(0.3),
		backoff.WithMaxElapsedTime(30*time.Minute),
	)

	rl := &RateLimiter{
		maxRequestsPerSecond:     cfg.MaxRequestsPerSecond,
		minInterval:              cfg.MinInterval,
		consecutiveFailThreshold: cfg.ConsecutiveFailThreshold,
		getConcurrencyFunc:       cfg.GetConcurrencyFunc,
		progressInterval:         cfg.ProgressInterval,
		onBackoffCallback:        cfg.OnBackoffCallback,
		backoffStrategy:          backoffStrategy,
		backoffRetry:             backoffStrategy.NewRetry(),
		windowSize:               1 * time.Second,
		recentRequests:           make([]time.Time, 0),
		rand:                     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	rl.cond = sync.NewCond(&rl.mu)
	return rl
}

func (r *RateLimiter) addJitter(d time.Duration, factor float64) time.Duration {
	if factor <= 0 {
		return d
	}
	delta := float64(d) * factor
	return time.Duration(float64(d) + (r.rand.Float64()*2-1)*delta)
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(100 * time.Millisecond).String()
}

func (r *RateLimiter) getConcurrencyInfo() string {
	return ""
}

func (r *RateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()
	r.activeRequests++
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.activeRequests--
		r.mu.Unlock()
	}()

	r.mu.Lock()

	for r.inBackoff {
		remaining := time.Until(r.backoffUntil)
		if remaining <= 0 {
			r.inBackoff = false
			break
		}

		logger.Infof("Rate limited, waiting %s...", formatDuration(remaining))

		progressInterval := 10 * time.Second
		if r.progressInterval > 0 {
			progressInterval = r.progressInterval
		}

		waitDone := make(chan struct{})
		go func(backoffTime time.Duration) {
			defer close(waitDone)
			ticker := time.NewTicker(progressInterval)
			defer ticker.Stop()
			timer := time.NewTimer(backoffTime)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					r.cond.Broadcast()
					return
				case <-timer.C:
					r.cond.Broadcast()
					return
				case <-ticker.C:
					r.mu.Lock()
					remaining := time.Until(r.backoffUntil)
					r.mu.Unlock()
					if remaining > 0 {
						logger.Infof("Still in backoff, %s remaining...", formatDuration(remaining))
					}
				}
			}
		}(remaining)

		r.cond.Wait()

		select {
		case <-waitDone:
		default:
		}

		select {
		case <-ctx.Done():
			r.mu.Unlock()
			return ctx.Err()
		default:
		}

		if time.Now().After(r.backoffUntil) {
			r.inBackoff = false
		}
	}

	now := time.Now()
	r.cleanOldRequests(now)

	for len(r.recentRequests) >= r.maxRequestsPerSecond {
		oldest := r.recentRequests[0]
		waitDuration := r.windowSize - now.Sub(oldest)
		if waitDuration <= 0 {
			break
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
		}
		r.mu.Lock()
		now = time.Now()
		r.cleanOldRequests(now)
	}

	if !r.lastRequestTime.IsZero() {
		elapsed := now.Sub(r.lastRequestTime)
		if elapsed < r.minInterval {
			waitDuration := r.addJitter(r.minInterval-elapsed, 0.2)
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitDuration):
			}
			r.mu.Lock()
			now = time.Now()
		}
	}

	r.lastRequestTime = now
	r.recentRequests = append(r.recentRequests, now)
	r.totalRequests++

	r.mu.Unlock()
	return nil
}

func (r *RateLimiter) IsInBackoff() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.inBackoff {
		return false
	}
	return time.Until(r.backoffUntil) > 0
}

func (r *RateLimiter) cleanOldRequests(now time.Time) {
	cutoff := now.Add(-r.windowSize)
	validIdx := 0
	for i, t := range r.recentRequests {
		if t.After(cutoff) {
			validIdx = i
			break
		}
	}
	if validIdx > 0 {
		r.recentRequests = r.recentRequests[validIdx:]
	}
}

func (r *RateLimiter) RecordSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.successRequests++
	r.consecutiveFails = 0

	if r.backoffRetry.Attempt() > 0 {
		r.backoffRetry = r.backoffStrategy.NewRetry()
	}
}

func (r *RateLimiter) RecordFailure(statusCode int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.failedRequests++

	isRateLimit := statusCode == 429 || statusCode == 503
	isPermanentError := statusCode == 403 || statusCode == 401 || statusCode == 404 || statusCode == 422

	if !isPermanentError {
		r.consecutiveFails++
	}

	shouldBackoff := isRateLimit || (!isPermanentError && r.consecutiveFails >= r.consecutiveFailThreshold)

	if shouldBackoff {
		interval, ok := r.backoffRetry.NextInterval()
		if !ok {
			newRetry := r.backoffStrategy.NewRetry()
			interval, _ = newRetry.NextInterval()
		}

		now := time.Now()
		r.backoffStartTime = now
		r.backoffUntil = now.Add(interval)
		r.currentBackoffInterval = interval
		r.inBackoff = true

		if isRateLimit {
			logger.Warnf("Rate limited by server (HTTP %d), backing off for %s",
				statusCode, formatDuration(interval))
		} else {
			logger.Warnf("Request failures: %d consecutive, backing off for %s",
				r.consecutiveFails, formatDuration(interval))
		}

		if r.onBackoffCallback != nil {
			r.onBackoffCallback()
		}
	}
}

func (r *RateLimiter) GetStats() (total, success, failed int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totalRequests, r.successRequests, r.failedRequests
}

func (r *RateLimiter) PrintStats() {
	total, success, failed := r.GetStats()
	if total > 0 {
		logger.Infof("Rate limiter stats: %d requests, %d success, %d failed",
			total, success, failed)
	}
}
