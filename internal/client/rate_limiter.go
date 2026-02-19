package client

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"tidbcloud-insight/internal/backoff"
	"tidbcloud-insight/internal/config"
)

func goroutineID() string {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	s := string(b)
	s = s[strings.Index(s, "goroutine ")+10:]
	s = s[:strings.Index(s, " ")]
	id, _ := strconv.ParseInt(s, 10, 64)
	return fmt.Sprintf("g%d", id%1000)
}

type RateLimiterConfig struct {
	MaxRequestsPerSecond     int
	MinInterval              time.Duration
	ConsecutiveFailThreshold int
	BackoffInitial           time.Duration
	BackoffMax               time.Duration
	Verbose                  bool
	GetConcurrencyFunc       func() int
	ProgressInterval         time.Duration
	OnBackoffCallback        func()
}

type RateLimiter struct {
	mu sync.Mutex

	maxRequestsPerSecond     int
	minInterval              time.Duration
	consecutiveFailThreshold int
	verbose                  bool
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
	backoffWaiters         int
	consecutiveFails       int
	backoffCount           int

	recentRequests []time.Time
	windowSize     time.Duration
	rand           *rand.Rand
}

func DefaultRateLimiterConfig() RateLimiterConfig {
	cfg := config.Get()
	maxBackoff := 5 * time.Minute
	if cfg != nil {
		maxBackoff = cfg.RateLimit.GetMaxBackoff()
	}

	return RateLimiterConfig{
		MaxRequestsPerSecond:     10,
		MinInterval:              100 * time.Millisecond,
		ConsecutiveFailThreshold: 3,
		BackoffInitial:           1 * time.Second,
		BackoffMax:               maxBackoff,
		Verbose:                  true,
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

	return &RateLimiter{
		maxRequestsPerSecond:     cfg.MaxRequestsPerSecond,
		minInterval:              cfg.MinInterval,
		consecutiveFailThreshold: cfg.ConsecutiveFailThreshold,
		verbose:                  cfg.Verbose,
		getConcurrencyFunc:       cfg.GetConcurrencyFunc,
		progressInterval:         cfg.ProgressInterval,
		onBackoffCallback:        cfg.OnBackoffCallback,
		backoffStrategy:          backoffStrategy,
		backoffRetry:             backoffStrategy.NewRetry(),
		windowSize:               1 * time.Second,
		recentRequests:           make([]time.Time, 0),
		rand:                     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
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

func (r *RateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.inBackoff {
		remaining := time.Until(r.backoffUntil)
		if remaining > 0 {
			r.backoffWaiters++
			waitDuration := remaining
			waitDuration = r.addJitter(waitDuration, 0.1)

			if r.verbose {
				concurrencyInfo := ""
				if r.getConcurrencyFunc != nil {
					concurrencyInfo = fmt.Sprintf(", concurrency: %d", r.getConcurrencyFunc())
				}
				gidInfo := ""
				if r.backoffWaiters > 1 {
					gidInfo = fmt.Sprintf(" [%s]", goroutineID())
				}
				fmt.Printf("[WARN] Rate limited%s%s, backing off for %s...\n", concurrencyInfo, gidInfo, formatDuration(waitDuration))
			}
			r.mu.Unlock()
			if !r.waitWithProgress(ctx, waitDuration) {
				r.mu.Lock()
				r.backoffWaiters--
				return ctx.Err()
			}
			r.mu.Lock()
			r.backoffWaiters--
		}
		r.inBackoff = false
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
			r.mu.Lock()
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
				r.mu.Lock()
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

	return nil
}

func (r *RateLimiter) waitWithProgress(ctx context.Context, totalWait time.Duration) bool {
	if r.progressInterval <= 0 || totalWait <= r.progressInterval {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(totalWait):
			return true
		}
	}

	deadline := time.Now().Add(totalWait)
	ticker := time.NewTicker(r.progressInterval)
	defer ticker.Stop()

	showGID := r.backoffWaiters > 1

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return true
			}
			if r.verbose {
				concurrencyInfo := ""
				if r.getConcurrencyFunc != nil {
					concurrencyInfo = fmt.Sprintf(", concurrency: %d", r.getConcurrencyFunc())
				}
				gidInfo := ""
				if showGID {
					gidInfo = fmt.Sprintf(" [%s]", goroutineID())
				}
				fmt.Printf("[INFO] Still waiting for rate limit to expire%s%s, %s remaining...\n", concurrencyInfo, gidInfo, formatDuration(remaining))
			}
		}
	}
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
	r.consecutiveFails++

	isRateLimit := statusCode == 429 || statusCode == 503
	shouldBackoff := isRateLimit || r.consecutiveFails >= r.consecutiveFailThreshold

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
		r.backoffWaiters = 0

		if r.verbose {
			concurrencyInfo := ""
			if r.getConcurrencyFunc != nil {
				concurrencyInfo = fmt.Sprintf(", concurrency: %d", r.getConcurrencyFunc())
			}
			if isRateLimit {
				fmt.Printf("[WARN] Rate limited by server (HTTP %d)%s, backing off for %s...\n", statusCode, concurrencyInfo, formatDuration(interval))
			} else {
				fmt.Printf("[WARN] Request rate slowed: %d consecutive failures%s, backing off for %s...\n", r.consecutiveFails, concurrencyInfo, formatDuration(interval))
			}
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
		concurrencyInfo := ""
		if r.getConcurrencyFunc != nil {
			concurrencyInfo = fmt.Sprintf(", current concurrency: %d", r.getConcurrencyFunc())
		}
		fmt.Printf("[INFO] Rate limiter stats: %d requests, %d success, %d failed%s\n", total, success, failed, concurrencyInfo)
	}
}
