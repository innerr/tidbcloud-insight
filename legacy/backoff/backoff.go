package backoff

import (
	"context"
	"math/rand"
	"time"
)

type Strategy struct {
	initialInterval     time.Duration
	maxInterval         time.Duration
	multiplier          float64
	randomizationFactor float64
	maxElapsedTime      time.Duration
}

type Option func(*Strategy)

func WithInitialInterval(d time.Duration) Option {
	return func(s *Strategy) { s.initialInterval = d }
}

func WithMaxInterval(d time.Duration) Option {
	return func(s *Strategy) { s.maxInterval = d }
}

func WithMultiplier(m float64) Option {
	return func(s *Strategy) { s.multiplier = m }
}

func WithRandomizationFactor(f float64) Option {
	return func(s *Strategy) { s.randomizationFactor = f }
}

func WithMaxElapsedTime(d time.Duration) Option {
	return func(s *Strategy) { s.maxElapsedTime = d }
}

func NewStrategy(opts ...Option) *Strategy {
	s := &Strategy{
		initialInterval:     1 * time.Second,
		maxInterval:         30 * time.Second,
		multiplier:          2.0,
		randomizationFactor: 0.5,
		maxElapsedTime:      5 * time.Minute,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Strategy) NewRetry() *Retry {
	return &Retry{
		strategy:        s,
		currentInterval: s.initialInterval,
		startTime:       time.Now(),
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type Retry struct {
	strategy        *Strategy
	currentInterval time.Duration
	startTime       time.Time
	attempt         int
	rand            *rand.Rand
}

func (r *Retry) NextInterval() (time.Duration, bool) {
	if r.strategy.maxElapsedTime > 0 && time.Since(r.startTime) > r.strategy.maxElapsedTime {
		return 0, false
	}

	interval := r.currentInterval

	randomization := r.strategy.randomizationFactor
	if randomization > 0 {
		delta := float64(interval) * randomization
		interval = time.Duration(float64(interval) + (r.rand.Float64()*2-1)*delta)
	}

	nextInterval := time.Duration(float64(r.currentInterval) * r.strategy.multiplier)
	if nextInterval > r.strategy.maxInterval {
		nextInterval = r.strategy.maxInterval
	}
	r.currentInterval = nextInterval
	r.attempt++

	return interval, true
}

func (r *Retry) Attempt() int {
	return r.attempt
}

func Do(ctx context.Context, strategy *Strategy, fn func() error) error {
	retry := strategy.NewRetry()
	var lastErr error

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		interval, ok := retry.NextInterval()
		if !ok {
			return lastErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func DoWithRetryCount(ctx context.Context, strategy *Strategy, maxRetries int, fn func() error) error {
	retry := strategy.NewRetry()
	var lastErr error

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if retry.Attempt() >= maxRetries {
			return lastErr
		}

		interval, ok := retry.NextInterval()
		if !ok {
			return lastErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

type BackoffError struct {
	Err     error
	Attempt int
}

func (e *BackoffError) Error() string {
	return e.Err.Error()
}

func (e *BackoffError) Unwrap() error {
	return e.Err
}
