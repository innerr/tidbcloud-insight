package client

import (
	"context"
	"sync"
	"time"

	"tidbcloud-insight/pkg/logger"
)

type ConcurrencyState int

const (
	StateNormal ConcurrencyState = iota
	StateReducing
	StateMinimal
	StateRecovering
)

func (s ConcurrencyState) String() string {
	switch s {
	case StateNormal:
		return "normal"
	case StateReducing:
		return "reducing"
	case StateMinimal:
		return "minimal"
	case StateRecovering:
		return "recovering"
	default:
		return "unknown"
	}
}

type AdaptiveConcurrencyConfig struct {
	DesiredConcurrency  int
	MinConcurrency      int
	ReduceStep          int
	RecoveryInterval    time.Duration
	MinRecoveryInterval time.Duration
	RecoveryStep        int
	MaxRecoveryStep     int
	Verbose             bool
}

func DefaultAdaptiveConcurrencyConfig() AdaptiveConcurrencyConfig {

	desired := 3
	recoveryInterval := 30 * time.Second
	minRecoveryInterval := 10 * time.Second

	return AdaptiveConcurrencyConfig{
		DesiredConcurrency:  desired,
		MinConcurrency:      1,
		ReduceStep:          1,
		RecoveryInterval:    recoveryInterval,
		MinRecoveryInterval: minRecoveryInterval,
		RecoveryStep:        1,
		MaxRecoveryStep:     3,
		Verbose:             true,
	}
}

type AdaptiveConcurrencyController struct {
	mu sync.RWMutex

	config AdaptiveConcurrencyConfig

	currentConcurrency int
	state              ConcurrencyState
	stateChangedAt     time.Time

	lastRateLimitedAt    time.Time
	rateLimitedCount     int
	consecutiveRateLimit int

	lastSuccessAt      time.Time
	successCount       int64
	consecutiveSuccess int

	currentRecoveryInterval time.Duration
	recoveryAttempts        int

	taskQueue   chan func()
	workerPool  chan struct{}
	activeTasks int

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewAdaptiveConcurrencyController(cfg AdaptiveConcurrencyConfig) *AdaptiveConcurrencyController {
	if cfg.DesiredConcurrency <= 0 {
		cfg.DesiredConcurrency = 3
	}
	if cfg.MinConcurrency <= 0 {
		cfg.MinConcurrency = 1
	}
	if cfg.ReduceStep <= 0 {
		cfg.ReduceStep = 1
	}
	if cfg.RecoveryInterval <= 0 {
		cfg.RecoveryInterval = 30 * time.Second
	}

	acc := &AdaptiveConcurrencyController{
		config:                  cfg,
		currentConcurrency:      cfg.DesiredConcurrency,
		state:                   StateNormal,
		stateChangedAt:          time.Now(),
		currentRecoveryInterval: cfg.RecoveryInterval,
		taskQueue:               make(chan func(), 100),
		workerPool:              make(chan struct{}, cfg.DesiredConcurrency),
		stopCh:                  make(chan struct{}),
	}

	for i := 0; i < cfg.DesiredConcurrency; i++ {
		acc.workerPool <- struct{}{}
	}

	acc.startWorkers()

	return acc
}

func (acc *AdaptiveConcurrencyController) startWorkers() {
	acc.wg.Add(acc.currentConcurrency)
	for i := 0; i < acc.currentConcurrency; i++ {
		go acc.worker(i)
	}
}

func (acc *AdaptiveConcurrencyController) worker(id int) {
	defer acc.wg.Done()

	for {
		select {
		case <-acc.stopCh:
			return
		case task, ok := <-acc.taskQueue:
			if !ok {
				return
			}

			acc.mu.Lock()
			if acc.currentConcurrency < acc.getMinConcurrency() {
				acc.mu.Unlock()
				select {
				case acc.taskQueue <- task:
				default:
				}
				return
			}
			acc.activeTasks++
			acc.mu.Unlock()

			task()

			acc.mu.Lock()
			acc.activeTasks--
			acc.mu.Unlock()
		}
	}
}

func (acc *AdaptiveConcurrencyController) getMinConcurrency() int {
	return acc.config.MinConcurrency
}

func (acc *AdaptiveConcurrencyController) Submit(task func()) bool {
	select {
	case acc.taskQueue <- task:
		return true
	default:
		if acc.config.Verbose {
			logger.SetConcurrencyProvider(acc)
			logger.Warn("Task queue full, task rejected")
		}
		return false
	}
}

func (acc *AdaptiveConcurrencyController) SubmitAndWait(task func()) {
	done := make(chan struct{})
	wrappedTask := func() {
		defer close(done)
		task()
	}

	if !acc.Submit(wrappedTask) {
		task()
		return
	}

	<-done
}

func (acc *AdaptiveConcurrencyController) OnRateLimited() {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	now := time.Now()
	acc.lastRateLimitedAt = now
	acc.rateLimitedCount++
	acc.consecutiveRateLimit++
	acc.consecutiveSuccess = 0

	if acc.state == StateNormal || acc.state == StateRecovering {
		acc.reduceConcurrency()
	} else if acc.state == StateReducing {
		acc.reduceConcurrency()
	}
}

func (acc *AdaptiveConcurrencyController) OnSuccess() {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	now := time.Now()
	acc.lastSuccessAt = now
	acc.successCount++
	acc.consecutiveSuccess++
	acc.consecutiveRateLimit = 0

	timeSinceLastRateLimit := now.Sub(acc.lastRateLimitedAt)
	if acc.lastRateLimitedAt.IsZero() {
		timeSinceLastRateLimit = time.Hour
	}

	if acc.state == StateMinimal && timeSinceLastRateLimit > acc.currentRecoveryInterval {
		acc.tryRecover()
	} else if acc.state == StateReducing && timeSinceLastRateLimit > acc.currentRecoveryInterval {
		acc.tryRecover()
	} else if acc.state == StateRecovering && acc.consecutiveSuccess >= 5 {
		acc.tryRecover()
	}
}

func (acc *AdaptiveConcurrencyController) reduceConcurrency() {
	if acc.currentConcurrency <= acc.config.MinConcurrency {
		acc.state = StateMinimal
		acc.stateChangedAt = time.Now()
		if acc.config.Verbose {
			logger.SetConcurrencyProvider(acc)
			logger.Infof("Concurrency at minimum (%d), entering minimal state", acc.currentConcurrency)
		}
		return
	}

	acc.currentConcurrency -= acc.config.ReduceStep
	if acc.currentConcurrency < acc.config.MinConcurrency {
		acc.currentConcurrency = acc.config.MinConcurrency
	}

	acc.state = StateReducing
	acc.stateChangedAt = time.Now()

	acc.recoveryAttempts = 0
	acc.currentRecoveryInterval = acc.config.RecoveryInterval

	if acc.config.Verbose {
		logger.SetConcurrencyProvider(acc)
		logger.Infof("Reduced concurrency to %d", acc.currentConcurrency)
	}
}

func (acc *AdaptiveConcurrencyController) tryRecover() {
	if acc.currentConcurrency >= acc.config.DesiredConcurrency {
		acc.state = StateNormal
		acc.stateChangedAt = time.Now()
		acc.currentRecoveryInterval = acc.config.RecoveryInterval
		acc.recoveryAttempts = 0
		if acc.config.Verbose {
			logger.SetConcurrencyProvider(acc)
			logger.Infof("Recovered to normal state, concurrency: %d", acc.currentConcurrency)
		}
		return
	}

	acc.recoveryAttempts++
	step := acc.config.RecoveryStep
	if acc.recoveryAttempts > 3 {
		step = min(acc.config.MaxRecoveryStep, acc.recoveryAttempts/2)
	}

	newConcurrency := acc.currentConcurrency + step
	if newConcurrency > acc.config.DesiredConcurrency {
		newConcurrency = acc.config.DesiredConcurrency
	}

	acc.currentConcurrency = newConcurrency
	acc.state = StateRecovering
	acc.stateChangedAt = time.Now()

	acc.currentRecoveryInterval = max(
		acc.config.MinRecoveryInterval,
		acc.currentRecoveryInterval-time.Duration(acc.recoveryAttempts)*time.Second,
	)

	if acc.config.Verbose {
		logger.SetConcurrencyProvider(acc)
		logger.Infof("Recovering concurrency to %d (attempt %d, next interval: %v)",
			acc.currentConcurrency, acc.recoveryAttempts, acc.currentRecoveryInterval)
	}
}

func (acc *AdaptiveConcurrencyController) GetCurrentConcurrency() int {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	return acc.currentConcurrency
}

func (acc *AdaptiveConcurrencyController) GetDesiredConcurrency() int {
	return acc.config.DesiredConcurrency
}

func (acc *AdaptiveConcurrencyController) GetState() ConcurrencyState {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	return acc.state
}

func (acc *AdaptiveConcurrencyController) GetStats() (concurrency, active, queued int, state ConcurrencyState) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	return acc.currentConcurrency, acc.activeTasks, len(acc.taskQueue), acc.state
}

func (acc *AdaptiveConcurrencyController) AcquireSlot(ctx context.Context) bool {
	acc.mu.Lock()
	currentLimit := acc.currentConcurrency
	acc.mu.Unlock()

	select {
	case acc.workerPool <- struct{}{}:
		if len(acc.workerPool) > currentLimit {
			<-acc.workerPool
			return false
		}
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (acc *AdaptiveConcurrencyController) ReleaseSlot() {
	select {
	case <-acc.workerPool:
	default:
	}
}

func (acc *AdaptiveConcurrencyController) Stop() {
	close(acc.stopCh)
	close(acc.taskQueue)
	acc.wg.Wait()
}

func (acc *AdaptiveConcurrencyController) ShouldYieldTask() bool {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	return acc.state == StateMinimal || acc.state == StateReducing
}

func (acc *AdaptiveConcurrencyController) WaitForRecovery(ctx context.Context) bool {
	acc.mu.RLock()
	interval := acc.currentRecoveryInterval
	acc.mu.RUnlock()

	select {
	case <-time.After(interval):
		return true
	case <-ctx.Done():
		return false
	}
}

type ConcurrentExecutor struct {
	controller  *AdaptiveConcurrencyController
	rateLimiter *RateLimiter
}

func NewConcurrentExecutor(acc *AdaptiveConcurrencyController, rl *RateLimiter) *ConcurrentExecutor {
	return &ConcurrentExecutor{
		controller:  acc,
		rateLimiter: rl,
	}
}

func (ce *ConcurrentExecutor) Execute(ctx context.Context, tasks []func()) {
	var wg sync.WaitGroup

	for _, task := range tasks {
		wg.Add(1)

		go func(t func()) {
			defer wg.Done()

			if !ce.controller.AcquireSlot(ctx) {
				if ce.controller.ShouldYieldTask() {
					ce.controller.WaitForRecovery(ctx)
				}
				if !ce.controller.AcquireSlot(ctx) {
					t()
					return
				}
			}
			defer ce.controller.ReleaseSlot()

			t()
		}(task)
	}

	wg.Wait()
}

func (ce *ConcurrentExecutor) ExecuteWithSemaphore(ctx context.Context, tasks []func(), baseSem chan struct{}) {
	var wg sync.WaitGroup

	for _, task := range tasks {
		wg.Add(1)

		go func(t func()) {
			defer wg.Done()

			select {
			case baseSem <- struct{}{}:
				defer func() { <-baseSem }()
			case <-ctx.Done():
				return
			}

			if ce.controller.ShouldYieldTask() {
				ce.controller.WaitForRecovery(ctx)
			}

			t()
		}(task)
	}

	wg.Wait()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
