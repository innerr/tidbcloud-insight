package impl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"tidbcloud-insight/pkg/client"
	"tidbcloud-insight/pkg/prometheus_storage"

	"github.com/innerr/ticat/pkg/core/model"
)

type MetricsFetcherConfig struct {
	TargetChunkSizeMB int
	MaxConcurrency    int
	Step              int
}

type FetchResult struct {
	FetchedMetrics int
	SkippedMetrics int
	Errors         []error
}

type AdaptiveChunkSizer struct {
	targetBytesPerChunk int64
	minChunkSeconds     int
	maxChunkSeconds     int
	samplesPerSecond    float64
	mu                  sync.RWMutex
}

func NewAdaptiveChunkSizer(targetBytesPerChunk int64) *AdaptiveChunkSizer {
	return &AdaptiveChunkSizer{
		targetBytesPerChunk: targetBytesPerChunk,
		minChunkSeconds:     300,
		maxChunkSeconds:     7200,
		samplesPerSecond:    0,
	}
}

func (s *AdaptiveChunkSizer) EstimateChunkDuration(totalDurationSeconds int64) int {
	s.mu.RLock()
	sps := s.samplesPerSecond
	s.mu.RUnlock()

	if sps <= 0 {
		return 1800
	}

	bytesPerSample := 100.0
	estimatedSeconds := float64(s.targetBytesPerChunk) / (bytesPerSample * sps)

	if estimatedSeconds < float64(s.minChunkSeconds) {
		estimatedSeconds = float64(s.minChunkSeconds)
	}
	if estimatedSeconds > float64(s.maxChunkSeconds) {
		estimatedSeconds = float64(s.maxChunkSeconds)
	}

	return int(estimatedSeconds)
}

func (s *AdaptiveChunkSizer) Update(bytesWritten int64, durationSeconds int64, pointCount int) {
	if durationSeconds <= 0 || pointCount <= 0 {
		return
	}

	samplesPerSecond := float64(pointCount) / float64(durationSeconds)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.samplesPerSecond == 0 {
		s.samplesPerSecond = samplesPerSecond
	} else {
		s.samplesPerSecond = s.samplesPerSecond*0.7 + samplesPerSecond*0.3
	}
}

func (s *AdaptiveChunkSizer) GetSamplesPerSecond() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.samplesPerSecond
}

type MetricsFetcher struct {
	client     *client.Client
	storage    *prometheus_storage.PrometheusStorage
	config     MetricsFetcherConfig
	chunkSizer *AdaptiveChunkSizer
}

func NewMetricsFetcherConfigFromEnv(env *model.Env) MetricsFetcherConfig {
	step := 120
	stepStr := env.GetRaw(EnvKeyMetricsFetchStep)
	if stepStr != "" {
		if duration, err := time.ParseDuration(stepStr); err == nil {
			step = int(duration.Seconds())
		}
	}
	return MetricsFetcherConfig{
		TargetChunkSizeMB: env.GetInt(EnvKeyTargetChunkSizeMB),
		MaxConcurrency:    env.GetInt(EnvKeyRateLimitDesiredConcurrency),
		Step:              step,
	}
}

func NewMetricsFetcher(c *client.Client, storage *prometheus_storage.PrometheusStorage, config MetricsFetcherConfig) *MetricsFetcher {
	targetBytes := int64(config.TargetChunkSizeMB) * 1024 * 1024
	if targetBytes <= 0 {
		targetBytes = 1024 * 1024
	}

	return &MetricsFetcher{
		client:     c,
		storage:    storage,
		config:     config,
		chunkSizer: NewAdaptiveChunkSizer(targetBytes),
	}
}

type FetchTask struct {
	ID        string
	ClusterID string
	Metric    string
	GapStart  int64
	GapEnd    int64
	DSURL     string
	Step      int
	ChunkSize int
}

func (f *MetricsFetcher) Fetch(ctx context.Context, clusterID, dsURL string, metrics []string, startTS, endTS int64, step int) (*FetchResult, error) {
	result := &FetchResult{}

	var tasks []*FetchTask

	for _, metric := range metrics {
		existingRanges, err := f.storage.GetExistingTimeRanges(clusterID, metric)
		if err != nil {
			existingRanges = nil
		}

		gaps := f.storage.CalculateNonOverlappingRanges(startTS, endTS, existingRanges)

		if len(gaps) == 0 {
			result.SkippedMetrics++
			continue
		}

		for _, gap := range gaps {
			gapDuration := gap[1] - gap[0]
			chunkDuration := f.chunkSizer.EstimateChunkDuration(gapDuration)

			tasks = append(tasks, &FetchTask{
				Metric:    metric,
				GapStart:  gap[0],
				GapEnd:    gap[1],
				DSURL:     dsURL,
				ClusterID: clusterID,
				Step:      step,
				ChunkSize: chunkDuration,
			})
		}
	}

	if len(tasks) == 0 {
		return result, nil
	}

	handler := func(ctx context.Context, task *FetchTask) *TaskResult {
		return f.executeTask(ctx, task)
	}

	queue := NewFetchQueue(ctx, f.config.MaxConcurrency, handler)
	queue.SubmitBatch(tasks)

	results := queue.Wait()

	var mu sync.Mutex
	for _, r := range results {
		mu.Lock()
		if r.Success {
			result.FetchedMetrics++
		} else if r.Error != nil {
			result.Errors = append(result.Errors, fmt.Errorf("%s [%d-%d]: %w", r.Task.Metric, r.Task.GapStart, r.Task.GapEnd, r.Error))
		}
		mu.Unlock()
	}

	if len(result.Errors) > 0 && result.FetchedMetrics == 0 {
		return result, fmt.Errorf("all fetch tasks failed")
	}

	for _, metric := range metrics {
		if err := f.storage.MergeAdjacentFiles(clusterID, metric); err != nil {
			mu.Lock()
			result.Errors = append(result.Errors, fmt.Errorf("%s: merge failed: %w", metric, err))
			mu.Unlock()
		}
	}

	return result, nil
}

func (f *MetricsFetcher) executeTask(ctx context.Context, task *FetchTask) *TaskResult {
	writer, err := f.storage.NewMetricWriter(task.ClusterID, task.Metric, task.GapStart, task.GapEnd)
	if err != nil {
		return &TaskResult{
			Task:    task,
			Success: false,
			Error:   fmt.Errorf("failed to create writer: %w", err),
		}
	}

	res, err := f.client.QueryMetricChunkedWithWriter(
		ctx,
		task.DSURL,
		task.Metric,
		int(task.GapStart),
		int(task.GapEnd),
		task.Step,
		task.ChunkSize,
		writer,
	)

	if err != nil {
		writer.Close()

		if res != nil && res.TooManySamples {
			return &TaskResult{
				Task:       task,
				Success:    false,
				Error:      err,
				NeedSplit:  true,
				FailedSize: res.FailedSize,
			}
		}

		if isFatalError(err) {
			return &TaskResult{
				Task:         task,
				Success:      false,
				Error:        err,
				AbortCluster: true,
			}
		}

		return &TaskResult{
			Task:    task,
			Success: false,
			Error:   err,
		}
	}

	if closeErr := writer.Close(); closeErr != nil {
		return &TaskResult{
			Task:    task,
			Success: false,
			Error:   fmt.Errorf("failed to close writer: %w", closeErr),
		}
	}

	return &TaskResult{
		Task:    task,
		Success: true,
	}
}

func (f *MetricsFetcher) fetchGap(ctx context.Context, task FetchTask) error {
	gapDuration := task.GapEnd - task.GapStart
	chunkDuration := f.chunkSizer.EstimateChunkDuration(gapDuration)
	task.ChunkSize = chunkDuration

	result := f.executeTask(ctx, &task)
	if result.Success {
		return nil
	}
	return result.Error
}

func isFatalError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return contains(errStr, "403") || contains(errStr, "401") || contains(errStr, "unauthorized") || contains(errStr, "forbidden")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (f *MetricsFetcher) calculateStep(durationSeconds int64) int {
	if f.config.Step > 0 {
		return f.config.Step
	}
	return 120
}

func (f *MetricsFetcher) GetChunkSizer() *AdaptiveChunkSizer {
	return f.chunkSizer
}

type MetricFetchProgress struct {
	Metric       string
	GapStart     int64
	GapEnd       int64
	Status       string
	Error        error
	BytesWritten int64
}

func (f *MetricsFetcher) FetchWithProgress(ctx context.Context, clusterID, dsURL string, metrics []string, startTS, endTS int64, step int, progressChan chan<- MetricFetchProgress) (*FetchResult, error) {
	result := &FetchResult{}

	var tasks []FetchTask

	for _, metric := range metrics {
		existingRanges, err := f.storage.GetExistingTimeRanges(clusterID, metric)
		if err != nil {
			existingRanges = nil
		}

		gaps := f.storage.CalculateNonOverlappingRanges(startTS, endTS, existingRanges)

		if len(gaps) == 0 {
			result.SkippedMetrics++
			if progressChan != nil {
				progressChan <- MetricFetchProgress{
					Metric: metric,
					Status: "skipped",
				}
			}
			continue
		}

		for _, gap := range gaps {
			tasks = append(tasks, FetchTask{
				Metric:    metric,
				GapStart:  gap[0],
				GapEnd:    gap[1],
				DSURL:     dsURL,
				ClusterID: clusterID,
				Step:      step,
			})
		}
	}

	if len(tasks) == 0 {
		if progressChan != nil {
			close(progressChan)
		}
		return result, nil
	}

	sem := make(chan struct{}, f.config.MaxConcurrency)
	for i := 0; i < f.config.MaxConcurrency; i++ {
		sem <- struct{}{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, task := range tasks {
		wg.Add(1)

		go func(t FetchTask) {
			defer wg.Done()

			if progressChan != nil {
				progressChan <- MetricFetchProgress{
					Metric:   t.Metric,
					GapStart: t.GapStart,
					GapEnd:   t.GapEnd,
					Status:   "started",
				}
			}

			select {
			case <-sem:
			case <-ctx.Done():
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Errorf("%s: context cancelled", t.Metric))
				mu.Unlock()
				if progressChan != nil {
					progressChan <- MetricFetchProgress{
						Metric:   t.Metric,
						GapStart: t.GapStart,
						GapEnd:   t.GapEnd,
						Status:   "cancelled",
					}
				}
				return
			}
			defer func() { sem <- struct{}{} }()

			err := f.fetchGap(ctx, t)
			if err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Errorf("%s [%d-%d]: %w", t.Metric, t.GapStart, t.GapEnd, err))
				mu.Unlock()
				if progressChan != nil {
					progressChan <- MetricFetchProgress{
						Metric:   t.Metric,
						GapStart: t.GapStart,
						GapEnd:   t.GapEnd,
						Status:   "error",
						Error:    err,
					}
				}
				return
			}

			mu.Lock()
			result.FetchedMetrics++
			mu.Unlock()

			if progressChan != nil {
				progressChan <- MetricFetchProgress{
					Metric:   t.Metric,
					GapStart: t.GapStart,
					GapEnd:   t.GapEnd,
					Status:   "completed",
				}
			}
		}(task)
	}

	wg.Wait()

	for _, metric := range metrics {
		if err := f.storage.MergeAdjacentFiles(clusterID, metric); err != nil {
			mu.Lock()
			result.Errors = append(result.Errors, fmt.Errorf("%s: merge failed: %w", metric, err))
			mu.Unlock()
		}
	}

	if progressChan != nil {
		close(progressChan)
	}

	if len(result.Errors) > 0 && result.FetchedMetrics == 0 {
		return result, fmt.Errorf("all fetch tasks failed")
	}

	return result, nil
}

func FormatDuration(seconds int64) string {
	d := time.Duration(seconds) * time.Second
	return d.String()
}
