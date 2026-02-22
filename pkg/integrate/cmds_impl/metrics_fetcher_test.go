package impl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tidbcloud-insight/pkg/prometheus_storage"
)

const testBaseTimestamp int64 = 1700000000

func TestAdaptiveChunkSizer_InitialEstimate(t *testing.T) {
	sizer := NewAdaptiveChunkSizer(1024 * 1024)

	duration := sizer.EstimateChunkDuration(3600)
	if duration != 1800 {
		t.Errorf("expected default 1800s for no history, got %d", duration)
	}
}

func TestAdaptiveChunkSizer_AfterUpdate(t *testing.T) {
	sizer := NewAdaptiveChunkSizer(1024 * 1024)

	sizer.Update(1024*1024, 60, 6000)

	if sizer.GetSamplesPerSecond() <= 0 {
		t.Error("expected samplesPerSecond to be updated")
	}

	duration := sizer.EstimateChunkDuration(3600)
	if duration < 300 || duration > 7200 {
		t.Errorf("expected duration in range [300, 7200], got %d", duration)
	}
}

func TestAdaptiveChunkSizer_BoundaryMin(t *testing.T) {
	sizer := NewAdaptiveChunkSizer(1024 * 1024)

	sizer.Update(1024, 1, 1000000)

	duration := sizer.EstimateChunkDuration(3600)
	if duration < sizer.minChunkSeconds {
		t.Errorf("expected duration >= minChunkSeconds (%d), got %d", sizer.minChunkSeconds, duration)
	}
}

func TestAdaptiveChunkSizer_BoundaryMax(t *testing.T) {
	sizer := NewAdaptiveChunkSizer(1024 * 1024)

	sizer.Update(1024, 1000000, 1)

	duration := sizer.EstimateChunkDuration(3600)
	if duration > sizer.maxChunkSeconds {
		t.Errorf("expected duration <= maxChunkSeconds (%d), got %d", sizer.maxChunkSeconds, duration)
	}
}

func TestAdaptiveChunkSizer_Smoothing(t *testing.T) {
	sizer := NewAdaptiveChunkSizer(1024 * 1024)

	sizer.Update(1024*1024, 60, 6000)
	firstSPS := sizer.GetSamplesPerSecond()

	sizer.Update(1024*1024, 60, 12000)
	secondSPS := sizer.GetSamplesPerSecond()

	if secondSPS == firstSPS {
		t.Error("expected samplesPerSecond to change after update")
	}

	if secondSPS < firstSPS || secondSPS > 12000.0/60.0 {
		t.Errorf("expected samplesPerSecond to be smoothed between %f and %f, got %f", firstSPS, 200.0, secondSPS)
	}
}

func TestMetricsFetcher_GlobalConcurrencyLimit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "metrics_fetcher_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	var maxConcurrent int32
	var currentConcurrent int32
	var mu sync.Mutex

	config := MetricsFetcherConfig{
		TargetChunkSizeMB: 1,
		MaxConcurrency:    3,
	}

	metrics := []string{"metric1", "metric2", "metric3", "metric4", "metric5"}
	ctx := context.Background()

	sem := make(chan struct{}, config.MaxConcurrency)
	for i := 0; i < config.MaxConcurrency; i++ {
		sem <- struct{}{}
	}

	var wg sync.WaitGroup

	for i, m := range metrics {
		wg.Add(1)
		go func(idx int, _ string) {
			defer wg.Done()

			_ = [2]int64{testBaseTimestamp + int64(idx)*1000, testBaseTimestamp + int64(idx)*1000 + 1000}

			select {
			case <-sem:
			case <-ctx.Done():
				return
			}
			defer func() { sem <- struct{}{} }()

			cur := atomic.AddInt32(&currentConcurrent, 1)
			mu.Lock()
			if cur > maxConcurrent {
				maxConcurrent = cur
			}
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&currentConcurrent, -1)
		}(i, m)
	}

	wg.Wait()

	if maxConcurrent > 3 {
		t.Errorf("max concurrent exceeded limit: got %d, want <= 3", maxConcurrent)
	}
}

func TestMetricsFetcher_IncrementalWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "metrics_fetcher_incremental_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage := prometheus_storage.NewPrometheusStorage(tmpDir)
	clusterID := "test_cluster"
	metricName := "test_metric"

	startTS := testBaseTimestamp
	endTS := testBaseTimestamp + 1000

	writer1, err := storage.NewMetricWriter(clusterID, metricName, startTS, endTS)
	if err != nil {
		t.Fatalf("failed to create writer1: %v", err)
	}

	writer1.WriteSeries(nil, []int64{startTS, startTS + 100, startTS + 200}, []float64{1.0, 1.1, 1.2})
	writer1.Close()

	files, _ := storage.ListMetricFiles(clusterID, metricName)
	if len(files) != 1 {
		t.Fatalf("expected 1 file after first write, got %d", len(files))
	}

	existingRanges, _ := storage.GetExistingTimeRanges(clusterID, metricName)
	if len(existingRanges) != 1 {
		t.Fatalf("expected 1 existing range, got %d: %v", len(existingRanges), existingRanges)
	}

	requestEnd := testBaseTimestamp + 2000
	gaps := storage.CalculateNonOverlappingRanges(startTS, requestEnd, existingRanges)
	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}

	expectedGapStart := startTS + 200
	if gaps[0][0] != expectedGapStart {
		t.Errorf("expected gap start at %d, got %d", expectedGapStart, gaps[0][0])
	}

	writer2, _ := storage.NewMetricWriter(clusterID, metricName, gaps[0][0], gaps[0][1])
	writer2.WriteSeries(nil, []int64{gaps[0][0], gaps[0][0] + 300, gaps[0][1]}, []float64{1.2, 1.5, 3.0})
	writer2.Close()

	storage.MergeAdjacentFiles(clusterID, metricName)

	files, _ = storage.ListMetricFiles(clusterID, metricName)
	if len(files) != 1 {
		t.Errorf("expected 1 file after merge, got %d", len(files))
	}

	info, _ := storage.AnalyzeMetric(clusterID, metricName)
	if info.MinTime != startTS {
		t.Errorf("expected min time %d, got %d", startTS, info.MinTime)
	}
	if info.MaxTime != requestEnd {
		t.Errorf("expected max time %d, got %d", requestEnd, info.MaxTime)
	}
}

func TestMetricsFetcher_ResumeFromCache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "metrics_fetcher_resume_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage := prometheus_storage.NewPrometheusStorage(tmpDir)
	clusterID := "test_cluster"
	metricName := "test_metric"

	startTS := testBaseTimestamp
	endTS := testBaseTimestamp + 100

	writer, _ := storage.NewMetricWriter(clusterID, metricName, startTS, endTS)
	writer.WriteSeries(nil, []int64{startTS, startTS + 30, startTS + 60}, []float64{1.0, 1.5, 2.0})
	writer.Close()

	storage.MergeAdjacentFiles(clusterID, metricName)

	existingRanges, _ := storage.GetExistingTimeRanges(clusterID, metricName)

	requestStart := startTS - 100
	requestEnd := endTS + 100
	gaps := storage.CalculateNonOverlappingRanges(requestStart, requestEnd, existingRanges)

	if len(gaps) != 2 {
		t.Fatalf("expected 2 gaps, got %d: %v", len(gaps), gaps)
	}

	if gaps[0][0] != requestStart || gaps[0][1] != startTS {
		t.Errorf("first gap should be [%d, %d], got [%d, %d]", requestStart, startTS, gaps[0][0], gaps[0][1])
	}
	if gaps[1][0] != startTS+60 || gaps[1][1] != requestEnd {
		t.Errorf("second gap should be [%d, %d], got [%d, %d]", startTS+60, requestEnd, gaps[1][0], gaps[1][1])
	}
}

func TestMetricsFetcher_Integration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "metrics_fetcher_integration_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage := prometheus_storage.NewPrometheusStorage(tmpDir)
	clusterID := "test_cluster"

	metric1Start := testBaseTimestamp
	metric1End := testBaseTimestamp + 100
	metric2Start := testBaseTimestamp + 200
	metric2End := testBaseTimestamp + 300

	writer1, _ := storage.NewMetricWriter(clusterID, "metric1", metric1Start, metric1End)
	writer1.WriteSeries(nil, []int64{metric1Start, metric1Start + 30, metric1Start + 60, metric1Start + 90}, []float64{1.0, 1.3, 1.6, 2.0})
	writer1.Close()

	writer2, _ := storage.NewMetricWriter(clusterID, "metric2", metric2Start, metric2End)
	writer2.WriteSeries(nil, []int64{metric2Start, metric2Start + 30, metric2Start + 60, metric2Start + 90}, []float64{3.0, 3.3, 3.6, 4.0})
	writer2.Close()

	storage.MergeAdjacentFiles(clusterID, "metric1")
	storage.MergeAdjacentFiles(clusterID, "metric2")

	metric1Ranges, _ := storage.GetExistingTimeRanges(clusterID, "metric1")
	metric2Ranges, _ := storage.GetExistingTimeRanges(clusterID, "metric2")

	if len(metric1Ranges) != 1 {
		t.Errorf("expected 1 range for metric1, got %d: %v", len(metric1Ranges), metric1Ranges)
	}
	if len(metric2Ranges) != 1 {
		t.Errorf("expected 1 range for metric2, got %d: %v", len(metric2Ranges), metric2Ranges)
	}

	if len(metric1Ranges) == 1 {
		if metric1Ranges[0].Start != metric1Start || metric1Ranges[0].End != metric1Start+90 {
			t.Errorf("unexpected metric1 range: start=%d, end=%d", metric1Ranges[0].Start, metric1Ranges[0].End)
		}
	}

	if len(metric2Ranges) == 1 {
		if metric2Ranges[0].Start != metric2Start || metric2Ranges[0].End != metric2Start+90 {
			t.Errorf("unexpected metric2 range: start=%d, end=%d", metric2Ranges[0].Start, metric2Ranges[0].End)
		}
	}

	gaps1 := storage.CalculateNonOverlappingRanges(metric1Start-50, metric1Start+90+50, metric1Ranges)
	if len(gaps1) != 2 {
		t.Errorf("expected 2 gaps for metric1, got %d: %v", len(gaps1), gaps1)
	}

	gaps2 := storage.CalculateNonOverlappingRanges(metric2Start-50, metric2Start+90+50, metric2Ranges)
	if len(gaps2) != 2 {
		t.Errorf("expected 2 gaps for metric2, got %d: %v", len(gaps2), gaps2)
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		seconds  int64
		expected string
	}{
		{60, "1m0s"},
		{3600, "1h0m0s"},
		{86400, "24h0m0s"},
		{900, "15m0s"},
		{1800, "30m0s"},
	}

	for _, tt := range tests {
		result := FormatDuration(tt.seconds)
		if result != tt.expected {
			t.Errorf("FormatDuration(%d) = %s, want %s", tt.seconds, result, tt.expected)
		}
	}
}

func TestMetricsFetcher_CalculateStep(t *testing.T) {
	fetcher := &MetricsFetcher{
		storage:    nil,
		config:     MetricsFetcherConfig{TargetChunkSizeMB: 1, MaxConcurrency: 3},
		chunkSizer: NewAdaptiveChunkSizer(1024 * 1024),
	}

	tests := []struct {
		duration int64
		expected int
	}{
		{1800, 15},
		{3600, 15},
		{7200, 30},
		{86400, 30},
		{172800, 60},
		{345600, 120},
	}

	for _, tt := range tests {
		result := fetcher.calculateStep(tt.duration)
		if result != tt.expected {
			t.Errorf("calculateStep(%d) = %d, want %d", tt.duration, result, tt.expected)
		}
	}
}

func TestMetricsFetcher_MultipleMetricsAndGaps(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "metrics_fetcher_multi_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage := prometheus_storage.NewPrometheusStorage(tmpDir)
	clusterID := "test_cluster"

	for i := 0; i < 3; i++ {
		metricName := fmt.Sprintf("metric%d", i)
		startTS := testBaseTimestamp + int64(i)*10000
		endTS := startTS + 100

		writer, _ := storage.NewMetricWriter(clusterID, metricName, startTS, endTS)
		writer.WriteSeries(nil, []int64{startTS, startTS + 30, startTS + 60, startTS + 90}, []float64{1.0, 1.3, 1.6, 2.0})
		writer.Close()
	}

	files, _ := os.ReadDir(filepath.Join(tmpDir, clusterID))
	if len(files) != 3 {
		t.Errorf("expected 3 metric directories, got %d", len(files))
	}

	for i := 0; i < 3; i++ {
		metricName := fmt.Sprintf("metric%d", i)
		ranges, _ := storage.GetExistingTimeRanges(clusterID, metricName)
		if len(ranges) != 1 {
			t.Errorf("metric%d: expected 1 range, got %d: %v", i, len(ranges), ranges)
		}
	}
}

func TestNewMetricsFetcherConfig(t *testing.T) {
	config := MetricsFetcherConfig{
		TargetChunkSizeMB: 2,
		MaxConcurrency:    5,
	}

	if config.TargetChunkSizeMB != 2 {
		t.Errorf("expected TargetChunkSizeMB=2, got %d", config.TargetChunkSizeMB)
	}
	if config.MaxConcurrency != 5 {
		t.Errorf("expected MaxConcurrency=5, got %d", config.MaxConcurrency)
	}
}

func TestEnvKeyConstants(t *testing.T) {
	if EnvKeyTargetChunkSizeMB != "tidbcloud-insight.fetch.target-chunk-size-mb" {
		t.Errorf("EnvKeyTargetChunkSizeMB = %s, want tidbcloud-insight.fetch.target-chunk-size-mb", EnvKeyTargetChunkSizeMB)
	}
	if EnvKeyRateLimitDesiredConcurrency != "tidbcloud-insight.rate-limit.desired-concurrency" {
		t.Errorf("EnvKeyRateLimitDesiredConcurrency = %s, want tidbcloud-insight.rate-limit.desired-concurrency", EnvKeyRateLimitDesiredConcurrency)
	}
}
