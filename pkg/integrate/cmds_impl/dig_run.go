package impl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"tidbcloud-insight/pkg/analysis"
	"tidbcloud-insight/pkg/prometheus_storage"
)

type AlgorithmTiming struct {
	Name     string
	Duration time.Duration
}

type DigProfileResult struct {
	ClusterID string
	Profile   *analysis.LoadProfile
	Timings   []AlgorithmTiming
}

type DigAbnormalResult struct {
	ClusterID string
	Anomalies []analysis.DetectedAnomaly
	Summary   *analysis.MergeSummary
	Timings   []AlgorithmTiming
}

type DigResult struct {
	LoadProfile *analysis.LoadProfile      `json:"load_profile"`
	Anomalies   []analysis.DetectedAnomaly `json:"anomalies"`
	Summary     *analysis.MergeSummary     `json:"summary,omitempty"`
}

type metricsData struct {
	qpsData     []analysis.TimeSeriesPoint
	latencyData []analysis.TimeSeriesPoint
}

func loadMetricsData(cacheDir, clusterID string, startTS, endTS int64) (*metricsData, error) {
	promStorage := prometheus_storage.NewPrometheusStorage(filepath.Join(cacheDir, "metrics"))

	qpsData, err := loadMetricTimeSeries(promStorage, clusterID, "tidb_server_query_total", startTS, endTS)
	if err != nil {
		return nil, fmt.Errorf("failed to load QPS data: %w", err)
	}

	latencyData, err := loadMetricHistogramP99(promStorage, clusterID, "tidb_server_handle_query_duration_seconds_bucket", startTS, endTS)
	if err != nil {
		fmt.Printf("Warning: failed to load latency data: %v\n", err)
	}

	return &metricsData{
		qpsData:     qpsData,
		latencyData: latencyData,
	}, nil
}

func DigProfile(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	clusterID string, startTS, endTS int64, jsonOutput bool) error {

	fmt.Printf("Profiling cluster %s\n", clusterID)
	fmt.Printf("Time range: %s ~ %s\n",
		time.Unix(startTS, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTS, 0).Format("2006-01-02 15:04:05"))

	fmt.Println("\nLoading metrics data...")
	data, err := loadMetricsData(cacheDir, clusterID, startTS, endTS)
	if err != nil {
		return err
	}

	fmt.Printf("Loaded %d QPS data points\n", len(data.qpsData))
	if len(data.latencyData) > 0 {
		fmt.Printf("Loaded %d latency data points\n", len(data.latencyData))
	}

	if len(data.qpsData) == 0 {
		return fmt.Errorf("no QPS data available for analysis")
	}

	var timings []AlgorithmTiming

	fmt.Println("\nAnalyzing load profile...")
	start := time.Now()
	profile := analysis.AnalyzeLoadProfile(clusterID, data.qpsData, data.latencyData)
	timings = append(timings, AlgorithmTiming{Name: "AnalyzeLoadProfile", Duration: time.Since(start)})

	if profile == nil {
		return fmt.Errorf("failed to analyze load profile")
	}

	result := &DigProfileResult{
		ClusterID: clusterID,
		Profile:   profile,
		Timings:   timings,
	}

	if jsonOutput {
		printProfileJSON(result)
	} else {
		printProfileResult(result)
	}

	return nil
}

func DigAbnormal(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	clusterID string, startTS, endTS int64, jsonOutput bool) error {

	fmt.Printf("Detecting anomalies for cluster %s\n", clusterID)
	fmt.Printf("Time range: %s ~ %s\n",
		time.Unix(startTS, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTS, 0).Format("2006-01-02 15:04:05"))

	fmt.Println("\nLoading metrics data...")
	data, err := loadMetricsData(cacheDir, clusterID, startTS, endTS)
	if err != nil {
		return err
	}

	fmt.Printf("Loaded %d QPS data points\n", len(data.qpsData))
	if len(data.latencyData) > 0 {
		fmt.Printf("Loaded %d latency data points\n", len(data.latencyData))
	}

	if len(data.qpsData) == 0 {
		return fmt.Errorf("no QPS data available for analysis")
	}

	var timings []AlgorithmTiming
	algorithmResults := make(map[string][]analysis.DetectedAnomaly)

	fmt.Println("\nRunning anomaly detection algorithms...")

	start := time.Now()
	detector := analysis.NewAnomalyDetector(analysis.DefaultAnomalyConfig())
	qpsAnomalies := detector.DetectAll(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "AnomalyDetector.DetectAll", Duration: time.Since(start)})
	algorithmResults["AnomalyDetector"] = qpsAnomalies

	if len(data.latencyData) > 0 {
		start = time.Now()
		latencyAnomalies := detector.DetectLatencyAnomalies(data.latencyData, data.latencyData)
		timings = append(timings, AlgorithmTiming{Name: "DetectLatencyAnomalies", Duration: time.Since(start)})
		algorithmResults["LatencyDetector"] = latencyAnomalies
	}

	start = time.Now()
	advancedDetector := analysis.NewAdvancedAnomalyDetector(analysis.DefaultAdvancedAnomalyConfig())
	advancedAnomalies := advancedDetector.DetectAllAdvanced(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "AdvancedAnomalyDetector.DetectAllAdvanced", Duration: time.Since(start)})
	algorithmResults["AdvancedAnomalyDetector"] = advancedAnomalies

	start = time.Now()
	srDetector := analysis.NewSpectralResidualDetector(analysis.DefaultSRConfig())
	srAnomalies := srDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "SpectralResidualDetector", Duration: time.Since(start)})
	algorithmResults["SpectralResidualDetector"] = srAnomalies

	start = time.Now()
	msDetector := analysis.NewMultiScaleAnomalyDetector(analysis.DefaultMultiScaleConfig())
	msAnomalies := msDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "MultiScaleAnomalyDetector", Duration: time.Since(start)})
	algorithmResults["MultiScaleDetector"] = msAnomalies

	start = time.Now()
	dbDetector := analysis.NewDBSCANAnomalyDetector(analysis.DefaultDBSCANConfig())
	dbAnomalies := dbDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "DBSCANAnomalyDetector", Duration: time.Since(start)})
	algorithmResults["DBSCANDetector"] = dbAnomalies

	start = time.Now()
	hwDetector := analysis.NewHoltWintersDetector(analysis.DefaultHoltWintersConfig())
	hwAnomalies := hwDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "HoltWintersDetector", Duration: time.Since(start)})
	algorithmResults["HoltWintersDetector"] = hwAnomalies

	start = time.Now()
	ifDetector := analysis.NewIsolationForest(analysis.DefaultIsolationForestConfig())
	ifAnomalies := ifDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "IsolationForest", Duration: time.Since(start)})
	algorithmResults["IsolationForest"] = ifAnomalies

	start = time.Now()
	shesdDetector := analysis.NewSHESDDetector(analysis.DefaultSHESDConfig())
	shesdAnomalies := shesdDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "SHESDDetector", Duration: time.Since(start)})
	algorithmResults["SHESDDetector"] = shesdAnomalies

	start = time.Now()
	ensembleDetector := analysis.NewEnsembleAnomalyDetector(analysis.DefaultEnsembleAnomalyConfig())
	ensembleAnomalies := ensembleDetector.DetectAll(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "EnsembleAnomalyDetector", Duration: time.Since(start)})
	algorithmResults["EnsembleDetector"] = ensembleAnomalies

	start = time.Now()
	dtDetector := analysis.NewDynamicThresholdDetector(analysis.DefaultDynamicThresholdConfig())
	_, dtAnomalies := dtDetector.ComputeThresholds(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "DynamicThresholdDetector", Duration: time.Since(start)})
	algorithmResults["DynamicThresholdDetector"] = dtAnomalies

	start = time.Now()
	ctxDetector := analysis.NewContextualAnomalyDetector(analysis.DefaultContextualAnomalyConfig())
	ctxAnomalies := ctxDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "ContextualAnomalyDetector", Duration: time.Since(start)})
	algorithmResults["ContextualDetector"] = ctxAnomalies

	start = time.Now()
	peakDetector := analysis.NewPeakDetector(analysis.DefaultPeakDetectorConfig())
	peakAnomalies := peakDetector.DetectPeakAnomalies(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "PeakDetector", Duration: time.Since(start)})
	algorithmResults["PeakDetector"] = peakAnomalies

	sampleIntervalSec := estimateMedianSampleInterval(data.qpsData)

	mergeConfig := analysis.DefaultAnomalyMergerConfig()
	mergeConfig.BaseWindowSeconds = int(clampInt64(sampleIntervalSec*4, 120, 1800))
	mergeConfig.MinWindowSeconds = int(clampInt64(sampleIntervalSec, 60, 600))
	mergeConfig.MaxWindowSeconds = int(clampInt64(sampleIntervalSec*12, 600, 7200))
	mergeConfig.MinConfidence = 0.25

	start = time.Now()
	merged := analysis.NewAnomalyMerger(mergeConfig).Merge(algorithmResults)
	timings = append(timings, AlgorithmTiming{Name: "AnomalyMerger.Merge", Duration: time.Since(start)})

	mergeGapSec := calculateDynamicEventGap(merged.Anomalies, sampleIntervalSec)
	mergedAnomalies := compactContiguousAnomalies(merged.Anomalies, mergeGapSec, sampleIntervalSec)
	mergedAnomalies = mergeNearbyEventsByCadence(mergedAnomalies, sampleIntervalSec)

	result := &DigAbnormalResult{
		ClusterID: clusterID,
		Anomalies: mergedAnomalies,
		Summary:   &merged.Summary,
		Timings:   timings,
	}

	if jsonOutput {
		printAbnormalJSON(result)
	} else {
		printAbnormalResult(result)
	}

	return nil
}

func loadMetricTimeSeries(storage *prometheus_storage.PrometheusStorage, clusterID, metricName string, startTS, endTS int64) ([]analysis.TimeSeriesPoint, error) {
	files, err := storage.ListMetricFiles(clusterID, metricName)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no data files found for metric %s", metricName)
	}

	if isCounterMetric(metricName) {
		return loadCounterMetricAsRate(files, startTS, endTS)
	}

	dataByTs := make(map[int64]float64)
	for _, file := range files {
		err := parsePromFileForTimeSeries(file, startTS, endTS, dataByTs)
		if err != nil {
			continue
		}
	}

	var result []analysis.TimeSeriesPoint
	for ts, val := range dataByTs {
		result = append(result, analysis.TimeSeriesPoint{
			Timestamp: ts,
			Value:     val,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result, nil
}

func estimateMedianSampleInterval(values []analysis.TimeSeriesPoint) int64 {
	if len(values) < 2 {
		return 300
	}
	sorted := make([]analysis.TimeSeriesPoint, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})
	var diffs []int64
	for i := 1; i < len(sorted); i++ {
		d := sorted[i].Timestamp - sorted[i-1].Timestamp
		if d > 0 {
			diffs = append(diffs, d)
		}
	}
	if len(diffs) == 0 {
		return 300
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i] < diffs[j] })
	median := diffs[len(diffs)/2]
	if median < 60 {
		return 60
	}
	if median > 900 {
		return 900
	}
	return median
}

func compactContiguousAnomalies(anomalies []analysis.DetectedAnomaly, mergeGapSec, sampleIntervalSec int64) []analysis.DetectedAnomaly {
	if len(anomalies) == 0 {
		return anomalies
	}
	if sampleIntervalSec <= 0 {
		sampleIntervalSec = 60
	}
	if mergeGapSec < 120 {
		mergeGapSec = 120
	}
	if mergeGapSec > 1800 {
		mergeGapSec = 1800
	}

	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].Timestamp < anomalies[j].Timestamp
	})

	var result []analysis.DetectedAnomaly
	current := anomalies[0]
	if current.EndTime == 0 {
		current.EndTime = current.Timestamp
	}
	typeSet := map[analysis.AnomalyType]bool{current.Type: true}
	detectorSet := make(map[string]bool)
	for _, d := range current.DetectedBy {
		detectorSet[d] = true
	}
	coverageSec := int64(calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec))
	mergedCount := 1

	mergeOne := func(idx int, a analysis.DetectedAnomaly) {
		nextEnd := a.EndTime
		if nextEnd == 0 {
			nextEnd = a.Timestamp
		}
		localWindow := calculateAdaptiveMergeWindow(anomalies, idx, mergeGapSec, sampleIntervalSec, current)
		nextDurationSec := int64(calculateEventDurationSec(a.Timestamp, nextEnd, sampleIntervalSec))
		proposedEnd := maxInt64(current.EndTime, nextEnd)
		proposedSpanSec := proposedEnd - current.Timestamp + sampleIntervalSec
		proposedCoverageSec := coverageSec + nextDurationSec
		gapSec := a.Timestamp - current.EndTime
		if shouldMergeIntoCurrent(current, a, localWindow, sampleIntervalSec, detectorSet, typeSet) &&
			shouldContinueCompactMerge(current, a, gapSec, proposedSpanSec, proposedCoverageSec, sampleIntervalSec, mergeGapSec, mergedCount) {
			if nextEnd > current.EndTime {
				current.EndTime = nextEnd
			}
			current.Duration = int(current.EndTime - current.Timestamp)
			typeSet[a.Type] = true
			if a.Severity == analysis.SeverityCritical ||
				(a.Severity == analysis.SeverityHigh && current.Severity != analysis.SeverityCritical) ||
				(a.Severity == analysis.SeverityMedium && current.Severity == analysis.SeverityLow) {
				current.Severity = a.Severity
			}
			if a.Confidence > current.Confidence {
				current.Confidence = a.Confidence
			}
			if absFloat(a.ZScore) > absFloat(current.ZScore) {
				current.ZScore = a.ZScore
				current.Value = a.Value
				current.Baseline = a.Baseline
			}
			for _, d := range a.DetectedBy {
				detectorSet[d] = true
			}
			coverageSec = proposedCoverageSec
			mergedCount++
			return
		}

		finalizeCurrent := func() {
			current.DetectedBy = setToSortedList(detectorSet)
			if len(typeSet) > 1 {
				current.Type = analysis.AnomalyQPSSpike
			}
			current.Duration = calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec)
			current.Detail = fmt.Sprintf("Merged anomaly event (%d signals, %s)", len(current.DetectedBy), formatAnomalyDuration(current.Duration))
			result = append(result, current)
		}

		finalizeCurrent()
		current = a
		if current.EndTime == 0 {
			current.EndTime = current.Timestamp
		}
		typeSet = map[analysis.AnomalyType]bool{current.Type: true}
		detectorSet = make(map[string]bool)
		for _, d := range current.DetectedBy {
			detectorSet[d] = true
		}
		coverageSec = int64(calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec))
		mergedCount = 1
	}

	for i := 1; i < len(anomalies); i++ {
		mergeOne(i, anomalies[i])
	}

	current.DetectedBy = setToSortedList(detectorSet)
	if len(typeSet) > 1 {
		current.Type = analysis.AnomalyQPSSpike
	}
	current.Duration = calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec)
	current.Detail = fmt.Sprintf("Merged anomaly event (%d signals, %s)", len(current.DetectedBy), formatAnomalyDuration(current.Duration))
	result = append(result, current)

	return result
}

func calculateEventDurationSec(startTS, endTS, sampleIntervalSec int64) int {
	if sampleIntervalSec <= 0 {
		sampleIntervalSec = 60
	}
	if endTS < startTS {
		endTS = startTS
	}
	// Inclusive duration so single-point anomaly still has a visible duration.
	return int((endTS - startTS) + sampleIntervalSec)
}

func calculateDynamicEventGap(anomalies []analysis.DetectedAnomaly, sampleIntervalSec int64) int64 {
	if sampleIntervalSec <= 0 {
		sampleIntervalSec = 60
	}
	if len(anomalies) < 2 {
		return clampInt64(sampleIntervalSec*2, 120, 1800)
	}
	sorted := make([]analysis.DetectedAnomaly, len(anomalies))
	copy(sorted, anomalies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	var gaps []int64
	for i := 1; i < len(sorted); i++ {
		g := sorted[i].Timestamp - sorted[i-1].Timestamp
		if g > 0 {
			gaps = append(gaps, g)
		}
	}
	if len(gaps) == 0 {
		return clampInt64(sampleIntervalSec*2, 120, 1800)
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	median := gaps[len(gaps)/2]
	p75 := gaps[(len(gaps)*3)/4]
	candidate := (median + p75) / 2
	minGap := sampleIntervalSec * 2
	maxGap := sampleIntervalSec * 10
	return clampInt64(candidate, clampInt64(minGap, 120, 1800), clampInt64(maxGap, 240, 1800))
}

func calculateAdaptiveMergeWindow(
	sortedAnomalies []analysis.DetectedAnomaly,
	idx int,
	baseGapSec, sampleIntervalSec int64,
	current analysis.DetectedAnomaly,
) int64 {
	if sampleIntervalSec <= 0 {
		sampleIntervalSec = 60
	}
	if baseGapSec <= 0 {
		baseGapSec = sampleIntervalSec * 2
	}

	localGap := estimateLocalGap(sortedAnomalies, idx, baseGapSec)
	window := clampInt64(maxInt64(baseGapSec, localGap), sampleIntervalSec*2, sampleIntervalSec*20)

	currentDuration := current.EndTime - current.Timestamp
	if currentDuration >= sampleIntervalSec*8 {
		window = clampInt64(window+sampleIntervalSec*2, sampleIntervalSec*2, 1800)
	}

	return clampInt64(window, 120, 1800)
}

func estimateLocalGap(sortedAnomalies []analysis.DetectedAnomaly, idx int, fallback int64) int64 {
	var gaps []int64
	if idx > 0 {
		if g := sortedAnomalies[idx].Timestamp - sortedAnomalies[idx-1].Timestamp; g > 0 {
			gaps = append(gaps, g)
		}
	}
	if idx+1 < len(sortedAnomalies) {
		if g := sortedAnomalies[idx+1].Timestamp - sortedAnomalies[idx].Timestamp; g > 0 {
			gaps = append(gaps, g)
		}
	}
	if idx > 1 {
		if g := sortedAnomalies[idx-1].Timestamp - sortedAnomalies[idx-2].Timestamp; g > 0 {
			gaps = append(gaps, g)
		}
	}
	// Need at least two local gap samples; otherwise keep conservative fallback.
	if len(gaps) < 2 {
		return fallback
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	return gaps[len(gaps)/2]
}

func shouldMergeIntoCurrent(
	current, next analysis.DetectedAnomaly,
	windowSec int64,
	sampleIntervalSec int64,
	currentDetectors map[string]bool,
	currentTypes map[analysis.AnomalyType]bool,
) bool {
	if sampleIntervalSec <= 0 {
		sampleIntervalSec = 60
	}
	if next.Timestamp > current.EndTime+windowSec {
		return false
	}
	// Always merge very close points.
	if next.Timestamp <= current.EndTime+sampleIntervalSec*2 {
		return true
	}
	typeOverlap := hasTypeFamilyOverlap(currentTypes, next.Type)
	detectorOverlap := false
	for _, d := range next.DetectedBy {
		if currentDetectors[d] {
			detectorOverlap = true
			break
		}
	}
	if detectorOverlap {
		return true
	}
	// Without detector overlap, require stricter type match.
	return typeOverlap && current.Type == next.Type
}

func shouldContinueCompactMerge(
	current, next analysis.DetectedAnomaly,
	gapSec int64,
	proposedSpanSec int64,
	proposedCoverageSec int64,
	sampleIntervalSec int64,
	mergeGapSec int64,
	mergedCount int,
) bool {
	if gapSec <= sampleIntervalSec*2 {
		return true
	}
	if proposedSpanSec <= sampleIntervalSec*12 {
		return true
	}
	if proposedSpanSec <= 0 {
		return false
	}

	maxSpanSec := clampInt64(maxInt64(mergeGapSec*30, 3*3600), 3*3600, 6*3600)
	currentDurationSec := int64(calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec))
	nextDurationSec := int64(calculateEventDurationSec(next.Timestamp, next.EndTime, sampleIntervalSec))
	if currentDurationSec >= 20*60 || nextDurationSec >= 20*60 {
		maxSpanSec = clampInt64(maxSpanSec+60*60, 3*3600, 8*3600)
	}

	density := float64(proposedCoverageSec) / float64(proposedSpanSec)
	minDensity := 0.20
	switch {
	case proposedSpanSec >= 3*3600:
		minDensity = 0.30
	case proposedSpanSec >= 2*3600:
		minDensity = 0.26
	case proposedSpanSec >= 90*60:
		minDensity = 0.22
	}
	if currentDurationSec >= 20*60 || nextDurationSec >= 20*60 {
		minDensity -= 0.04
	}
	if mergedCount >= 6 {
		minDensity += 0.02
	}
	minDensity = math.Min(0.40, math.Max(0.14, minDensity))

	if proposedSpanSec > maxSpanSec {
		// Soft cap: keep merging for dense incidents when idle gap is tiny.
		hardMaxSpanSec := clampInt64(maxSpanSec*2, 4*3600, 18*3600)
		if proposedSpanSec > hardMaxSpanSec {
			return false
		}
		softGapSec := clampInt64(maxInt64(sampleIntervalSec*6, mergeGapSec/2), sampleIntervalSec*3, 20*60)
		if gapSec > softGapSec {
			return false
		}
		return density >= math.Min(0.45, minDensity+0.08)
	}

	return density >= minDensity
}

func hasTypeFamilyOverlap(typeSet map[analysis.AnomalyType]bool, t analysis.AnomalyType) bool {
	if typeSet[t] {
		return true
	}
	isQPSFamily := func(x analysis.AnomalyType) bool {
		return x == analysis.AnomalyQPSSpike ||
			x == analysis.AnomalyQPSDrop ||
			x == analysis.AnomalySustainedHighQPS ||
			x == analysis.AnomalySustainedLowQPS
	}
	if isQPSFamily(t) {
		for k := range typeSet {
			if isQPSFamily(k) {
				return true
			}
		}
	}
	return false
}

func clampInt64(v, minV, maxV int64) int64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func mergeNearbyEventsByCadence(anomalies []analysis.DetectedAnomaly, sampleIntervalSec int64) []analysis.DetectedAnomaly {
	if len(anomalies) <= 1 {
		return anomalies
	}
	if sampleIntervalSec <= 0 {
		sampleIntervalSec = 60
	}

	sorted := make([]analysis.DetectedAnomaly, len(anomalies))
	copy(sorted, anomalies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	cadenceGap := estimateCadenceGap(sorted, sampleIntervalSec)

	var out []analysis.DetectedAnomaly
	current := sorted[0]
	if current.EndTime == 0 {
		current.EndTime = current.Timestamp
	}
	coverageSec := int64(calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec))
	mergedCount := 1

	for i := 1; i < len(sorted); i++ {
		next := sorted[i]
		if next.EndTime == 0 {
			next.EndTime = next.Timestamp
		}
		gap := next.Timestamp - current.EndTime
		localCadence := estimateLocalCadenceGap(sorted, i, cadenceGap, sampleIntervalSec)
		localMergeWindow := calculateCadenceMergeWindow(current, next, localCadence, sampleIntervalSec, mergedCount)
		nextDurationSec := int64(calculateEventDurationSec(next.Timestamp, next.EndTime, sampleIntervalSec))
		proposedEnd := maxInt64(current.EndTime, next.EndTime)
		proposedSpanSec := proposedEnd - current.Timestamp + sampleIntervalSec
		proposedCoverageSec := coverageSec + nextDurationSec

		if gap >= 0 &&
			gap <= localMergeWindow &&
			isCompatibleEvent(current, next) &&
			shouldKeepMergingByDensity(current, next, gap, proposedSpanSec, proposedCoverageSec, sampleIntervalSec, mergedCount, localCadence) {
			if next.EndTime > current.EndTime {
				current.EndTime = next.EndTime
			}
			current.Duration = calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec)
			if next.Severity == analysis.SeverityCritical ||
				(next.Severity == analysis.SeverityHigh && current.Severity != analysis.SeverityCritical) ||
				(next.Severity == analysis.SeverityMedium && current.Severity == analysis.SeverityLow) {
				current.Severity = next.Severity
			}
			if next.Confidence > current.Confidence {
				current.Confidence = next.Confidence
			}
			if absFloat(next.ZScore) > absFloat(current.ZScore) {
				current.ZScore = next.ZScore
				current.Value = next.Value
				current.Baseline = next.Baseline
			}
			current.DetectedBy = unionDetectors(current.DetectedBy, next.DetectedBy)
			current.Detail = fmt.Sprintf("Merged anomaly event (%d signals, %s)", len(current.DetectedBy), formatAnomalyDuration(current.Duration))
			coverageSec = proposedCoverageSec
			mergedCount++
			continue
		}
		current.Duration = calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec)
		current.Detail = fmt.Sprintf("Merged anomaly event (%d signals, %s)", len(current.DetectedBy), formatAnomalyDuration(current.Duration))
		out = append(out, current)
		current = next
		coverageSec = nextDurationSec
		mergedCount = 1
	}
	current.Duration = calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec)
	current.Detail = fmt.Sprintf("Merged anomaly event (%d signals, %s)", len(current.DetectedBy), formatAnomalyDuration(current.Duration))
	out = append(out, current)
	return out
}

func estimateCadenceGap(anomalies []analysis.DetectedAnomaly, sampleIntervalSec int64) int64 {
	var gaps []int64
	for i := 1; i < len(anomalies); i++ {
		if !isCompatibleEvent(anomalies[i-1], anomalies[i]) {
			continue
		}
		g := anomalies[i].Timestamp - anomalies[i-1].Timestamp
		if g > 0 {
			gaps = append(gaps, g)
		}
	}
	if len(gaps) == 0 {
		return clampInt64(sampleIntervalSec*6, sampleIntervalSec*3, 1200)
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	median := gaps[len(gaps)/2]
	return clampInt64(median, sampleIntervalSec*3, 1200)
}

func estimateLocalCadenceGap(
	anomalies []analysis.DetectedAnomaly,
	idx int,
	fallbackCadenceSec int64,
	sampleIntervalSec int64,
) int64 {
	var gaps []int64
	collectGap := func(left, right int) {
		if left < 0 || right >= len(anomalies) || left >= right {
			return
		}
		if !isCompatibleEvent(anomalies[left], anomalies[right]) {
			return
		}
		g := anomalies[right].Timestamp - anomalies[left].Timestamp
		if g > 0 {
			gaps = append(gaps, g)
		}
	}
	collectGap(idx-1, idx)
	collectGap(idx-2, idx-1)
	collectGap(idx, idx+1)
	collectGap(idx-3, idx-2)

	if len(gaps) == 0 {
		return clampInt64(fallbackCadenceSec, sampleIntervalSec*2, 1200)
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	return clampInt64(gaps[len(gaps)/2], sampleIntervalSec*2, 1200)
}

func calculateCadenceMergeWindow(
	current, next analysis.DetectedAnomaly,
	localCadenceSec int64,
	sampleIntervalSec int64,
	mergedCount int,
) int64 {
	windowSec := maxInt64(sampleIntervalSec*3, localCadenceSec*3/2)
	currentDurationSec := int64(calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec))
	nextDurationSec := int64(calculateEventDurationSec(next.Timestamp, next.EndTime, sampleIntervalSec))
	if currentDurationSec >= 15*60 || nextDurationSec >= 15*60 {
		windowSec += sampleIntervalSec * 2
	}
	if mergedCount >= 6 {
		windowSec = minInt64(windowSec, localCadenceSec+sampleIntervalSec*2)
	}
	return clampInt64(windowSec, sampleIntervalSec*2, 1800)
}

func shouldKeepMergingByDensity(
	current, next analysis.DetectedAnomaly,
	gapSec int64,
	proposedSpanSec int64,
	proposedCoverageSec int64,
	sampleIntervalSec int64,
	mergedCount int,
	localCadenceSec int64,
) bool {
	if gapSec <= sampleIntervalSec*2 {
		return true
	}
	if proposedSpanSec <= sampleIntervalSec*12 {
		return true
	}
	if proposedSpanSec <= 0 {
		return false
	}

	density := float64(proposedCoverageSec) / float64(proposedSpanSec)
	minDensity := 0.16
	switch {
	case proposedSpanSec >= 3*3600:
		minDensity = 0.28
	case proposedSpanSec >= 90*60:
		minDensity = 0.24
	case proposedSpanSec >= 30*60:
		minDensity = 0.20
	}

	currentDurationSec := int64(calculateEventDurationSec(current.Timestamp, current.EndTime, sampleIntervalSec))
	nextDurationSec := int64(calculateEventDurationSec(next.Timestamp, next.EndTime, sampleIntervalSec))
	maxSpanSec := clampInt64(localCadenceSec*20, 3600, 4*3600)
	if currentDurationSec >= 20*60 || nextDurationSec >= 20*60 {
		maxSpanSec = clampInt64(maxSpanSec+30*60, 3600, 6*3600)
	}
	if proposedSpanSec > maxSpanSec {
		return false
	}

	if currentDurationSec >= 15*60 || nextDurationSec >= 15*60 {
		minDensity -= 0.04
	}
	if mergedCount >= 6 {
		minDensity += 0.02
	}
	minDensity = math.Min(0.35, math.Max(0.12, minDensity))

	if proposedSpanSec > maxSpanSec {
		// Soft cap for dense, near-continuous incidents; hard cap still protects against runaway chains.
		hardMaxSpanSec := clampInt64(maxSpanSec*2, 4*3600, 18*3600)
		if proposedSpanSec > hardMaxSpanSec {
			return false
		}
		softGapSec := clampInt64(maxInt64(sampleIntervalSec*6, localCadenceSec/2), sampleIntervalSec*3, 20*60)
		if gapSec > softGapSec {
			return false
		}
		return density >= math.Min(0.45, minDensity+0.08)
	}

	return density >= minDensity
}

func isCompatibleEvent(a, b analysis.DetectedAnomaly) bool {
	// Prefer merging same type-family events.
	typeSet := map[analysis.AnomalyType]bool{a.Type: true}
	if !hasTypeFamilyOverlap(typeSet, b.Type) {
		return false
	}

	// Require detector overlap to avoid over-merging unrelated anomalies.
	dset := make(map[string]bool)
	for _, d := range a.DetectedBy {
		dset[d] = true
	}
	for _, d := range b.DetectedBy {
		if dset[d] {
			return true
		}
	}
	return false
}

func unionDetectors(a, b []string) []string {
	set := make(map[string]bool)
	for _, d := range a {
		set[d] = true
	}
	for _, d := range b {
		set[d] = true
	}
	return setToSortedList(set)
}

func setToSortedList(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func loadCounterMetricAsRate(files []string, startTS, endTS int64) ([]analysis.TimeSeriesPoint, error) {
	dataByLabelAndTs := make(map[string]map[int64]float64)

	for _, file := range files {
		err := parsePromFileForCounterWithLabels(file, startTS, endTS, dataByLabelAndTs)
		if err != nil {
			continue
		}
	}

	rateByTs := make(map[int64]float64)
	for _, tsToVal := range dataByLabelAndTs {
		rates := calculateRateFromMap(tsToVal)
		for ts, rate := range rates {
			rateByTs[ts] += rate
		}
	}

	var result []analysis.TimeSeriesPoint
	for ts, rate := range rateByTs {
		result = append(result, analysis.TimeSeriesPoint{
			Timestamp: ts,
			Value:     rate,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result, nil
}

func parsePromFileForCounterWithLabels(filePath string, startTS, endTS int64, dataByLabelAndTs map[string]map[int64]float64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		tsStr := parts[len(parts)-1]
		valueStr := parts[len(parts)-2]

		var ts int64
		if _, err := fmt.Sscanf(tsStr, "%d", &ts); err != nil {
			continue
		}

		if ts > 1e12 {
			ts = ts / 1000
		}

		if ts < startTS || ts > endTS {
			continue
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			continue
		}

		labelHash := extractLabelHash(line)
		if dataByLabelAndTs[labelHash] == nil {
			dataByLabelAndTs[labelHash] = make(map[int64]float64)
		}
		dataByLabelAndTs[labelHash][ts] = value
	}

	return scanner.Err()
}

func extractLabelHash(line string) string {
	start := strings.Index(line, "{")
	end := strings.LastIndex(line, "}")
	if start == -1 || end == -1 || end <= start {
		return "default"
	}
	return line[start+1 : end]
}

func calculateRateFromMap(tsToVal map[int64]float64) map[int64]float64 {
	if len(tsToVal) < 2 {
		return nil
	}

	type tsVal struct {
		ts int64
		v  float64
	}
	var sorted []tsVal
	for ts, v := range tsToVal {
		sorted = append(sorted, tsVal{ts, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ts < sorted[j].ts
	})

	result := make(map[int64]float64)
	for i := 1; i < len(sorted); i++ {
		tsDiff := sorted[i].ts - sorted[i-1].ts
		if tsDiff <= 0 {
			continue
		}

		valDiff := sorted[i].v - sorted[i-1].v
		if valDiff < 0 {
			valDiff = sorted[i].v
		}

		rate := valDiff / float64(tsDiff)
		result[sorted[i].ts] = rate
	}

	return result
}

func isCounterMetric(metricName string) bool {
	counterMetrics := []string{
		"tidb_server_query_total",
		"tidb_executor_statement_total",
		"tikv_grpc_msg_duration_seconds_count",
	}
	for _, m := range counterMetrics {
		if metricName == m {
			return true
		}
	}
	return false
}

func calculateRate(data []analysis.TimeSeriesPoint) []analysis.TimeSeriesPoint {
	if len(data) < 2 {
		return nil
	}

	var result []analysis.TimeSeriesPoint
	for i := 1; i < len(data); i++ {
		tsDiff := data[i].Timestamp - data[i-1].Timestamp
		if tsDiff <= 0 {
			continue
		}

		valDiff := data[i].Value - data[i-1].Value
		if valDiff < 0 {
			valDiff = data[i].Value
		}

		rate := valDiff / float64(tsDiff)
		result = append(result, analysis.TimeSeriesPoint{
			Timestamp: data[i].Timestamp,
			Value:     rate,
		})
	}

	return result
}

func loadMetricHistogramP99(storage *prometheus_storage.PrometheusStorage, clusterID, metricName string, startTS, endTS int64) ([]analysis.TimeSeriesPoint, error) {
	files, err := storage.ListMetricFiles(clusterID, metricName)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no data files found for metric %s", metricName)
	}

	dataByTs := make(map[int64]map[string]float64)

	for _, file := range files {
		err := parsePromFileForHistogram(file, startTS, endTS, dataByTs)
		if err != nil {
			continue
		}
	}

	// Histogram buckets are cumulative counters, so convert them to per-interval deltas
	// before calculating quantiles.
	ratesByTs := calcHistogramCounterRates(dataByTs)

	var result []analysis.TimeSeriesPoint
	for ts, buckets := range ratesByTs {
		p99 := analysis.HistogramQuantile(buckets, 0.99)
		if !isNaN(p99) {
			result = append(result, analysis.TimeSeriesPoint{
				Timestamp: ts,
				Value:     p99,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result, nil
}

func calcHistogramCounterRates(tsData map[int64]map[string]float64) map[int64]map[string]float64 {
	var sortedTS []int64
	for ts := range tsData {
		sortedTS = append(sortedTS, ts)
	}
	sort.Slice(sortedTS, func(i, j int) bool {
		return sortedTS[i] < sortedTS[j]
	})

	result := make(map[int64]map[string]float64)
	for i := 1; i < len(sortedTS); i++ {
		prevTS := sortedTS[i-1]
		currTS := sortedTS[i]

		prevBuckets := tsData[prevTS]
		currBuckets := tsData[currTS]

		delta := make(map[string]float64)
		for le, curr := range currBuckets {
			prev := prevBuckets[le]
			d := curr - prev
			if d >= 0 {
				delta[le] = d
			}
		}
		for le, curr := range currBuckets {
			if _, ok := prevBuckets[le]; !ok {
				delta[le] = curr
			}
		}

		if delta["+Inf"] > 0 {
			result[currTS] = delta
		}
	}

	return result
}

func isNaN(f float64) bool {
	return f != f
}

func parsePromFileForTimeSeries(filePath string, startTS, endTS int64, dataByTs map[int64]float64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		tsStr := parts[len(parts)-1]
		valueStr := parts[len(parts)-2]

		var ts int64
		if _, err := fmt.Sscanf(tsStr, "%d", &ts); err != nil {
			continue
		}

		if ts > 1e12 {
			ts = ts / 1000
		}

		if ts < startTS || ts > endTS {
			continue
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			continue
		}

		dataByTs[ts] += value
	}

	return scanner.Err()
}

func parsePromFileForHistogram(filePath string, startTS, endTS int64, dataByTs map[int64]map[string]float64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		tsStr := parts[len(parts)-1]
		valueStr := parts[len(parts)-2]

		var ts int64
		if _, err := fmt.Sscanf(tsStr, "%d", &ts); err != nil {
			continue
		}

		if ts > 1e12 {
			ts = ts / 1000
		}

		if ts < startTS || ts > endTS {
			continue
		}

		le := ""
		if idx := strings.Index(line, "le=\""); idx >= 0 {
			endIdx := strings.Index(line[idx+4:], "\"")
			if endIdx >= 0 {
				le = line[idx+4 : idx+4+endIdx]
			}
		}

		if le == "" {
			continue
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			continue
		}

		if dataByTs[ts] == nil {
			dataByTs[ts] = make(map[string]float64)
		}
		dataByTs[ts][le] += value
	}

	return scanner.Err()
}

func printProfileJSON(result *DigProfileResult) {
	output := map[string]interface{}{
		"cluster_id":   result.ClusterID,
		"load_profile": result.Profile,
		"timings":      formatTimings(result.Timings),
	}
	fmt.Println(mustMarshalJSON(output))
}

func printAbnormalJSON(result *DigAbnormalResult) {
	output := map[string]interface{}{
		"cluster_id": result.ClusterID,
		"anomalies":  result.Anomalies,
		"summary":    result.Summary,
		"timings":    formatTimings(result.Timings),
	}
	fmt.Println(mustMarshalJSON(output))
}

func formatTimings(timings []AlgorithmTiming) []map[string]interface{} {
	result := make([]map[string]interface{}, len(timings))
	for i, t := range timings {
		result[i] = map[string]interface{}{
			"algorithm": t.Name,
			"duration":  t.Duration.String(),
		}
	}
	return result
}

func mustMarshalJSON(v interface{}) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	return string(data)
}

func printProfileResult(result *DigProfileResult) {
	analysis.PrintLoadProfile(result.Profile, false)
	printSummaryWithTimings(result.ClusterID, result.Timings)
}

func printAbnormalResult(result *DigAbnormalResult) {
	if len(result.Anomalies) > 0 {
		fmt.Println()
		fmt.Println("ANOMALY DETECTION RESULTS")
		fmt.Println("=========================")
		fmt.Println()

		for _, a := range result.Anomalies {
			printAnomalyLine(a)
		}
	} else {
		fmt.Println()
		fmt.Println("No significant anomalies detected.")
	}
	if result.Summary != nil {
		fmt.Println()
		fmt.Printf("Merged anomaly events: %d (window=%ds)\n", result.Summary.TotalAnomalies, result.Summary.MergeWindowSec)
	}
	printSummaryWithTimings(result.ClusterID, result.Timings)
}

func printSummaryWithTimings(clusterID string, timings []AlgorithmTiming) {
	fmt.Println()
	fmt.Println(stringsRepeat("=", 60))
	fmt.Println("PROFILING PERFORMANCE")
	fmt.Println(stringsRepeat("=", 60))
	fmt.Printf("  Cluster ID:    %s\n", clusterID)

	if len(timings) > 0 {
		fmt.Println()
		fmt.Println("  Algorithm Timings:")
		maxNameLen := 0
		for _, t := range timings {
			if len(t.Name) > maxNameLen {
				maxNameLen = len(t.Name)
			}
		}
		for _, t := range timings {
			fmt.Printf("    %-*s  %s\n", maxNameLen, t.Name, formatDurationTime(t.Duration))
		}
	}
	fmt.Println()
}

func formatDurationTime(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	} else if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(time.Millisecond).String()
}

func printAnomalyLine(a analysis.DetectedAnomaly) {
	severity := formatSeverityColored(a.Severity)
	timeRange := formatTimeRange(a)
	change := formatChange(a.Value, a.Baseline)
	confBar := formatConfidenceBar(a.Confidence)
	algos := strings.Join(a.DetectedBy, ",")

	duration := ""
	if a.Duration > 0 {
		duration = " (" + formatAnomalyDuration(int(a.Duration)) + ")"
	}

	fmt.Printf("%-10s %s  %-25s %-12s %s %s%s\n",
		severity, timeRange, a.Type, change, confBar, algos, duration)
}

func formatSeverityColored(s analysis.Severity) string {
	switch s {
	case analysis.SeverityCritical:
		return "[CRITICAL]"
	case analysis.SeverityHigh:
		return "[HIGH]    "
	case analysis.SeverityMedium:
		return "[MEDIUM]  "
	case analysis.SeverityLow:
		return "[LOW]     "
	default:
		return "[????]    "
	}
}

func formatTimeRange(a analysis.DetectedAnomaly) string {
	t := time.Unix(a.Timestamp, 0)
	timeStr := t.Format("15:04:05")

	if a.EndTime > 0 && a.EndTime > a.Timestamp {
		endStr := time.Unix(a.EndTime, 0).Format("15:04:05")
		return timeStr + "~" + endStr
	}
	return timeStr + "         "
}

func formatChange(value, baseline float64) string {
	if baseline == 0 {
		return "N/A"
	}

	change := ((value - baseline) / baseline) * 100
	if change >= 0 {
		return fmt.Sprintf("+%.0f%%", change)
	}
	return fmt.Sprintf("%.0f%%", change)
}

func formatConfidenceBar(conf float64) string {
	filled := int(conf * 5)
	if filled > 5 {
		filled = 5
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", 5-filled)
	return fmt.Sprintf("│%s %2.0f%%│", bar, conf*100)
}

func formatAnomalyDuration(seconds int) string {
	if seconds < 60 {
		return "<1m"
	}
	minutes := seconds / 60
	if minutes < 60 {
		return fmt.Sprintf("%dm", minutes)
	}
	hours := minutes / 60
	remainingMinutes := minutes % 60
	if remainingMinutes == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh%dm", hours, remainingMinutes)
}

func stringsRepeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}

func DigRandomProfile(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	startTS, endTS int64, jsonOutput bool) error {

	clusterID, err := selectRandomCluster(cacheDir)
	if err != nil {
		return err
	}

	fmt.Printf("Random cluster from cache: %s\n\n", clusterID)

	return DigProfile(cacheDir, metaDir, cp, maxBackoff, authMgr, config,
		clusterID, startTS, endTS, jsonOutput)
}

func DigRandomAbnormal(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	startTS, endTS int64, jsonOutput bool) error {

	clusterID, err := selectRandomCluster(cacheDir)
	if err != nil {
		return err
	}

	fmt.Printf("Random cluster from cache: %s\n\n", clusterID)

	return DigAbnormal(cacheDir, metaDir, cp, maxBackoff, authMgr, config,
		clusterID, startTS, endTS, jsonOutput)
}

func selectRandomCluster(cacheDir string) (string, error) {
	metricsDir := filepath.Join(cacheDir, "metrics")

	entries, err := os.ReadDir(metricsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no cached metrics found, please fetch metrics first")
		}
		return "", fmt.Errorf("failed to read metrics cache: %w", err)
	}

	var cachedClusters []string
	for _, entry := range entries {
		if entry.IsDir() {
			clusterDir := filepath.Join(metricsDir, entry.Name())
			metricEntries, err := os.ReadDir(clusterDir)
			if err != nil {
				continue
			}
			if len(metricEntries) > 0 {
				cachedClusters = append(cachedClusters, entry.Name())
			}
		}
	}

	if len(cachedClusters) == 0 {
		return "", fmt.Errorf("no cached clusters found, please fetch metrics first")
	}

	randSeed := time.Now().UnixNano()
	selectedIdx := randSeed % int64(len(cachedClusters))
	return cachedClusters[selectedIdx], nil
}

func DigWalkProfile(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	startTS, endTS int64) error {

	clusters, err := getActiveClusters(cacheDir, metaDir)
	if err != nil {
		return err
	}

	fmt.Printf("Walking through %d clusters (profile)\n\n", len(clusters))

	for i, c := range clusters {
		fmt.Printf("[%d/%d] Processing cluster %s (%s)\n", i+1, len(clusters), c.clusterID, c.bizType)

		err := DigProfile(cacheDir, metaDir, cp, maxBackoff, authMgr, config,
			c.clusterID, startTS, endTS, false)
		if err != nil {
			fmt.Printf("Error processing cluster %s: %v\n", c.clusterID, err)
		}
		fmt.Println("\n------------------------------------------------------------")
	}

	fmt.Println("Walk completed")
	return nil
}

func DigWalkAbnormal(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	startTS, endTS int64) error {

	clusters, err := getActiveClusters(cacheDir, metaDir)
	if err != nil {
		return err
	}

	fmt.Printf("Walking through %d clusters (abnormal)\n\n", len(clusters))

	for i, c := range clusters {
		fmt.Printf("[%d/%d] Processing cluster %s (%s)\n", i+1, len(clusters), c.clusterID, c.bizType)

		err := DigAbnormal(cacheDir, metaDir, cp, maxBackoff, authMgr, config,
			c.clusterID, startTS, endTS, false)
		if err != nil {
			fmt.Printf("Error processing cluster %s: %v\n", c.clusterID, err)
		}
		fmt.Println("\n------------------------------------------------------------")
	}

	fmt.Println("Walk completed")
	return nil
}

func getActiveClusters(cacheDir, metaDir string) ([]clusterInfo, error) {
	inactive := loadInactiveClusters(cacheDir)
	if len(inactive) > 0 {
		fmt.Printf("Excluding %d inactive clusters\n", len(inactive))
	}

	var allClusters []clusterInfo
	for _, bizType := range []string{"dedicated", "premium"} {
		clusters, err := loadClustersFromList(metaDir, bizType)
		if err != nil {
			continue
		}
		for _, c := range clusters {
			if !inactive[c.clusterID] {
				allClusters = append(allClusters, c)
			}
		}
	}

	if len(allClusters) == 0 {
		return nil, fmt.Errorf("no active clusters found")
	}

	return allClusters, nil
}

func DigLocal(cacheDir string, startTS, endTS int64, cacheID string, jsonOutput bool) {
	fmt.Println("DigLocal not implemented yet")
}
