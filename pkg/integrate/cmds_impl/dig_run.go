package impl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
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
	qpsData             []analysis.TimeSeriesPoint
	latencyData         []analysis.TimeSeriesPoint
	statementData       []analysis.TimeSeriesPoint
	tikvGRPCData        []analysis.TimeSeriesPoint
	tikvGRPCLatencyData []analysis.TimeSeriesPoint
}

func loadMetricsData(cacheDir, clusterID string, startTS, endTS int64) (*metricsData, error) {
	promStorage := prometheus_storage.NewPrometheusStorage(filepath.Join(cacheDir, "metrics"))

	qpsData, err := loadMetricTimeSeries(promStorage, clusterID, "tidb_server_query_total", startTS, endTS)
	if err != nil {
		return nil, fmt.Errorf("failed to load QPS data: %w", err)
	}
	// When there are too few QPS points, anomaly inference is unstable and
	// loading heavy histogram metrics is wasted work. Return early so caller can
	// treat it as insufficient-data case.
	if len(qpsData) < 80 {
		return &metricsData{qpsData: qpsData}, nil
	}

	latencyData, err := loadMetricHistogramP99(promStorage, clusterID, "tidb_server_handle_query_duration_seconds_bucket", startTS, endTS)
	if err != nil {
		fmt.Printf("Warning: failed to load latency data: %v\n", err)
	}

	statementData, err := loadMetricTimeSeries(promStorage, clusterID, "tidb_executor_statement_total", startTS, endTS)
	if err != nil {
		fmt.Printf("Warning: failed to load statement data: %v\n", err)
	}

	tikvGRPCData, err := loadMetricTimeSeries(promStorage, clusterID, "tikv_grpc_msg_duration_seconds_count", startTS, endTS)
	if err != nil {
		fmt.Printf("Warning: failed to load TiKV gRPC data: %v\n", err)
	}

	tikvGRPCLatencyData, err := loadMetricHistogramP99(promStorage, clusterID, "tikv_grpc_msg_duration_seconds_bucket", startTS, endTS)
	if err != nil {
		fmt.Printf("Warning: failed to load TiKV gRPC latency data: %v\n", err)
	}

	return &metricsData{
		qpsData:             qpsData,
		latencyData:         latencyData,
		statementData:       statementData,
		tikvGRPCData:        tikvGRPCData,
		tikvGRPCLatencyData: tikvGRPCLatencyData,
	}, nil
}

func loadMetricsDataWithFallback(cacheDir, clusterID string, startTS, endTS int64) (*metricsData, int64, int64, error) {
	data, err := loadMetricsData(cacheDir, clusterID, startTS, endTS)
	if err != nil {
		return nil, startTS, endTS, err
	}
	if len(data.qpsData) > 0 {
		return data, startTS, endTS, nil
	}
	fallbackStart, fallbackEnd, ok := inferLatestCachedMetricRange(cacheDir, clusterID, "tidb_server_query_total")
	if !ok || (fallbackStart == startTS && fallbackEnd == endTS) {
		return data, startTS, endTS, nil
	}
	fallbackData, err := loadMetricsData(cacheDir, clusterID, fallbackStart, fallbackEnd)
	if err != nil {
		return nil, startTS, endTS, err
	}
	return fallbackData, fallbackStart, fallbackEnd, nil
}

func DigProfile(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	clusterID string, startTS, endTS int64, jsonOutput bool) error {

	if isInactiveCluster(metaDir, clusterID) {
		return fmt.Errorf("cluster %s is inactive (no QPS data), see meta/inactive_clusters.txt", clusterID)
	}

	fmt.Printf("Profiling cluster %s\n", clusterID)
	fmt.Printf("Time range: %s ~ %s\n",
		time.Unix(startTS, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTS, 0).Format("2006-01-02 15:04:05"))

	fmt.Println("\nLoading metrics data...")
	data, usedStartTS, usedEndTS, err := loadMetricsDataWithFallback(cacheDir, clusterID, startTS, endTS)
	if err != nil {
		return err
	}
	if usedStartTS != startTS || usedEndTS != endTS {
		fmt.Printf("No QPS in requested range, fallback to cached range: %s ~ %s\n",
			time.Unix(usedStartTS, 0).Format("2006-01-02 15:04:05"),
			time.Unix(usedEndTS, 0).Format("2006-01-02 15:04:05"))
	}

	fmt.Printf("Loaded %d QPS data points\n", len(data.qpsData))
	if len(data.latencyData) > 0 {
		fmt.Printf("Loaded %d latency data points\n", len(data.latencyData))
	}

	if len(data.qpsData) == 0 {
		saveErr := saveInactiveCluster(metaDir, clusterID)
		if saveErr != nil {
			fmt.Printf("Warning: failed to save inactive cluster: %v\n", saveErr)
		}
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

	if isInactiveCluster(metaDir, clusterID) {
		return fmt.Errorf("cluster %s is inactive (no QPS data), see meta/inactive_clusters.txt", clusterID)
	}

	fmt.Printf("Detecting anomalies for cluster %s\n", clusterID)
	fmt.Printf("Time range: %s ~ %s\n",
		time.Unix(startTS, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTS, 0).Format("2006-01-02 15:04:05"))

	fmt.Println("\nLoading metrics data...")
	data, usedStartTS, usedEndTS, err := loadMetricsDataWithFallback(cacheDir, clusterID, startTS, endTS)
	if err != nil {
		return err
	}
	if usedStartTS != startTS || usedEndTS != endTS {
		fmt.Printf("No QPS in requested range, fallback to cached range: %s ~ %s\n",
			time.Unix(usedStartTS, 0).Format("2006-01-02 15:04:05"),
			time.Unix(usedEndTS, 0).Format("2006-01-02 15:04:05"))
	}

	fmt.Printf("Loaded %d QPS data points\n", len(data.qpsData))
	if len(data.latencyData) > 0 {
		fmt.Printf("Loaded %d latency data points\n", len(data.latencyData))
	}

	if len(data.qpsData) == 0 {
		saveErr := saveInactiveCluster(metaDir, clusterID)
		if saveErr != nil {
			fmt.Printf("Warning: failed to save inactive cluster: %v\n", saveErr)
		}
		return fmt.Errorf("no QPS data available for analysis")
	}
	if len(data.qpsData) < 80 {
		fmt.Println("\nNo significant anomalies detected (insufficient QPS samples).")
		fmt.Printf("\nMerged anomaly events: 0 (window=0s)\n")
		return nil
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

	start = time.Now()
	rawQPSAnomalies := detectRawRollingAnomalies(data.qpsData, analysis.AnomalyQPSSpike, "RawQPSDetector")
	timings = append(timings, AlgorithmTiming{Name: "RawQPSDetector", Duration: time.Since(start)})
	algorithmResults["RawQPSDetector"] = rawQPSAnomalies

	if len(data.latencyData) > 0 {
		start = time.Now()
		rawLatencyAnomalies := detectRawRollingAnomalies(data.latencyData, analysis.AnomalyLatencySpike, "RawLatencyDetector")
		timings = append(timings, AlgorithmTiming{Name: "RawLatencyDetector", Duration: time.Since(start)})
		algorithmResults["RawLatencyDetector"] = rawLatencyAnomalies

		start = time.Now()
		rawLatencySustained := detectSustainedLatencyDegradation(data.latencyData, "RawLatencySustainedDetector")
		timings = append(timings, AlgorithmTiming{Name: "RawLatencySustainedDetector", Duration: time.Since(start)})
		algorithmResults["RawLatencySustainedDetector"] = rawLatencySustained
	}

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
	mergedAnomalies = validateAndExplainAnomalies(mergedAnomalies, data, sampleIntervalSec)
	summary := merged.Summary
	summary.TotalAnomalies = len(mergedAnomalies)

	result := &DigAbnormalResult{
		ClusterID: clusterID,
		Anomalies: mergedAnomalies,
		Summary:   &summary,
		Timings:   timings,
	}

	if jsonOutput {
		printAbnormalJSON(result)
	} else {
		printAbnormalResult(result)
	}

	return nil
}

func inferLatestCachedMetricRange(cacheDir, clusterID, metricName string) (int64, int64, bool) {
	metricDir := filepath.Join(cacheDir, "metrics", clusterID, metricName)
	entries, err := os.ReadDir(metricDir)
	if err != nil {
		return 0, 0, false
	}
	var bestStart, bestEnd int64
	found := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".prom") {
			continue
		}
		start, end, ok := parsePromRangeFromFilename(e.Name())
		if !ok {
			continue
		}
		if !found || end > bestEnd || (end == bestEnd && start < bestStart) {
			bestStart = start
			bestEnd = end
			found = true
		}
	}
	return bestStart, bestEnd, found
}

func parsePromRangeFromFilename(name string) (int64, int64, bool) {
	trimmed := strings.TrimSuffix(name, ".prom")
	parts := strings.Split(trimmed, "_")
	if len(parts) < 4 {
		return 0, 0, false
	}
	startStr := parts[0] + "_" + parts[1]
	endStr := parts[2] + "_" + parts[3]
	start, err1 := time.Parse("2006-01-02_15-04-05", startStr)
	end, err2 := time.Parse("2006-01-02_15-04-05", endStr)
	if err1 != nil || err2 != nil || end.Before(start) {
		return 0, 0, false
	}
	return start.Unix(), end.Unix(), true
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
		err := parsePromFileForTimeSeries(file, metricName, startTS, endTS, dataByTs)
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

type windowStats struct {
	count   int
	mean    float64
	median  float64
	p90     float64
	p95     float64
	p99     float64
	max     float64
	min     float64
	zeroRat float64
}

type anomalyEvidence struct {
	reason      string
	confidence  float64
	keep        bool
	qpsSignal   bool
	latSignal   bool
	qpsBaseMed  float64
	qpsEventP95 float64
	latBaseP99  float64
	latEventP99 float64
	stmtCorr    float64
	tikvCorr    float64
}

// validateAndExplainAnomalies performs a source-data verification pass:
// 1) Treat detector output as candidates only.
// 2) Re-check each candidate against raw metric windows (QPS/latency + side signals).
// 3) Keep only events with measurable source-level evidence.
// This avoids trusting model output in low-baseline or data-poor scenarios.
func validateAndExplainAnomalies(
	anomalies []analysis.DetectedAnomaly,
	data *metricsData,
	sampleIntervalSec int64,
) []analysis.DetectedAnomaly {
	if len(anomalies) == 0 || data == nil {
		return anomalies
	}
	if sampleIntervalSec <= 0 {
		sampleIntervalSec = 120
	}

	var out []analysis.DetectedAnomaly
	for _, a := range anomalies {
		endTS := a.EndTime
		if endTS <= a.Timestamp {
			endTS = a.Timestamp
		}
		// Use a fixed-length baseline window before the event to compare local behavior,
		// so daily pattern and long-term drift do not dominate anomaly decisions.
		baseLenSec := int64(6 * 3600)
		if d := endTS - a.Timestamp; d > 0 && d < baseLenSec {
			baseLenSec = maxInt64(3600, d*3)
		}
		baseEnd := a.Timestamp - sampleIntervalSec
		baseStart := baseEnd - baseLenSec
		if baseEnd <= baseStart {
			baseStart = a.Timestamp - maxInt64(3600, sampleIntervalSec*30)
			baseEnd = a.Timestamp - sampleIntervalSec
		}

		qBase := calcWindowStats(data.qpsData, baseStart, baseEnd)
		qEvent := calcWindowStats(data.qpsData, a.Timestamp, endTS)
		edgeBaseStart := maxInt64(baseStart, a.Timestamp-1800)
		edgeBaseEnd := a.Timestamp - sampleIntervalSec
		edgeEventEnd := minInt64(endTS, a.Timestamp+1800)
		qEdgeBase := calcWindowStats(data.qpsData, edgeBaseStart, edgeBaseEnd)
		qEdgeEvent := calcWindowStats(data.qpsData, a.Timestamp, edgeEventEnd)
		latBase := calcWindowStats(data.latencyData, baseStart, baseEnd)
		latEvent := calcWindowStats(data.latencyData, a.Timestamp, endTS)
		tikvLatBase := calcWindowStats(data.tikvGRPCLatencyData, baseStart, baseEnd)
		tikvLatEvent := calcWindowStats(data.tikvGRPCLatencyData, a.Timestamp, endTS)

		stmtCorr := calcWindowCorrelation(data.qpsData, data.statementData, a.Timestamp, endTS)
		tikvCorr := calcWindowCorrelation(data.qpsData, data.tikvGRPCData, a.Timestamp, endTS)

		ev := evaluateAnomalyEvidence(
			qBase, qEvent, qEdgeBase, qEdgeEvent,
			latBase, latEvent, tikvLatBase, tikvLatEvent,
			stmtCorr, tikvCorr, endTS-a.Timestamp+sampleIntervalSec,
		)
		if !ev.keep {
			continue
		}
		latHighCoverage := math.NaN()
		// Long incidents are compared against the previous-day same window to
		// suppress recurring diurnal patterns (business-hour ramps/plateaus).
		// We only apply this to load-related labels to avoid hiding real
		// latency degradations that are independent of daily traffic shape.
		if endTS-a.Timestamp+sampleIntervalSec >= 2*3600 &&
			(ev.reason == "WORKLOAD_LINKED_SHIFT" || ev.reason == "LOAD_AND_LATENCY_COUPLED") {
			prevQ := calcWindowStats(data.qpsData, a.Timestamp-24*3600, endTS-24*3600)
			prevLat := calcWindowStats(data.latencyData, a.Timestamp-24*3600, endTS-24*3600)
			if isLikelyDiurnalRepeat(qEvent, prevQ, latEvent, prevLat) {
				continue
			}
			// Fallback guard: very long workload-only shifts without previous-day
			// evidence are typically low-confidence (window starts near cache head),
			// so prefer dropping them over reporting multi-hour uncertain incidents.
			if endTS-a.Timestamp+sampleIntervalSec >= 6*3600 &&
				ev.reason == "WORKLOAD_LINKED_SHIFT" &&
				prevQ.count < 3 {
				continue
			}
		}
		if ev.reason == "LATENCY_ONLY_DEGRADATION" || ev.reason == "BACKEND_LATENCY_DEGRADATION" {
			// For long latency incidents, require enough high-latency coverage and
			// tighten bounds to high-latency span so sparse fragments do not appear
			// as a continuous multi-hour degradation.
			highThreshold := maxFloat64(ev.latBaseP99*1.25, ev.latBaseP99+0.05)
			if highThreshold < 0.2 {
				highThreshold = 0.2
			}
			coverage, firstHighTS, lastHighTS := calculateHighValueCoverage(
				data.latencyData,
				a.Timestamp,
				endTS,
				highThreshold,
			)
			latHighCoverage = coverage
			if endTS-a.Timestamp+sampleIntervalSec >= 2*3600 && coverage < 0.42 {
				continue
			}
			if firstHighTS > 0 && lastHighTS >= firstHighTS {
				a.Timestamp = firstHighTS
				a.EndTime = lastHighTS
				a.Duration = calculateEventDurationSec(a.Timestamp, a.EndTime, sampleIntervalSec)
			}
		}

		a.Confidence = ev.confidence
		applyReasonToAnomalyMeasurement(&a, ev)
		applyReasonToAnomalyType(&a, ev.reason)
		a.Detail = fmt.Sprintf(
			"reason=%s qps_p95=%.3f base_qps_median=%.3f lat_p99=%.3fs base_lat_p99=%.3fs lat_high_coverage=%.2f corr(stmt)=%.2f corr(tikv)=%.2f",
			ev.reason, ev.qpsEventP95, ev.qpsBaseMed, ev.latEventP99, ev.latBaseP99, latHighCoverage, ev.stmtCorr, ev.tikvCorr)
		out = append(out, a)
	}
	return out
}

// applyReasonToAnomalyType aligns final event type with evidence reason so the
// reported anomaly category matches root-cause interpretation.
func applyReasonToAnomalyType(a *analysis.DetectedAnomaly, reason string) {
	if a == nil {
		return
	}
	switch reason {
	case "LATENCY_ONLY_DEGRADATION", "BACKEND_LATENCY_DEGRADATION":
		a.Type = analysis.AnomalyLatencyDegraded
	}
}

// applyReasonToAnomalyMeasurement rewrites Value/Baseline using evidence windows
// so output percentage reflects the same signal used for root-cause labeling.
func applyReasonToAnomalyMeasurement(a *analysis.DetectedAnomaly, ev anomalyEvidence) {
	if a == nil {
		return
	}
	switch ev.reason {
	case "LATENCY_ONLY_DEGRADATION", "BACKEND_LATENCY_DEGRADATION":
		// Only override when evidence shows clear worsening; otherwise keep
		// original detector measurement to avoid contradictory negative deltas.
		if ev.latEventP99 > 0 && ev.latBaseP99 > 0 && ev.latEventP99 > ev.latBaseP99*1.02 {
			a.Value = ev.latEventP99
			a.Baseline = ev.latBaseP99
		}
	default:
		if ev.qpsEventP95 > 0 {
			a.Value = ev.qpsEventP95
		}
		if ev.qpsBaseMed > 0 {
			a.Baseline = ev.qpsBaseMed
		}
	}
}

// isLikelyDiurnalRepeat detects whether current event level is close to the
// previous-day same-time window, which usually indicates recurring workload
// shape instead of a new operational incident.
func isLikelyDiurnalRepeat(currQ, prevQ, currLat, prevLat windowStats) bool {
	if currQ.count < 3 || prevQ.count < 3 {
		return false
	}
	qRatio := (currQ.p90 + 1e-9) / (prevQ.p90 + 1e-9)
	qSimilar := qRatio >= 0.75 && qRatio <= 1.35
	if !qSimilar {
		return false
	}

	// If latency windows are both available, require them to be similar too.
	// This prevents filtering true long degradations that have new latency pain.
	if currLat.count >= 3 && prevLat.count >= 3 {
		latRatio := (currLat.p95 + 1e-9) / (prevLat.p95 + 1e-9)
		return latRatio >= 0.70 && latRatio <= 1.50
	}
	return true
}

// evaluateAnomalyEvidence labels anomaly cause with explicit source-signal rules.
// Design goals:
// - suppress low-baseline false positives,
// - keep workload-linked shifts with multi-metric support,
// - keep latency-only degradations even when QPS is stable,
// - suppress long diurnal ramps that look like anomalies only because baseline is earlier low load.
func evaluateAnomalyEvidence(
	qBase, qEvent, qEdgeBase, qEdgeEvent windowStats,
	latBase, latEvent windowStats,
	tikvLatBase, tikvLatEvent windowStats,
	stmtCorr, tikvCorr float64,
	eventDurationSec int64,
) anomalyEvidence {
	ev := anomalyEvidence{
		reason:      "MIXED_OR_UNCERTAIN",
		confidence:  0.35,
		keep:        false,
		qpsBaseMed:  qBase.median,
		qpsEventP95: qEvent.p95,
		latBaseP99:  latBase.p99,
		latEventP99: latEvent.p99,
		stmtCorr:    stmtCorr,
		tikvCorr:    tikvCorr,
	}
	if qEvent.count < 3 {
		return ev
	}

	if qBase.median < 1.0 {
		// Low-baseline clusters are noisy in percentage terms; require an absolute
		// jump (>=5 QPS) to avoid interpreting tiny background variance as incidents.
		ev.qpsSignal = qEvent.p95 >= maxFloat64(5.0, qBase.p95*3.0)
	} else {
		// For normal baseline traffic, require both distribution shift and absolute
		// median lift so periodic oscillation around baseline does not trigger.
		ev.qpsSignal = qEvent.p90 > qBase.p95*1.25 &&
			(qEvent.median-qBase.median) > maxFloat64(0.2, qBase.median*0.15)
	}

	if latEvent.count >= 3 && latBase.count >= 3 {
		// Latency spike: relative jump on both tail and high quantile.
		ev.latSignal = latEvent.p99 > maxFloat64(1.0, latBase.p99*2.5) &&
			latEvent.p90 > maxFloat64(0.2, latBase.p90*1.8)
	}
	// Backend latency signal from TiKV gRPC histogram helps differentiate whether
	// front-end latency degradation is likely propagated from storage/backend path.
	backendLatencySignal := tikvLatEvent.count >= 3 && tikvLatBase.count >= 3 &&
		tikvLatEvent.p99 > maxFloat64(0.05, tikvLatBase.p99*2.0)
	// Chronic latency path: if p95 is already >1s and remains significantly above
	// local baseline, keep it as latency degradation even without sharp spikes.
	chronicLatency := latEvent.count >= 6 && latEvent.p95 > 1.0 && latEvent.p95 > maxFloat64(1.0, latBase.p95*1.3)

	if !ev.qpsSignal && !ev.latSignal && !chronicLatency {
		return ev
	}

	qpsAmplification := (qEvent.p95 + 1e-9) / (qBase.p95 + 1e-9)
	qpsEdgeRatio := (qEdgeEvent.median + 1e-9) / (qEdgeBase.median + 1e-9)
	qpsEdgeDelta := qEdgeEvent.median - qEdgeBase.median
	hasEdgeJump := qEdgeBase.count >= 3 && qEdgeEvent.count >= 3 &&
		(qpsEdgeRatio >= 1.2 || qpsEdgeDelta > maxFloat64(5.0, qBase.median*0.2))
	// Long workload-shift incidents require stronger source linkage than short bursts.
	// This avoids accepting long daytime ramps with only weak correlation evidence.
	workloadLinkedEvidence := (stmtCorr >= 0.65 || tikvCorr >= 0.5)
	if eventDurationSec >= 2*3600 {
		workloadLinkedEvidence = (stmtCorr >= 0.78 || tikvCorr >= 0.62) &&
			(qpsEdgeRatio >= 1.3 || qpsEdgeDelta > maxFloat64(8.0, qBase.median*0.25))
	}

	// Long "workload-linked" incidents must start with a visible edge jump;
	// otherwise this is usually a smooth day/night ramp, not an operational anomaly.
	if eventDurationSec >= 3*3600 && ev.qpsSignal && !ev.latSignal && !hasEdgeJump {
		return ev
	}
	// Sparse long windows (mostly zero points) are usually bursty fragments that
	// were merged by cadence, not a continuously degraded period.
	if eventDurationSec >= 2*3600 && ev.qpsSignal && !ev.latSignal && qEvent.zeroRat >= 0.60 {
		return ev
	}
	// Long coupled events must also satisfy strong workload linkage; otherwise
	// they are usually cadence-merged artifacts with weak source correlation.
	if eventDurationSec >= 2*3600 && ev.qpsSignal && ev.latSignal && !workloadLinkedEvidence {
		return ev
	}

	ev.keep = true
	switch {
	case qBase.median < 1.0 && qEvent.p95 >= 5.0:
		ev.reason = "LOW_BASELINE_BURST"
		ev.confidence = 0.55
	case (chronicLatency || ev.latSignal) && !ev.qpsSignal && backendLatencySignal:
		ev.reason = "BACKEND_LATENCY_DEGRADATION"
		ev.confidence = 0.78
	case chronicLatency && !ev.qpsSignal:
		ev.reason = "LATENCY_ONLY_DEGRADATION"
		ev.confidence = 0.68
	case ev.latSignal && !ev.qpsSignal:
		ev.reason = "LATENCY_ONLY_DEGRADATION"
		ev.confidence = 0.72
	case ev.qpsSignal && qpsAmplification >= 1.4 && hasEdgeJump && workloadLinkedEvidence:
		// Workload-linked shift needs source-level linkage from TiDB statement or
		// TiKV gRPC trajectories; this avoids labeling isolated QPS spikes as load shifts.
		ev.reason = "WORKLOAD_LINKED_SHIFT"
		ev.confidence = 0.74
	case ev.qpsSignal && ev.latSignal:
		ev.reason = "LOAD_AND_LATENCY_COUPLED"
		ev.confidence = 0.74
	case ev.qpsSignal &&
		eventDurationSec <= 2*3600 &&
		qEvent.p95 > qBase.p95*1.8 &&
		qEvent.median > maxFloat64(qBase.median*1.8, qBase.median+10.0):
		ev.reason = "SHORT_INTENSE_BURST"
		ev.confidence = 0.70
	case ev.qpsSignal && qpsAmplification >= 1.8:
		ev.reason = "MIXED_OR_UNCERTAIN"
		ev.confidence = 0.56
	default:
		ev.keep = false
		return ev
	}

	if isFinite(stmtCorr) && stmtCorr > 0.9 {
		ev.confidence += 0.04
	}
	if isFinite(tikvCorr) && tikvCorr > 0.8 {
		ev.confidence += 0.04
	}
	if ev.confidence > 0.95 {
		ev.confidence = 0.95
	}
	return ev
}

func calcWindowStats(values []analysis.TimeSeriesPoint, startTS, endTS int64) windowStats {
	if len(values) == 0 || endTS < startTS {
		return windowStats{}
	}
	startIdx := sort.Search(len(values), func(i int) bool {
		return values[i].Timestamp >= startTS
	})
	var sample []float64
	zeroCnt := 0
	sum := 0.0
	minV := math.MaxFloat64
	maxV := -math.MaxFloat64
	for i := startIdx; i < len(values) && values[i].Timestamp <= endTS; i++ {
		v := values[i].Value
		sample = append(sample, v)
		sum += v
		if v == 0 {
			zeroCnt++
		}
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	if len(sample) == 0 {
		return windowStats{}
	}
	sort.Float64s(sample)
	return windowStats{
		count:   len(sample),
		mean:    sum / float64(len(sample)),
		median:  percentileSorted(sample, 50),
		p90:     percentileSorted(sample, 90),
		p95:     percentileSorted(sample, 95),
		p99:     percentileSorted(sample, 99),
		max:     maxV,
		min:     minV,
		zeroRat: float64(zeroCnt) / float64(len(sample)),
	}
}

func calcWindowCorrelation(
	a []analysis.TimeSeriesPoint,
	b []analysis.TimeSeriesPoint,
	startTS, endTS int64,
) float64 {
	if len(a) == 0 || len(b) == 0 || endTS < startTS {
		return math.NaN()
	}
	bMap := make(map[int64]float64, len(b))
	for _, p := range b {
		if p.Timestamp >= startTS && p.Timestamp <= endTS {
			bMap[p.Timestamp] = p.Value
		}
	}
	var xs, ys []float64
	for _, p := range a {
		if p.Timestamp < startTS || p.Timestamp > endTS {
			continue
		}
		if y, ok := bMap[p.Timestamp]; ok {
			xs = append(xs, p.Value)
			ys = append(ys, y)
		}
	}
	if len(xs) < 3 {
		return math.NaN()
	}
	meanX, meanY := 0.0, 0.0
	for i := range xs {
		meanX += xs[i]
		meanY += ys[i]
	}
	meanX /= float64(len(xs))
	meanY /= float64(len(xs))

	cov, varX, varY := 0.0, 0.0, 0.0
	for i := range xs {
		dx := xs[i] - meanX
		dy := ys[i] - meanY
		cov += dx * dy
		varX += dx * dx
		varY += dy * dy
	}
	if varX == 0 || varY == 0 {
		return math.NaN()
	}
	return cov / math.Sqrt(varX*varY)
}

// calculateHighValueCoverage returns coverage ratio and first/last high-value
// timestamp in [startTS, endTS].
func calculateHighValueCoverage(
	values []analysis.TimeSeriesPoint,
	startTS, endTS int64,
	threshold float64,
) (coverage float64, firstHighTS int64, lastHighTS int64) {
	if len(values) == 0 || endTS < startTS {
		return 0, 0, 0
	}
	startIdx := sort.Search(len(values), func(i int) bool {
		return values[i].Timestamp >= startTS
	})
	total := 0
	high := 0
	for i := startIdx; i < len(values) && values[i].Timestamp <= endTS; i++ {
		total++
		if values[i].Value >= threshold {
			high++
			if firstHighTS == 0 {
				firstHighTS = values[i].Timestamp
			}
			lastHighTS = values[i].Timestamp
		}
	}
	if total == 0 {
		return 0, 0, 0
	}
	return float64(high) / float64(total), firstHighTS, lastHighTS
}

func percentileSorted(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if p < 0 {
		p = 0
	}
	if p > 100 {
		p = 100
	}
	idx := (len(sorted) - 1) * p / 100
	return sorted[idx]
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// detectRawRollingAnomalies detects anomalies directly from source time-series
// using rolling median + MAD. This detector does not depend on model-specific
// assumptions and is intended as a robust candidate generator.
func detectRawRollingAnomalies(
	values []analysis.TimeSeriesPoint,
	anomalyType analysis.AnomalyType,
	detectorName string,
) []analysis.DetectedAnomaly {
	if len(values) < 80 {
		return nil
	}
	sorted := make([]analysis.TimeSeriesPoint, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	overall := make([]float64, 0, len(sorted))
	for _, p := range sorted {
		overall = append(overall, p.Value)
	}
	sort.Float64s(overall)
	globalMedian := percentileSorted(overall, 50)
	minDelta := maxFloat64(0.2, globalMedian*0.10)
	if anomalyType == analysis.AnomalyLatencySpike {
		minDelta = maxFloat64(0.02, globalMedian*0.50)
	}
	if globalMedian < 1 && anomalyType == analysis.AnomalyQPSSpike {
		minDelta = 5.0
	}

	step := estimateMedianSampleInterval(sorted)
	window := 60
	if len(sorted) < window+5 {
		window = len(sorted) / 2
	}
	if window < 10 {
		return nil
	}

	type pointCandidate struct {
		ts       int64
		value    float64
		baseline float64
		zScore   float64
		severity analysis.Severity
	}
	var candidates []pointCandidate

	for i := window; i < len(sorted); i++ {
		w := make([]float64, 0, window)
		for j := i - window; j < i; j++ {
			w = append(w, sorted[j].Value)
		}
		sort.Float64s(w)
		med := percentileSorted(w, 50)
		mad := calculateMADFromSorted(w, med)
		sigma := 1.4826*mad + 1e-9
		delta := sorted[i].Value - med
		z := delta / sigma
		if anomalyType == analysis.AnomalyQPSSpike || anomalyType == analysis.AnomalyLatencySpike {
			if delta < minDelta {
				continue
			}
		}
		if math.Abs(z) < 6.0 {
			continue
		}
		severity := analysis.SeverityMedium
		if math.Abs(z) >= 12 {
			severity = analysis.SeverityCritical
		} else if math.Abs(z) >= 9 {
			severity = analysis.SeverityHigh
		}
		candidates = append(candidates, pointCandidate{
			ts:       sorted[i].Timestamp,
			value:    sorted[i].Value,
			baseline: med,
			zScore:   z,
			severity: severity,
		})
	}
	if len(candidates) == 0 {
		return nil
	}

	var out []analysis.DetectedAnomaly
	current := candidates[0]
	endTS := current.ts
	maxAbsZ := math.Abs(current.zScore)
	maxValue := current.value
	baseline := current.baseline
	severity := current.severity
	pointCount := 1

	flush := func() {
		if pointCount < 3 {
			return
		}
		durationSec := calculateEventDurationSec(current.ts, endTS, step)
		conf := 0.55 + math.Min(0.35, maxAbsZ/30.0)
		if conf > 0.95 {
			conf = 0.95
		}
		out = append(out, analysis.DetectedAnomaly{
			Type:       anomalyType,
			Severity:   severity,
			Timestamp:  current.ts,
			EndTime:    endTS,
			Duration:   durationSec,
			Value:      maxValue,
			Baseline:   baseline,
			ZScore:     maxAbsZ,
			Confidence: conf,
			DetectedBy: []string{detectorName},
			Detail:     "raw_rolling_mad_detector",
		})
	}

	for i := 1; i < len(candidates); i++ {
		next := candidates[i]
		if next.ts-endTS <= step*3 {
			endTS = next.ts
			pointCount++
			if absFloat(next.zScore) > maxAbsZ {
				maxAbsZ = absFloat(next.zScore)
				maxValue = next.value
				baseline = next.baseline
			}
			if next.severity == analysis.SeverityCritical ||
				(next.severity == analysis.SeverityHigh && severity != analysis.SeverityCritical) {
				severity = next.severity
			}
			continue
		}
		flush()
		current = next
		endTS = next.ts
		maxAbsZ = absFloat(next.zScore)
		maxValue = next.value
		baseline = next.baseline
		severity = next.severity
		pointCount = 1
	}
	flush()
	return out
}

func calculateMADFromSorted(sorted []float64, median float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	dev := make([]float64, len(sorted))
	for i, v := range sorted {
		dev[i] = math.Abs(v - median)
	}
	sort.Float64s(dev)
	return percentileSorted(dev, 50)
}

// detectSustainedLatencyDegradation finds long runs where latency stays above
// an adaptive absolute threshold. This complements spike detectors and catches
// chronic degradation patterns that rolling z-score may under-detect.
func detectSustainedLatencyDegradation(
	values []analysis.TimeSeriesPoint,
	detectorName string,
) []analysis.DetectedAnomaly {
	if len(values) < 60 {
		return nil
	}
	sorted := make([]analysis.TimeSeriesPoint, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})
	all := make([]float64, 0, len(sorted))
	for _, p := range sorted {
		all = append(all, p.Value)
	}
	sort.Float64s(all)
	globalP50 := percentileSorted(all, 50)
	globalP75 := percentileSorted(all, 75)
	globalP99 := percentileSorted(all, 99)
	// Threshold combines absolute floor (1s) and adaptive baseline multipliers.
	// p50*8 and p75*3 were chosen to capture persistent regressions while filtering
	// normal long-tail variation in light/medium traffic clusters.
	threshold := maxFloat64(1.0, maxFloat64(globalP50*8.0, globalP75*3.0))
	if globalP99 > threshold*5 {
		threshold = maxFloat64(1.0, threshold*1.5)
	}

	step := estimateMedianSampleInterval(sorted)
	minPoints := 6
	var out []analysis.DetectedAnomaly
	start := int64(0)
	end := int64(0)
	pointCount := 0
	maxLatency := 0.0

	flush := func() {
		if pointCount < minPoints {
			return
		}
		conf := 0.72
		if maxLatency >= threshold*1.8 {
			conf = 0.85
		}
		out = append(out, analysis.DetectedAnomaly{
			Type:       analysis.AnomalyLatencyDegraded,
			Severity:   analysis.SeverityHigh,
			Timestamp:  start,
			EndTime:    end,
			Duration:   calculateEventDurationSec(start, end, step),
			Value:      maxLatency,
			Baseline:   globalP75,
			Confidence: conf,
			DetectedBy: []string{detectorName},
			Detail:     "raw_sustained_latency_detector",
		})
	}

	for _, p := range sorted {
		if p.Value >= threshold {
			if pointCount == 0 {
				start = p.Timestamp
				end = p.Timestamp
				pointCount = 1
				maxLatency = p.Value
				continue
			}
			if p.Timestamp-end <= step*3 {
				end = p.Timestamp
				pointCount++
				if p.Value > maxLatency {
					maxLatency = p.Value
				}
				continue
			}
			flush()
			start = p.Timestamp
			end = p.Timestamp
			pointCount = 1
			maxLatency = p.Value
			continue
		}
		if pointCount > 0 && p.Timestamp-end > step*3 {
			flush()
			pointCount = 0
		}
	}
	flush()
	return out
}

// compactContiguousAnomalies is phase-1 event stitching.
// Algorithm:
// 1) Sort by timestamp and greedily grow the current incident.
// 2) For each candidate point, compute an adaptive local merge window from nearby cadence.
// 3) Merge only when both checks pass:
//   - compatibility check (gap/type/detector overlap),
//   - continuity check (density + span guard) to prevent runaway 10h+ chains.
//
// This phase preserves dense contiguous bursts while splitting sparse periodic points.
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
	// Use midpoint between p50 and p75 of observed gaps:
	// - p50 anchors to dominant cadence,
	// - p75 allows moderate burst drift without exploding the window.
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

// mergeNearbyEventsByCadence is phase-2 stitching after contiguous compacting.
// It merges short incidents that repeat with stable cadence (e.g. every 6-12 minutes)
// into one operational incident, but still enforces density/span constraints.
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
	latencyDominant := isLatencyDominantEvent(current) || isLatencyDominantEvent(next)
	if currentDurationSec >= 20*60 || nextDurationSec >= 20*60 {
		maxSpanSec = clampInt64(maxSpanSec+30*60, 3600, 6*3600)
	}
	if latencyDominant {
		// Latency-only chains are often sparse recurrent spikes; keep them tighter
		// than workload chains to avoid multi-hour over-merge artifacts.
		maxSpanSec = minInt64(maxSpanSec, 3*3600)
	}

	if currentDurationSec >= 15*60 || nextDurationSec >= 15*60 {
		minDensity -= 0.04
	}
	if latencyDominant && proposedSpanSec >= 90*60 {
		minDensity += 0.08
	}
	if mergedCount >= 6 {
		minDensity += 0.02
	}
	minDensity = math.Min(0.35, math.Max(0.12, minDensity))

	if proposedSpanSec > maxSpanSec {
		// Soft cap for dense, near-continuous incidents.
		// Keep an extension only when:
		// - gap is still small versus cadence,
		// - density is clearly higher than normal threshold.
		// Hard cap bounds absolute incident length to avoid over-merge chains.
		hardMaxSpanSec := clampInt64(maxSpanSec*2, 4*3600, 12*3600)
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

func isLatencyDominantEvent(a analysis.DetectedAnomaly) bool {
	if len(a.DetectedBy) == 0 {
		return false
	}
	latencyLike := 0
	for _, d := range a.DetectedBy {
		if strings.Contains(strings.ToLower(d), "latency") {
			latencyLike++
		}
	}
	return latencyLike*2 >= len(a.DetectedBy)
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

func parsePromFileForTimeSeries(filePath, metricName string, startTS, endTS int64, dataByTs map[int64]float64) error {
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
		if shouldSkipMetricLine(metricName, line) {
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

// shouldSkipMetricLine filters out noisy label-variants that are not part of
// the target workload signal used by anomaly detection.
func shouldSkipMetricLine(metricName, line string) bool {
	switch metricName {
	case "tidb_server_query_total":
		// Keep only successful query series for workload QPS signal.
		if strings.Contains(line, `result="`) && !strings.Contains(line, `result="OK"`) {
			return true
		}
	}
	return false
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
		fmt.Printf("Merged anomaly events: %d (window=%ds)\n", len(result.Anomalies), result.Summary.MergeWindowSec)
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
	reason := extractAnomalyReason(a.Detail)
	if reason == "" {
		reason = "UNSPECIFIED"
	}

	duration := ""
	if a.Duration > 0 {
		duration = " (" + formatAnomalyDuration(int(a.Duration)) + ")"
	}

	fmt.Printf("%-10s %s  %-25s %-12s %s %-24s %s%s\n",
		severity, timeRange, a.Type, change, confBar, reason, algos, duration)
}

func extractAnomalyReason(detail string) string {
	if detail == "" {
		return ""
	}
	idx := strings.Index(detail, "reason=")
	if idx < 0 {
		return ""
	}
	rest := detail[idx+len("reason="):]
	end := strings.IndexAny(rest, " ,")
	if end < 0 {
		return rest
	}
	return rest[:end]
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

	clusterID, err := selectRandomCluster(cacheDir, metaDir)
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

	clusterID, err := selectRandomCluster(cacheDir, metaDir)
	if err != nil {
		return err
	}

	fmt.Printf("Random cluster from cache: %s\n\n", clusterID)

	return DigAbnormal(cacheDir, metaDir, cp, maxBackoff, authMgr, config,
		clusterID, startTS, endTS, jsonOutput)
}

func selectRandomCluster(cacheDir, metaDir string) (string, error) {
	inactive := loadInactiveClusters(metaDir)
	if len(inactive) > 0 {
		fmt.Printf("Excluding %d inactive clusters\n", len(inactive))
	}

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
			if inactive[entry.Name()] {
				continue
			}
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

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(clusters), func(i, j int) {
		clusters[i], clusters[j] = clusters[j], clusters[i]
	})

	fmt.Printf("Walking through %d clusters (profile)\n\n", len(clusters))

	for i, c := range clusters {
		fmt.Printf("[%d/%d] Processing cluster %s\n", i+1, len(clusters), c.clusterID)

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

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(clusters), func(i, j int) {
		clusters[i], clusters[j] = clusters[j], clusters[i]
	})

	fmt.Printf("Walking through %d clusters (abnormal)\n\n", len(clusters))

	for i, c := range clusters {
		fmt.Printf("[%d/%d] Processing cluster %s\n", i+1, len(clusters), c.clusterID)

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
	inactive := loadInactiveClusters(metaDir)
	if len(inactive) > 0 {
		fmt.Printf("Excluding %d inactive clusters\n", len(inactive))
	}

	metricsDir := filepath.Join(cacheDir, "metrics")
	entries, err := os.ReadDir(metricsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no cached metrics found, please fetch metrics first")
		}
		return nil, fmt.Errorf("failed to read metrics cache: %w", err)
	}

	var allClusters []clusterInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		clusterID := entry.Name()
		if inactive[clusterID] {
			continue
		}
		clusterDir := filepath.Join(metricsDir, clusterID)
		metricEntries, err := os.ReadDir(clusterDir)
		if err != nil || len(metricEntries) == 0 {
			continue
		}
		allClusters = append(allClusters, clusterInfo{clusterID: clusterID})
	}

	if len(allClusters) == 0 {
		return nil, fmt.Errorf("no active clusters found")
	}

	return allClusters, nil
}

func DigLocal(cacheDir string, startTS, endTS int64, cacheID string, jsonOutput bool) {
	fmt.Println("DigLocal not implemented yet")
}
