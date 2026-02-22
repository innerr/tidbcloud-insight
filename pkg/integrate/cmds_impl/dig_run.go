package impl

import (
	"bufio"
	"encoding/json"
	"fmt"
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
	var allAnomalies []analysis.DetectedAnomaly

	fmt.Println("\nRunning anomaly detection algorithms...")

	start := time.Now()
	detector := analysis.NewAnomalyDetector(analysis.DefaultAnomalyConfig())
	qpsAnomalies := detector.DetectAll(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "AnomalyDetector.DetectAll", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, qpsAnomalies...)

	if len(data.latencyData) > 0 {
		start = time.Now()
		latencyAnomalies := detector.DetectLatencyAnomalies(data.latencyData, data.latencyData)
		timings = append(timings, AlgorithmTiming{Name: "DetectLatencyAnomalies", Duration: time.Since(start)})
		allAnomalies = append(allAnomalies, latencyAnomalies...)
	}

	start = time.Now()
	advancedDetector := analysis.NewAdvancedAnomalyDetector(analysis.DefaultAdvancedAnomalyConfig())
	advancedAnomalies := advancedDetector.DetectAllAdvanced(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "AdvancedAnomalyDetector.DetectAllAdvanced", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, advancedAnomalies...)

	start = time.Now()
	srDetector := analysis.NewSpectralResidualDetector(analysis.DefaultSRConfig())
	srAnomalies := srDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "SpectralResidualDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, srAnomalies...)

	start = time.Now()
	msDetector := analysis.NewMultiScaleAnomalyDetector(analysis.DefaultMultiScaleConfig())
	msAnomalies := msDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "MultiScaleAnomalyDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, msAnomalies...)

	start = time.Now()
	dbDetector := analysis.NewDBSCANAnomalyDetector(analysis.DefaultDBSCANConfig())
	dbAnomalies := dbDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "DBSCANAnomalyDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, dbAnomalies...)

	start = time.Now()
	hwDetector := analysis.NewHoltWintersDetector(analysis.DefaultHoltWintersConfig())
	hwAnomalies := hwDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "HoltWintersDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, hwAnomalies...)

	start = time.Now()
	ifDetector := analysis.NewIsolationForest(analysis.DefaultIsolationForestConfig())
	ifAnomalies := ifDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "IsolationForest", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, ifAnomalies...)

	start = time.Now()
	shesdDetector := analysis.NewSHESDDetector(analysis.DefaultSHESDConfig())
	shesdAnomalies := shesdDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "SHESDDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, shesdAnomalies...)

	start = time.Now()
	ensembleDetector := analysis.NewEnsembleAnomalyDetector(analysis.DefaultEnsembleAnomalyConfig())
	ensembleAnomalies := ensembleDetector.DetectAll(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "EnsembleAnomalyDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, ensembleAnomalies...)

	start = time.Now()
	dtDetector := analysis.NewDynamicThresholdDetector(analysis.DefaultDynamicThresholdConfig())
	_, dtAnomalies := dtDetector.ComputeThresholds(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "DynamicThresholdDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, dtAnomalies...)

	start = time.Now()
	ctxDetector := analysis.NewContextualAnomalyDetector(analysis.DefaultContextualAnomalyConfig())
	ctxAnomalies := ctxDetector.Detect(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "ContextualAnomalyDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, ctxAnomalies...)

	start = time.Now()
	peakDetector := analysis.NewPeakDetector(analysis.DefaultPeakDetectorConfig())
	peakAnomalies := peakDetector.DetectPeakAnomalies(data.qpsData)
	timings = append(timings, AlgorithmTiming{Name: "PeakDetector", Duration: time.Since(start)})
	allAnomalies = append(allAnomalies, peakAnomalies...)

	sort.Slice(allAnomalies, func(i, j int) bool {
		return allAnomalies[i].Timestamp < allAnomalies[j].Timestamp
	})

	result := &DigAbnormalResult{
		ClusterID: clusterID,
		Anomalies: allAnomalies,
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

	var result []analysis.TimeSeriesPoint
	for ts, buckets := range dataByTs {
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
		dataByTs[ts][le] = value
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
