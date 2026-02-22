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

func Dig(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	clusterID string, startTS, endTS int64, jsonOutput bool) error {

	fmt.Printf("Analyzing cluster %s\n", clusterID)
	fmt.Printf("Time range: %s ~ %s\n",
		time.Unix(startTS, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTS, 0).Format("2006-01-02 15:04:05"))

	promStorage := prometheus_storage.NewPrometheusStorage(filepath.Join(cacheDir, "metrics"))

	fmt.Println("\nLoading metrics data...")

	qpsData, err := loadMetricTimeSeries(promStorage, clusterID, "tidb_server_query_total", startTS, endTS)
	if err != nil {
		return fmt.Errorf("failed to load QPS data: %w", err)
	}

	latencyData, err := loadMetricHistogramP99(promStorage, clusterID, "tidb_server_handle_query_duration_seconds_bucket", startTS, endTS)
	if err != nil {
		fmt.Printf("Warning: failed to load latency data: %v\n", err)
	}

	fmt.Printf("Loaded %d QPS data points\n", len(qpsData))
	if len(latencyData) > 0 {
		fmt.Printf("Loaded %d latency data points\n", len(latencyData))
	}

	if len(qpsData) == 0 {
		return fmt.Errorf("no QPS data available for analysis")
	}

	fmt.Println("\nAnalyzing load profile...")
	profile := analysis.AnalyzeLoadProfile(clusterID, qpsData, latencyData)
	if profile == nil {
		return fmt.Errorf("failed to analyze load profile")
	}

	fmt.Println("\nDetecting anomalies...")
	detector := analysis.NewAnomalyDetector(analysis.DefaultAnomalyConfig())
	qpsAnomalies := detector.DetectAll(qpsData)

	var latencyAnomalies []analysis.DetectedAnomaly
	if len(latencyData) > 0 {
		latencyAnomalies = detector.DetectLatencyAnomalies(latencyData, latencyData)
	}

	allAnomalies := append(qpsAnomalies, latencyAnomalies...)
	sort.Slice(allAnomalies, func(i, j int) bool {
		return allAnomalies[i].Timestamp < allAnomalies[j].Timestamp
	})

	profileWithAnomalies := &DigResult{
		LoadProfile: profile,
		Anomalies:   allAnomalies,
	}

	if jsonOutput {
		printJSONResult(profileWithAnomalies)
	} else {
		printTextResult(profileWithAnomalies)
	}

	return nil
}

type DigResult struct {
	LoadProfile *analysis.LoadProfile      `json:"load_profile"`
	Anomalies   []analysis.DetectedAnomaly `json:"anomalies"`
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

func printJSONResult(result *DigResult) {
	fmt.Println(mustMarshalJSON(result))
}

func mustMarshalJSON(v interface{}) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	return string(data)
}

func printTextResult(result *DigResult) {
	analysis.PrintLoadProfile(result.LoadProfile, false)

	if len(result.Anomalies) > 0 {
		fmt.Println()
		fmt.Println("============================================================")
		fmt.Println("ANOMALY DETECTION RESULTS")
		fmt.Println("============================================================")
		fmt.Printf("\nDetected %d anomalies:\n\n", len(result.Anomalies))

		for i, a := range result.Anomalies {
			fmt.Printf("%d. [%s] %s\n", i+1, a.Severity, a.Type)
			fmt.Printf("   Time: %s\n", a.TimeStr)
			fmt.Printf("   Detail: %s\n", a.Detail)
			fmt.Println()
		}
	} else {
		fmt.Println()
		fmt.Println("No significant anomalies detected.")
	}
}

func DigRandom(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	startTS, endTS int64, jsonOutput bool) error {

	metricsDir := filepath.Join(cacheDir, "metrics")

	entries, err := os.ReadDir(metricsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no cached metrics found, please fetch metrics first")
		}
		return fmt.Errorf("failed to read metrics cache: %w", err)
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
		return fmt.Errorf("no cached clusters found, please fetch metrics first")
	}

	randSeed := time.Now().UnixNano()
	selectedIdx := randSeed % int64(len(cachedClusters))
	selectedClusterID := cachedClusters[selectedIdx]

	fmt.Printf("Random cluster from cache: %s\n\n", selectedClusterID)

	return Dig(cacheDir, metaDir, cp, maxBackoff, authMgr, config,
		selectedClusterID, startTS, endTS, jsonOutput)
}

func DigWalk(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, config MetricsFetcherConfig,
	startTS, endTS int64) error {

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
		return fmt.Errorf("no active clusters found")
	}

	fmt.Printf("Walking through %d clusters\n\n", len(allClusters))

	for i, c := range allClusters {
		fmt.Printf("[%d/%d] Processing cluster %s (%s)\n", i+1, len(allClusters), c.clusterID, c.bizType)

		err := Dig(cacheDir, metaDir, cp, maxBackoff, authMgr, config,
			c.clusterID, startTS, endTS, false)
		if err != nil {
			fmt.Printf("Error processing cluster %s: %v\n", c.clusterID, err)
		}
		fmt.Println("\n------------------------------------------------------------")
	}

	fmt.Println("Walk completed")
	return nil
}

func DigLocal(cacheDir string, startTS, endTS int64, cacheID string, jsonOutput bool) {
	fmt.Println("DigLocal not implemented yet")
}
