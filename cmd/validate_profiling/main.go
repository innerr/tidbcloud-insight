package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"tidbcloud-insight/pkg/analysis"
)

type TimeSeriesPoint struct {
	Timestamp int64
	Value     float64
}

type ValidationResult struct {
	ClusterID          string  `json:"cluster_id"`
	Samples            int     `json:"samples"`
	DurationHours      float64 `json:"duration_hours"`
	MeanQPS            float64 `json:"mean_qps"`
	MedianQPS          float64 `json:"median_qps"`
	CV                 float64 `json:"cv"`
	PeakToAvg          float64 `json:"peak_to_avg"`
	Burstiness         float64 `json:"burstiness"`
	StabilityClass     string  `json:"stability_class"`
	LoadClass          string  `json:"load_class"`
	TrafficType        string  `json:"traffic_type"`
	Seasonality        float64 `json:"seasonality"`
	TrendSlope         float64 `json:"trend_slope"`
	HasDailyPattern    bool    `json:"has_daily_pattern"`
	PeakToOffPeak      float64 `json:"peak_to_off_peak"`
	BusinessHoursRatio float64 `json:"business_hours_ratio"`
	NightDrop          float64 `json:"night_drop"`
	Autocorrelation    float64 `json:"autocorrelation"`
	QPSLatencyCorr     float64 `json:"qps_latency_corr"`
	HasLatencyData     bool    `json:"has_latency_data"`
	P99LatencyMs       float64 `json:"p99_latency_ms"`
	LoadSensitivity    string  `json:"load_sensitivity"`
}

type StatsSummary struct {
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Median float64 `json:"median"`
}

func main() {
	cacheDir := "/Volumes/2t/cache/metrics"
	if len(os.Args) > 1 {
		cacheDir = os.Args[1]
	}

	minClusters := 50
	minDurationHours := 24.0
	minSamples := 50
	clusterLimit := 60
	maxFilesPerCluster := 1

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading cache directory: %v\n", err)
		os.Exit(1)
	}

	var results []ValidationResult
	var allQPSValues []float64
	var allCVValues []float64
	var allBurstinessValues []float64
	var allSeasonalityValues []float64

	processedCount := 0
	skippedCount := 0
	errorCount := 0
	errorReasons := make(map[string]int)

	fmt.Printf("Validating profiling algorithm...\n")
	fmt.Printf("Cache directory: %s\n", cacheDir)
	fmt.Printf("Minimum clusters: %d\n", minClusters)
	fmt.Printf("Minimum duration: %.0f hours\n", minDurationHours)
	fmt.Printf("Cluster limit: %d\n\n", clusterLimit)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if processedCount+skippedCount+errorCount >= clusterLimit {
			break
		}

		clusterID := entry.Name()
		result, err := validateCluster(cacheDir, clusterID, minDurationHours, minSamples, maxFilesPerCluster)
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "insufficient") {
				skippedCount++
			} else {
				errorCount++
			}
			errorReasons[errStr]++
			continue
		}

		results = append(results, *result)
		allQPSValues = append(allQPSValues, result.MeanQPS)
		allCVValues = append(allCVValues, result.CV)
		allBurstinessValues = append(allBurstinessValues, result.Burstiness)
		allSeasonalityValues = append(allSeasonalityValues, result.Seasonality)
		processedCount++

		if processedCount%20 == 0 {
			fmt.Printf("Processed %d clusters...\n", processedCount)
		}
	}

	fmt.Printf("\n")
	fmt.Printf("=== Validation Summary ===\n")
	fmt.Printf("Processed: %d clusters\n", processedCount)
	fmt.Printf("Skipped (insufficient data): %d clusters\n", skippedCount)
	fmt.Printf("Errors: %d clusters\n", errorCount)

	if len(errorReasons) > 0 && processedCount < 10 {
		fmt.Printf("\nError reasons:\n")
		for reason, count := range errorReasons {
			fmt.Printf("  %s: %d\n", reason, count)
		}
	}

	if processedCount < minClusters {
		fmt.Printf("\nWARNING: Only %d clusters processed, expected at least %d\n", processedCount, minClusters)
	}

	analyzeResults(results, allQPSValues, allCVValues, allBurstinessValues, allSeasonalityValues)

	if len(os.Args) > 2 && os.Args[2] == "--json" {
		data, _ := json.MarshalIndent(results, "", "  ")
		fmt.Println(string(data))
	}
}

func validateCluster(cacheDir, clusterID string, minDurationHours float64, minSamples, maxFiles int) (*ValidationResult, error) {
	metricDir := filepath.Join(cacheDir, clusterID, "tidb_server_query_total")
	files, err := findPromFiles(metricDir)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no prom files found")
	}

	if len(files) > maxFiles {
		files = files[:maxFiles]
	}

	allData, err := loadCounterMetricAsRate(files)
	if err != nil {
		return nil, fmt.Errorf("load error: %v", err)
	}
	if len(allData) < minSamples {
		return nil, fmt.Errorf("insufficient data points: %d (need %d)", len(allData), minSamples)
	}

	sort.Slice(allData, func(i, j int) bool {
		return allData[i].Timestamp < allData[j].Timestamp
	})

	durationHours := float64(allData[len(allData)-1].Timestamp-allData[0].Timestamp) / 3600
	if durationHours < minDurationHours {
		return nil, fmt.Errorf("insufficient duration: %.1f hours", durationHours)
	}

	var latencyData []analysis.TimeSeriesPoint
	latencyDir := filepath.Join(cacheDir, clusterID, "tidb_server_handle_query_duration_seconds_bucket")
	latencyFiles, err := findPromFiles(latencyDir)
	if err == nil && len(latencyFiles) > 0 {
		latRaw, _ := loadHistogramP99(latencyFiles)
		latencyData = convertToAnalysisPoints(latRaw)
	}

	profile := analysis.AnalyzeLoadProfile(clusterID, convertToAnalysisPoints(allData), latencyData)
	if profile == nil {
		return nil, fmt.Errorf("failed to analyze profile")
	}

	result := &ValidationResult{
		ClusterID:          clusterID,
		Samples:            profile.Samples,
		DurationHours:      profile.DurationHours,
		MeanQPS:            profile.QPSProfile.Mean,
		MedianQPS:          profile.QPSProfile.Median,
		CV:                 profile.QPSProfile.CV,
		PeakToAvg:          profile.QPSProfile.PeakToAvg,
		Burstiness:         profile.Characteristics.Burstiness,
		StabilityClass:     profile.Characteristics.StabilityClass,
		LoadClass:          profile.Characteristics.LoadClass,
		TrafficType:        profile.Characteristics.TrafficType,
		Seasonality:        profile.Characteristics.Seasonality,
		TrendSlope:         profile.Characteristics.TrendSlope,
		HasDailyPattern:    profile.DailyPattern.PeakToOffPeak > 1.5,
		PeakToOffPeak:      profile.DailyPattern.PeakToOffPeak,
		BusinessHoursRatio: profile.DailyPattern.BusinessHours.Ratio,
		NightDrop:          profile.DailyPattern.NightDrop,
		Autocorrelation:    profile.Characteristics.Autocorrelation,
		QPSLatencyCorr:     profile.Correlation.QPSLatencyCorr,
		HasLatencyData:     len(latencyData) > 0,
		P99LatencyMs:       profile.LatencyProfile.P99Ms,
		LoadSensitivity:    profile.Correlation.LoadSensitivity,
	}

	return result, nil
}

func convertToAnalysisPoints(points []TimeSeriesPoint) []analysis.TimeSeriesPoint {
	result := make([]analysis.TimeSeriesPoint, len(points))
	for i, p := range points {
		result[i] = analysis.TimeSeriesPoint{Timestamp: p.Timestamp, Value: p.Value}
	}
	return result
}

func findPromFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".prom") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

func loadCounterMetricAsRate(files []string) ([]TimeSeriesPoint, error) {
	dataByTs := make(map[int64]float64)

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		lineCount := 0
		parseSuccess := 0
		scanner := newScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "#") || line == "" {
				continue
			}
			lineCount++

			ts, val, ok := parsePromLine(line)
			if ok {
				dataByTs[ts] += val
				parseSuccess++
			}
		}
		f.Close()
	}

	var sorted []struct {
		ts int64
		v  float64
	}
	for ts, v := range dataByTs {
		sorted = append(sorted, struct {
			ts int64
			v  float64
		}{ts, v})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ts < sorted[j].ts })

	result := make([]TimeSeriesPoint, 0)
	for i := 1; i < len(sorted); i++ {
		tsDiff := sorted[i].ts - sorted[i-1].ts
		if tsDiff <= 0 || tsDiff > 600000 {
			continue
		}

		valDiff := sorted[i].v - sorted[i-1].v
		if valDiff < 0 {
			valDiff = sorted[i].v
		}

		rate := valDiff / float64(tsDiff)
		result = append(result, TimeSeriesPoint{
			Timestamp: sorted[i].ts,
			Value:     rate,
		})
	}

	return result, nil
}

func loadHistogramP99(files []string) ([]TimeSeriesPoint, error) {
	dataByTs := make(map[int64]map[string]float64)

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		scanner := newScanner(f)

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "#") || line == "" {
				continue
			}

			if strings.Contains(line, "{") {
				labels, val, ts, ok := parsePromLineWithLabels(line)
				if !ok {
					continue
				}

				le, hasLe := labels["le"]
				if !hasLe {
					continue
				}

				if _, exists := dataByTs[ts]; !exists {
					dataByTs[ts] = make(map[string]float64)
				}
				dataByTs[ts][le] += val
			}
		}
		f.Close()
	}

	ratesByTs := calcHistogramCounterRates(dataByTs)

	var result []TimeSeriesPoint
	for ts, buckets := range ratesByTs {
		p99 := histogramQuantile(buckets, 0.99)
		if !math.IsNaN(p99) {
			result = append(result, TimeSeriesPoint{
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

func histogramQuantile(buckets map[string]float64, quantile float64) float64 {
	total, exists := buckets["+Inf"]
	if !exists || total == 0 {
		return math.NaN()
	}

	target := total * quantile

	var sortedLE []struct {
		le    float64
		count float64
	}

	for le, count := range buckets {
		if le != "+Inf" {
			leVal := parseFloat(le)
			sortedLE = append(sortedLE, struct {
				le    float64
				count float64
			}{leVal, count})
		}
	}

	sort.Slice(sortedLE, func(i, j int) bool {
		return sortedLE[i].le < sortedLE[j].le
	})

	prevLE := 0.0
	prevCount := 0.0

	for _, item := range sortedLE {
		if item.count >= target {
			if item.count == prevCount {
				return prevLE
			}
			return prevLE + (item.le-prevLE)/(item.count-prevCount)*(target-prevCount)
		}
		prevLE = item.le
		prevCount = item.count
	}

	return prevLE
}

func parseFloat(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

func newScanner(f *os.File) *bufio.Scanner {
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	return scanner
}

func parsePromLine(line string) (int64, float64, bool) {
	if strings.HasPrefix(line, "#") || line == "" {
		return 0, 0, false
	}

	braceClose := strings.LastIndex(line, "}")
	if braceClose == -1 {
		return 0, 0, false
	}

	afterBrace := strings.TrimSpace(line[braceClose+1:])
	parts := strings.Fields(afterBrace)
	if len(parts) < 2 {
		return 0, 0, false
	}

	var val float64
	fmt.Sscanf(parts[0], "%f", &val)

	var ts int64
	fmt.Sscanf(parts[1], "%d", &ts)

	return ts, val, true
}

func parsePromLineWithLabels(line string) (map[string]string, float64, int64, bool) {
	startIdx := strings.Index(line, "{")
	endIdx := strings.LastIndex(line, "}")
	if startIdx == -1 || endIdx == -1 || endIdx <= startIdx {
		return nil, 0, 0, false
	}

	labelsStr := line[startIdx+1 : endIdx]
	rest := strings.TrimSpace(line[endIdx+1:])
	parts := strings.Fields(rest)
	if len(parts) < 2 {
		return nil, 0, 0, false
	}

	labels := parseLabels(labelsStr)

	var val float64
	fmt.Sscanf(parts[0], "%f", &val)

	var ts int64
	fmt.Sscanf(parts[1], "%d", &ts)

	return labels, val, ts, true
}

func parseLabels(s string) map[string]string {
	labels := make(map[string]string)
	inQuotes := false
	var currentKey strings.Builder
	var currentValue strings.Builder
	isValue := false

	for _, c := range s {
		switch c {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				if currentKey.Len() > 0 {
					labels[currentKey.String()] = currentValue.String()
				}
				currentKey.Reset()
				currentValue.Reset()
				isValue = false
			} else {
				currentValue.WriteRune(c)
			}
		case '=':
			if !inQuotes {
				isValue = true
			} else {
				currentValue.WriteRune(c)
			}
		default:
			if !inQuotes && !isValue {
				currentKey.WriteRune(c)
			} else {
				currentValue.WriteRune(c)
			}
		}
	}

	if currentKey.Len() > 0 {
		labels[currentKey.String()] = currentValue.String()
	}

	return labels
}

func analyzeResults(results []ValidationResult, allQPS, allCV, allBurstiness, allSeasonality []float64) {
	if len(results) == 0 {
		fmt.Println("No results to analyze")
		return
	}

	fmt.Printf("\n=== Distribution Analysis ===\n")

	printDistribution("Load Classes", countByField(results, func(r ValidationResult) string { return r.LoadClass }))
	printDistribution("Stability Classes", countByField(results, func(r ValidationResult) string { return r.StabilityClass }))
	printDistribution("Traffic Types", countByField(results, func(r ValidationResult) string { return r.TrafficType }))
	printDistribution("Load Sensitivity", countByField(results, func(r ValidationResult) string { return r.LoadSensitivity }))

	fmt.Printf("\n=== Statistical Summary ===\n")
	fmt.Printf("Mean QPS: %s\n", formatStats(allQPS))
	fmt.Printf("CV: %s\n", formatStats(allCV))
	fmt.Printf("Burstiness: %s\n", formatStats(allBurstiness))
	fmt.Printf("Seasonality: %s\n", formatStats(allSeasonality))

	dailyPatternCount := 0
	latencyCount := 0
	for _, r := range results {
		if r.HasDailyPattern {
			dailyPatternCount++
		}
		if r.HasLatencyData {
			latencyCount++
		}
	}
	fmt.Printf("\nClusters with Daily Pattern: %d (%.1f%%)\n", dailyPatternCount,
		float64(dailyPatternCount)/float64(len(results))*100)
	fmt.Printf("Clusters with Latency Data: %d (%.1f%%)\n", latencyCount,
		float64(latencyCount)/float64(len(results))*100)

	fmt.Printf("\n=== Potential Issues ===\n")
	issueCount := 0

	for _, r := range results {
		if r.CV < 0 {
			fmt.Printf("  WARNING: Negative CV for %s\n", r.ClusterID)
			issueCount++
		}
		if r.Burstiness < 0 || r.Burstiness > 1 {
			fmt.Printf("  WARNING: Invalid burstiness %.2f for %s\n", r.Burstiness, r.ClusterID)
			issueCount++
		}
		if r.Seasonality < 0 || r.Seasonality > 1 {
			fmt.Printf("  WARNING: Invalid seasonality %.2f for %s\n", r.Seasonality, r.ClusterID)
			issueCount++
		}
		if r.PeakToOffPeak < 0 {
			fmt.Printf("  WARNING: Negative PeakToOffPeak for %s\n", r.ClusterID)
			issueCount++
		}
	}

	if issueCount == 0 {
		fmt.Println("  No issues found - all values are within expected ranges")
	} else {
		fmt.Printf("  Total issues: %d\n", issueCount)
	}

	fmt.Printf("\n=== Algorithm Validation ===\n")
	validateAlgorithmDistribution(results, allCV, allBurstiness)
}

func validateAlgorithmDistribution(results []ValidationResult, allCV, allBurstiness []float64) {
	cvLow := 0
	cvMed := 0
	cvHigh := 0
	for _, cv := range allCV {
		if cv < 0.25 {
			cvLow++
		} else if cv < 0.5 {
			cvMed++
		} else {
			cvHigh++
		}
	}
	n := float64(len(allCV))
	fmt.Printf("CV Distribution: low(<0.25)=%.0f%%, medium(0.25-0.5)=%.0f%%, high(>0.5)=%.0f%%\n",
		float64(cvLow)/n*100, float64(cvMed)/n*100, float64(cvHigh)/n*100)

	burstLow := 0
	burstMed := 0
	burstHigh := 0
	for _, b := range allBurstiness {
		if b < 0.3 {
			burstLow++
		} else if b < 0.6 {
			burstMed++
		} else {
			burstHigh++
		}
	}
	fmt.Printf("Burstiness Distribution: low(<0.3)=%.0f%%, medium(0.3-0.6)=%.0f%%, high(>0.6)=%.0f%%\n",
		float64(burstLow)/n*100, float64(burstMed)/n*100, float64(burstHigh)/n*100)

	trafficTypeCounts := countByField(results, func(r ValidationResult) string { return r.TrafficType })
	fmt.Printf("Traffic Types: ")
	first := true
	for k, v := range trafficTypeCounts {
		if !first {
			fmt.Printf(", ")
		}
		fmt.Printf("%s=%.0f%%", k, float64(v)/n*100)
		first = false
	}
	fmt.Println()

	loadClassCounts := countByField(results, func(r ValidationResult) string { return r.LoadClass })
	fmt.Printf("Load Classes: ")
	first = true
	for k, v := range loadClassCounts {
		if !first {
			fmt.Printf(", ")
		}
		fmt.Printf("%s=%.0f%%", k, float64(v)/n*100)
		first = false
	}
	fmt.Println()

	fmt.Printf("\n=== Duration Distribution ===\n")
	shortTerm := 0
	mediumTerm := 0
	longTerm := 0
	for _, r := range results {
		if r.DurationHours < 24*3 {
			shortTerm++
		} else if r.DurationHours < 24*7 {
			mediumTerm++
		} else {
			longTerm++
		}
	}
	fmt.Printf("Duration: <3days=%d, 3-7days=%d, >7days=%d\n", shortTerm, mediumTerm, longTerm)
}

func formatStats(vals []float64) string {
	if len(vals) == 0 {
		return "N/A"
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	mean := sum / float64(len(sorted))
	median := sorted[len(sorted)/2]

	return fmt.Sprintf("min=%.2f, max=%.2f, mean=%.2f, median=%.2f", sorted[0], sorted[len(sorted)-1], mean, median)
}

func printDistribution(name string, counts map[string]int) {
	fmt.Printf("\n%s:\n", name)
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	total := 0
	for _, v := range counts {
		total += v
	}
	for _, k := range keys {
		pct := float64(counts[k]) / float64(total) * 100
		fmt.Printf("  %s: %d (%.1f%%)\n", k, counts[k], pct)
	}
}

func countByField(results []ValidationResult, getField func(ValidationResult) string) map[string]int {
	counts := make(map[string]int)
	for _, r := range results {
		field := getField(r)
		if field == "" {
			field = "unknown"
		}
		counts[field]++
	}
	return counts
}
