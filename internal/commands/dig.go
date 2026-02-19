package commands

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tidbcloud-insight/internal/analysis"
	"tidbcloud-insight/internal/auth"
	"tidbcloud-insight/internal/cache"
	"tidbcloud-insight/internal/client"
	"tidbcloud-insight/internal/config"

	"github.com/spf13/cobra"
)

func getInactiveClustersPath() string {
	cfg := config.Get()
	var cacheDir string
	if cfg != nil && cfg.Cache != "" {
		cacheDir = cfg.Cache
	} else {
		cacheDir = "./cache"
	}
	return filepath.Join(cacheDir, "inactive_clusters.txt")
}

func loadInactiveClusters() map[string]bool {
	path := getInactiveClustersPath()
	result := make(map[string]bool)

	file, err := os.Open(path)
	if err != nil {
		return result
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			result[line] = true
		}
	}
	return result
}

func saveInactiveCluster(clusterID string) {
	path := getInactiveClustersPath()

	inactive := loadInactiveClusters()
	if inactive[clusterID] {
		return
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	file.WriteString(clusterID + "\n")
}

func fromAPIBizType(apiBizType string) string {
	switch apiBizType {
	case "tidbcloud":
		return "dedicated"
	case "prod-tidbcloud-nextgen":
		return "premium"
	default:
		return apiBizType
	}
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%.0f", f)
}

type clusterMeta struct {
	clusterID string
	bizType   string
	vendor    string
	region    string
}

func NewDigCmd(c *cache.Cache) *cobra.Command {
	var duration string
	var jsonOutput bool
	var bizType string
	var local string

	cmd := &cobra.Command{
		Use:   "dig [cluster_id]",
		Short: "Analyze cluster for anomalies and load characteristics",
		Long: `Analyze cluster for anomalies, load patterns, daily/weekly cycles.

Examples:
  # Analyze a specific cluster
  tidbcloud-insight dig 1234567890 --duration 7d

  # Analyze a random cluster
  tidbcloud-insight dig random --duration 7d

  # Analyze a random premium cluster
  tidbcloud-insight dig random --bizType premium --duration 1h

  # Re-analyze cached data
  tidbcloud-insight dig --local 1234567890/1708320000
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if local != "" {
				runDigFromLocalCacheByID(local, jsonOutput)
				return
			}

			if len(args) == 0 {
				fmt.Fprintln(os.Stderr, "Error: cluster_id is required, or use 'dig random' or 'dig --local <id>'")
				cmd.Help()
				os.Exit(1)
			}
			clusterID := args[0]
			runDig(c, clusterMeta{clusterID: clusterID}, duration, jsonOutput)
		},
	}

	cmd.Flags().StringVar(&duration, "duration", "7d", "Duration to analyze (e.g., 7d, 1h, 30m)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	cmd.Flags().StringVar(&bizType, "bizType", "", "Business type (dedicated, premium). Used with 'random'.")
	cmd.Flags().StringVar(&local, "local", "", "Re-analyze cached data by id (format: cluster_id-timestamp)")

	cmd.AddCommand(newDigRandomCmd(c))
	cmd.AddCommand(newDigWalkCmd(c))

	return cmd
}

func newDigRandomCmd(c *cache.Cache) *cobra.Command {
	var duration string
	var jsonOutput bool
	var bizType string
	var local bool

	cmd := &cobra.Command{
		Use:   "random",
		Short: "Analyze a random cluster",
		Run: func(cmd *cobra.Command, args []string) {
			if local {
				runDigFromLocalCache(jsonOutput)
				return
			}

			meta, err := pickRandomCluster(bizType)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error picking random cluster: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Random cluster: %s (%s/%s, %s)\n\n", meta.clusterID, meta.vendor, meta.region, meta.bizType)
			runDig(c, meta, duration, jsonOutput)
		},
	}

	cmd.Flags().StringVar(&duration, "duration", "7d", "Duration to analyze (e.g., 7d, 1h, 30m)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	cmd.Flags().StringVar(&bizType, "bizType", "", "Business type (dedicated, premium). If empty, picks from both.")
	cmd.Flags().BoolVar(&local, "local", false, "Pick from local cache instead of remote")

	return cmd
}

func newDigWalkCmd(c *cache.Cache) *cobra.Command {
	var duration string
	var concurrency int

	cmd := &cobra.Command{
		Use:   "walk",
		Short: "Analyze all clusters sequentially",
		Long: `Analyze all clusters that haven't been processed yet.
Processes clusters one at a time with automatic rate limiting and backoff.

Examples:
  # Process all unprocessed clusters
  tidbcloud-insight dig walk

  # Process with custom duration
  tidbcloud-insight dig walk --duration 1d
`,
		Run: func(cmd *cobra.Command, args []string) {
			runDigWalk(c, duration, concurrency)
		},
	}

	cmd.Flags().StringVar(&duration, "duration", "7d", "Duration to analyze (e.g., 7d, 1h, 30m)")
	cmd.Flags().IntVar(&concurrency, "concurrency", 1, "Number of concurrent analyses (currently only 1 supported)")

	return cmd
}

func runDigWalk(c *cache.Cache, duration string, concurrency int) {
	cfg := config.Get()
	var cacheDir string
	if cfg != nil && cfg.Cache != "" {
		cacheDir = cfg.Cache
	} else {
		cacheDir = "./cache"
	}

	var metaDir string
	if cfg != nil && cfg.Meta != "" {
		metaDir = cfg.Meta
	} else {
		metaDir = "./meta"
	}

	processed := 0
	failed := 0
	skipped := 0

	for {
		allClusters := make(map[string]clusterMeta)

		for _, bizType := range []string{"dedicated", "premium"} {
			file := filepath.Join(metaDir, bizType, "clusters.txt")
			data, err := os.ReadFile(file)
			if err != nil {
				continue
			}

			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				parts := strings.Fields(line)
				if len(parts) < 1 {
					continue
				}
				clusterID := parts[0]

				vendor := "unknown"
				region := "unknown"
				if strings.Contains(line, "(") && strings.Contains(line, ")") {
					locStart := strings.Index(line, "(")
					locEnd := strings.Index(line, ")")
					if locStart < locEnd {
						loc := line[locStart+1 : locEnd]
						locParts := strings.Split(loc, "/")
						if len(locParts) >= 2 {
							vendor = locParts[0]
							region = locParts[1]
						}
					}
				}

				allClusters[clusterID] = clusterMeta{
					clusterID: clusterID,
					vendor:    vendor,
					region:    region,
					bizType:   bizType,
				}
			}
		}

		storage := analysis.NewDigStorage(filepath.Join(cacheDir, "dig"))
		cachedClusters, _ := storage.ListAllClusters()

		cachedSet := make(map[string]bool)
		for _, c := range cachedClusters {
			cachedSet[c] = true
		}

		inactive := loadInactiveClusters()

		var pending []clusterMeta
		for id, meta := range allClusters {
			if !cachedSet[id] && !inactive[id] {
				pending = append(pending, meta)
			}
		}

		if len(pending) == 0 {
			fmt.Println("\n============================================================")
			fmt.Printf("Walk complete! Processed: %d, Failed: %d, Skipped: %d\n", processed, failed, skipped)
			fmt.Println("============================================================")
			return
		}

		rand.Shuffle(len(pending), func(i, j int) {
			pending[i], pending[j] = pending[j], pending[i]
		})

		meta := pending[0]
		clusterID := meta.clusterID

		fmt.Printf("\n============================================================\n")
		fmt.Printf("Cluster: %s (%s/%s/%s)\n", clusterID, meta.bizType, meta.vendor, meta.region)
		fmt.Printf("Progress: %d done, %d failed, %d remaining\n", processed, failed, len(pending)-1)
		fmt.Printf("Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
		fmt.Printf("============================================================\n")

		startTime := time.Now()
		runDig(c, meta, duration, false)
		elapsed := time.Since(startTime)

		storage = analysis.NewDigStorage(filepath.Join(cacheDir, "dig"))
		newCached, _ := storage.ListAllClusters()
		nowCached := false
		for _, c := range newCached {
			if c == clusterID {
				nowCached = true
				break
			}
		}

		if nowCached {
			processed++
			fmt.Printf("\n✓ Completed in %s\n", formatDuration(elapsed))
		} else {
			failed++
			fmt.Printf("\n✗ Failed after %s\n", formatDuration(elapsed))

			if elapsed > 2*time.Minute {
				waitTime := 30 * time.Second
				cfg := config.Get()
				if cfg != nil {
					maxBackoff := cfg.RateLimit.GetMaxBackoff()
					if elapsed > 5*time.Minute {
						waitTime = maxBackoff / 4
					}
				}
				fmt.Printf("Cooling down for %s...\n", formatDuration(waitTime))
				time.Sleep(waitTime)
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func runDigFromLocalCache(jsonOutput bool) {
	cfg := config.Get()
	var cacheDir string
	if cfg != nil && cfg.Cache != "" {
		cacheDir = cfg.Cache
	} else {
		cacheDir = "./cache"
	}

	storage := analysis.NewDigStorage(filepath.Join(cacheDir, "dig"))
	clusters, err := storage.ListAllClusters()
	if err != nil || len(clusters) == 0 {
		fmt.Fprintln(os.Stderr, "No cached analysis found. Run 'dig random' first to fetch data.")
		os.Exit(1)
	}

	inactive := loadInactiveClusters()

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(clusters), func(i, j int) {
		clusters[i], clusters[j] = clusters[j], clusters[i]
	})

	excludedClusters := make(map[string]bool)
	for k, v := range inactive {
		excludedClusters[k] = v
	}

	for _, selectedCluster := range clusters {
		if inactive[selectedCluster] {
			fmt.Printf("Skipping inactive cluster: %s\n", selectedCluster)
			continue
		}

		timestamp, err := storage.GetLatestTimestamp(selectedCluster)
		if err != nil {
			continue
		}

		rawData, err := storage.LoadRawData(selectedCluster, timestamp)
		if err != nil {
			continue
		}

		if len(rawData.QPSData) == 0 {
			continue
		}

		var qpsValues []float64
		for _, p := range rawData.QPSData {
			qpsValues = append(qpsValues, p.Value)
		}
		qpsMean := mean(qpsValues)
		activityLevel := classifyTrafficLevel(qpsMean)
		isLowActivity := activityLevel == "very_low" || activityLevel == "low"

		if isLowActivity {
			excludedClusters[selectedCluster] = true
			saveInactiveCluster(selectedCluster)
			if len(excludedClusters) >= len(clusters) {
				fmt.Fprintln(os.Stderr, "All cached clusters are low activity. Try fetching new data with 'dig random'.")
				os.Exit(1)
			}
			fmt.Printf("Skipping low-activity cluster: %s (QPS mean: %.1f)\n", selectedCluster, qpsMean)
			continue
		}

		cacheID := fmt.Sprintf("%s/%d", selectedCluster, timestamp)
		fmt.Printf("Random cached analysis: %s (analyzed at %s)\n\n", cacheID, rawData.TimeStr)

		runDigAnalysisFromRawData(selectedCluster, timestamp, rawData, jsonOutput, storage)
		return
	}

	fmt.Fprintln(os.Stderr, "No suitable cached cluster found. Try fetching new data with 'dig random'.")
	os.Exit(1)
}

func runDigFromLocalCacheByID(cacheID string, jsonOutput bool) {
	cfg := config.Get()
	var cacheDir string
	if cfg != nil && cfg.Cache != "" {
		cacheDir = cfg.Cache
	} else {
		cacheDir = "./cache"
	}

	parts := strings.Split(cacheID, "/")
	if len(parts) < 2 {
		fmt.Fprintf(os.Stderr, "Invalid cache id format: %s (expected: cluster_id/timestamp)\n", cacheID)
		os.Exit(1)
	}

	timestampStr := parts[len(parts)-1]
	clusterID := strings.Join(parts[:len(parts)-1], "/")

	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid timestamp in cache id: %s\n", cacheID)
		os.Exit(1)
	}

	storage := analysis.NewDigStorage(filepath.Join(cacheDir, "dig"))
	rawData, err := storage.LoadRawData(clusterID, timestamp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load cached data for %s: %v\n", cacheID, err)
		os.Exit(1)
	}

	fmt.Printf("Re-analyzing cached data: %s (from %s)\n\n", cacheID, rawData.TimeStr)

	runDigAnalysisFromRawData(clusterID, timestamp, rawData, jsonOutput, storage)
}

func runDigAnalysisFromRawData(clusterID string, timestamp int64, rawData *analysis.DigRawData, jsonOutput bool, storage *analysis.DigStorage) {
	startTime := time.Now()

	qpsTimeSeries := rawData.QPSData
	latencyP99 := rawData.LatencyData

	if len(qpsTimeSeries) == 0 {
		if jsonOutput {
			json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
				"error":      "No QPS data in cache",
				"cluster_id": clusterID,
			})
		} else {
			fmt.Fprintln(os.Stderr, "No QPS data in cache")
		}
		return
	}

	var allTS []int64
	var qpsValues []float64
	for _, p := range qpsTimeSeries {
		allTS = append(allTS, p.Timestamp)
		qpsValues = append(qpsValues, p.Value)
	}

	sort.Slice(allTS, func(i, j int) bool { return allTS[i] < allTS[j] })

	firstTS := allTS[0]
	lastTS := allTS[len(allTS)-1]
	durationH := float64(lastTS-firstTS) / 3600
	durationDays := durationH / 24
	if durationDays == 0 {
		durationDays = 1
	}

	qpsMean := mean(qpsValues)
	activityLevel := classifyTrafficLevel(qpsMean)
	isLowActivity := activityLevel == "very_low" || activityLevel == "low"

	var uniqueAnomalies []analysis.DetectedAnomaly
	var anomalies []map[string]interface{}
	var workloadProfile *analysis.WorkloadProfile
	var loadProfile *analysis.LoadProfile

	if isLowActivity {
		saveInactiveCluster(clusterID)
		loadProfile = analysis.AnalyzeLoadProfileWithWorkload(clusterID, qpsTimeSeries, latencyP99, nil, nil, nil, nil)
		if loadProfile != nil {
			loadProfile.Characteristics.LoadClass = "low_activity"
		}
	} else {
		detector := analysis.NewAnomalyDetector(analysis.DefaultAnomalyConfig())
		detectedAnomalies := detector.DetectAll(qpsTimeSeries)

		hourlyAnomalies := analysis.DetectHourlyBaselineAnomalies(qpsTimeSeries, analysis.DefaultAnomalyConfig())
		detectedAnomalies = append(detectedAnomalies, hourlyAnomalies...)

		if len(latencyP99) > 0 {
			latencyAnomalies := detector.DetectLatencyAnomalies(nil, latencyP99)
			detectedAnomalies = append(detectedAnomalies, latencyAnomalies...)
		}

		uniqueAnomalies = analysis.MergeConsecutiveAnomalies(detectedAnomalies)

		for _, a := range uniqueAnomalies {
			anomalies = append(anomalies, map[string]interface{}{
				"type":     string(a.Type),
				"severity": string(a.Severity),
				"time":     a.TimeStr,
				"detail":   a.Detail,
			})
		}

		loadProfile = analysis.AnalyzeLoadProfileWithWorkload(clusterID, qpsTimeSeries, latencyP99, nil, nil, nil, nil)
	}

	anomalyRecord := &analysis.DigAnomalyRecord{
		ClusterID:  clusterID,
		Timestamp:  startTime.Unix(),
		TimeStr:    startTime.Format("2006-01-02 15:04"),
		Duration:   rawData.Duration,
		TotalCount: len(uniqueAnomalies),
		PerDay:     float64(len(uniqueAnomalies)) / durationDays,
		ByType:     make(map[string]int),
		Anomalies:  uniqueAnomalies,
		Summary: map[string]interface{}{
			"duration_hours": durationH,
			"samples":        len(allTS),
			"qps_mean":       qpsMean,
			"activity_level": activityLevel,
			"low_activity":   isLowActivity,
		},
	}
	for _, a := range uniqueAnomalies {
		anomalyRecord.ByType[string(a.Type)]++
	}
	storage.SaveAnomalies(clusterID, anomalyRecord)

	if jsonOutput {
		elapsed := time.Since(startTime)
		result := map[string]interface{}{
			"cluster_id":        clusterID,
			"source_time":       rawData.TimeStr,
			"duration_hours":    durationH,
			"samples":           len(allTS),
			"qps_mean":          qpsMean,
			"activity_level":    activityLevel,
			"low_activity":      isLowActivity,
			"total_anomalies":   len(anomalies),
			"anomalies_per_day": float64(len(anomalies)) / durationDays,
			"anomalies":         anomalies,
			"analysis_time_ms":  elapsed.Milliseconds(),
		}
		if workloadProfile != nil {
			result["workload"] = workloadProfile
		}
		if loadProfile != nil {
			result["load_profile"] = loadProfile
		}
		json.NewEncoder(os.Stdout).Encode(result)
		return
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("RE-ANALYSIS REPORT - %s\n", clusterID)
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Source Data:    %s\n", rawData.TimeStr)
	fmt.Printf("Time Range:     %s ~ %s\n",
		time.Unix(firstTS, 0).Format("2006-01-02 15:04"),
		time.Unix(lastTS, 0).Format("2006-01-02 15:04"))
	fmt.Printf("Duration:       %.1f hours (%.1f days)\n", durationH, durationDays)
	fmt.Printf("Samples:        %d\n\n", len(allTS))

	if len(qpsValues) > 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("QPS Summary")
		fmt.Println(strings.Repeat("-", 60))

		fmt.Printf("  Mean: %s", formatFloat(qpsMean))
		if isLowActivity {
			fmt.Printf(" (%s activity)", activityLevel)
		}
		fmt.Println()

		if qpsMean >= 10 {
			peakToAvg := max(qpsValues) / qpsMean
			if peakToAvg < 10 || qpsMean > 100 {
				fmt.Printf("  Min:  %s\n", formatFloat(min(qpsValues)))
				fmt.Printf("  Max:  %s\n", formatFloat(max(qpsValues)))
			} else {
				fmt.Printf("  Peak/Avg: %.1fx (highly bursty)\n", peakToAvg)
			}
		}
		fmt.Println()
	}

	if isLowActivity {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Printf("ACTIVITY LEVEL: %s\n", strings.ToUpper(activityLevel))
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("  Low activity cluster - detailed analysis skipped")
		fmt.Printf("  Data cached for future reference: %s/%d\n", clusterID, timestamp)
		fmt.Println()

		elapsed := time.Since(startTime)
		fmt.Printf("Re-analysis completed in %s\n", formatDuration(elapsed))
		return
	}

	byType := make(map[string][]map[string]interface{})
	for _, a := range anomalies {
		t := a["type"].(string)
		byType[t] = append(byType[t], a)
	}

	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Anomalies Summary (total: %d, per day: %.1f)\n", len(anomalies), float64(len(anomalies))/durationDays)
	fmt.Println(strings.Repeat("-", 60))
	for _, atype := range []string{
		"QPS_DROP", "QPS_SPIKE",
		"SUSTAINED_LOW_QPS", "SUSTAINED_HIGH_QPS",
		"VOLATILITY_SPIKE",
		"LATENCY_SPIKE", "LATENCY_DEGRADED",
		"INSTANCE_IMBALANCE",
	} {
		if items, exists := byType[atype]; exists {
			fmt.Printf("  %s: %d\n", atype, len(items))
		}
	}
	fmt.Println()

	for _, atype := range []string{
		"QPS_DROP", "QPS_SPIKE",
		"SUSTAINED_LOW_QPS", "SUSTAINED_HIGH_QPS",
		"VOLATILITY_SPIKE",
		"LATENCY_SPIKE", "LATENCY_DEGRADED",
		"INSTANCE_IMBALANCE",
	} {
		if items, exists := byType[atype]; exists {
			fmt.Println(strings.Repeat("-", 60))
			fmt.Printf("[%s] %d events\n", atype, len(items))
			fmt.Println(strings.Repeat("-", 60))
			for i, a := range items {
				if i >= 10 {
					break
				}
				timeStr, _ := a["time"].(string)
				if timeStr != "" {
					fmt.Printf("  [%s] %s: %s\n", a["severity"], timeStr, a["detail"])
				} else {
					fmt.Printf("  [%s] %s\n", a["severity"], a["detail"])
				}
			}
			if len(items) > 10 {
				fmt.Printf("  ... and %d more\n", len(items)-10)
			}
			fmt.Println()
		}
	}

	if len(anomalies) == 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("No significant anomalies detected")
		fmt.Println(strings.Repeat("-", 60))
	}

	if workloadProfile != nil {
		analysis.PrintWorkloadProfile(workloadProfile)
	}

	if loadProfile != nil {
		if durationDays >= 1 {
			analysis.PrintDailyPattern(loadProfile)
			if durationDays >= 3 {
				analysis.PrintWeeklyPattern(loadProfile)
			}
		}
	}

	if len(qpsTimeSeries) >= 64 {
		advancedProfiling := analysis.AnalyzeAdvancedProfile(qpsTimeSeries, analysis.DefaultAdvancedProfilingConfig())
		if advancedProfiling != nil {
			analysis.PrintAdvancedProfiling(advancedProfiling)
		}
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\nRe-analysis completed in %s\n", formatDuration(elapsed))
}

func pickRandomCluster(bizType string) (clusterMeta, error) {
	cfg := config.Get()
	var metaDir string
	if cfg != nil && cfg.Meta != "" {
		metaDir = cfg.Meta
	} else {
		metaDir = "./meta"
	}

	var files []struct {
		path    string
		bizType string
	}
	if bizType == "" {
		files = []struct {
			path    string
			bizType string
		}{
			{filepath.Join(metaDir, "dedicated", "clusters.txt"), "dedicated"},
			{filepath.Join(metaDir, "premium", "clusters.txt"), "premium"},
		}
	} else {
		files = []struct {
			path    string
			bizType string
		}{
			{filepath.Join(metaDir, bizType, "clusters.txt"), bizType},
		}
	}

	inactive := loadInactiveClusters()

	var allClusters []clusterMeta
	for _, f := range files {
		data, err := os.ReadFile(f.path)
		if err != nil {
			continue
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			meta := parseClusterLine(line, f.bizType)
			if meta.clusterID != "" && !inactive[meta.clusterID] {
				allClusters = append(allClusters, meta)
			}
		}
	}

	if len(allClusters) == 0 {
		return clusterMeta{}, fmt.Errorf("no clusters found in meta files (excluding inactive)")
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return allClusters[r.Intn(len(allClusters))], nil
}

func parseClusterLine(line, bizType string) clusterMeta {
	// Format: "cluster_id  [name]  (vendor/region)"
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return clusterMeta{}
	}

	clusterID := parts[0]

	// Find (vendor/region) part
	for _, part := range parts {
		if strings.HasPrefix(part, "(") && strings.HasSuffix(part, ")") {
			inner := strings.Trim(part, "()")
			vr := strings.SplitN(inner, "/", 2)
			if len(vr) == 2 {
				return clusterMeta{
					clusterID: clusterID,
					bizType:   bizType,
					vendor:    vr[0],
					region:    vr[1],
				}
			}
		}
	}

	return clusterMeta{clusterID: clusterID, bizType: bizType}
}

func runDig(c *cache.Cache, meta clusterMeta, duration string, jsonOutput bool) {
	startTime := time.Now()
	durationSeconds := parseDuration(duration)

	clusterID := meta.clusterID

	if !jsonOutput {
		authMgr := auth.GetManager()
		_, _ = authMgr.GetToken()
		source := authMgr.GetTokenSource()
		if source == "cache" {
			fmt.Printf("Auth: from cache\n")
		} else {
			fmt.Printf("Auth: freshly fetched\n")
		}
		fmt.Printf("Querying cluster info for: %s\n", clusterID)
	}

	cl := client.NewClient(c)
	var cluster *client.Cluster
	ctx := context.Background()

	// If we have vendor/region/bizType from meta, use them directly
	if meta.vendor != "" && meta.region != "" && meta.bizType != "" {
		apiBizType := toAPIBizType(meta.bizType)
		if !jsonOutput {
			fmt.Printf("Looking up cluster at %s/%s (%s)...\n", meta.vendor, meta.region, meta.bizType)
		}
		clusters, err := cl.ListClustersPaginated(ctx, meta.vendor, meta.region, apiBizType, 500)
		if err == nil {
			for i := range clusters {
				if clusters[i].ApplicationID == clusterID {
					cluster = &clusters[i]
					break
				}
			}
		}
	}

	// Fallback: exhaustive search if not found
	if cluster == nil {
		bizTypes := []string{"tidbcloud", "prod-tidbcloud-nextgen", "prod-devtier", "prod-dedicated"}
		vendors := []string{"aws", "gcp", "alicloud"}
		regions := []string{
			"us-west-2", "us-east-1", "us-east-2",
			"eu-central-1", "eu-west-1", "eu-west-2",
			"ap-southeast-1", "ap-southeast-2",
			"ap-northeast-1", "ap-northeast-2", "ap-south-1",
		}

		for _, bt := range bizTypes {
			for _, v := range vendors {
				for _, r := range regions {
					clusters, err := cl.ListClustersPaginated(ctx, v, r, bt, 100)
					if err != nil {
						continue
					}
					for i := range clusters {
						if clusters[i].ApplicationID == clusterID {
							cluster = &clusters[i]
							break
						}
					}
					if cluster != nil {
						break
					}
				}
				if cluster != nil {
					break
				}
			}
			if cluster != nil {
				break
			}
		}
	}

	if cluster == nil {
		fmt.Fprintf(os.Stderr, "No cluster found for %s\n", clusterID)
		os.Exit(1)
	}

	provider := cluster.Vendor
	region := cluster.Region
	bizType := cluster.BizType
	displayBizType := fromAPIBizType(bizType)
	dsURL := cluster.InternalReadURI

	if !jsonOutput {
		fmt.Printf("Provider:        %s\n", provider)
		fmt.Printf("Region:          %s\n", region)
		fmt.Printf("BizType:         %s\n\n", displayBizType)
	}

	if dsURL == "" {
		fmt.Fprintln(os.Stderr, "No internal URL available")
		os.Exit(1)
	}

	endTS := int(time.Now().Unix())
	startTS := endTS - durationSeconds

	if !jsonOutput {
		fmt.Printf("Querying metrics (last %s)...\n", duration)
		fmt.Printf("  Metrics: qps, latency, sqlType, tikvOp, tikvLatency\n\n")
	}

	var initialStep int
	if durationSeconds <= 3600 {
		initialStep = 15
	} else if durationSeconds <= 86400 {
		initialStep = 30
	} else if durationSeconds <= 86400*3 {
		initialStep = 60
	} else {
		initialStep = 120
	}

	qpsResult, latencyResult, sqlTypeResult, tikvOpResult, tikvLatencyResult := fetchDigMetricsConcurrent(ctx, cl, dsURL, startTS, endTS, initialStep, jsonOutput)
	if qpsResult == nil {
		saveInactiveCluster(clusterID)
		if jsonOutput {
			json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
				"error":      "No QPS data",
				"cluster_id": clusterID,
			})
		} else {
			fmt.Fprintln(os.Stderr, "No QPS data found")
		}
		return
	}

	topology := fetchClusterTopology(ctx, cl, dsURL)

	runDigAnalysis(clusterID, provider, region, bizType, qpsResult, latencyResult, sqlTypeResult, tikvOpResult, tikvLatencyResult, durationSeconds, jsonOutput, startTime, topology)
}

type ClusterTopology struct {
	TiDBInstances    []InstanceInfo
	TiKVInstances    []InstanceInfo
	PDInstances      []InstanceInfo
	TiFlashInstances []InstanceInfo
	TiCDCInstances   []InstanceInfo
	TiDBVersion      string
	TiKVVersion      string
	PDVersion        string
}

type InstanceInfo struct {
	Instance string
	Job      string
	Status   string
}

func fetchDigMetricsConcurrent(ctx context.Context, cl *client.Client, dsURL string, startTS, endTS, step int, silent bool) (qpsResult, latencyResult, sqlTypeResult, tikvOpResult, tikvLatencyResult map[string]interface{}) {
	var mu sync.Mutex
	var wg sync.WaitGroup

	type metricResult struct {
		name   string
		result map[string]interface{}
		step   int
		err    error
	}
	results := make(chan metricResult, 5)

	metrics := []struct {
		name   string
		metric string
	}{
		{"qps", "tidb_server_query_total"},
		{"latency", "tidb_server_handle_query_duration_seconds_bucket"},
		{"sqlType", "tidb_executor_statement_total"},
		{"tikvOp", "tikv_grpc_msg_duration_seconds_count"},
		{"tikvLatency", "tikv_grpc_msg_duration_seconds_bucket"},
	}

	var sem chan struct{}
	if cc := cl.GetConcurrencyController(); cc != nil {
		sem = make(chan struct{}, cc.GetCurrentConcurrency())
	} else {
		sem = make(chan struct{}, 3)
	}

	completed := 0
	total := len(metrics)

	for _, m := range metrics {
		wg.Add(1)
		go func(name, metric string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			result, actualStep, err := cl.QueryMetricWithRetry(ctx, dsURL, metric, startTS, endTS, step)
			_ = actualStep

			results <- metricResult{name: name, result: result, step: actualStep, err: err}

			if !silent {
				mu.Lock()
				completed++
				if err != nil {
					fmt.Printf("  * (%d/%d) %s: FAILED (%v)\n", completed, total, name, err)
				} else {
					fmt.Printf("  * (%d/%d) %s: OK\n", completed, total, name)
				}
				mu.Unlock()
			}
		}(m.name, m.metric)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	progressTicker := time.NewTicker(15 * time.Second)
	defer progressTicker.Stop()

	for {
		select {
		case r, ok := <-results:
			if !ok {
				return
			}
			mu.Lock()
			switch r.name {
			case "qps":
				qpsResult = r.result
			case "latency":
				latencyResult = r.result
			case "sqlType":
				sqlTypeResult = r.result
			case "tikvOp":
				tikvOpResult = r.result
			case "tikvLatency":
				tikvLatencyResult = r.result
			}
			mu.Unlock()
		case <-progressTicker.C:
			if !silent {
				mu.Lock()
				pending := total - completed
				concurrencyInfo := ""
				if cc := cl.GetConcurrencyController(); cc != nil {
					concurrencyInfo = fmt.Sprintf(", concurrency: %d", cc.GetCurrentConcurrency())
				}
				bytesInfo := formatBytes(cl.GetBytesReceived())
				if pending > 0 {
					fmt.Printf("  [INFO] Still fetching metrics%s, %d/%d completed, %s received...\n", concurrencyInfo, completed, total, bytesInfo)
				}
				mu.Unlock()
			}
		}
	}
}

func fetchClusterTopology(ctx context.Context, cl *client.Client, dsURL string) *ClusterTopology {
	topology := &ClusterTopology{}

	now := time.Now().Unix()

	instanceMap := make(map[string]*InstanceInfo)

	tidbInfoResult, _ := cl.QueryMetric(ctx, dsURL, "tidb_server_query_total", int(now)-300, int(now), 60)
	tidbInfoData := getResults(tidbInfoResult)
	for _, series := range tidbInfoData {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}
		if instance == "" {
			continue
		}

		if instanceMap[instance] == nil {
			instanceMap[instance] = &InstanceInfo{
				Instance: instance,
				Job:      "tidb",
				Status:   "up",
			}
		}
	}

	tidbInfoResult2, _ := cl.QueryMetric(ctx, dsURL, "tidb_server_info", int(now)-300, int(now), 60)
	tidbInfoData2 := getResults(tidbInfoResult2)
	for _, series := range tidbInfoData2 {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}
		if instance == "" {
			continue
		}

		version := getLabel(series, "version")
		if version != "" && topology.TiDBVersion == "" {
			topology.TiDBVersion = version
		}
	}

	tikvEngineResult, _ := cl.QueryMetric(ctx, dsURL, "tikv_engine_size_bytes", int(now)-300, int(now), 60)
	tikvEngineData := getResults(tikvEngineResult)
	for _, series := range tikvEngineData {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}
		if instance == "" {
			continue
		}

		if instanceMap[instance] == nil {
			instanceMap[instance] = &InstanceInfo{
				Instance: instance,
				Job:      "tikv",
				Status:   "up",
			}
		}
	}

	tikvGaugeResult, _ := cl.QueryMetric(ctx, dsURL, "tikv_engine_write_bytes", int(now)-300, int(now), 60)
	tikvGaugeData := getResults(tikvGaugeResult)
	for _, series := range tikvGaugeData {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}
		if instance == "" {
			continue
		}

		if instanceMap[instance] == nil {
			instanceMap[instance] = &InstanceInfo{
				Instance: instance,
				Job:      "tikv",
				Status:   "up",
			}
		}
	}

	tikvInfoResult, _ := cl.QueryMetric(ctx, dsURL, "tikv_build_info", int(now)-300, int(now), 60)
	tikvInfoData := getResults(tikvInfoResult)
	for _, series := range tikvInfoData {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}

		version := getLabel(series, "release_version")
		if version == "" {
			version = getLabel(series, "version")
		}
		if version != "" && topology.TiKVVersion == "" {
			topology.TiKVVersion = version
		}
	}

	pdRegionResult, _ := cl.QueryMetric(ctx, dsURL, "pd_cluster_region_count", int(now)-300, int(now), 60)
	pdRegionData := getResults(pdRegionResult)
	for _, series := range pdRegionData {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}
		if instance == "" {
			continue
		}

		if instanceMap[instance] == nil {
			instanceMap[instance] = &InstanceInfo{
				Instance: instance,
				Job:      "pd",
				Status:   "up",
			}
		}
	}

	pdInfoResult, _ := cl.QueryMetric(ctx, dsURL, "pd_cluster_status", int(now)-300, int(now), 60)
	pdInfoData := getResults(pdInfoResult)
	for _, series := range pdInfoData {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}

		version := getLabel(series, "version")
		if version != "" && topology.PDVersion == "" {
			topology.PDVersion = version
		}
	}

	upResult, _ := cl.QueryMetric(ctx, dsURL, "up", int(now)-300, int(now), 60)
	upData := getResults(upResult)
	for _, series := range upData {
		instance := getLabel(series, "instance")
		if instance == "" {
			instance = getLabel(series, "address")
		}
		if instance == "" {
			continue
		}

		job := getLabel(series, "job")

		if instanceMap[instance] == nil {
			instanceMap[instance] = &InstanceInfo{
				Instance: instance,
				Job:      job,
				Status:   "unknown",
			}
		} else if instanceMap[instance].Job == "" {
			instanceMap[instance].Job = job
		}

		values := getValues(series)
		if len(values) > 0 && len(values[len(values)-1]) >= 2 {
			v := analysis.ToFloat64(values[len(values)-1][1])
			if v == 1 {
				instanceMap[instance].Status = "up"
			} else {
				instanceMap[instance].Status = "down"
			}
		}
	}

	for instance, info := range instanceMap {
		job := strings.ToLower(info.Job)
		instLower := strings.ToLower(instance)

		if job == "" {
			if strings.Contains(instLower, "tidb") {
				job = "tidb"
			} else if strings.Contains(instLower, "tikv") {
				job = "tikv"
			} else if strings.Contains(instLower, "pd") && !strings.Contains(instLower, "tipd") {
				job = "pd"
			} else if strings.Contains(instLower, "tiflash") {
				job = "tiflash"
			} else if strings.Contains(instLower, "ticdc") {
				job = "ticdc"
			}
			info.Job = job
		}

		switch {
		case strings.Contains(job, "tidb") && !strings.Contains(job, "tikv"):
			topology.TiDBInstances = append(topology.TiDBInstances, *info)
		case strings.Contains(job, "tikv") && !strings.Contains(job, "tiflash"):
			topology.TiKVInstances = append(topology.TiKVInstances, *info)
		case job == "pd" || (strings.Contains(job, "pd") && !strings.Contains(job, "tidb") && !strings.Contains(job, "tikv")):
			topology.PDInstances = append(topology.PDInstances, *info)
		case strings.Contains(job, "tiflash"):
			topology.TiFlashInstances = append(topology.TiFlashInstances, *info)
		case strings.Contains(job, "ticdc"):
			topology.TiCDCInstances = append(topology.TiCDCInstances, *info)
		}
	}

	return topology
}

func runDigAnalysis(clusterID, provider, region, bizType string, qpsResult, latencyResult, sqlTypeResult, tikvOpResult, tikvLatencyResult map[string]interface{}, durationSeconds int, jsonOutput bool, startTime time.Time, topology *ClusterTopology) {
	qpsData := getResults(qpsResult)
	latencyData := getResults(latencyResult)
	sqlTypeData := getResults(sqlTypeResult)
	tikvOpData := getResults(tikvOpResult)
	tikvLatencyData := getResults(tikvLatencyResult)

	cfg := config.Get()
	var storage *analysis.DigStorage
	if cfg != nil && cfg.Cache != "" {
		storage = analysis.NewDigStorage(filepath.Join(cfg.Cache, "dig"))
	}

	qpsRates := calcQPSRates(qpsData)
	qpsRatesByInstAndType := calcQPSRatesByInstAndType(qpsData)
	latencyByInst := calcLatencyByInstP99(latencyData)
	tikvOpRatesByInst := calcTiKVOpRatesByInst(tikvOpData)
	tikvLatencyByInst := calcTiKVLatencyByInstP99(tikvLatencyData)
	_ = calcLatencyPercentiles(latencyData)

	var allTS []int64
	tsSet := make(map[int64]bool)
	for inst := range qpsRates {
		for ts := range qpsRates[inst] {
			if !tsSet[ts] {
				tsSet[ts] = true
				allTS = append(allTS, ts)
			}
		}
	}

	if len(allTS) == 0 {
		if jsonOutput {
			json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
				"error":      "No valid QPS data",
				"cluster_id": clusterID,
			})
		} else {
			fmt.Fprintln(os.Stderr, "No valid QPS data")
		}
		return
	}

	sort.Slice(allTS, func(i, j int) bool { return allTS[i] < allTS[j] })

	firstTS := allTS[0]
	lastTS := allTS[len(allTS)-1]
	durationH := float64(lastTS-firstTS) / 3600
	durationDays := durationH / 24
	if durationDays == 0 {
		durationDays = 1
	}

	totalQPSTS := make(map[int64]float64)
	for inst := range qpsRates {
		for ts, qps := range qpsRates[inst] {
			totalQPSTS[ts] += qps
		}
	}

	var qpsValues []float64
	for _, ts := range allTS {
		if qps, exists := totalQPSTS[ts]; exists {
			qpsValues = append(qpsValues, qps)
		}
	}

	qpsMean := mean(qpsValues)
	activityLevel := classifyTrafficLevel(qpsMean)
	isLowActivity := activityLevel == "very_low" || activityLevel == "low"

	var qpsTimeSeries []analysis.TimeSeriesPoint
	for _, ts := range allTS {
		if qps, exists := totalQPSTS[ts]; exists {
			qpsTimeSeries = append(qpsTimeSeries, analysis.TimeSeriesPoint{
				Timestamp: ts,
				Value:     qps,
			})
		}
	}

	var uniqueAnomalies []analysis.DetectedAnomaly
	var anomalies []map[string]interface{}
	var workloadProfile *analysis.WorkloadProfile
	var loadProfile *analysis.LoadProfile
	var sqlTypeTS map[string][]analysis.TimeSeriesPoint
	latencyP99 := calcLatencyP99Series(latencyData)

	if isLowActivity {
		saveInactiveCluster(clusterID)
		loadProfile = analysis.AnalyzeLoadProfileWithWorkload(clusterID, qpsTimeSeries, latencyP99, nil, nil, nil, nil)
		if loadProfile != nil {
			loadProfile.Characteristics.LoadClass = "low_activity"
		}
	} else {
		detector := analysis.NewAnomalyDetector(analysis.DefaultAnomalyConfig())
		detectedAnomalies := detector.DetectAll(qpsTimeSeries)

		hourlyAnomalies := analysis.DetectHourlyBaselineAnomalies(qpsTimeSeries, analysis.DefaultAnomalyConfig())
		detectedAnomalies = append(detectedAnomalies, hourlyAnomalies...)

		if len(latencyP99) > 0 {
			latencyAnomalies := detector.DetectLatencyAnomalies(nil, latencyP99)
			detectedAnomalies = append(detectedAnomalies, latencyAnomalies...)
		}

		sqlTypeRates := calcSQLTypeRates(sqlTypeData)
		tikvOpRates := calcTiKVOpRates(tikvOpData)
		tikvLatencyP99 := calcTiKVLatencyP99Series(tikvLatencyData)

		if len(sqlTypeRates) > 0 || len(tikvOpRates) > 0 {
			sqlTypeTS = convertToTimeSeriesMap(sqlTypeRates)
			tikvOpTS := convertToTimeSeriesMap(tikvOpRates)

			workloadProfile = analysis.AnalyzeWorkloadProfile(sqlTypeTS, nil, tikvOpTS, nil)

			workloadAnomalies := analysis.DetectWorkloadAnomalies(sqlTypeTS, tikvOpTS, analysis.DefaultAnomalyConfig())
			for _, wa := range workloadAnomalies {
				detectedAnomalies = append(detectedAnomalies, analysis.DetectedAnomaly{
					Type:      wa.Type,
					Severity:  wa.Severity,
					Timestamp: wa.Timestamp,
					TimeStr:   wa.TimeStr,
					Value:     wa.Value,
					Baseline:  wa.Baseline,
					Detail:    wa.Detail,
				})
			}
		}

		_ = tikvLatencyP99

		instTimeSeries := make(map[string][]analysis.TimeSeriesPoint)
		for inst, rates := range qpsRates {
			for ts, qps := range rates {
				instTimeSeries[inst] = append(instTimeSeries[inst], analysis.TimeSeriesPoint{
					Timestamp: ts,
					Value:     qps,
				})
			}
		}
		imbalanceAnomalies := detector.DetectInstanceImbalance(instTimeSeries)
		detectedAnomalies = append(detectedAnomalies, imbalanceAnomalies...)

		detailedImbalance := analysis.DetectDetailedImbalance(
			qpsRatesByInstAndType,
			latencyByInst,
			tikvOpRatesByInst,
			tikvLatencyByInst,
			analysis.DefaultAnomalyConfig(),
		)
		detectedAnomalies = append(detectedAnomalies, detailedImbalance...)

		ensembleDetector := analysis.NewEnsembleAnomalyDetector(analysis.DefaultEnsembleAnomalyConfig())
		ensembleAnomalies := ensembleDetector.DetectAll(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, ensembleAnomalies...)

		dynamicThresholdDetector := analysis.NewDynamicThresholdDetector(analysis.DefaultDynamicThresholdConfig())
		_, dynamicAnomalies := dynamicThresholdDetector.ComputeThresholds(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, dynamicAnomalies...)

		contextualDetector := analysis.NewContextualAnomalyDetector(analysis.DefaultContextualAnomalyConfig())
		contextualAnomalies := contextualDetector.Detect(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, contextualAnomalies...)

		srDetector := analysis.NewSpectralResidualDetector(analysis.DefaultSRConfig())
		srAnomalies := srDetector.Detect(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, srAnomalies...)

		multiScaleDetector := analysis.NewMultiScaleAnomalyDetector(analysis.DefaultMultiScaleConfig())
		multiScaleAnomalies := multiScaleDetector.Detect(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, multiScaleAnomalies...)

		dbscanDetector := analysis.NewDBSCANAnomalyDetector(analysis.DefaultDBSCANConfig())
		dbscanAnomalies := dbscanDetector.Detect(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, dbscanAnomalies...)

		hwDetector := analysis.NewHoltWintersDetector(analysis.DefaultHoltWintersConfig())
		hwAnomalies := hwDetector.Detect(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, hwAnomalies...)

		peakDetector := analysis.NewPeakDetector(analysis.DefaultPeakDetectorConfig())
		peakAnomalies := peakDetector.DetectPeakAnomalies(qpsTimeSeries)
		detectedAnomalies = append(detectedAnomalies, peakAnomalies...)

		sort.Slice(detectedAnomalies, func(i, j int) bool {
			return detectedAnomalies[i].Timestamp < detectedAnomalies[j].Timestamp
		})

		seenAnomalies := make(map[string]bool)
		for _, a := range detectedAnomalies {
			key := a.TimeStr + string(a.Type)
			if !seenAnomalies[key] {
				seenAnomalies[key] = true
				uniqueAnomalies = append(uniqueAnomalies, a)
			}
		}

		for _, a := range uniqueAnomalies {
			anomaly := map[string]interface{}{
				"type":     string(a.Type),
				"severity": string(a.Severity),
				"time":     a.TimeStr,
				"value":    a.Value,
				"detail":   a.Detail,
			}
			if a.Instance != "" {
				anomaly["instance"] = a.Instance
			}
			if a.Duration > 0 {
				anomaly["duration"] = a.Duration
			}
			anomalies = append(anomalies, anomaly)
		}

		loadProfile = analysis.AnalyzeLoadProfileWithWorkload(clusterID, qpsTimeSeries, latencyP99, nil, nil, nil, nil)
	}

	hourlyQPS := make(map[int][]float64)
	for _, ts := range allTS {
		if qps, exists := totalQPSTS[ts]; exists {
			hour := time.Unix(ts, 0).Hour()
			hourlyQPS[hour] = append(hourlyQPS[hour], qps)
		}
	}

	hourlyMedian := make(map[int]float64)
	for hour, vals := range hourlyQPS {
		sortedVals := make([]float64, len(vals))
		copy(sortedVals, vals)
		sort.Float64s(sortedVals)
		hourlyMedian[hour] = sortedVals[len(sortedVals)/2]
	}

	var advancedProfiling *analysis.AdvancedProfilingResult
	if len(qpsTimeSeries) >= 64 {
		advancedProfiling = analysis.AnalyzeAdvancedProfileWithSQLTypes(qpsTimeSeries, sqlTypeTS, analysis.DefaultAdvancedProfilingConfig())
		if advancedProfiling != nil && loadProfile != nil {
			loadProfile.EnhanceWithAdvancedAnalysis(qpsTimeSeries)
		}
	}

	var rawPath, anomalyPath, profilePath string
	var cacheTimestamp int64
	if storage != nil {
		now := time.Now()
		cacheTimestamp = now.Unix()

		topologyData := map[string]interface{}{}
		if topology != nil {
			topologyData["tidb_count"] = len(topology.TiDBInstances)
			topologyData["tikv_count"] = len(topology.TiKVInstances)
			topologyData["pd_count"] = len(topology.PDInstances)
			topologyData["tiflash_count"] = len(topology.TiFlashInstances)
			topologyData["ticdc_count"] = len(topology.TiCDCInstances)
			if topology.TiDBVersion != "" {
				topologyData["tidb_version"] = topology.TiDBVersion
			}
			if topology.TiKVVersion != "" {
				topologyData["tikv_version"] = topology.TiKVVersion
			}
			if topology.PDVersion != "" {
				topologyData["pd_version"] = topology.PDVersion
			}
		}

		rawData := &analysis.DigRawData{
			ClusterID:       clusterID,
			Timestamp:       cacheTimestamp,
			TimeStr:         now.Format("2006-01-02 15:04"),
			Duration:        durationSeconds,
			QPSData:         qpsTimeSeries,
			LatencyData:     latencyP99,
			SQLTypeData:     sqlTypeResult,
			TiKVOpData:      tikvOpResult,
			TiKVLatencyData: tikvLatencyResult,
			TopologyData:    topologyData,
		}
		rawPath, _ = storage.SaveRawData(clusterID, rawData)

		anomalyRecord := &analysis.DigAnomalyRecord{
			ClusterID:  clusterID,
			Timestamp:  cacheTimestamp,
			TimeStr:    now.Format("2006-01-02 15:04"),
			Duration:   durationSeconds,
			TotalCount: len(uniqueAnomalies),
			PerDay:     float64(len(uniqueAnomalies)) / durationDays,
			ByType:     make(map[string]int),
			Anomalies:  uniqueAnomalies,
			Summary: map[string]interface{}{
				"provider":       provider,
				"region":         region,
				"biz_type":       fromAPIBizType(bizType),
				"duration_hours": durationH,
				"samples":        len(allTS),
				"qps_mean":       qpsMean,
				"activity_level": activityLevel,
				"low_activity":   isLowActivity,
			},
		}
		for _, a := range uniqueAnomalies {
			anomalyRecord.ByType[string(a.Type)]++
		}
		anomalyPath, _ = storage.SaveAnomalies(clusterID, anomalyRecord)

		if loadProfile == nil {
			loadProfile = analysis.AnalyzeLoadProfileWithWorkload(clusterID, qpsTimeSeries, latencyP99, nil, nil, nil, nil)
		}

		profileRecord := &analysis.DigProfileRecord{
			ClusterID:       clusterID,
			Timestamp:       cacheTimestamp,
			TimeStr:         now.Format("2006-01-02 15:04"),
			Duration:        durationSeconds,
			Provider:        provider,
			Region:          region,
			BizType:         fromAPIBizType(bizType),
			Topology:        topologyData,
			LoadProfile:     loadProfile,
			WorkloadProfile: workloadProfile,
			ActivityLevel:   activityLevel,
			QPSMean:         qpsMean,
		}
		if topology != nil {
			profileRecord.TiDBVersion = topology.TiDBVersion
			profileRecord.TiKVVersion = topology.TiKVVersion
			profileRecord.PDVersion = topology.PDVersion
		}
		profilePath, _ = storage.SaveProfile(clusterID, profileRecord)
	}

	if jsonOutput {
		elapsed := time.Since(startTime)
		result := map[string]interface{}{
			"cluster_id":        clusterID,
			"provider":          provider,
			"region":            region,
			"biz_type":          fromAPIBizType(bizType),
			"duration_hours":    durationH,
			"samples":           len(allTS),
			"qps_mean":          qpsMean,
			"activity_level":    activityLevel,
			"low_activity":      isLowActivity,
			"total_anomalies":   len(anomalies),
			"anomalies_per_day": float64(len(anomalies)) / durationDays,
			"anomalies":         anomalies,
			"analysis_time_ms":  elapsed.Milliseconds(),
			"topology":          topology,
		}
		if cacheTimestamp > 0 {
			result["cache_id"] = fmt.Sprintf("%s/%d", clusterID, cacheTimestamp)
		}
		if workloadProfile != nil {
			result["workload"] = workloadProfile
		}
		if loadProfile != nil {
			result["load_profile"] = loadProfile
		}
		if advancedProfiling != nil {
			result["advanced_profiling"] = advancedProfiling
		}
		json.NewEncoder(os.Stdout).Encode(result)
		return
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("ANALYSIS REPORT - %s\n", clusterID)
	fmt.Println(strings.Repeat("=", 60))

	if topology != nil && (len(topology.TiDBInstances) > 0 || len(topology.TiKVInstances) > 0 || len(topology.PDInstances) > 0 || len(topology.TiFlashInstances) > 0 || len(topology.TiCDCInstances) > 0) {
		fmt.Println("\nCLUSTER TOPOLOGY")
		fmt.Println(strings.Repeat("-", 60))
		if len(topology.TiDBInstances) > 0 {
			fmt.Printf("  TiDB:     %d instances\n", len(topology.TiDBInstances))
		}
		if len(topology.TiKVInstances) > 0 {
			fmt.Printf("  TiKV:     %d instances\n", len(topology.TiKVInstances))
		}
		if len(topology.PDInstances) > 0 {
			fmt.Printf("  PD:       %d instances\n", len(topology.PDInstances))
		}
		if len(topology.TiFlashInstances) > 0 {
			fmt.Printf("  TiFlash:  %d instances\n", len(topology.TiFlashInstances))
		}
		if len(topology.TiCDCInstances) > 0 {
			fmt.Printf("  TiCDC:    %d instances\n", len(topology.TiCDCInstances))
		}
		if topology.TiDBVersion != "" || topology.TiKVVersion != "" || topology.PDVersion != "" {
			fmt.Println()
			if topology.TiDBVersion != "" {
				fmt.Printf("  Version:  %s", topology.TiDBVersion)
				if topology.TiKVVersion != "" || topology.PDVersion != "" {
					fmt.Printf(" / %s", topology.TiKVVersion)
				}
				if topology.PDVersion != "" {
					fmt.Printf(" / %s", topology.PDVersion)
				}
				fmt.Println()
			}
		}
		fmt.Println()
	}

	fmt.Printf("Time Range: %s ~ %s\n",
		time.Unix(firstTS, 0).Format("2006-01-02 15:04"),
		time.Unix(lastTS, 0).Format("2006-01-02 15:04"))
	fmt.Printf("Duration: %.1f hours (%.1f days)\n", durationH, durationDays)
	fmt.Printf("Samples: %d\n\n", len(allTS))

	if len(qpsValues) > 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("QPS Summary")
		fmt.Println(strings.Repeat("-", 60))

		fmt.Printf("  Mean: %s", formatFloat(qpsMean))
		if isLowActivity {
			fmt.Printf(" (%s activity)", activityLevel)
		}
		fmt.Println()

		if qpsMean >= 10 {
			peakToAvg := max(qpsValues) / qpsMean
			if peakToAvg < 10 || qpsMean > 100 {
				fmt.Printf("  Min:  %s\n", formatFloat(min(qpsValues)))
				fmt.Printf("  Max:  %s\n", formatFloat(max(qpsValues)))
			} else {
				fmt.Printf("  Peak/Avg: %.1fx (highly bursty)\n", peakToAvg)
			}
		}
		fmt.Println()
	}

	if isLowActivity {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Printf("ACTIVITY LEVEL: %s\n", strings.ToUpper(activityLevel))
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("  Low activity cluster - detailed analysis skipped")
		if cacheTimestamp > 0 {
			fmt.Printf("  Data cached: %s/%d\n", clusterID, cacheTimestamp)
		}
		fmt.Println()

		elapsed := time.Since(startTime)
		fmt.Printf("Analysis completed in %s\n", formatDuration(elapsed))
		return
	}

	byType := make(map[string][]map[string]interface{})
	for _, a := range anomalies {
		t := a["type"].(string)
		byType[t] = append(byType[t], a)
	}

	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Anomalies Summary (total: %d, per day: %.1f)\n", len(anomalies), float64(len(anomalies))/durationDays)
	fmt.Println(strings.Repeat("-", 60))
	for _, atype := range []string{
		"QPS_DROP", "QPS_SPIKE",
		"SUSTAINED_LOW_QPS", "SUSTAINED_HIGH_QPS",
		"VOLATILITY_SPIKE",
		"LATENCY_SPIKE", "LATENCY_DEGRADED",
		"INSTANCE_IMBALANCE",
	} {
		if items, exists := byType[atype]; exists {
			fmt.Printf("  %s: %d\n", atype, len(items))
		}
	}
	fmt.Println()

	for _, atype := range []string{
		"QPS_DROP", "QPS_SPIKE",
		"SUSTAINED_LOW_QPS", "SUSTAINED_HIGH_QPS",
		"VOLATILITY_SPIKE",
		"LATENCY_SPIKE", "LATENCY_DEGRADED",
		"INSTANCE_IMBALANCE",
	} {
		if items, exists := byType[atype]; exists {
			fmt.Println(strings.Repeat("-", 60))
			fmt.Printf("[%s] %d events\n", atype, len(items))
			fmt.Println(strings.Repeat("-", 60))
			for i, a := range items {
				if i >= 10 {
					break
				}
				timeStr, _ := a["time"].(string)
				detail, extra := splitDetailAndExtra(a["detail"].(string))
				if timeStr != "" {
					fmt.Printf("  [%s] %s: %s\n", a["severity"], timeStr, detail)
				} else {
					fmt.Printf("  [%s] %s\n", a["severity"], detail)
				}
				if extra != "" {
					fmt.Printf("         %s\n", extra)
				}
			}
			if len(items) > 10 {
				fmt.Printf("  ... and %d more\n", len(items)-10)
			}
			fmt.Println()
		}
	}

	if len(anomalies) == 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("No significant anomalies detected")
		fmt.Println(strings.Repeat("-", 60))
	}

	if workloadProfile != nil {
		analysis.PrintWorkloadProfile(workloadProfile)
	}

	if durationDays >= 1 {
		loadProfile := analysis.AnalyzeLoadProfileWithWorkload(clusterID, qpsTimeSeries, latencyP99, nil, nil, nil, nil)
		if loadProfile != nil {
			analysis.PrintDailyPattern(loadProfile)
			if durationDays >= 3 {
				analysis.PrintWeeklyPattern(loadProfile)
			}
		}
	}

	if advancedProfiling != nil && !isLowActivity {
		analysis.PrintAdvancedProfiling(advancedProfiling)
	}

	if rawPath != "" || anomalyPath != "" {
		analysis.PrintDigStoragePath(rawPath, anomalyPath, profilePath)
	}

	elapsed := time.Since(startTime)
	if cacheTimestamp > 0 {
		fmt.Printf("\nCache ID: %s/%d\n", clusterID, cacheTimestamp)
	}
	fmt.Printf("\nAnalysis completed in %s\n", formatDuration(elapsed))
}

func splitDetailAndExtra(detail string) (main, extra string) {
	idx := strings.LastIndex(detail, "(")
	if idx == -1 {
		return detail, ""
	}
	return detail[:idx], detail[idx:]
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	} else if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}

func formatBytes(n int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case n >= GB:
		return fmt.Sprintf("%.1fGB", float64(n)/float64(GB))
	case n >= MB:
		return fmt.Sprintf("%.1fMB", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.1fKB", float64(n)/float64(KB))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func getResults(data map[string]interface{}) []map[string]interface{} {
	if data == nil {
		return nil
	}
	d, ok := data["data"].(map[string]interface{})
	if !ok {
		return nil
	}
	r, ok := d["result"].([]interface{})
	if !ok {
		return nil
	}

	var results []map[string]interface{}
	for _, item := range r {
		if m, ok := item.(map[string]interface{}); ok {
			results = append(results, m)
		}
	}
	return results
}

func calcQPSRates(seriesList []map[string]interface{}) map[string]map[int64]float64 {
	result := make(map[string]map[int64]float64)

	for _, series := range seriesList {
		inst := getLabel(series, "instance")
		if inst == "" {
			continue
		}

		sqlType := getLabel(series, "sql_type")
		if sqlType == "internal" {
			continue
		}

		if result[inst] == nil {
			result[inst] = make(map[int64]float64)
		}

		values := getValues(series)
		for _, rate := range analysis.CalculateRate(values) {
			result[inst][rate.Timestamp] += rate.Rate
		}
	}

	return result
}

func calcLatencyPercentiles(seriesList []map[string]interface{}) map[string]map[int64]map[int]float64 {
	result := make(map[string]map[int64]map[int]float64)

	instLatency := make(map[string]map[int64]map[string]float64)

	for _, series := range seriesList {
		inst := getLabel(series, "instance")
		le := getLabel(series, "le")
		if inst == "" || le == "" {
			continue
		}

		if instLatency[inst] == nil {
			instLatency[inst] = make(map[int64]map[string]float64)
		}

		for _, v := range getValues(series) {
			if len(v) >= 2 {
				ts := int64(analysis.ToFloat64(v[0]))
				val := analysis.ToFloat64(v[1])

				if instLatency[inst][ts] == nil {
					instLatency[inst][ts] = make(map[string]float64)
				}
				instLatency[inst][ts][le] = val
			}
		}
	}

	for inst := range instLatency {
		result[inst] = make(map[int64]map[int]float64)

		for ts, buckets := range instLatency[inst] {
			result[inst][ts] = calculatePercentiles(buckets)
		}
	}

	return result
}

func calcLatencyP99Series(seriesList []map[string]interface{}) []analysis.TimeSeriesPoint {
	instLatency := make(map[string]map[int64]map[string]float64)

	for _, series := range seriesList {
		inst := getLabel(series, "instance")
		le := getLabel(series, "le")
		if inst == "" || le == "" {
			continue
		}

		if instLatency[inst] == nil {
			instLatency[inst] = make(map[int64]map[string]float64)
		}

		for _, v := range getValues(series) {
			if len(v) >= 2 {
				ts := int64(analysis.ToFloat64(v[0]))
				val := analysis.ToFloat64(v[1])

				if instLatency[inst][ts] == nil {
					instLatency[inst][ts] = make(map[string]float64)
				}
				instLatency[inst][ts][le] = val
			}
		}
	}

	tsMap := make(map[int64][]float64)
	for _, tsData := range instLatency {
		for ts, buckets := range tsData {
			p99 := analysis.HistogramQuantile(buckets, 0.99)
			if !isNaN(p99) {
				tsMap[ts] = append(tsMap[ts], p99)
			}
		}
	}

	var result []analysis.TimeSeriesPoint
	for ts, vals := range tsMap {
		avgP99 := mean(vals)
		result = append(result, analysis.TimeSeriesPoint{
			Timestamp: ts,
			Value:     avgP99,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

func isNaN(f float64) bool {
	return f != f
}

func calculatePercentiles(buckets map[string]float64) map[int]float64 {
	percentiles := make(map[int]float64)

	var sortedBuckets []struct {
		le    float64
		count float64
	}
	for le, count := range buckets {
		if le != "" {
			leVal := analysis.ToFloat64(le)
			sortedBuckets = append(sortedBuckets, struct {
				le    float64
				count float64
			}{leVal, count})
		}
	}

	sort.Slice(sortedBuckets, func(i, j int) bool {
		return sortedBuckets[i].le < sortedBuckets[j].le
	})

	if len(sortedBuckets) < 2 {
		return percentiles
	}

	total := sortedBuckets[len(sortedBuckets)-1].count
	if total == 0 {
		return percentiles
	}

	for _, p := range []int{50, 90, 99} {
		target := total * float64(p) / 100
		prevLE, prevCount := 0.0, 0.0

		for _, item := range sortedBuckets {
			if item.count >= target {
				if item.le == prevLE || item.le == 0 {
					percentiles[p] = item.le * 1000
				} else {
					interp := prevLE + (item.le-prevLE)*(target-prevCount)/(item.count-prevCount)
					percentiles[p] = interp * 1000
				}
				break
			}
			prevLE, prevCount = item.le, item.count
		}
	}

	return percentiles
}

func getLabel(series map[string]interface{}, key string) string {
	metric, ok := series["metric"].(map[string]interface{})
	if !ok {
		return ""
	}
	val, ok := metric[key].(string)
	if !ok {
		return ""
	}
	return val
}

func getValues(series map[string]interface{}) [][]interface{} {
	vals, ok := series["values"].([]interface{})
	if !ok {
		return nil
	}

	var values [][]interface{}
	for _, v := range vals {
		if arr, ok := v.([]interface{}); ok {
			values = append(values, arr)
		}
	}
	return values
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func min(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals {
		if v < m {
			m = v
		}
	}
	return m
}

func max(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals {
		if v > m {
			m = v
		}
	}
	return m
}

func calcSQLTypeRates(seriesList []map[string]interface{}) map[string]map[int64]float64 {
	result := make(map[string]map[int64]float64)

	for _, series := range seriesList {
		sqlType := getLabel(series, "type")
		if sqlType == "" {
			continue
		}

		if result[sqlType] == nil {
			result[sqlType] = make(map[int64]float64)
		}

		values := getValues(series)
		for _, rate := range analysis.CalculateRate(values) {
			result[sqlType][rate.Timestamp] += rate.Rate
		}
	}

	return result
}

func calcTiKVOpRates(seriesList []map[string]interface{}) map[string]map[int64]float64 {
	result := make(map[string]map[int64]float64)

	for _, series := range seriesList {
		op := getLabel(series, "type")
		if op == "" {
			op = getLabel(series, "grpc_method")
		}
		if op == "" {
			continue
		}

		if result[op] == nil {
			result[op] = make(map[int64]float64)
		}

		values := getValues(series)
		for _, rate := range analysis.CalculateRate(values) {
			result[op][rate.Timestamp] += rate.Rate
		}
	}

	return result
}

func calcTiKVLatencyP99Series(seriesList []map[string]interface{}) []analysis.TimeSeriesPoint {
	opLatency := make(map[string]map[int64]map[string]float64)

	for _, series := range seriesList {
		op := getLabel(series, "type")
		if op == "" {
			op = getLabel(series, "grpc_method")
		}
		le := getLabel(series, "le")
		if op == "" || le == "" {
			continue
		}

		if opLatency[op] == nil {
			opLatency[op] = make(map[int64]map[string]float64)
		}

		for _, v := range getValues(series) {
			if len(v) >= 2 {
				ts := int64(analysis.ToFloat64(v[0]))
				val := analysis.ToFloat64(v[1])

				if opLatency[op][ts] == nil {
					opLatency[op][ts] = make(map[string]float64)
				}
				opLatency[op][ts][le] = val
			}
		}
	}

	tsMap := make(map[int64][]float64)
	for _, tsData := range opLatency {
		for ts, buckets := range tsData {
			p99 := analysis.HistogramQuantile(buckets, 0.99)
			if !isNaN(p99) {
				tsMap[ts] = append(tsMap[ts], p99)
			}
		}
	}

	var result []analysis.TimeSeriesPoint
	for ts, vals := range tsMap {
		avgP99 := mean(vals)
		result = append(result, analysis.TimeSeriesPoint{
			Timestamp: ts,
			Value:     avgP99,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

func convertToTimeSeriesMap(rates map[string]map[int64]float64) map[string][]analysis.TimeSeriesPoint {
	result := make(map[string][]analysis.TimeSeriesPoint)

	for key, tsVals := range rates {
		var points []analysis.TimeSeriesPoint
		for ts, val := range tsVals {
			points = append(points, analysis.TimeSeriesPoint{
				Timestamp: ts,
				Value:     val,
			})
		}
		sort.Slice(points, func(i, j int) bool {
			return points[i].Timestamp < points[j].Timestamp
		})
		result[key] = points
	}

	return result
}

func classifyTrafficLevel(meanQPS float64) string {
	if meanQPS < 50 {
		return "very_low"
	} else if meanQPS < 200 {
		return "low"
	} else if meanQPS < 1000 {
		return "medium"
	} else if meanQPS < 10000 {
		return "high"
	}
	return "very_high"
}

func calcQPSRatesByInstAndType(seriesList []map[string]interface{}) map[string]map[string][]analysis.TimeSeriesPoint {
	result := make(map[string]map[string][]analysis.TimeSeriesPoint)

	for _, series := range seriesList {
		inst := getLabel(series, "instance")
		sqlType := getLabel(series, "sql_type")
		if inst == "" || sqlType == "" || sqlType == "internal" {
			continue
		}

		if result[inst] == nil {
			result[inst] = make(map[string][]analysis.TimeSeriesPoint)
		}

		values := getValues(series)
		for _, rate := range analysis.CalculateRate(values) {
			result[inst][sqlType] = append(result[inst][sqlType], analysis.TimeSeriesPoint{
				Timestamp: rate.Timestamp,
				Value:     rate.Rate,
			})
		}
	}

	for inst := range result {
		for sqlType := range result[inst] {
			sort.Slice(result[inst][sqlType], func(i, j int) bool {
				return result[inst][sqlType][i].Timestamp < result[inst][sqlType][j].Timestamp
			})
		}
	}

	return result
}

func calcLatencyByInstP99(seriesList []map[string]interface{}) map[string][]analysis.TimeSeriesPoint {
	instLatency := make(map[string]map[int64]map[string]float64)

	for _, series := range seriesList {
		inst := getLabel(series, "instance")
		le := getLabel(series, "le")
		if inst == "" || le == "" {
			continue
		}

		if instLatency[inst] == nil {
			instLatency[inst] = make(map[int64]map[string]float64)
		}

		for _, v := range getValues(series) {
			if len(v) >= 2 {
				ts := int64(analysis.ToFloat64(v[0]))
				val := analysis.ToFloat64(v[1])

				if instLatency[inst][ts] == nil {
					instLatency[inst][ts] = make(map[string]float64)
				}
				instLatency[inst][ts][le] = val
			}
		}
	}

	result := make(map[string][]analysis.TimeSeriesPoint)
	for inst, tsData := range instLatency {
		for ts, buckets := range tsData {
			p99 := analysis.HistogramQuantile(buckets, 0.99)
			if !isNaN(p99) {
				result[inst] = append(result[inst], analysis.TimeSeriesPoint{
					Timestamp: ts,
					Value:     p99,
				})
			}
		}
		sort.Slice(result[inst], func(i, j int) bool {
			return result[inst][i].Timestamp < result[inst][j].Timestamp
		})
	}

	return result
}

func calcTiKVOpRatesByInst(seriesList []map[string]interface{}) map[string]map[string][]analysis.TimeSeriesPoint {
	result := make(map[string]map[string][]analysis.TimeSeriesPoint)

	for _, series := range seriesList {
		inst := getLabel(series, "instance")
		op := getLabel(series, "type")
		if op == "" {
			op = getLabel(series, "grpc_method")
		}
		if inst == "" || op == "" {
			continue
		}

		if result[inst] == nil {
			result[inst] = make(map[string][]analysis.TimeSeriesPoint)
		}

		values := getValues(series)
		for _, rate := range analysis.CalculateRate(values) {
			result[inst][op] = append(result[inst][op], analysis.TimeSeriesPoint{
				Timestamp: rate.Timestamp,
				Value:     rate.Rate,
			})
		}
	}

	for inst := range result {
		for op := range result[inst] {
			sort.Slice(result[inst][op], func(i, j int) bool {
				return result[inst][op][i].Timestamp < result[inst][op][j].Timestamp
			})
		}
	}

	return result
}

func calcTiKVLatencyByInstP99(seriesList []map[string]interface{}) map[string]map[string][]analysis.TimeSeriesPoint {
	opLatencyByInst := make(map[string]map[string]map[int64]map[string]float64)

	for _, series := range seriesList {
		inst := getLabel(series, "instance")
		op := getLabel(series, "type")
		if op == "" {
			op = getLabel(series, "grpc_method")
		}
		le := getLabel(series, "le")
		if inst == "" || op == "" || le == "" {
			continue
		}

		if opLatencyByInst[inst] == nil {
			opLatencyByInst[inst] = make(map[string]map[int64]map[string]float64)
		}
		if opLatencyByInst[inst][op] == nil {
			opLatencyByInst[inst][op] = make(map[int64]map[string]float64)
		}

		for _, v := range getValues(series) {
			if len(v) >= 2 {
				ts := int64(analysis.ToFloat64(v[0]))
				val := analysis.ToFloat64(v[1])

				if opLatencyByInst[inst][op][ts] == nil {
					opLatencyByInst[inst][op][ts] = make(map[string]float64)
				}
				opLatencyByInst[inst][op][ts][le] = val
			}
		}
	}

	result := make(map[string]map[string][]analysis.TimeSeriesPoint)
	for inst, opData := range opLatencyByInst {
		result[inst] = make(map[string][]analysis.TimeSeriesPoint)
		for op, tsData := range opData {
			for ts, buckets := range tsData {
				p99 := analysis.HistogramQuantile(buckets, 0.99)
				if !isNaN(p99) {
					result[inst][op] = append(result[inst][op], analysis.TimeSeriesPoint{
						Timestamp: ts,
						Value:     p99,
					})
				}
			}
			sort.Slice(result[inst][op], func(i, j int) bool {
				return result[inst][op][i].Timestamp < result[inst][op][j].Timestamp
			})
		}
	}

	return result
}
