package impl

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"tidbcloud-insight/pkg/prometheus_storage"
)

var digMetrics = []string{
	"tidb_server_query_total",
	"tidb_server_handle_query_duration_seconds_bucket",
	"tidb_executor_statement_total",
	"tikv_grpc_msg_duration_seconds_count",
	"tikv_grpc_msg_duration_seconds_bucket",
}

func MetricsCacheList(cacheDir, clusterID, metricName string) error {
	promStorage := prometheus_storage.NewPrometheusStorage(filepath.Join(cacheDir, "metrics"))

	if clusterID == "" {
		return fmt.Errorf("cluster-id is required")
	}

	var metrics []string
	if metricName != "" {
		metrics = []string{metricName}
	} else {
		var err error
		metrics, err = promStorage.ListMetrics(clusterID)
		if err != nil {
			return fmt.Errorf("failed to list metrics: %w", err)
		}
		if len(metrics) == 0 {
			fmt.Printf("No cached metrics found for cluster %s\n", clusterID)
			return nil
		}
	}

	fmt.Printf("Cluster: %s\n\n", clusterID)

	for _, mName := range metrics {
		info, err := promStorage.AnalyzeMetric(clusterID, mName)
		if err != nil {
			fmt.Printf("  %s: ERROR - %v\n\n", mName, err)
			continue
		}

		fmt.Printf("  %s\n", mName)
		fmt.Printf("    Size: %s\n", info.FormatFileSize())

		if len(info.Files) > 0 {
			fmt.Printf("    Files (%d):\n", len(info.Files))
			for _, f := range info.Files {
				fmt.Printf("      %s (%s)\n", filepath.Base(f.Path), f.FormatSize())
			}
		}

		if len(info.Segments) == 0 {
			fmt.Printf("    No data points\n\n")
			continue
		}

		fmt.Printf("    Total duration: %s\n", info.FormatDuration())
		fmt.Printf("    Total points: %d\n", info.TotalPoints)

		if info.GapCount > 0 {
			fmt.Printf("    Gaps detected: %d\n", info.GapCount)
		}

		if len(info.Segments) > 1 {
			fmt.Printf("    Time segments (%d):\n", len(info.Segments))
			for i, seg := range info.Segments {
				fmt.Printf("      [%d] %s ~ %s (%s, %d points)\n",
					i+1,
					seg.FormatStart(),
					seg.FormatEnd(),
					seg.FormatDuration(),
					seg.PointCount)
			}
		} else if len(info.Segments) == 1 {
			fmt.Printf("    Time range: %s ~ %s\n",
				info.Segments[0].FormatStart(),
				info.Segments[0].FormatEnd())
		}
		fmt.Println()
	}

	return nil
}

func MetricsCacheClear(cacheDir string) error {
	metricsDir := filepath.Join(cacheDir, "metrics")

	entries, err := os.ReadDir(metricsDir)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("No metrics cache found")
			return nil
		}
		return fmt.Errorf("failed to read metrics cache directory: %w", err)
	}

	if len(entries) == 0 {
		fmt.Println("Metrics cache is already empty")
		return nil
	}

	clusterCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			clusterCount++
		}
	}

	fmt.Printf("Clearing metrics cache (%d clusters)...\n", clusterCount)

	if err := os.RemoveAll(metricsDir); err != nil {
		return fmt.Errorf("failed to clear metrics cache: %w", err)
	}

	fmt.Printf("Cleared metrics cache: %s\n", metricsDir)
	return nil
}

func MetricsCacheClearCluster(cacheDir, clusterID, metricName string) error {
	metricsDir := filepath.Join(cacheDir, "metrics")

	if metricName != "" {
		metricDir := filepath.Join(metricsDir, clusterID, metricName)
		entries, err := os.ReadDir(metricDir)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Printf("No cached metrics found for cluster %s, metric %s\n", clusterID, metricName)
				return nil
			}
			return fmt.Errorf("failed to read metric cache directory: %w", err)
		}

		fileCount := len(entries)
		fmt.Printf("Clearing metrics cache for cluster %s, metric %s (%d files)...\n",
			clusterID, metricName, fileCount)

		if err := os.RemoveAll(metricDir); err != nil {
			return fmt.Errorf("failed to clear metric cache: %w", err)
		}

		fmt.Printf("Cleared metrics cache for cluster %s, metric %s\n", clusterID, metricName)
		return nil
	}

	clusterDir := filepath.Join(metricsDir, clusterID)
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("No cached metrics found for cluster %s\n", clusterID)
			return nil
		}
		return fmt.Errorf("failed to read cluster cache directory: %w", err)
	}

	metricCount := len(entries)

	fmt.Printf("Clearing metrics cache for cluster %s (%d metrics)...\n",
		clusterID, metricCount)

	if err := os.RemoveAll(clusterDir); err != nil {
		return fmt.Errorf("failed to clear cluster metrics cache: %w", err)
	}

	fmt.Printf("Cleared metrics cache for cluster: %s\n", clusterID)
	return nil
}

func MetricsFetchWithConfig(cacheDir, metaDir string, cp ClientParams, authMgr *AuthManager,
	clusterID string, startTS, endTS int64, metricFilter string, config MetricsFetcherConfig) (int, error) {

	cl, err := NewClient(cacheDir, cp, authMgr)
	if err != nil {
		return 0, fmt.Errorf("failed to create client: %w", err)
	}

	clusterInfo, err := findClusterInfo(metaDir, clusterID)
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	apiBizType := toAPIBizType(clusterInfo.bizType)
	dsURL, err := cl.GetDsURL(ctx, clusterID, clusterInfo.vendor, clusterInfo.region, apiBizType, "")
	if err != nil {
		return 0, fmt.Errorf("failed to get datasource URL: %w", err)
	}

	fmt.Printf("  Fetching from %s/%s (%s)...\n", clusterInfo.vendor, clusterInfo.region, clusterInfo.bizType)

	promStorage := prometheus_storage.NewPrometheusStorage(filepath.Join(cacheDir, "metrics"))

	metricsToFetch := digMetrics
	if metricFilter != "" {
		metricsToFetch = []string{metricFilter}
	}

	fetcher := NewMetricsFetcher(cl, promStorage, config)

	durationSeconds := endTS - startTS
	step := fetcher.calculateStep(durationSeconds)

	result, err := fetcher.Fetch(ctx, clusterID, dsURL, metricsToFetch, startTS, endTS, step)
	if err != nil {
		return 0, err
	}

	for _, m := range metricsToFetch {
		existingRanges, _ := promStorage.GetExistingTimeRanges(clusterID, m)
		gaps := promStorage.CalculateNonOverlappingRanges(startTS, endTS, existingRanges)

		if len(gaps) == 0 {
			fmt.Printf("    %s: already cached\n", m)
		} else if len(gaps) == 1 {
			fmt.Printf("    - %s: fetched range\n      [%s ~ %s]\n", m,
				time.Unix(gaps[0][0], 0).Format("2006-01-02 15:04:05"),
				time.Unix(gaps[0][1], 0).Format("2006-01-02 15:04:05"))
		} else {
			fmt.Printf("    - %s: fetched %d gaps\n", m, len(gaps))
			for _, gap := range gaps {
				fmt.Printf("      [%s ~ %s]\n",
					time.Unix(gap[0], 0).Format("2006-01-02 15:04:05"),
					time.Unix(gap[1], 0).Format("2006-01-02 15:04:05"))
			}
		}
	}

	if result.SkippedMetrics > 0 {
		fmt.Printf("  Skipped %d metrics (already cached)\n", result.SkippedMetrics)
	}
	if result.FetchedMetrics > 0 {
		fmt.Printf("  Fetched %d metrics\n", result.FetchedMetrics)
	}

	if len(result.Errors) > 0 {
		fmt.Printf("  Errors: %v\n", result.Errors)
	}

	return result.FetchedMetrics, nil
}

func MetricsFetchRandomWithConfig(cacheDir, metaDir string, cp ClientParams, authMgr *AuthManager,
	startTS, endTS int64, metricFilter string, config MetricsFetcherConfig) (string, int, error) {

	inactive := loadInactiveClusters(cacheDir)

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
		return "", 0, fmt.Errorf("no active clusters found")
	}

	rand.Seed(time.Now().UnixNano())
	selected := allClusters[rand.Intn(len(allClusters))]

	fmt.Printf("Random cluster: %s (%s)\n", selected.clusterID, selected.bizType)

	fetchedCount, err := MetricsFetchWithConfig(cacheDir, metaDir, cp, authMgr, selected.clusterID, startTS, endTS, metricFilter, config)
	return selected.clusterID, fetchedCount, err
}

func findClusterInfo(metaDir, clusterID string) (*clusterInfo, error) {
	for _, bizType := range []string{"dedicated", "premium"} {
		clusters, err := loadClustersFromList(metaDir, bizType)
		if err != nil {
			continue
		}
		for _, c := range clusters {
			if c.clusterID == clusterID {
				return &c, nil
			}
		}
	}
	return nil, fmt.Errorf("cluster %s not found in cluster lists", clusterID)
}

type clusterInfo struct {
	clusterID string
	bizType   string
	vendor    string
	region    string
}

func loadInactiveClusters(cacheDir string) map[string]bool {
	result := make(map[string]bool)
	path := filepath.Join(cacheDir, "inactive_clusters.txt")

	file, err := os.Open(path)
	if err != nil {
		return result
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			result[line] = true
		}
	}
	return result
}

func loadClustersFromList(metaDir, bizType string) ([]clusterInfo, error) {
	path := filepath.Join(metaDir, bizType, "clusters.txt")

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	var clusters []clusterInfo
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "(", 2)
		if len(parts) != 2 {
			continue
		}

		clusterID := strings.TrimSpace(parts[0])
		locPart := strings.TrimSuffix(parts[1], ")")
		locParts := strings.SplitN(locPart, "/", 2)
		if len(locParts) != 2 {
			continue
		}

		clusters = append(clusters, clusterInfo{
			clusterID: clusterID,
			bizType:   bizType,
			vendor:    locParts[0],
			region:    locParts[1],
		})
	}

	return clusters, scanner.Err()
}

func MetricsFetchAllWithConfig(cacheDir, metaDir string, cp ClientParams, authMgr *AuthManager,
	startTS, endTS int64, config MetricsFetcherConfig) error {

	inactive := loadInactiveClusters(cacheDir)
	if len(inactive) > 0 {
		fmt.Printf("Excluding %d inactive clusters\n", len(inactive))
	}

	var allClusters []clusterInfo
	for _, bizType := range []string{"dedicated", "premium"} {
		clusters, err := loadClustersFromList(metaDir, bizType)
		if err != nil {
			return fmt.Errorf("failed to load %s clusters: %w", bizType, err)
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

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allClusters), func(i, j int) {
		allClusters[i], allClusters[j] = allClusters[j], allClusters[i]
	})

	fmt.Printf("Fetching metrics for %d clusters\n\n", len(allClusters))

	successCount := 0
	failCount := 0

	for i, c := range allClusters {
		fmt.Printf("[%d/%d] Fetching %s (%s)...\n", i+1, len(allClusters), c.clusterID, c.bizType)

		_, err := MetricsFetchWithConfig(cacheDir, metaDir, cp, authMgr, c.clusterID, startTS, endTS, "", config)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			failCount++
		} else {
			successCount++
		}
		fmt.Println("------------------------------------------------------------")
	}

	fmt.Printf("Completed: %d success, %d failed\n", successCount, failCount)
	return nil
}
