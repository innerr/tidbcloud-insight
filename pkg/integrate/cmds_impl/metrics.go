package impl

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"tidbcloud-insight/pkg/prometheus_storage"
)

var digMetrics = []struct {
	name   string
	metric string
}{
	{"qps", "tidb_server_query_total"},
	{"latency", "tidb_server_handle_query_duration_seconds_bucket"},
	{"sqlType", "tidb_executor_statement_total"},
	{"tikvOp", "tikv_grpc_msg_duration_seconds_count"},
	{"tikvLatency", "tikv_grpc_msg_duration_seconds_bucket"},
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

func MetricsCacheClearCluster(cacheDir, clusterID string) error {
	clusterDir := filepath.Join(cacheDir, "metrics", clusterID)

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

func MetricsFetch(cacheDir, metaDir string, cp ClientParams, authMgr *AuthManager,
	clusterID string, startTS, endTS int64) (string, error) {

	cl, err := NewClient(cacheDir, cp, authMgr)
	if err != nil {
		return "", fmt.Errorf("failed to create client: %w", err)
	}

	clusterInfo, err := findClusterInfo(metaDir, clusterID)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	apiBizType := toAPIBizType(clusterInfo.bizType)
	dsURL, err := cl.GetDsURL(ctx, clusterID, clusterInfo.vendor, clusterInfo.region, apiBizType, "")
	if err != nil {
		return "", fmt.Errorf("failed to get datasource URL: %w", err)
	}

	fmt.Printf("  Fetching from %s/%s (%s)...\n", clusterInfo.vendor, clusterInfo.region, clusterInfo.bizType)

	durationSeconds := endTS - startTS
	var step int
	if durationSeconds <= 3600 {
		step = 15
	} else if durationSeconds <= 86400 {
		step = 30
	} else if durationSeconds <= 86400*3 {
		step = 60
	} else {
		step = 120
	}

	promStorage := prometheus_storage.NewPrometheusStorage(filepath.Join(cacheDir, "metrics"))

	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	for _, m := range digMetrics {
		wg.Add(1)
		go func(name, metric string) {
			defer wg.Done()

			result, err := cl.QueryMetric(ctx, dsURL, metric, int(startTS), int(endTS), step)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("%s: %w", name, err))
				mu.Unlock()
				return
			}

			if result == nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("%s: no data", name))
				mu.Unlock()
				return
			}

			_, err = promStorage.SaveMetricData(clusterID, metric, result, startTS, endTS)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("%s: save failed: %w", name, err))
				mu.Unlock()
				return
			}

			mu.Lock()
			fmt.Printf("    %s: OK\n", name)
			mu.Unlock()
		}(m.name, m.metric)
	}

	wg.Wait()

	if len(errors) > 0 {
		return "", fmt.Errorf("errors: %v", errors)
	}

	cacheID := fmt.Sprintf("%s_%d_%d", clusterID, startTS, endTS)
	return cacheID, nil
}

func MetricsFetchRandom(cacheDir, metaDir string, cp ClientParams, authMgr *AuthManager,
	startTS, endTS int64) (string, error) {

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
		return "", fmt.Errorf("no active clusters found")
	}

	rand.Seed(time.Now().UnixNano())
	selected := allClusters[rand.Intn(len(allClusters))]

	fmt.Printf("Random cluster: %s (%s)\n", selected.clusterID, selected.bizType)

	return MetricsFetch(cacheDir, metaDir, cp, authMgr, selected.clusterID, startTS, endTS)
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

func MetricsFetchAll(cacheDir, metaDir string, cp ClientParams, authMgr *AuthManager,
	startTS, endTS int64) error {

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

		_, err := MetricsFetch(cacheDir, metaDir, cp, authMgr, c.clusterID, startTS, endTS)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			failCount++
		} else {
			successCount++
		}
	}

	fmt.Printf("\nCompleted: %d success, %d failed\n", successCount, failCount)
	return nil
}
