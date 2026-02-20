package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"tidbcloud-insight/internal/auth"
	"tidbcloud-insight/internal/config"
	"tidbcloud-insight/internal/local_cache"
	"tidbcloud-insight/internal/logger"
)

const GSEndpoint = "http://www.gs.us-west-2.aws.observability.tidbcloud.com/api/v1/clusters/"

type Cluster struct {
	ApplicationID   string `json:"applicationID"`
	ID              string `json:"id"`
	DisplayName     string `json:"displayName"`
	Name            string `json:"name"`
	BizType         string `json:"bizType"`
	Vendor          string `json:"vendor"`
	Region          string `json:"region"`
	InternalReadURI string `json:"internalReadUri"`
}

func (c *Cluster) GetDisplayName() string {
	if c.DisplayName != "" {
		return c.DisplayName
	}
	return c.Name
}

type GSResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ClustersResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Clusters []Cluster `json:"clusters"`
	} `json:"data"`
}

type Client struct {
	cache         *cache.Cache
	rateLimiter   *RateLimiter
	concurrency   *AdaptiveConcurrencyController
	bytesReceived int64
	bytesMu       sync.RWMutex
	httpClient    *http.Client
	idleTimeout   time.Duration
}

func NewClient(c *cache.Cache) *Client {
	return NewClientWithTimeout(c, 0, 0)
}

func NewClientWithTimeout(c *cache.Cache, timeout time.Duration, idleTimeout time.Duration) *Client {
	acc := NewAdaptiveConcurrencyController(DefaultAdaptiveConcurrencyConfig())

	rl := NewRateLimiter(RateLimiterConfig{
		MaxRequestsPerSecond:     10,
		MinInterval:              100 * time.Millisecond,
		ConsecutiveFailThreshold: 3,
		BackoffInitial:           1 * time.Second,
		BackoffMax:               30 * time.Second,
		ProgressInterval:         10 * time.Second,
		GetConcurrencyFunc: func() int {
			return acc.GetCurrentConcurrency()
		},
		OnBackoffCallback: func() {
			acc.OnRateLimited()
		},
	})

	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  false,
		MaxIdleConnsPerHost: 5,
	}

	httpClient := &http.Client{
		Transport: transport,
	}
	if timeout > 0 {
		httpClient.Timeout = timeout
	}

	if idleTimeout <= 0 {
		idleTimeout = 3 * time.Minute
	}

	return &Client{
		cache:       c,
		rateLimiter: rl,
		concurrency: acc,
		httpClient:  httpClient,
		idleTimeout: idleTimeout,
	}
}

func (c *Client) NewClientWithRateLimit(cfg RateLimiterConfig) *Client {
	acc := NewAdaptiveConcurrencyController(DefaultAdaptiveConcurrencyConfig())
	if cfg.GetConcurrencyFunc == nil {
		cfg.GetConcurrencyFunc = func() int {
			return acc.GetCurrentConcurrency()
		}
	}
	if cfg.OnBackoffCallback == nil {
		cfg.OnBackoffCallback = func() {
			acc.OnRateLimited()
		}
	}
	return &Client{
		cache:       c.cache,
		rateLimiter: NewRateLimiter(cfg),
		concurrency: acc,
		httpClient:  c.httpClient,
	}
}

func (c *Client) GetRateLimiter() *RateLimiter {
	return c.rateLimiter
}

func (c *Client) GetConcurrencyController() *AdaptiveConcurrencyController {
	return c.concurrency
}

func (c *Client) addBytesReceived(n int64) {
	c.bytesMu.Lock()
	defer c.bytesMu.Unlock()
	c.bytesReceived += n
}

func (c *Client) GetBytesReceived() int64 {
	c.bytesMu.RLock()
	defer c.bytesMu.RUnlock()
	return c.bytesReceived
}

func (c *Client) doRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return nil, err
	}

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		c.rateLimiter.RecordFailure(0, err)
		return nil, err
	}

	if resp.StatusCode == 401 {
		resp.Body.Close()
		auth.GetManager().InvalidateToken()
		c.rateLimiter.RecordFailure(resp.StatusCode, fmt.Errorf("unauthorized"))
		return nil, fmt.Errorf("unauthorized: token expired")
	}

	if resp.StatusCode == 429 || resp.StatusCode == 503 {
		c.rateLimiter.RecordFailure(resp.StatusCode, nil)
		resp.Body.Close()
		return nil, fmt.Errorf("rate limited: HTTP %d", resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		c.rateLimiter.RecordFailure(resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode))
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	c.rateLimiter.RecordSuccess()
	if c.concurrency != nil {
		c.concurrency.OnSuccess()
	}
	return resp, nil
}

func (c *Client) doRequestWithRetry(ctx context.Context, req *http.Request, maxRetries int) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}

		token, err := auth.GetManager().GetToken()
		if err != nil {
			lastErr = err
			continue
		}

		reqClone := req.Clone(ctx)
		reqClone.Header.Set("Authorization", "Bearer "+token)

		resp, err := c.doRequest(ctx, reqClone)
		if err != nil {
			lastErr = err
			continue
		}
		return resp, nil
	}

	return nil, lastErr
}

func (c *Client) fetchClustersPage(ctx context.Context, vendor, region, bizType string, page, pageSize int) ([]Cluster, error) {
	params := url.Values{}
	params.Set("vendor", vendor)
	params.Set("region", region)
	params.Set("bizType", bizType)
	params.Set("page", strconv.Itoa(page))
	params.Set("pageSize", strconv.Itoa(pageSize))

	req, err := http.NewRequestWithContext(ctx, "GET", GSEndpoint+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequestWithRetry(ctx, req, 2)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result ClustersResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if result.Code != 0 {
		return nil, fmt.Errorf("query failed: %s", result.Message)
	}

	return result.Data.Clusters, nil
}

func (c *Client) ListClustersPaginated(ctx context.Context, vendor, region, bizType string, pageSize int) ([]Cluster, error) {
	var allClusters []Cluster
	page := 1

	for {
		clusters, err := c.fetchClustersPage(ctx, vendor, region, bizType, page, pageSize)
		if err != nil {
			return nil, err
		}

		allClusters = append(allClusters, clusters...)

		if len(clusters) < pageSize {
			break
		}
		page++
	}

	return allClusters, nil
}

type VendorRegion struct {
	Vendor string
	Region string
}

func (c *Client) ListClustersConcurrent(ctx context.Context, vendorRegions []VendorRegion, bizType string, pageSize int, onProgress func(vendor, region string, count int)) ([]Cluster, error) {
	var mu sync.Mutex
	var allClusters []Cluster
	var wg sync.WaitGroup

	var sem chan struct{}
	if c.concurrency != nil {
		sem = make(chan struct{}, c.concurrency.GetCurrentConcurrency())
	} else {
		sem = make(chan struct{}, 5)
	}

	for _, vr := range vendorRegions {
		wg.Add(1)
		go func(vendor, region string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			clusters, err := c.ListClustersPaginated(ctx, vendor, region, bizType, pageSize)
			if err != nil {
				return
			}

			mu.Lock()
			if onProgress != nil {
				onProgress(vendor, region, len(clusters))
			}
			allClusters = append(allClusters, clusters...)
			mu.Unlock()
		}(vr.Vendor, vr.Region)
	}

	wg.Wait()

	return allClusters, nil
}

func (c *Client) GetDsURL(ctx context.Context, clusterID, vendor, region, bizType, projectID string) (string, error) {
	if cachedURL, exists := c.cache.GetDsURLCache(clusterID); exists {
		return cachedURL, nil
	}

	params := url.Values{}
	params.Set("vendor", vendor)
	params.Set("region", region)
	params.Set("bizType", bizType)

	if clusterID == "k8s-infra" && projectID != "" {
		params.Set("applicationID", "k8s-infra")
		params.Set("projectID", projectID)
	} else {
		params.Set("applicationID", clusterID)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", GSEndpoint+"?"+params.Encode(), nil)
	if err != nil {
		return "", err
	}

	resp, err := c.doRequestWithRetry(ctx, req, 2)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result ClustersResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if result.Code != 0 {
		return "", fmt.Errorf("query failed: %s", result.Message)
	}

	if len(result.Data.Clusters) == 0 {
		return "", fmt.Errorf("no cluster found for %s", clusterID)
	}

	dsURL := result.Data.Clusters[0].InternalReadURI
	c.cache.SetDsURLCache(clusterID, dsURL)

	return dsURL, nil
}

func (c *Client) QueryMetric(ctx context.Context, dsURL, metric string, start, end, step int) (map[string]interface{}, error) {
	var reqURL string
	var params url.Values

	if start > 0 && end > 0 {
		reqURL = dsURL + "query_range"
		params = url.Values{}
		params.Set("query", metric)
		params.Set("start", strconv.Itoa(start))
		params.Set("end", strconv.Itoa(end))
		params.Set("step", strconv.Itoa(step)+"s")
	} else {
		reqURL = dsURL + "query"
		params = url.Values{}
		params.Set("query", metric)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "GET", reqURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequestWithRetry(ctx, httpReq, 3)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	logAll := false
	if cfg := config.Get(); cfg != nil {
		logAll = cfg.Logging.LogAllHTTPCodes
	}
	logger.SetConcurrencyProvider(c.concurrency)
	logger.LogHTTP(metric, resp.StatusCode, logAll)

	data, err := c.readBodyWithIdleTimeout(ctx, resp.Body)
	if err != nil {
		return nil, err
	}

	c.addBytesReceived(int64(len(data)))

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Client) readBodyWithIdleTimeout(ctx context.Context, body io.Reader) ([]byte, error) {
	if c.idleTimeout <= 0 {
		return io.ReadAll(body)
	}

	type readResult struct {
		data []byte
		err  error
	}

	resultChan := make(chan readResult, 1)
	progressChan := make(chan struct{}, 100)
	idleTimer := time.NewTimer(c.idleTimeout)
	defer idleTimer.Stop()

	go func() {
		buf := make([]byte, 32*1024)
		var allData []byte
		for {
			n, err := body.Read(buf)
			if n > 0 {
				allData = append(allData, buf[:n]...)
				select {
				case progressChan <- struct{}{}:
				default:
				}
			}
			if err != nil {
				if err == io.EOF {
					resultChan <- readResult{data: allData, err: nil}
				} else {
					resultChan <- readResult{data: allData, err: err}
				}
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-idleTimer.C:
			return nil, fmt.Errorf("idle timeout after %v (no progress)", c.idleTimeout)
		case result := <-resultChan:
			return result.data, result.err
		case <-progressChan:
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(c.idleTimeout)
		}
	}
}

type ChunkedMetricResult struct {
	Result   map[string]interface{}
	DataSize int
}

func (c *Client) QueryMetricChunked(ctx context.Context, dsURL, metric string, start, end, step, chunkSize int) (*ChunkedMetricResult, error) {
	if chunkSize <= 0 {
		chunkSize = 1800
	}

	var chunks [][2]int
	chunkStart := start
	for chunkStart < end {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > end {
			chunkEnd = end
		}
		chunks = append(chunks, [2]int{chunkStart, chunkEnd})
		chunkStart = chunkEnd
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("no time range to query")
	}

	if len(chunks) == 1 {
		result, _, err := c.QueryMetricWithRetry(ctx, dsURL, metric, start, end, step)
		if err != nil {
			return nil, err
		}
		dataSize := estimateDataSize(result)
		return &ChunkedMetricResult{Result: result, DataSize: dataSize}, nil
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	chunkResults := make([]map[string]interface{}, len(chunks))
	errors := make([]error, len(chunks))

	maxConcurrency := 3
	if c.concurrency != nil {
		maxConcurrency = c.concurrency.GetCurrentConcurrency()
	}
	sem := make(chan struct{}, maxConcurrency)

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, chunkStart, chunkEnd int) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				mu.Lock()
				errors[idx] = ctx.Err()
				mu.Unlock()
				return
			}

			result, _, err := c.QueryMetricWithRetry(ctx, dsURL, metric, chunkStart, chunkEnd, step)

			mu.Lock()
			chunkResults[idx] = result
			errors[idx] = err
			mu.Unlock()
		}(i, chunk[0], chunk[1])
	}

	wg.Wait()

	var validResults []map[string]interface{}
	for i, result := range chunkResults {
		if errors[i] == nil && result != nil {
			validResults = append(validResults, result)
		}
	}

	if len(validResults) == 0 {
		return nil, fmt.Errorf("all chunks failed for metric %s", metric)
	}

	merged := mergeChunkedResults(validResults)
	dataSize := estimateDataSize(merged)
	return &ChunkedMetricResult{Result: merged, DataSize: dataSize}, nil
}

func estimateDataSize(result map[string]interface{}) int {
	if result == nil {
		return 0
	}
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return 0
	}
	results, ok := data["result"].([]interface{})
	if !ok {
		return 0
	}
	totalPoints := 0
	for _, r := range results {
		series, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		values, ok := series["values"].([]interface{})
		if ok {
			totalPoints += len(values)
		}
	}
	return totalPoints
}

func (c *Client) QueryMetricWithRetry(ctx context.Context, dsURL, metric string, start, end, initialStep int) (map[string]interface{}, int, error) {
	stepsToTry := []int{initialStep, 120, 300, 600}

	for _, step := range stepsToTry {
		result, err := c.QueryMetric(ctx, dsURL, metric, start, end, step)
		if err != nil {
			continue
		}

		data, ok := result["data"].(map[string]interface{})
		if !ok {
			continue
		}
		results, ok := data["result"].([]interface{})
		if !ok || len(results) == 0 {
			continue
		}

		hasValues := false
		for _, r := range results {
			series, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			vals, ok := series["values"].([]interface{})
			if ok && len(vals) > 1 {
				hasValues = true
				break
			}
		}

		if hasValues {
			return result, step, nil
		}
	}

	return nil, 0, fmt.Errorf("no data found")
}

func (c *Client) FetchMetricsConcurrent(ctx context.Context, metrics []string, start, end, step int, dsURL string, onProgress func(metric string, err error)) map[string]map[string]interface{} {
	var mu sync.Mutex
	allData := make(map[string]map[string]interface{})
	var wg sync.WaitGroup

	var sem chan struct{}
	if c.concurrency != nil {
		sem = make(chan struct{}, c.concurrency.GetCurrentConcurrency())
	} else {
		sem = make(chan struct{}, 3)
	}

	for _, metric := range metrics {
		wg.Add(1)
		go func(m string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			result, err := c.QueryMetric(ctx, dsURL, m, start, end, step)

			mu.Lock()
			if err != nil {
				allData[m] = nil
			} else {
				allData[m] = result
			}
			if onProgress != nil {
				onProgress(m, err)
			}
			mu.Unlock()
		}(metric)
	}

	wg.Wait()

	return allData
}

func (c *Client) FetchMetricsChunked(ctx context.Context, metrics []string, start, end, step int, clusterID, vendor, region, bizType string) (map[string]map[string]interface{}, error) {
	chunkHours := 6
	chunkSeconds := chunkHours * 3600

	histogramMetrics := make([]string, 0)
	simpleMetrics := make([]string, 0)
	for _, m := range metrics {
		if contains(m, "_bucket") {
			histogramMetrics = append(histogramMetrics, m)
		} else {
			simpleMetrics = append(simpleMetrics, m)
		}
	}

	allData := make(map[string]map[string]interface{})

	dsURL, err := c.GetDsURL(ctx, clusterID, vendor, region, bizType, "")
	if err != nil {
		return nil, err
	}

	fmt.Printf("Fetching %d simple metrics...\n", len(simpleMetrics))
	simpleResults := c.FetchMetricsConcurrent(ctx, simpleMetrics, start, end, step, dsURL, func(metric string, err error) {
		if err != nil {
			fmt.Printf("  %s: FAILED (%v)\n", metric, err)
		} else {
			fmt.Printf("  %s: OK\n", metric)
		}
	})
	for k, v := range simpleResults {
		allData[k] = v
	}

	if len(histogramMetrics) > 0 {
		var chunks [][2]int
		chunkStart := start
		for chunkStart < end {
			chunkEnd := chunkStart + chunkSeconds
			if chunkEnd > end {
				chunkEnd = end
			}
			chunks = append(chunks, [2]int{chunkStart, chunkEnd})
			chunkStart = chunkEnd
		}

		fmt.Printf("Fetching %d histogram metrics in %d chunks...\n", len(histogramMetrics), len(chunks))

		for _, metric := range histogramMetrics {
			fmt.Printf("  %s...\n", metric)
			var chunkedResults []map[string]interface{}

			for i, chunk := range chunks {
				result, err := c.QueryMetric(ctx, dsURL, metric, chunk[0], chunk[1], step)
				if err != nil {
					fmt.Printf("    chunk %d/%d: FAILED\n", i+1, len(chunks))
					continue
				}

				status, _ := result["status"].(string)
				data, _ := result["data"].(map[string]interface{})
				results, _ := data["result"].([]interface{})
				if status == "success" && len(results) > 0 {
					chunkedResults = append(chunkedResults, result)
					fmt.Printf("    chunk %d/%d: OK (%d series)\n", i+1, len(chunks), len(results))
				} else {
					fmt.Printf("    chunk %d/%d: empty\n", i+1, len(chunks))
				}
			}

			if len(chunkedResults) > 0 {
				merged := mergeChunkedResults(chunkedResults)
				allData[metric] = merged
			} else {
				allData[metric] = map[string]interface{}{
					"status": "success",
					"data": map[string]interface{}{
						"resultType": "matrix",
						"result":     []interface{}{},
					},
				}
			}
		}
	}

	c.rateLimiter.PrintStats()

	return allData, nil
}

func mergeChunkedResults(results []map[string]interface{}) map[string]interface{} {
	if len(results) == 0 {
		return map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": "matrix",
				"result":     []interface{}{},
			},
		}
	}

	if len(results) == 1 {
		return results[0]
	}

	seriesMap := make(map[string]map[string]interface{})

	for _, result := range results {
		data, ok := result["data"].(map[string]interface{})
		if !ok {
			continue
		}
		seriesList, ok := data["result"].([]interface{})
		if !ok {
			continue
		}

		for _, s := range seriesList {
			series, ok := s.(map[string]interface{})
			if !ok {
				continue
			}
			labels, ok := series["metric"].(map[string]interface{})
			if !ok {
				continue
			}

			key := makeSeriesKey(labels)
			if _, exists := seriesMap[key]; !exists {
				seriesMap[key] = map[string]interface{}{
					"metric": labels,
					"values": []interface{}{},
				}
			}

			values, ok := series["values"].([]interface{})
			if ok {
				existingValues := seriesMap[key]["values"].([]interface{})
				seriesMap[key]["values"] = append(existingValues, values...)
			}
		}
	}

	var mergedResult []interface{}
	for _, series := range seriesMap {
		values, ok := series["values"].([]interface{})
		if !ok {
			continue
		}

		sortedValues := sortAndDedupValues(values)
		series["values"] = sortedValues
		mergedResult = append(mergedResult, series)
	}

	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     mergedResult,
		},
	}
}

func makeSeriesKey(labels map[string]interface{}) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		if k != "__name__" {
			keys = append(keys, k)
		}
	}
	return joinStrings(keys, ",")
}

func sortAndDedupValues(values []interface{}) []interface{} {
	if len(values) == 0 {
		return values
	}

	tsMap := make(map[float64]interface{})
	for _, v := range values {
		val, ok := v.([]interface{})
		if !ok || len(val) < 2 {
			continue
		}
		ts, ok := val[0].(float64)
		if ok {
			tsMap[ts] = v
		}
	}

	var timestamps []float64
	for ts := range tsMap {
		timestamps = append(timestamps, ts)
	}

	sortFloat64(timestamps)

	var result []interface{}
	for _, ts := range timestamps {
		result = append(result, tsMap[ts])
	}

	return result
}

func sortFloat64(s []float64) {
	for i := 0; i < len(s)-1; i++ {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func joinStrings(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	}
	result := s[0]
	for i := 1; i < len(s); i++ {
		result += sep + s[i]
	}
	return result
}
