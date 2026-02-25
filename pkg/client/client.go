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

	"tidbcloud-insight/pkg/auth"
	cache "tidbcloud-insight/pkg/local_cache"
	"tidbcloud-insight/pkg/logger"
)

const GSEndpoint = "http://www.gs.us-west-2.aws.observability.tidbcloud.com/api/v1/clusters/"

type TooManySamplesError struct {
	StatusCode int
	Body       string
}

func (e *TooManySamplesError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

func isTooManySamplesError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*TooManySamplesError)
	return ok
}

func containsMaxSamplesError(body string) bool {
	return len(body) > 0 && (contains(body, "maxSamplesPerQuery") || contains(body, "cannot select more than"))
}

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
	displayVerb   bool
	authMgr       *auth.Manager
}

func NewClientWithAuth(cacheDir string, fetchTimeout, idleTimeout time.Duration, displayVerb bool, authMgr *auth.Manager) (*Client, error) {
	return NewClientWithAuthAndConcurrencyConfig(cacheDir, fetchTimeout, idleTimeout, displayVerb, 0, 0, 0, authMgr)
}

func NewClientWithAuthAndConcurrency(cacheDir string, fetchTimeout, idleTimeout time.Duration, displayVerb bool, desiredConcurrency int, authMgr *auth.Manager) (*Client, error) {
	return NewClientWithAuthAndConcurrencyConfig(cacheDir, fetchTimeout, idleTimeout, displayVerb, desiredConcurrency, 0, 0, authMgr)
}

func NewClientWithAuthAndConcurrencyConfig(cacheDir string, fetchTimeout, idleTimeout time.Duration, displayVerb bool, desiredConcurrency int, recoveryInterval, minRecoveryInterval time.Duration, authMgr *auth.Manager) (*Client, error) {
	c, err := cache.NewCache(cacheDir)
	if err != nil {
		return nil, err
	}
	return NewClientWithTimeoutAndAuthAndConcurrencyConfig(c, fetchTimeout, idleTimeout, displayVerb, desiredConcurrency, recoveryInterval, minRecoveryInterval, authMgr), nil
}

func NewClientWithTimeoutAndAuth(c *cache.Cache, timeout time.Duration, idleTimeout time.Duration, displayVerb bool, authMgr *auth.Manager) *Client {
	return NewClientWithTimeoutAndAuthAndConcurrencyConfig(c, timeout, idleTimeout, displayVerb, 0, 0, 0, authMgr)
}

func NewClientWithTimeoutAndAuthAndConcurrency(c *cache.Cache, timeout time.Duration, idleTimeout time.Duration, displayVerb bool, desiredConcurrency int, authMgr *auth.Manager) *Client {
	return NewClientWithTimeoutAndAuthAndConcurrencyConfig(c, timeout, idleTimeout, displayVerb, desiredConcurrency, 0, 0, authMgr)
}

func NewClientWithTimeoutAndAuthAndConcurrencyConfig(c *cache.Cache, timeout time.Duration, idleTimeout time.Duration, displayVerb bool, desiredConcurrency int, recoveryInterval, minRecoveryInterval time.Duration, authMgr *auth.Manager) *Client {
	cfg := DefaultAdaptiveConcurrencyConfig()
	if desiredConcurrency > 0 {
		cfg.DesiredConcurrency = desiredConcurrency
	}
	if recoveryInterval > 0 {
		cfg.RecoveryInterval = recoveryInterval
	}
	if minRecoveryInterval > 0 {
		cfg.MinRecoveryInterval = minRecoveryInterval
	}
	acc := NewAdaptiveConcurrencyController(cfg)

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
		Timeout:   0,
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
		displayVerb: displayVerb,
		authMgr:     authMgr,
	}
}

func NewClient(c *cache.Cache) *Client {
	return NewClientWithTimeout(c, 0, 0, true)
}

func NewClientWithConfig(cacheDir string, fetchTimeout, idleTimeout time.Duration, displayVerb bool) (*Client, error) {
	c, err := cache.NewCache(cacheDir)
	if err != nil {
		return nil, err
	}
	return NewClientWithTimeout(c, fetchTimeout, idleTimeout, displayVerb), nil
}

func NewClientSerial(c *cache.Cache) *Client {
	rl := NewRateLimiter(RateLimiterConfig{
		MaxRequestsPerSecond:     10,
		MinInterval:              100 * time.Millisecond,
		ConsecutiveFailThreshold: 3,
		BackoffInitial:           1 * time.Second,
		BackoffMax:               30 * time.Second,
		ProgressInterval:         10 * time.Second,
	})

	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  false,
		MaxIdleConnsPerHost: 5,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   0,
	}

	return &Client{
		cache:       c,
		rateLimiter: rl,
		concurrency: nil,
		httpClient:  httpClient,
		idleTimeout: 3 * time.Minute,
		displayVerb: true,
	}
}

func NewClientSerialWithAuth(c *cache.Cache, authMgr *auth.Manager) *Client {
	cl := NewClientSerial(c)
	cl.authMgr = authMgr
	return cl
}

func NewClientWithTimeout(c *cache.Cache, timeout time.Duration, idleTimeout time.Duration, displayVerb bool) *Client {
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
		Timeout:   0,
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
		displayVerb: displayVerb,
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
		logger.Errorf("%s -> ERROR: %v", req.URL.Path, err)
		return nil, err
	}

	if resp.StatusCode == 401 {
		_ = resp.Body.Close()
		if c.authMgr != nil {
			c.authMgr.InvalidateToken()
		}
		c.rateLimiter.RecordFailure(resp.StatusCode, fmt.Errorf("unauthorized"))
		logger.Errorf("%s -> HTTP %d (unauthorized)", req.URL.Path, resp.StatusCode)
		return nil, fmt.Errorf("unauthorized: token expired")
	}

	if resp.StatusCode == 429 || resp.StatusCode == 503 {
		c.rateLimiter.RecordFailure(resp.StatusCode, nil)
		_ = resp.Body.Close()
		logger.Errorf("%s -> HTTP %d (rate limited)", req.URL.Path, resp.StatusCode)
		return nil, fmt.Errorf("rate limited: HTTP %d", resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		bodyStr := string(body)
		c.rateLimiter.RecordFailure(resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode))
		logger.Errorf("%s -> HTTP %d: %s", req.URL.Path, resp.StatusCode, bodyStr)
		if resp.StatusCode == 422 && containsMaxSamplesError(bodyStr) {
			return nil, &TooManySamplesError{StatusCode: resp.StatusCode, Body: bodyStr}
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyStr)
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

		if c.authMgr == nil {
			resp, err := c.doRequest(ctx, req)
			if err != nil {
				lastErr = err
				continue
			}
			return resp, nil
		}

		token, err := c.authMgr.GetToken()
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
	defer func() { _ = resp.Body.Close() }()

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

func (c *Client) ListClusters(ctx context.Context, vendorRegions []VendorRegion, bizType string, pageSize int, onProgress func(vendor, region string, count int)) ([]Cluster, error) {
	var allClusters []Cluster
	var firstErr error

	for _, vr := range vendorRegions {
		clusters, err := c.ListClustersPaginated(ctx, vr.Vendor, vr.Region, bizType, pageSize)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		if onProgress != nil && len(clusters) > 0 {
			onProgress(vr.Vendor, vr.Region, len(clusters))
		}
		allClusters = append(allClusters, clusters...)
	}

	if len(allClusters) == 0 && firstErr != nil {
		return nil, firstErr
	}
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
	defer func() { _ = resp.Body.Close() }()

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
	_ = c.cache.SetDsURLCache(clusterID, dsURL)

	return dsURL, nil
}

func (c *Client) QueryMetric(ctx context.Context, clusterID, dsURL, metric string, start, end, step int) (map[string]interface{}, error) {
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

	fetchTimeout := 5 * time.Minute

	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "GET", reqURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequestWithRetry(ctx, httpReq, 3)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	logger.SetConcurrencyProvider(c.concurrency)

	if resp.StatusCode != 200 {
		logger.LogMetricsArrival(clusterID, metric, start, end, resp.StatusCode, 0, true)
	}

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

	done := make(chan struct{})
	defer close(done)

	go func() {
		buf := make([]byte, 32*1024)
		var allData []byte
		for {
			n, err := body.Read(buf)
			if n > 0 {
				allData = append(allData, buf[:n]...)
				select {
				case progressChan <- struct{}{}:
				case <-done:
					return
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

type MetricWriter interface {
	WriteSeries(labels map[string]string, timestamps []int64, values []float64) error
	CheckCacheLimit() error
	Close() error
}

type ChunkedMetricResult struct {
	Result         map[string]interface{}
	DataSize       int
	ByteSize       int64
	TooManySamples bool
	FailedSize     int
}

func (c *Client) QueryMetricChunked(ctx context.Context, clusterID, dsURL, metric string, start, end, step, chunkSize int) (*ChunkedMetricResult, error) {
	return c.QueryMetricChunkedWithWriter(ctx, clusterID, dsURL, metric, start, end, step, chunkSize, 0, nil)
}

func (c *Client) QueryMetricChunkedWithWriter(ctx context.Context, clusterID, dsURL, metric string, start, end, step, chunkSize int, targetBytes int64, writer MetricWriter) (*ChunkedMetricResult, error) {
	if chunkSize <= 0 {
		chunkSize = 1800
	}

	const maxChunkSize = 86400
	const maxGrowth = 8

	var totalDataSize int
	var totalByteSize int64
	currentChunk := chunkSize

	for currentStart := start; currentStart < end; {
		adjustedEnd := currentStart + currentChunk
		if adjustedEnd > end {
			adjustedEnd = end
		}

		if writer != nil {
			if err := writer.CheckCacheLimit(); err != nil {
				return nil, err
			}
		}

		result, _, err := c.QueryMetricWithRetry(ctx, clusterID, dsURL, metric, currentStart, adjustedEnd, step)
		if err != nil {
			if isTooManySamplesError(err) {
				return &ChunkedMetricResult{
					TooManySamples: true,
					FailedSize:     currentChunk,
				}, err
			}
			if isNoDataError(err) {
				logger.LogMetricsArrival(clusterID, metric, currentStart, adjustedEnd, 200, 0, true)
				currentChunk = maxChunkSize
				currentStart = adjustedEnd
				continue
			}
			return nil, err
		}

		var chunkByteSize int64
		if result != nil && writer != nil {
			if err := writeResultToWriter(result, writer); err != nil {
				return nil, err
			}
			totalDataSize += estimateDataSize(result)
			chunkByteSize = estimateByteSize(result)
			totalByteSize += chunkByteSize
		}

		logger.LogMetricsArrival(clusterID, metric, currentStart, adjustedEnd, 200, chunkByteSize, true)

		if targetBytes > 0 && chunkByteSize > 0 && chunkByteSize < targetBytes/2 {
			estimatedChunk := int(float64(currentChunk) * float64(targetBytes) * 0.7 / float64(chunkByteSize))
			if estimatedChunk > currentChunk*maxGrowth {
				estimatedChunk = currentChunk * maxGrowth
			}
			if estimatedChunk > maxChunkSize {
				estimatedChunk = maxChunkSize
			}
			if estimatedChunk > currentChunk {
				currentChunk = estimatedChunk
			}
		} else if chunkByteSize > 0 && chunkByteSize < 512*1024 {
			newChunk := min(currentChunk*2, maxChunkSize)
			if newChunk > currentChunk {
				currentChunk = newChunk
			}
		}

		currentStart = adjustedEnd
	}

	return &ChunkedMetricResult{DataSize: totalDataSize, ByteSize: totalByteSize}, nil
}

func writeResultToWriter(result map[string]interface{}, writer MetricWriter) error {
	if result == nil {
		return nil
	}
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return nil
	}
	results, ok := data["result"].([]interface{})
	if !ok {
		return nil
	}
	for _, r := range results {
		series, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		labels := make(map[string]string)
		if metric, ok := series["metric"].(map[string]interface{}); ok {
			for k, v := range metric {
				if s, ok := v.(string); ok {
					labels[k] = s
				}
			}
		}
		values, ok := series["values"].([]interface{})
		if !ok {
			continue
		}
		var timestamps []int64
		var vals []float64
		for _, v := range values {
			arr, ok := v.([]interface{})
			if !ok || len(arr) < 2 {
				continue
			}
			ts, ok1 := arr[0].(float64)
			val, ok2 := arr[1].(string)
			if !ok1 || !ok2 {
				continue
			}
			timestamps = append(timestamps, int64(ts))
			var f float64
			_, _ = fmt.Sscanf(val, "%f", &f)
			vals = append(vals, f)
		}
		if len(timestamps) > 0 {
			if err := writer.WriteSeries(labels, timestamps, vals); err != nil {
				return err
			}
		}
	}
	return nil
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

func estimateByteSize(result map[string]interface{}) int64 {
	if result == nil {
		return 0
	}
	data, err := json.Marshal(result)
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (c *Client) QueryMetricWithRetry(ctx context.Context, clusterID, dsURL, metric string, start, end, initialStep int) (map[string]interface{}, int, error) {
	stepsToTry := []int{initialStep, 120, 300, 600}

	for _, step := range stepsToTry {
		result, err := c.QueryMetric(ctx, clusterID, dsURL, metric, start, end, step)
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

func (c *Client) FetchMetricsConcurrent(ctx context.Context, clusterID string, metrics []string, start, end, step int, dsURL string, onProgress func(metric string, err error)) map[string]map[string]interface{} {
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

			result, err := c.QueryMetric(ctx, clusterID, dsURL, m, start, end, step)

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
	simpleResults := c.FetchMetricsConcurrent(ctx, clusterID, simpleMetrics, start, end, step, dsURL, func(metric string, err error) {
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
				result, err := c.QueryMetric(ctx, clusterID, dsURL, metric, chunk[0], chunk[1], step)
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

func isNoDataError(err error) bool {
	if err == nil {
		return false
	}
	return contains(err.Error(), "no data found")
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
