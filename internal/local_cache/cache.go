package cache

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type MetricsIndex struct {
	NextID  int               `json:"next_id"`
	Queries map[string]string `json:"queries"`
}

type Cache struct {
	baseDir  string
	queryDir string
	index    MetricsIndex
	mutex    sync.RWMutex
}

func NewCache(baseDir string) (*Cache, error) {
	queryDir := filepath.Join(baseDir, "query")
	c := &Cache{
		baseDir:  baseDir,
		queryDir: queryDir,
		index: MetricsIndex{
			NextID:  1,
			Queries: make(map[string]string),
		},
	}

	if err := os.MkdirAll(queryDir, 0755); err != nil {
		return nil, err
	}

	c.loadIndex()
	return c, nil
}

func (c *Cache) loadIndex() {
	indexFile := filepath.Join(c.queryDir, "index.json")
	data, err := os.ReadFile(indexFile)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &c.index)
}

func (c *Cache) saveIndex() error {
	indexFile := filepath.Join(c.queryDir, "index.json")
	data, err := json.MarshalIndent(c.index, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(indexFile, data, 0644)
}

func (c *Cache) GetQueryCacheKey(clusterID string, metrics []string, start, end, step int) string {
	sort.Strings(metrics)
	metricsStr := strings.Join(metrics, ",")
	keyData := fmt.Sprintf("%s|%s|%d|%d|%d", clusterID, metricsStr, start, end, step)
	hash := md5.Sum([]byte(keyData))
	return hex.EncodeToString(hash[:])
}

func (c *Cache) GetCachedMetrics(cacheKey string) (map[string]map[string]interface{}, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	cacheID, exists := c.index.Queries[cacheKey]
	if !exists {
		return nil, nil
	}

	cacheDir := filepath.Join(c.queryDir, cacheID)
	info, err := os.Stat(cacheDir)
	if err != nil || !info.IsDir() {
		return nil, nil
	}

	files, err := filepath.Glob(filepath.Join(cacheDir, "*.json"))
	if err != nil {
		return nil, err
	}

	result := make(map[string]map[string]interface{})
	for _, f := range files {
		metricName := strings.TrimSuffix(filepath.Base(f), ".json")
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var metricData map[string]interface{}
		if err := json.Unmarshal(data, &metricData); err != nil {
			continue
		}
		result[metricName] = metricData
	}

	if len(result) == 0 {
		return nil, nil
	}

	return result, nil
}

func (c *Cache) SaveMetricsCache(cacheKey string, metricsData map[string]map[string]interface{}) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var cacheID string
	if existingID, exists := c.index.Queries[cacheKey]; exists {
		cacheID = existingID
	} else {
		cacheID = strconv.Itoa(c.index.NextID)
		c.index.NextID++
		c.index.Queries[cacheKey] = cacheID
	}

	cacheDir := filepath.Join(c.queryDir, cacheID)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", err
	}

	for metricName, data := range metricsData {
		cacheFile := filepath.Join(cacheDir, metricName+".json")
		jsonData, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(cacheFile, jsonData, 0644); err != nil {
			return "", err
		}
	}

	if err := c.saveIndex(); err != nil {
		return "", err
	}

	return cacheID, nil
}

func (c *Cache) GetCacheDir(cacheID string) string {
	return filepath.Join(c.queryDir, cacheID)
}

func (c *Cache) ListCaches() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make([]string, 0, len(c.index.Queries))
	for _, id := range c.index.Queries {
		result = append(result, id)
	}
	return result
}

func (c *Cache) Clear() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	entries, err := os.ReadDir(c.queryDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.Name() == "index.json" {
			continue
		}
		path := filepath.Join(c.queryDir, entry.Name())
		_ = os.RemoveAll(path)
	}

	c.index = MetricsIndex{
		NextID:  1,
		Queries: make(map[string]string),
	}

	return c.saveIndex()
}

func (c *Cache) GetIndex() MetricsIndex {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.index
}

func (c *Cache) GetDsURLCache(clusterID string) (string, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	cacheFile := filepath.Join(c.baseDir, "ds_url", "cache.json")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return "", false
	}

	var cache map[string]string
	if err := json.Unmarshal(data, &cache); err != nil {
		return "", false
	}

	url, exists := cache[clusterID]
	return url, exists
}

func (c *Cache) SetDsURLCache(clusterID, dsURL string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	cacheDir := filepath.Join(c.baseDir, "ds_url")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return err
	}

	cacheFile := filepath.Join(cacheDir, "cache.json")

	cache := make(map[string]string)
	data, err := os.ReadFile(cacheFile)
	if err == nil {
		_ = json.Unmarshal(data, &cache)
	}

	cache[clusterID] = dsURL

	jsonData, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(cacheFile, jsonData, 0644)
}
