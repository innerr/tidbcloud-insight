package analysis

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type DigStorage struct {
	baseDir string
}

type DigRawData struct {
	ClusterID       string                 `json:"cluster_id"`
	Timestamp       int64                  `json:"timestamp"`
	TimeStr         string                 `json:"time_str"`
	Duration        int                    `json:"duration_seconds"`
	QPSData         []TimeSeriesPoint      `json:"qps_data"`
	LatencyData     []TimeSeriesPoint      `json:"latency_data"`
	SQLTypeData     map[string]interface{} `json:"sql_type_data,omitempty"`
	TiKVOpData      map[string]interface{} `json:"tikv_op_data,omitempty"`
	TiKVLatencyData map[string]interface{} `json:"tikv_latency_data,omitempty"`
	TopologyData    map[string]interface{} `json:"topology_data,omitempty"`
	RawMetrics      map[string]interface{} `json:"raw_metrics,omitempty"`
}

type DigAnomalyRecord struct {
	ClusterID  string                 `json:"cluster_id"`
	Timestamp  int64                  `json:"timestamp"`
	TimeStr    string                 `json:"time_str"`
	Duration   int                    `json:"duration_seconds"`
	TotalCount int                    `json:"total_anomalies"`
	PerDay     float64                `json:"anomalies_per_day"`
	ByType     map[string]int         `json:"by_type"`
	Anomalies  []DetectedAnomaly      `json:"anomalies"`
	Summary    map[string]interface{} `json:"summary"`
}

type DigProfileRecord struct {
	ClusterID       string                 `json:"cluster_id"`
	Timestamp       int64                  `json:"timestamp"`
	TimeStr         string                 `json:"time_str"`
	Duration        int                    `json:"duration_seconds"`
	Provider        string                 `json:"provider,omitempty"`
	Region          string                 `json:"region,omitempty"`
	BizType         string                 `json:"biz_type,omitempty"`
	ClusterVersion  string                 `json:"cluster_version,omitempty"`
	TiDBVersion     string                 `json:"tidb_version,omitempty"`
	TiKVVersion     string                 `json:"tikv_version,omitempty"`
	PDVersion       string                 `json:"pd_version,omitempty"`
	Topology        map[string]interface{} `json:"topology,omitempty"`
	LoadProfile     *LoadProfile           `json:"load_profile,omitempty"`
	WorkloadProfile *WorkloadProfile       `json:"workload_profile,omitempty"`
	ActivityLevel   string                 `json:"activity_level,omitempty"`
	QPSMean         float64                `json:"qps_mean,omitempty"`
}

type DigIndex struct {
	ClusterID  string  `json:"cluster_id"`
	LastUpdate int64   `json:"last_update"`
	Count      int     `json:"count"`
	Timestamps []int64 `json:"timestamps"`
}

func NewDigStorage(digDir string) *DigStorage {
	return &DigStorage{
		baseDir: digDir,
	}
}

func (s *DigStorage) getClusterDir(clusterID string) string {
	return filepath.Join(s.baseDir, clusterID)
}

func (s *DigStorage) getSessionDir(clusterID string, timestamp int64) string {
	return filepath.Join(s.getClusterDir(clusterID), fmt.Sprintf("%d", timestamp))
}

func (s *DigStorage) getRawDir(clusterID string, timestamp int64) string {
	return filepath.Join(s.getSessionDir(clusterID, timestamp), "raw")
}

func (s *DigStorage) SaveRawData(clusterID string, data *DigRawData) (string, error) {
	rawDir := s.getRawDir(clusterID, data.Timestamp)

	if err := os.MkdirAll(rawDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create raw directory: %w", err)
	}

	qpsFile := filepath.Join(rawDir, "qps.json")
	qpsData := make([]map[string]interface{}, len(data.QPSData))
	for i, p := range data.QPSData {
		qpsData[i] = map[string]interface{}{
			"timestamp": p.Timestamp,
			"value":     p.Value,
		}
	}
	qpsJSON, _ := json.MarshalIndent(qpsData, "", "  ")
	if err := os.WriteFile(qpsFile, qpsJSON, 0644); err != nil {
		return "", fmt.Errorf("failed to save QPS data: %w", err)
	}

	latencyFile := filepath.Join(rawDir, "latency.json")
	latencyJSON, _ := json.MarshalIndent(data.LatencyData, "", "  ")
	if err := os.WriteFile(latencyFile, latencyJSON, 0644); err != nil {
		return "", fmt.Errorf("failed to save latency data: %w", err)
	}

	if data.SQLTypeData != nil {
		sqlTypeFile := filepath.Join(rawDir, "sql_type.json")
		sqlTypeJSON, _ := json.MarshalIndent(data.SQLTypeData, "", "  ")
		_ = os.WriteFile(sqlTypeFile, sqlTypeJSON, 0644)
	}

	if data.TiKVOpData != nil {
		tikvOpFile := filepath.Join(rawDir, "tikv_op.json")
		tikvOpJSON, _ := json.MarshalIndent(data.TiKVOpData, "", "  ")
		_ = os.WriteFile(tikvOpFile, tikvOpJSON, 0644)
	}

	if data.TiKVLatencyData != nil {
		tikvLatencyFile := filepath.Join(rawDir, "tikv_latency.json")
		tikvLatencyJSON, _ := json.MarshalIndent(data.TiKVLatencyData, "", "  ")
		_ = os.WriteFile(tikvLatencyFile, tikvLatencyJSON, 0644)
	}

	if data.TopologyData != nil {
		topologyFile := filepath.Join(rawDir, "topology.json")
		topologyJSON, _ := json.MarshalIndent(data.TopologyData, "", "  ")
		_ = os.WriteFile(topologyFile, topologyJSON, 0644)
	}

	metaFile := filepath.Join(rawDir, "meta.json")
	metaJSON, _ := json.MarshalIndent(map[string]interface{}{
		"cluster_id":       data.ClusterID,
		"timestamp":        data.Timestamp,
		"time_str":         data.TimeStr,
		"duration_seconds": data.Duration,
		"qps_count":        len(data.QPSData),
		"latency_count":    len(data.LatencyData),
	}, "", "  ")
	if err := os.WriteFile(metaFile, metaJSON, 0644); err != nil {
		return "", fmt.Errorf("failed to save meta data: %w", err)
	}

	s.updateIndex(clusterID, data.Timestamp)

	return rawDir, nil
}

func (s *DigStorage) SaveAnomalies(clusterID string, record *DigAnomalyRecord) (string, error) {
	sessionDir := s.getSessionDir(clusterID, record.Timestamp)
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create session directory: %w", err)
	}

	filePath := filepath.Join(sessionDir, "anomalies.json")

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal anomaly record: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return "", fmt.Errorf("failed to save anomaly record: %w", err)
	}

	s.updateIndex(clusterID, record.Timestamp)

	return filePath, nil
}

func (s *DigStorage) SaveProfile(clusterID string, record *DigProfileRecord) (string, error) {
	sessionDir := s.getSessionDir(clusterID, record.Timestamp)
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create session directory: %w", err)
	}

	filePath := filepath.Join(sessionDir, "profile.json")

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal profile record: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return "", fmt.Errorf("failed to save profile record: %w", err)
	}

	s.updateIndex(clusterID, record.Timestamp)

	return filePath, nil
}

func (s *DigStorage) updateIndex(clusterID string, timestamp int64) {
	indexPath := filepath.Join(s.getClusterDir(clusterID), "index.json")

	var index DigIndex
	index.ClusterID = clusterID

	if data, err := os.ReadFile(indexPath); err == nil {
		_ = json.Unmarshal(data, &index)
	}

	index.LastUpdate = time.Now().Unix()

	found := false
	for _, ts := range index.Timestamps {
		if ts == timestamp {
			found = true
			break
		}
	}
	if !found {
		index.Timestamps = append(index.Timestamps, timestamp)
		sort.Slice(index.Timestamps, func(i, j int) bool {
			return index.Timestamps[i] < index.Timestamps[j]
		})
		index.Count = len(index.Timestamps)
	}

	data, _ := json.MarshalIndent(index, "", "  ")
	_ = os.WriteFile(indexPath, data, 0644)
}

func (s *DigStorage) LoadAnomalies(clusterID string, timestamp int64) (*DigAnomalyRecord, error) {
	filePath := filepath.Join(s.getSessionDir(clusterID, timestamp), "anomalies.json")

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read anomaly file: %w", err)
	}

	var record DigAnomalyRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal anomaly record: %w", err)
	}

	return &record, nil
}

func (s *DigStorage) LoadProfile(clusterID string, timestamp int64) (*DigProfileRecord, error) {
	filePath := filepath.Join(s.getSessionDir(clusterID, timestamp), "profile.json")

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile file: %w", err)
	}

	var record DigProfileRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal profile record: %w", err)
	}

	return &record, nil
}

func (s *DigStorage) LoadRawData(clusterID string, timestamp int64) (*DigRawData, error) {
	rawDir := s.getRawDir(clusterID, timestamp)

	metaFile := filepath.Join(rawDir, "meta.json")
	metaData, err := os.ReadFile(metaFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta file: %w", err)
	}

	var raw DigRawData
	if err := json.Unmarshal(metaData, &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal meta data: %w", err)
	}

	qpsFile := filepath.Join(rawDir, "qps.json")
	if qpsData, err := os.ReadFile(qpsFile); err == nil {
		var qpsPoints []map[string]interface{}
		if json.Unmarshal(qpsData, &qpsPoints) == nil {
			raw.QPSData = make([]TimeSeriesPoint, len(qpsPoints))
			for i, p := range qpsPoints {
				raw.QPSData[i] = TimeSeriesPoint{
					Timestamp: int64(p["timestamp"].(float64)),
					Value:     p["value"].(float64),
				}
			}
		}
	}

	latencyFile := filepath.Join(rawDir, "latency.json")
	if latencyData, err := os.ReadFile(latencyFile); err == nil {
		var latencyPoints []TimeSeriesPoint
		if json.Unmarshal(latencyData, &latencyPoints) == nil {
			raw.LatencyData = latencyPoints
		}
	}

	sqlTypeFile := filepath.Join(rawDir, "sql_type.json")
	if data, err := os.ReadFile(sqlTypeFile); err == nil {
		var sqlTypeData map[string]interface{}
		if json.Unmarshal(data, &sqlTypeData) == nil {
			raw.SQLTypeData = sqlTypeData
		}
	}

	tikvOpFile := filepath.Join(rawDir, "tikv_op.json")
	if data, err := os.ReadFile(tikvOpFile); err == nil {
		var tikvOpData map[string]interface{}
		if json.Unmarshal(data, &tikvOpData) == nil {
			raw.TiKVOpData = tikvOpData
		}
	}

	tikvLatencyFile := filepath.Join(rawDir, "tikv_latency.json")
	if data, err := os.ReadFile(tikvLatencyFile); err == nil {
		var tikvLatencyData map[string]interface{}
		if json.Unmarshal(data, &tikvLatencyData) == nil {
			raw.TiKVLatencyData = tikvLatencyData
		}
	}

	topologyFile := filepath.Join(rawDir, "topology.json")
	if data, err := os.ReadFile(topologyFile); err == nil {
		var topologyData map[string]interface{}
		if json.Unmarshal(data, &topologyData) == nil {
			raw.TopologyData = topologyData
		}
	}

	return &raw, nil
}

func (s *DigStorage) ListClusterAnalyses(clusterID string) (*DigIndex, error) {
	indexPath := filepath.Join(s.getClusterDir(clusterID), "index.json")

	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read index: %w", err)
	}

	var index DigIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index: %w", err)
	}

	return &index, nil
}

func (s *DigStorage) ListAllClusters() ([]string, error) {
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var clusters []string
	for _, entry := range entries {
		if entry.IsDir() {
			s.cleanupInvalidSessions(entry.Name())
			if s.hasValidSessions(entry.Name()) {
				clusters = append(clusters, entry.Name())
			}
		}
	}

	sort.Strings(clusters)
	return clusters, nil
}

func (s *DigStorage) hasValidSessions(clusterID string) bool {
	clusterDir := s.getClusterDir(clusterID)
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.IsDir() {
			ts, err := strconv.ParseInt(entry.Name(), 10, 64)
			if err == nil {
				metaFile := filepath.Join(s.getRawDir(clusterID, ts), "meta.json")
				if _, err := os.Stat(metaFile); err == nil {
					return true
				}
			}
		}
	}
	return false
}

func (s *DigStorage) cleanupInvalidSessions(clusterID string) {
	clusterDir := s.getClusterDir(clusterID)
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			ts, err := strconv.ParseInt(entry.Name(), 10, 64)
			if err == nil {
				metaFile := filepath.Join(s.getRawDir(clusterID, ts), "meta.json")
				if _, err := os.Stat(metaFile); os.IsNotExist(err) {
					sessionDir := s.getSessionDir(clusterID, ts)
					_ = os.RemoveAll(sessionDir)
				}
			}
		}
	}
}

func (s *DigStorage) GetLatestTimestamp(clusterID string) (int64, error) {
	clusterDir := s.getClusterDir(clusterID)

	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		return 0, err
	}

	var timestamps []int64
	for _, entry := range entries {
		if entry.IsDir() {
			ts, err := strconv.ParseInt(entry.Name(), 10, 64)
			if err == nil {
				metaFile := filepath.Join(s.getRawDir(clusterID, ts), "meta.json")
				if _, err := os.Stat(metaFile); err == nil {
					timestamps = append(timestamps, ts)
				}
			}
		}
	}

	if len(timestamps) == 0 {
		return 0, fmt.Errorf("no valid sessions found")
	}

	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] > timestamps[j] })
	return timestamps[0], nil
}

func (s *DigStorage) GetLatestAnomalies(clusterID string) (*DigAnomalyRecord, error) {
	latest, err := s.GetLatestTimestamp(clusterID)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(s.getSessionDir(clusterID, latest), "anomalies.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("no anomaly records found")
	}

	var record DigAnomalyRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal anomaly record: %w", err)
	}

	return &record, nil
}

func (s *DigStorage) GetLatestProfile(clusterID string) (*DigProfileRecord, error) {
	latest, err := s.GetLatestTimestamp(clusterID)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(s.getSessionDir(clusterID, latest), "profile.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("no profile records found")
	}

	var record DigProfileRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal profile record: %w", err)
	}

	return &record, nil
}

func PrintDigStoragePath(rawPath, anomalyPath, profilePath string) {
	wd, _ := os.Getwd()

	fmt.Println("\n  Data saved to:")
	if rawPath != "" {
		fmt.Printf("    Raw data:     %s\n", toRelativePath(rawPath, wd))
	}
	if anomalyPath != "" {
		fmt.Printf("    Anomalies:    %s\n", toRelativePath(anomalyPath, wd))
	}
	if profilePath != "" {
		fmt.Printf("    Profile:      %s\n", toRelativePath(profilePath, wd))
	}
}

func toRelativePath(absPath, wd string) string {
	if wd == "" {
		return absPath
	}

	rel, err := filepath.Rel(wd, absPath)
	if err != nil {
		return absPath
	}

	if strings.HasPrefix(rel, "..") {
		return absPath
	}

	return "./" + rel
}

func (s *DigStorage) DeleteSession(clusterID string, timestamp int64) error {
	sessionDir := s.getSessionDir(clusterID, timestamp)
	if err := os.RemoveAll(sessionDir); err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}

	indexPath := filepath.Join(s.getClusterDir(clusterID), "index.json")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil
	}

	var index DigIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return nil
	}

	var newTimestamps []int64
	for _, ts := range index.Timestamps {
		if ts != timestamp {
			newTimestamps = append(newTimestamps, ts)
		}
	}
	index.Timestamps = newTimestamps
	index.Count = len(newTimestamps)
	index.LastUpdate = time.Now().Unix()

	updatedData, _ := json.MarshalIndent(index, "", "  ")
	_ = os.WriteFile(indexPath, updatedData, 0644)

	if len(newTimestamps) == 0 {
		_ = os.RemoveAll(s.getClusterDir(clusterID))
	}

	return nil
}
