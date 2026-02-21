package analysis

import (
	"os"
	"path/filepath"
	"testing"
)

func TestQueryStorage(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewQueryStorage(tmpDir)

	clusterID := "test_cluster"
	metricName := "tidb_server_query_total"
	lowerBound := int64(1700000000)
	upperBound := int64(1700003600)

	writer, err := storage.NewCSVWriter(clusterID, metricName, lowerBound, upperBound)
	if err != nil {
		t.Fatalf("NewCSVWriter failed: %v", err)
	}

	err = writer.WriteSeries(map[string]string{"instance": "10.0.0.1:4000"}, []int64{1700000000, 1700000060}, []float64{100.0, 110.0})
	if err != nil {
		t.Fatalf("WriteSeries failed: %v", err)
	}

	err = writer.WriteSeries(map[string]string{"instance": "10.0.0.2:4000"}, []int64{1700000000, 1700000060}, []float64{200.0, 210.0})
	if err != nil {
		t.Fatalf("WriteSeries failed: %v", err)
	}

	jsonPath, err := writer.Finalize()
	if err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	expectedPath := filepath.Join(tmpDir, clusterID, metricName, "1700000000-1700003600.json")
	if jsonPath != expectedPath {
		t.Errorf("Finalize path = %s, want %s", jsonPath, expectedPath)
	}

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("JSON file not created at expected path: %s", expectedPath)
	}

	tmpDirPath := filepath.Join(tmpDir, clusterID, metricName, ".tmp")
	if _, err := os.Stat(tmpDirPath); !os.IsNotExist(err) {
		t.Errorf("Temp directory should be cleaned up: %s", tmpDirPath)
	}

	loaded, err := storage.LoadMetric(clusterID, metricName, lowerBound, upperBound)
	if err != nil {
		t.Fatalf("LoadMetric failed: %v", err)
	}

	if loaded == nil {
		t.Fatal("LoadMetric returned nil")
	}

	data, ok := loaded["data"].(map[string]interface{})
	if !ok {
		t.Fatal("LoadMetric data field is not a map")
	}

	results, ok := data["result"].([]interface{})
	if !ok {
		t.Fatal("LoadMetric result field is not a slice")
	}

	if len(results) != 2 {
		t.Errorf("LoadMetric result length = %d, want 2", len(results))
	}

	ranges, err := storage.ListMetricTimeRanges(clusterID, metricName)
	if err != nil {
		t.Fatalf("ListMetricTimeRanges failed: %v", err)
	}

	if len(ranges) != 1 {
		t.Errorf("ListMetricTimeRanges length = %d, want 1", len(ranges))
	}

	if ranges[0][0] != lowerBound || ranges[0][1] != upperBound {
		t.Errorf("ListMetricTimeRanges[0] = [%d, %d], want [%d, %d]",
			ranges[0][0], ranges[0][1], lowerBound, upperBound)
	}

	clusters, err := storage.ListClusters()
	if err != nil {
		t.Fatalf("ListClusters failed: %v", err)
	}

	if len(clusters) != 1 || clusters[0] != clusterID {
		t.Errorf("ListClusters = %v, want [%s]", clusters, clusterID)
	}

	entries, err := storage.ListAllEntries()
	if err != nil {
		t.Fatalf("ListAllEntries failed: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("ListAllEntries length = %d, want 1", len(entries))
	}

	expectedEntry := "test_cluster/tidb_server_query_total/1700000000-1700003600"
	if entries[0] != expectedEntry {
		t.Errorf("ListAllEntries[0] = %s, want %s", entries[0], expectedEntry)
	}
}

func TestRawStorage(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewRawStorage(tmpDir)

	clusterID := "test_cluster"
	metricName := "qps"
	lowerBound := int64(1700000000)
	upperBound := int64(1700003600)

	data := map[string]interface{}{
		"data": []map[string]interface{}{
			{"timestamp": 1700000000, "value": 100.0},
			{"timestamp": 1700000100, "value": 110.0},
		},
	}

	path, err := storage.SaveMetric(clusterID, metricName, data, lowerBound, upperBound)
	if err != nil {
		t.Fatalf("SaveMetric failed: %v", err)
	}

	expectedPath := filepath.Join(tmpDir, clusterID, metricName, "1700000000-1700003600.json")
	if path != expectedPath {
		t.Errorf("SaveMetric path = %s, want %s", path, expectedPath)
	}

	loaded, err := storage.LoadMetric(clusterID, metricName, lowerBound, upperBound)
	if err != nil {
		t.Fatalf("LoadMetric failed: %v", err)
	}

	if loaded == nil {
		t.Fatal("LoadMetric returned nil")
	}

	ranges, err := storage.ListMetricTimeRanges(clusterID, metricName)
	if err != nil {
		t.Fatalf("ListMetricTimeRanges failed: %v", err)
	}

	if len(ranges) != 1 {
		t.Errorf("ListMetricTimeRanges length = %d, want 1", len(ranges))
	}

	if ranges[0][0] != lowerBound || ranges[0][1] != upperBound {
		t.Errorf("ListMetricTimeRanges[0] = [%d, %d], want [%d, %d]",
			ranges[0][0], ranges[0][1], lowerBound, upperBound)
	}
}

func TestDigStorageWithRaw(t *testing.T) {
	tmpDir := t.TempDir()
	digDir := filepath.Join(tmpDir, "dig")
	rawDir := filepath.Join(tmpDir, "raw")

	storage := NewDigStorageWithRaw(digDir, rawDir)

	clusterID := "test_cluster"
	lowerBound := int64(1700000000)
	upperBound := int64(1700003600)

	rawData := &DigRawData{
		ClusterID:  clusterID,
		Timestamp:  1700001000,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		TimeStr:    "2023-11-15 00:00",
		Duration:   3600,
		QPSData: []TimeSeriesPoint{
			{Timestamp: 1700000000, Value: 100.0},
			{Timestamp: 1700000100, Value: 110.0},
		},
		LatencyData: []TimeSeriesPoint{
			{Timestamp: 1700000000, Value: 0.005},
		},
	}

	path, err := storage.SaveRawData(clusterID, rawData)
	if err != nil {
		t.Fatalf("SaveRawData failed: %v", err)
	}

	expectedDir := filepath.Join(rawDir, clusterID, "meta")
	if path != expectedDir {
		t.Errorf("SaveRawData path = %s, want %s", path, expectedDir)
	}

	loaded, err := storage.LoadRawData(clusterID, lowerBound, upperBound)
	if err != nil {
		t.Fatalf("LoadRawData failed: %v", err)
	}

	if loaded.ClusterID != clusterID {
		t.Errorf("LoadRawData ClusterID = %s, want %s", loaded.ClusterID, clusterID)
	}

	if len(loaded.QPSData) != 2 {
		t.Errorf("LoadRawData QPSData length = %d, want 2", len(loaded.QPSData))
	}

	anomalyRecord := &DigAnomalyRecord{
		ClusterID:  clusterID,
		Timestamp:  1700001000,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		TimeStr:    "2023-11-15 00:00",
		Duration:   3600,
		TotalCount: 2,
	}
	anomalyPath, err := storage.SaveAnomalies(clusterID, anomalyRecord)
	if err != nil {
		t.Fatalf("SaveAnomalies failed: %v", err)
	}

	expectedAnomalyPath := filepath.Join(digDir, "1700000000-1700003600", clusterID, "anomalies.json")
	if anomalyPath != expectedAnomalyPath {
		t.Errorf("SaveAnomalies path = %s, want %s", anomalyPath, expectedAnomalyPath)
	}

	profileRecord := &DigProfileRecord{
		ClusterID:  clusterID,
		Timestamp:  1700001000,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		TimeStr:    "2023-11-15 00:00",
		Duration:   3600,
	}
	profilePath, err := storage.SaveProfile(clusterID, profileRecord)
	if err != nil {
		t.Fatalf("SaveProfile failed: %v", err)
	}

	expectedProfilePath := filepath.Join(digDir, "1700000000-1700003600", clusterID, "profile.json")
	if profilePath != expectedProfilePath {
		t.Errorf("SaveProfile path = %s, want %s", profilePath, expectedProfilePath)
	}

	clusters, err := storage.ListAllClusters()
	if err != nil {
		t.Fatalf("ListAllClusters failed: %v", err)
	}

	if len(clusters) != 1 || clusters[0] != clusterID {
		t.Errorf("ListAllClusters = %v, want [%s]", clusters, clusterID)
	}

	session, err := storage.GetLatestSession(clusterID)
	if err != nil {
		t.Fatalf("GetLatestSession failed: %v", err)
	}

	if session.LowerBound != lowerBound || session.UpperBound != upperBound {
		t.Errorf("GetLatestSession = [%d, %d], want [%d, %d]",
			session.LowerBound, session.UpperBound, lowerBound, upperBound)
	}
}

func TestDigStorageDirectoryStructure(t *testing.T) {
	tmpDir := t.TempDir()
	digDir := filepath.Join(tmpDir, "dig")
	rawDir := filepath.Join(tmpDir, "raw")

	storage := NewDigStorageWithRaw(digDir, rawDir)

	clusterID := "test_cluster"
	lowerBound := int64(1700000000)
	upperBound := int64(1700003600)

	rawData := &DigRawData{
		ClusterID:  clusterID,
		Timestamp:  1700001000,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		TimeStr:    "2023-11-15 00:00",
		Duration:   3600,
		QPSData: []TimeSeriesPoint{
			{Timestamp: 1700000000, Value: 100.0},
		},
	}

	_, _ = storage.SaveRawData(clusterID, rawData)

	qpsPath := filepath.Join(rawDir, clusterID, "qps", "1700000000-1700003600.json")
	if _, err := os.Stat(qpsPath); os.IsNotExist(err) {
		t.Errorf("QPS file not created at expected path: %s", qpsPath)
	}

	metaPath := filepath.Join(rawDir, clusterID, "meta", "1700000000-1700003600.json")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Errorf("Meta file not created at expected path: %s", metaPath)
	}

	sessionDir := filepath.Join(digDir, "1700000000-1700003600")
	if _, err := os.Stat(sessionDir); os.IsNotExist(err) {
		t.Errorf("Session directory not created at expected path: %s", sessionDir)
	}
}
