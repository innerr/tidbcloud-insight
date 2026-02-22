package prometheus_storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPrometheusMetricWriter_BasicWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	err = writer.WriteSeries(
		map[string]string{"instance": "localhost:9090"},
		[]int64{1000, 1015, 1030},
		[]float64{1.0, 2.0, 3.0},
	)
	if err != nil {
		t.Fatalf("failed to write series: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, err := s.ListMetricFiles("test_cluster", "test_metric")
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	content, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	if !strings.Contains(string(content), "test_metric{instance=\"localhost:9090\"}") {
		t.Errorf("file content does not contain expected metric: %s", string(content))
	}
}

func TestPrometheusMetricWriter_MultipleSeries(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	series := []struct {
		labels     map[string]string
		timestamps []int64
		values     []float64
	}{
		{
			labels:     map[string]string{"job": "prometheus", "instance": "localhost:9090"},
			timestamps: []int64{1000, 1015, 1030},
			values:     []float64{1.0, 2.0, 3.0},
		},
		{
			labels:     map[string]string{"job": "node", "instance": "localhost:9100"},
			timestamps: []int64{1000, 1015, 1030},
			values:     []float64{10.0, 20.0, 30.0},
		},
		{
			labels:     nil,
			timestamps: []int64{1000, 1015, 1030},
			values:     []float64{100.0, 200.0, 300.0},
		},
	}

	for _, s := range series {
		err = writer.WriteSeries(s.labels, s.timestamps, s.values)
		if err != nil {
			t.Fatalf("failed to write series: %v", err)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	content, _ := os.ReadFile(files[0])
	contentStr := string(content)

	if !strings.Contains(contentStr, `job="node"`) {
		t.Errorf("missing job=node series")
	}
	if !strings.Contains(contentStr, `job="prometheus"`) {
		t.Errorf("missing job=prometheus series")
	}
	if !strings.Contains(contentStr, "test_metric ") {
		t.Errorf("missing series without labels")
	}
}

func TestPrometheusMetricWriter_SlidingWindow_InOrder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 1600)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	chunks := []struct {
		start int64
		end   int64
	}{
		{1000, 1200},
		{1200, 1400},
		{1400, 1600},
	}

	for _, c := range chunks {
		ts := []int64{c.start, c.start + 15, c.end - 15, c.end}
		vals := []float64{float64(c.start), float64(c.start + 1), float64(c.end - 1), float64(c.end)}
		err = writer.WriteSeries(nil, ts, vals)
		if err != nil {
			t.Fatalf("failed to write chunk [%d-%d]: %v", c.start, c.end, err)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	content, _ := os.ReadFile(files[0])
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	var dataLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			dataLines = append(dataLines, line)
		}
	}

	for i := 1; i < len(dataLines); i++ {
		ts1 := extractTimestamp(dataLines[i-1])
		ts2 := extractTimestamp(dataLines[i])
		if ts1 > ts2 {
			t.Errorf("timestamps not in order: %d > %d\nline1: %s\nline2: %s", ts1, ts2, dataLines[i-1], dataLines[i])
		}
	}
}

func TestPrometheusMetricWriter_SlidingWindow_OutOfOrder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 1600)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	chunks := []struct {
		start int64
		end   int64
	}{
		{1200, 1400},
		{1400, 1600},
		{1000, 1200},
	}

	for _, c := range chunks {
		ts := []int64{c.start, c.start + 15, c.end - 15, c.end}
		vals := []float64{float64(c.start), float64(c.start + 1), float64(c.end - 1), float64(c.end)}
		err = writer.WriteSeries(nil, ts, vals)
		if err != nil {
			t.Fatalf("failed to write chunk [%d-%d]: %v", c.start, c.end, err)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	content, _ := os.ReadFile(files[0])
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	var dataLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			dataLines = append(dataLines, line)
		}
	}

	for i := 1; i < len(dataLines); i++ {
		ts1 := extractTimestamp(dataLines[i-1])
		ts2 := extractTimestamp(dataLines[i])
		if ts1 > ts2 {
			t.Errorf("timestamps not in order after out-of-order write: %d > %d\nline1: %s\nline2: %s", ts1, ts2, dataLines[i-1], dataLines[i])
		}
	}

	contentStr := string(content)
	for _, ts := range []int64{1000, 1015, 1185, 1200, 1200, 1215, 1385, 1400, 1400, 1415, 1585, 1600} {
		if !strings.Contains(contentStr, fmt.Sprintf(" %d", ts*1000)) {
			t.Errorf("missing timestamp %d in output", ts)
		}
	}
}

func TestPrometheusMetricWriter_SlidingWindow_PartialGap(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 1600)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	err = writer.WriteSeries(nil, []int64{1400, 1415, 1430}, []float64{14.0, 14.15, 14.30})
	if err != nil {
		t.Fatalf("failed to write chunk 3: %v", err)
	}

	err = writer.WriteSeries(nil, []int64{1000, 1015, 1030}, []float64{10.0, 10.15, 10.30})
	if err != nil {
		t.Fatalf("failed to write chunk 1: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	content, _ := os.ReadFile(files[0])
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	var dataLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			dataLines = append(dataLines, line)
		}
	}

	for i := 1; i < len(dataLines); i++ {
		ts1 := extractTimestamp(dataLines[i-1])
		ts2 := extractTimestamp(dataLines[i])
		if ts1 > ts2 {
			t.Errorf("timestamps not in order with partial gap: %d > %d", ts1, ts2)
		}
	}
}

func TestPrometheusMetricWriter_ConcurrentWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	pointsPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			labels := map[string]string{"goroutine": fmt.Sprintf("g%d", goroutineID)}
			for i := 0; i < pointsPerGoroutine; i++ {
				ts := int64(1000 + i*10)
				val := float64(goroutineID*1000 + i)
				err := writer.WriteSeries(labels, []int64{ts}, []float64{val})
				if err != nil {
					t.Errorf("goroutine %d failed to write: %v", goroutineID, err)
				}
			}
		}(g)
	}

	wg.Wait()

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	content, _ := os.ReadFile(files[0])
	contentStr := string(content)

	for g := 0; g < numGoroutines; g++ {
		label := fmt.Sprintf(`goroutine="g%d"`, g)
		if !strings.Contains(contentStr, label) {
			t.Errorf("missing data for goroutine %d", g)
		}
	}

	lines := strings.Split(strings.TrimSpace(contentStr), "\n")
	var dataLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			dataLines = append(dataLines, line)
		}
	}

	for i := 1; i < len(dataLines); i++ {
		ts1 := extractTimestamp(dataLines[i-1])
		ts2 := extractTimestamp(dataLines[i])
		if ts1 > ts2 {
			t.Errorf("concurrent write resulted in out-of-order timestamps: %d > %d", ts1, ts2)
		}
	}
}

func TestPrometheusMetricWriter_SlidingWindow_ComplexOutOfOrder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 3000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	chunks := []struct {
		start int64
		end   int64
	}{
		{2000, 2500},
		{2500, 3000},
		{1000, 1500},
		{1500, 2000},
	}

	for _, c := range chunks {
		ts := []int64{c.start, (c.start + c.end) / 2, c.end}
		vals := []float64{float64(c.start), float64((c.start + c.end) / 2), float64(c.end)}
		err = writer.WriteSeries(nil, ts, vals)
		if err != nil {
			t.Fatalf("failed to write chunk [%d-%d]: %v", c.start, c.end, err)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	content, _ := os.ReadFile(files[0])
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	var dataLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			dataLines = append(dataLines, line)
		}
	}

	for i := 1; i < len(dataLines); i++ {
		ts1 := extractTimestamp(dataLines[i-1])
		ts2 := extractTimestamp(dataLines[i])
		if ts1 > ts2 {
			t.Errorf("complex out-of-order test failed: %d > %d\nline1: %s\nline2: %s", ts1, ts2, dataLines[i-1], dataLines[i])
		}
	}
}

func TestPrometheusMetricWriter_BytesWritten(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for i := 0; i < 100; i++ {
		ts := int64(1000 + i*10)
		err = writer.WriteSeries(
			map[string]string{"idx": fmt.Sprintf("%d", i)},
			[]int64{ts},
			[]float64{float64(i)},
		)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}

	bytesWritten := writer.BytesWritten()
	if bytesWritten <= 0 {
		t.Errorf("expected bytes written > 0, got %d", bytesWritten)
	}

	writer.Close()

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	stat, _ := os.Stat(files[0])

	if stat.Size() < bytesWritten {
		t.Errorf("file size %d is less than bytes written %d", stat.Size(), bytesWritten)
	}
}

func TestPrometheusMetricWriter_EmptyWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	err = writer.WriteSeries(nil, []int64{}, []float64{})
	if err != nil {
		t.Errorf("empty write should not error: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
}

func TestPrometheusMetricWriter_GapTimeRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	start, end := writer.GapTimeRange()
	if start != 1000 || end != 2000 {
		t.Errorf("expected gap range [1000, 2000], got [%d, %d]", start, end)
	}

	writer.Close()
}

func TestPrometheusMetricWriter_DoubleClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Errorf("second close should not error: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
}

func TestPrometheusMetricWriter_WriteAfterClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Close()

	err = writer.WriteSeries(nil, []int64{1000}, []float64{1.0})
	if err == nil {
		t.Error("expected error when writing after close")
	}
}

func extractTimestamp(line string) int64 {
	parts := strings.Fields(line)
	if len(parts) < 3 {
		return 0
	}

	tsStr := parts[len(parts)-1]
	var ts int64
	fmt.Sscanf(tsStr, "%d", &ts)
	return ts
}

func TestPrometheusMetricWriter_SlidingWindow_WithOverlappingChunks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)
	writer, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	err = writer.WriteSeries(nil, []int64{1000, 1100, 1200}, []float64{1.0, 1.1, 1.2})
	if err != nil {
		t.Fatalf("failed to write chunk 1: %v", err)
	}

	err = writer.WriteSeries(nil, []int64{1100, 1200, 1300}, []float64{2.1, 2.2, 2.3})
	if err != nil {
		t.Fatalf("failed to write chunk 2 (overlapping): %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	files, _ := s.ListMetricFiles("test_cluster", "test_metric")
	content, _ := os.ReadFile(files[0])

	contentStr := string(content)

	ts1100Lines := strings.Count(contentStr, "1100000")
	if ts1100Lines < 2 {
		t.Errorf("expected at least 2 lines with timestamp 1100000, got %d", ts1100Lines)
	}
}

func TestPrometheusMetricWriter_MultipleGaps(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_writer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)

	writer1, err := s.NewMetricWriter("test_cluster", "test_metric", 1000, 2000)
	if err != nil {
		t.Fatalf("failed to create writer1: %v", err)
	}

	writer2, err := s.NewMetricWriter("test_cluster", "test_metric", 3000, 4000)
	if err != nil {
		t.Fatalf("failed to create writer2: %v", err)
	}

	err = writer1.WriteSeries(nil, []int64{1000, 1500, 2000}, []float64{1.0, 1.5, 2.0})
	if err != nil {
		t.Fatalf("failed to write to writer1: %v", err)
	}

	err = writer2.WriteSeries(nil, []int64{3000, 3500, 4000}, []float64{3.0, 3.5, 4.0})
	if err != nil {
		t.Fatalf("failed to write to writer2: %v", err)
	}

	err = writer1.Close()
	if err != nil {
		t.Fatalf("failed to close writer1: %v", err)
	}

	err = writer2.Close()
	if err != nil {
		t.Fatalf("failed to close writer2: %v", err)
	}

	files, err := s.ListMetricFiles("test_cluster", "test_metric")
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files for 2 gaps, got %d", len(files))
	}

	sort.Strings(files)

	file1Name := filepath.Base(files[0])
	file2Name := filepath.Base(files[1])

	expectedPrefix1 := time.Unix(1000, 0).Format("2006-01-02_15-04-05")
	expectedPrefix2 := time.Unix(3000, 0).Format("2006-01-02_15-04-05")

	if !strings.HasPrefix(file1Name, expectedPrefix1) {
		t.Errorf("file1 should start with %s, got %s", expectedPrefix1, file1Name)
	}
	if !strings.HasPrefix(file2Name, expectedPrefix2) {
		t.Errorf("file2 should start with %s, got %s", expectedPrefix2, file2Name)
	}
}
