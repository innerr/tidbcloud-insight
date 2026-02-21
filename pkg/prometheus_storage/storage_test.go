package prometheus_storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCalculateNonOverlappingRanges(t *testing.T) {
	tests := []struct {
		name           string
		startTS        int64
		endTS          int64
		existingRanges []TimeSegment
		expected       [][2]int64
	}{
		{
			name:           "no existing ranges - return full range",
			startTS:        1000,
			endTS:          2000,
			existingRanges: []TimeSegment{},
			expected:       [][2]int64{{1000, 2000}},
		},
		{
			name:    "existing range fully covers request - return empty",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 500, End: 2500},
			},
			expected: nil,
		},
		{
			name:    "existing range before request - no overlap",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 100, End: 500},
			},
			expected: [][2]int64{{1000, 2000}},
		},
		{
			name:    "existing range after request - no overlap",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 2500, End: 3000},
			},
			expected: [][2]int64{{1000, 2000}},
		},
		{
			name:    "existing range overlaps start - return tail",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 500, End: 1500},
			},
			expected: [][2]int64{{1500, 2000}},
		},
		{
			name:    "existing range overlaps end - return head",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 1500, End: 2500},
			},
			expected: [][2]int64{{1000, 1500}},
		},
		{
			name:    "existing range in middle - return two segments",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 1200, End: 1800},
			},
			expected: [][2]int64{{1000, 1200}, {1800, 2000}},
		},
		{
			name:    "multiple existing ranges - complex case",
			startTS: 1000,
			endTS:   3000,
			existingRanges: []TimeSegment{
				{Start: 500, End: 1200},
				{Start: 1800, End: 2200},
			},
			expected: [][2]int64{{1200, 1800}, {2200, 3000}},
		},
		{
			name:    "existing range matches request exactly - return empty",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 1000, End: 2000},
			},
			expected: nil,
		},
		{
			name:    "existing range touches start - no overlap",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 500, End: 1000},
			},
			expected: [][2]int64{{1000, 2000}},
		},
		{
			name:    "existing range touches end - no overlap",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 2000, End: 2500},
			},
			expected: [][2]int64{{1000, 2000}},
		},
		{
			name:    "three existing ranges in middle",
			startTS: 1000,
			endTS:   5000,
			existingRanges: []TimeSegment{
				{Start: 1200, End: 1500},
				{Start: 2000, End: 2500},
				{Start: 3000, End: 3500},
			},
			expected: [][2]int64{{1000, 1200}, {1500, 2000}, {2500, 3000}, {3500, 5000}},
		},
		{
			name:    "existing ranges fully cover request",
			startTS: 1000,
			endTS:   2000,
			existingRanges: []TimeSegment{
				{Start: 500, End: 1500},
				{Start: 1500, End: 2500},
			},
			expected: nil,
		},
		{
			name:    "small gap between existing ranges",
			startTS: 1000,
			endTS:   3000,
			existingRanges: []TimeSegment{
				{Start: 500, End: 1500},
				{Start: 1550, End: 3500},
			},
			expected: [][2]int64{{1500, 1550}},
		},
	}

	s := &PrometheusStorage{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.CalculateNonOverlappingRanges(tt.startTS, tt.endTS, tt.existingRanges)
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d ranges, got %d: %v", len(tt.expected), len(result), result)
				return
			}
			for i, exp := range tt.expected {
				if result[i][0] != exp[0] || result[i][1] != exp[1] {
					t.Errorf("range %d: expected [%d, %d], got [%d, %d]",
						i, exp[0], exp[1], result[i][0], result[i][1])
				}
			}
		})
	}
}

func TestFormatTimeRangeForFilename(t *testing.T) {
	loc, _ := time.LoadLocation("Local")
	tests := []struct {
		name     string
		startTS  int64
		endTS    int64
		expected string
	}{
		{
			name:     "normal range",
			startTS:  1640000600,
			endTS:    1640004200,
			expected: time.Unix(1640000600, 0).In(loc).Format("2006-01-02_15-04-05") + "_" + time.Unix(1640004200, 0).In(loc).Format("2006-01-02_15-04-05"),
		},
		{
			name:     "same start and end",
			startTS:  1640000600,
			endTS:    1640000600,
			expected: time.Unix(1640000600, 0).In(loc).Format("2006-01-02_15-04-05") + "_" + time.Unix(1640000600, 0).In(loc).Format("2006-01-02_15-04-05"),
		},
		{
			name:     "one hour range",
			startTS:  1700000000,
			endTS:    1700003600,
			expected: time.Unix(1700000000, 0).In(loc).Format("2006-01-02_15-04-05") + "_" + time.Unix(1700003600, 0).In(loc).Format("2006-01-02_15-04-05"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatTimeRangeForFilename(tt.startTS, tt.endTS)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestMergeAdjacentFiles(t *testing.T) {
	tests := []struct {
		name              string
		setupFiles        func(dir string) error
		expectedFileCount int
		expectedMerged    bool
	}{
		{
			name: "single file - no merge needed",
			setupFiles: func(metricDir string) error {
				content := `# TYPE test gauge
test{a="1"} 1 1640000000000
test{a="1"} 2 1640000015000
`
				return os.WriteFile(filepath.Join(metricDir, "2021-12-20_19-06-40_2021-12-20_19-07-40.prom"), []byte(content), 0644)
			},
			expectedFileCount: 1,
			expectedMerged:    false,
		},
		{
			name: "two adjacent files with gap > threshold - no merge",
			setupFiles: func(metricDir string) error {
				content1 := `# TYPE test gauge
test{a="1"} 1 1640000000000
`
				content2 := `# TYPE test gauge
test{a="1"} 2 1640003600000
`
				if err := os.WriteFile(filepath.Join(metricDir, "2021-12-20_19-06-40_2021-12-20_19-07-40.prom"), []byte(content1), 0644); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(metricDir, "2021-12-20_20-06-40_2021-12-20_20-07-40.prom"), []byte(content2), 0644)
			},
			expectedFileCount: 2,
			expectedMerged:    false,
		},
		{
			name: "two adjacent files with small gap - should merge",
			setupFiles: func(metricDir string) error {
				content1 := `# TYPE test gauge
test{a="1"} 1 1640000000000
test{a="1"} 2 1640000015000
`
				content2 := `# TYPE test gauge
test{a="1"} 3 1640000030000
test{a="1"} 4 1640000045000
`
				if err := os.WriteFile(filepath.Join(metricDir, "2021-12-20_19-06-40_2021-12-20_19-07-40.prom"), []byte(content1), 0644); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(metricDir, "2021-12-20_19-07-40_2021-12-20_19-08-40.prom"), []byte(content2), 0644)
			},
			expectedFileCount: 1,
			expectedMerged:    true,
		},
		{
			name: "three adjacent files - merge all",
			setupFiles: func(metricDir string) error {
				content1 := `# TYPE test gauge
test{a="1"} 1 1640000000000
`
				content2 := `# TYPE test gauge
test{a="1"} 2 1640000015000
`
				content3 := `# TYPE test gauge
test{a="1"} 3 1640000030000
`
				if err := os.WriteFile(filepath.Join(metricDir, "2021-12-20_19-06-40_2021-12-20_19-07-00.prom"), []byte(content1), 0644); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(metricDir, "2021-12-20_19-07-00_2021-12-20_19-07-30.prom"), []byte(content2), 0644); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(metricDir, "2021-12-20_19-07-30_2021-12-20_19-08-00.prom"), []byte(content3), 0644)
			},
			expectedFileCount: 1,
			expectedMerged:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "prometheus_test_*")
			if err != nil {
				t.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			metricDir := filepath.Join(tmpDir, "test_cluster", "test_metric")
			if err := os.MkdirAll(metricDir, 0755); err != nil {
				t.Fatalf("failed to create metric dir: %v", err)
			}

			if err := tt.setupFiles(metricDir); err != nil {
				t.Fatalf("failed to setup files: %v", err)
			}

			s := NewPrometheusStorage(tmpDir)
			if err := s.MergeAdjacentFiles("test_cluster", "test_metric"); err != nil {
				t.Fatalf("MergeAdjacentFiles failed: %v", err)
			}

			files, err := s.ListMetricFiles("test_cluster", "test_metric")
			if err != nil {
				t.Fatalf("ListMetricFiles failed: %v", err)
			}

			if len(files) != tt.expectedFileCount {
				t.Errorf("expected %d files, got %d: %v", tt.expectedFileCount, len(files), files)
			}
		})
	}
}

func TestSaveMetricData(t *testing.T) {
	tests := []struct {
		name        string
		data        map[string]interface{}
		startTS     int64
		endTS       int64
		expectError bool
	}{
		{
			name: "valid data",
			data: map[string]interface{}{
				"data": map[string]interface{}{
					"result": []interface{}{
						map[string]interface{}{
							"metric": map[string]interface{}{
								"__name__": "test_metric",
								"instance": "localhost:9090",
							},
							"values": []interface{}{
								[]interface{}{1640000000.0, "1.0"},
								[]interface{}{1640000015.0, "2.0"},
							},
						},
					},
				},
			},
			startTS:     1640000000,
			endTS:       1640000100,
			expectError: false,
		},
		{
			name: "missing data field",
			data: map[string]interface{}{
				"status": "success",
			},
			startTS:     1640000000,
			endTS:       1640000100,
			expectError: true,
		},
		{
			name: "empty results",
			data: map[string]interface{}{
				"data": map[string]interface{}{
					"result": []interface{}{},
				},
			},
			startTS:     1640000000,
			endTS:       1640000100,
			expectError: true,
		},
		{
			name: "multiple series",
			data: map[string]interface{}{
				"data": map[string]interface{}{
					"result": []interface{}{
						map[string]interface{}{
							"metric": map[string]interface{}{
								"__name__": "test_metric",
								"job":      "node",
							},
							"values": []interface{}{
								[]interface{}{1640000000.0, "1.0"},
							},
						},
						map[string]interface{}{
							"metric": map[string]interface{}{
								"__name__": "test_metric",
								"job":      "prometheus",
							},
							"values": []interface{}{
								[]interface{}{1640000000.0, "2.0"},
							},
						},
					},
				},
			},
			startTS:     1640000000,
			endTS:       1640000100,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "prometheus_test_*")
			if err != nil {
				t.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			s := NewPrometheusStorage(tmpDir)
			filePath, err := s.SaveMetricData("test_cluster", "test_metric", tt.data, tt.startTS, tt.endTS)

			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				t.Errorf("file not created: %s", filePath)
			}

			expectedFilename := formatTimeRangeForFilename(tt.startTS, tt.endTS) + ".prom"
			if filepath.Base(filePath) != expectedFilename {
				t.Errorf("expected filename %s, got %s", expectedFilename, filepath.Base(filePath))
			}
		})
	}
}

func TestAnalyzeMetric(t *testing.T) {
	tests := []struct {
		name             string
		setupFiles       func(dir string) error
		expectedSegments int
		expectedMinTime  int64
		expectedMaxTime  int64
		expectedTotalPts int
	}{
		{
			name: "single continuous file",
			setupFiles: func(dir string) error {
				content := `# TYPE test gauge
test{a="1"} 1 1640000000000
test{a="1"} 2 1640000015000
test{a="1"} 3 1640000030000
`
				return os.WriteFile(filepath.Join(dir, "2021-12-20_19-06-40_2021-12-20_19-08-00.prom"), []byte(content), 0644)
			},
			expectedSegments: 1,
			expectedMinTime:  1640000000,
			expectedMaxTime:  1640000030,
			expectedTotalPts: 3,
		},
		{
			name: "two files with gap",
			setupFiles: func(dir string) error {
				content1 := `# TYPE test gauge
test{a="1"} 1 1640000000000
test{a="1"} 2 1640000015000
`
				content2 := `# TYPE test gauge
test{a="1"} 3 1640003600000
test{a="1"} 4 1640003615000
`
				if err := os.WriteFile(filepath.Join(dir, "2021-12-20_19-06-40_2021-12-20_19-07-30.prom"), []byte(content1), 0644); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(dir, "2021-12-20_20-06-40_2021-12-20_20-07-30.prom"), []byte(content2), 0644)
			},
			expectedSegments: 2,
			expectedMinTime:  1640000000,
			expectedMaxTime:  1640003615,
			expectedTotalPts: 4,
		},
		{
			name: "file with internal gap",
			setupFiles: func(dir string) error {
				content := `# TYPE test gauge
test{a="1"} 1 1640000000000
test{a="1"} 2 1640000015000
test{a="1"} 3 1640003600000
test{a="1"} 4 1640003615000
`
				return os.WriteFile(filepath.Join(dir, "2021-12-20_19-06-40_2021-12-20_20-07-30.prom"), []byte(content), 0644)
			},
			expectedSegments: 2,
			expectedMinTime:  1640000000,
			expectedMaxTime:  1640003615,
			expectedTotalPts: 4,
		},
		{
			name: "multiple series with same timestamps",
			setupFiles: func(dir string) error {
				content := `# TYPE test gauge
test{a="1"} 1 1640000000000
test{a="2"} 2 1640000000000
test{a="1"} 3 1640000015000
test{a="2"} 4 1640000015000
`
				return os.WriteFile(filepath.Join(dir, "2021-12-20_19-06-40_2021-12-20_19-07-30.prom"), []byte(content), 0644)
			},
			expectedSegments: 1,
			expectedMinTime:  1640000000,
			expectedMaxTime:  1640000015,
			expectedTotalPts: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "prometheus_test_*")
			if err != nil {
				t.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			metricDir := filepath.Join(tmpDir, "test_cluster", "test_metric")
			if err := os.MkdirAll(metricDir, 0755); err != nil {
				t.Fatalf("failed to create metric dir: %v", err)
			}

			if err := tt.setupFiles(metricDir); err != nil {
				t.Fatalf("failed to setup files: %v", err)
			}

			s := NewPrometheusStorage(tmpDir)
			info, err := s.AnalyzeMetric("test_cluster", "test_metric")
			if err != nil {
				t.Fatalf("AnalyzeMetric failed: %v", err)
			}

			if len(info.Segments) != tt.expectedSegments {
				t.Errorf("expected %d segments, got %d", tt.expectedSegments, len(info.Segments))
			}

			if info.MinTime != tt.expectedMinTime {
				t.Errorf("expected min time %d, got %d", tt.expectedMinTime, info.MinTime)
			}

			if info.MaxTime != tt.expectedMaxTime {
				t.Errorf("expected max time %d, got %d", tt.expectedMaxTime, info.MaxTime)
			}

			if info.TotalPoints != tt.expectedTotalPts {
				t.Errorf("expected %d total points, got %d", tt.expectedTotalPts, info.TotalPoints)
			}
		})
	}
}

func TestComputeSegments(t *testing.T) {
	tests := []struct {
		name         string
		timestamps   []int64
		tsCounts     map[int64]int
		expectedSegs []TimeSegment
	}{
		{
			name:         "empty",
			timestamps:   []int64{},
			tsCounts:     map[int64]int{},
			expectedSegs: []TimeSegment{},
		},
		{
			name:         "single point",
			timestamps:   []int64{1000},
			tsCounts:     map[int64]int{1000: 1},
			expectedSegs: []TimeSegment{{Start: 1000, End: 1000, PointCount: 1}},
		},
		{
			name:         "continuous points",
			timestamps:   []int64{1000, 1015, 1030, 1045},
			tsCounts:     map[int64]int{1000: 1, 1015: 1, 1030: 1, 1045: 1},
			expectedSegs: []TimeSegment{{Start: 1000, End: 1045, PointCount: 4}},
		},
		{
			name:       "gap in middle",
			timestamps: []int64{1000, 1015, 2000, 2015},
			tsCounts:   map[int64]int{1000: 1, 1015: 1, 2000: 1, 2015: 1},
			expectedSegs: []TimeSegment{
				{Start: 1000, End: 1015, PointCount: 2},
				{Start: 2000, End: 2015, PointCount: 2},
			},
		},
		{
			name:         "multiple series at same timestamp",
			timestamps:   []int64{1000, 1000, 1015, 1015},
			tsCounts:     map[int64]int{1000: 2, 1015: 2},
			expectedSegs: []TimeSegment{{Start: 1000, End: 1015, PointCount: 8}},
		},
	}

	s := &PrometheusStorage{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.computeSegments(tt.timestamps, tt.tsCounts)
			if len(result) != len(tt.expectedSegs) {
				t.Errorf("expected %d segments, got %d", len(tt.expectedSegs), len(result))
				return
			}
			for i, exp := range tt.expectedSegs {
				if result[i].Start != exp.Start || result[i].End != exp.End || result[i].PointCount != exp.PointCount {
					t.Errorf("segment %d: expected %+v, got %+v", i, exp, result[i])
				}
			}
		})
	}
}

func TestIntegration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "prometheus_integration_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s := NewPrometheusStorage(tmpDir)

	data1 := map[string]interface{}{
		"data": map[string]interface{}{
			"result": []interface{}{
				map[string]interface{}{
					"metric": map[string]interface{}{
						"__name__": "test_metric",
						"instance": "localhost:9090",
					},
					"values": []interface{}{
						[]interface{}{1640000000.0, "1.0"},
						[]interface{}{1640000015.0, "2.0"},
						[]interface{}{1640000030.0, "3.0"},
					},
				},
			},
		},
	}

	file1, err := s.SaveMetricData("test_cluster", "test_metric", data1, 1640000000, 1640000100)
	if err != nil {
		t.Fatalf("failed to save first data: %v", err)
	}

	ranges, err := s.GetExistingTimeRanges("test_cluster", "test_metric")
	if err != nil {
		t.Fatalf("failed to get existing ranges: %v", err)
	}
	if len(ranges) != 1 {
		t.Errorf("expected 1 existing range, got %d", len(ranges))
	}

	// Existing range is 1640000000-1640000030 (actual data)
	// Request for 1640000000-1640000200 should return 1640000030-1640000200
	nonOverlap := s.CalculateNonOverlappingRanges(1640000000, 1640000200, ranges)
	if len(nonOverlap) != 1 || nonOverlap[0][0] != 1640000030 || nonOverlap[0][1] != 1640000200 {
		t.Errorf("expected non-overlap [[1640000030, 1640000200]], got %v", nonOverlap)
	}

	// Request for 1640000000-1640000050 should return 1640000030-1640000050 (20 seconds gap)
	nonOverlap2 := s.CalculateNonOverlappingRanges(1640000000, 1640000050, ranges)
	if len(nonOverlap2) != 1 || nonOverlap2[0][0] != 1640000030 || nonOverlap2[0][1] != 1640000050 {
		t.Errorf("expected non-overlap [[1640000030, 1640000050]], got %v", nonOverlap2)
	}

	// Request fully covered range should return empty
	nonOverlap3 := s.CalculateNonOverlappingRanges(1640000000, 1640000030, ranges)
	if len(nonOverlap3) != 0 {
		t.Errorf("expected no non-overlap for covered range, got %v", nonOverlap3)
	}

	data2 := map[string]interface{}{
		"data": map[string]interface{}{
			"result": []interface{}{
				map[string]interface{}{
					"metric": map[string]interface{}{
						"__name__": "test_metric",
						"instance": "localhost:9090",
					},
					"values": []interface{}{
						[]interface{}{1640000100.0, "4.0"},
						[]interface{}{1640000115.0, "5.0"},
						[]interface{}{1640000130.0, "6.0"},
					},
				},
			},
		},
	}

	file2, err := s.SaveMetricData("test_cluster", "test_metric", data2, 1640000100, 1640000200)
	if err != nil {
		t.Fatalf("failed to save second data: %v", err)
	}

	if filepath.Base(file1) == filepath.Base(file2) {
		t.Error("second file should have different name from first file")
	}

	files, err := s.ListMetricFiles("test_cluster", "test_metric")
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files before merge, got %d", len(files))
	}

	if err := s.MergeAdjacentFiles("test_cluster", "test_metric"); err != nil {
		t.Fatalf("failed to merge adjacent files: %v", err)
	}

	files, err = s.ListMetricFiles("test_cluster", "test_metric")
	if err != nil {
		t.Fatalf("failed to list files after merge: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file after merge, got %d", len(files))
	}

	info, err := s.AnalyzeMetric("test_cluster", "test_metric")
	if err != nil {
		t.Fatalf("failed to analyze metric: %v", err)
	}
	if info.TotalPoints != 6 {
		t.Errorf("expected 6 total points after merge, got %d", info.TotalPoints)
	}
	if len(info.Segments) != 1 {
		t.Errorf("expected 1 segment after merge, got %d", len(info.Segments))
	}
}

func TestTimeFormatting(t *testing.T) {
	seg := TimeSegment{Start: 1640000000, End: 1640003600}

	loc, _ := time.LoadLocation("Local")
	expectedStart := time.Unix(1640000000, 0).In(loc).Format("2006-01-02 15:04:05")
	expectedEnd := time.Unix(1640003600, 0).In(loc).Format("2006-01-02 15:04:05")

	if seg.FormatStart() != expectedStart {
		t.Errorf("FormatStart: expected %s, got %s", expectedStart, seg.FormatStart())
	}
	if seg.FormatEnd() != expectedEnd {
		t.Errorf("FormatEnd: expected %s, got %s", expectedEnd, seg.FormatEnd())
	}
	if seg.FormatDuration() != "1h0m0s" {
		t.Errorf("FormatDuration: expected 1h0m0s, got %s", seg.FormatDuration())
	}
}

func TestFileSizeFormatting(t *testing.T) {
	info := &MetricInfo{
		Files: []MetricFileInfo{
			{Size: 1024},
			{Size: 2048},
		},
	}

	result := info.FormatFileSize()
	if result != "3.0 KB" {
		t.Errorf("expected 3.0 KB, got %s", result)
	}

	info2 := &MetricInfo{
		Files: []MetricFileInfo{
			{Size: 1024 * 1024},
		},
	}
	result2 := info2.FormatFileSize()
	if result2 != "1.0 MB" {
		t.Errorf("expected 1.0 MB, got %s", result2)
	}

	info3 := &MetricInfo{
		Files: []MetricFileInfo{
			{Size: 512},
		},
	}
	result3 := info3.FormatFileSize()
	if result3 != "512 B" {
		t.Errorf("expected 512 B, got %s", result3)
	}
}
