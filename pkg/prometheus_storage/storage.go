package prometheus_storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	MergeThresholdBytes = 200 * 1024 * 1024
	GapThresholdSeconds = 300
)

type PrometheusStorage struct {
	baseDir string
}

func NewPrometheusStorage(baseDir string) *PrometheusStorage {
	return &PrometheusStorage{baseDir: baseDir}
}

func (s *PrometheusStorage) GetMetricDir(clusterID, metricName string) string {
	return filepath.Join(s.baseDir, clusterID, metricName)
}

func (s *PrometheusStorage) GetClusterDir(clusterID string) string {
	return filepath.Join(s.baseDir, clusterID)
}

func (s *PrometheusStorage) ListMetrics(clusterID string) ([]string, error) {
	clusterDir := s.GetClusterDir(clusterID)
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var metrics []string
	for _, entry := range entries {
		if entry.IsDir() {
			files, err := s.ListMetricFiles(clusterID, entry.Name())
			if err == nil && len(files) > 0 {
				metrics = append(metrics, entry.Name())
			}
		}
	}
	sort.Strings(metrics)
	return metrics, nil
}

func (s *PrometheusStorage) ListMetricFiles(clusterID, metricName string) ([]string, error) {
	metricDir := s.GetMetricDir(clusterID, metricName)
	entries, err := os.ReadDir(metricDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".prom") {
			files = append(files, filepath.Join(metricDir, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

type TimeSegment struct {
	Start      int64
	End        int64
	PointCount int
}

type MetricInfo struct {
	Name        string
	Files       []MetricFileInfo
	Segments    []TimeSegment
	TotalPoints int
	MinTime     int64
	MaxTime     int64
	GapCount    int
}

type MetricFileInfo struct {
	Path      string
	Size      int64
	MinTime   int64
	MaxTime   int64
	TimeRange string
}

func (s *PrometheusStorage) AnalyzeMetric(clusterID, metricName string) (*MetricInfo, error) {
	files, err := s.ListMetricFiles(clusterID, metricName)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no metric files found")
	}

	info := &MetricInfo{
		Name:  metricName,
		Files: []MetricFileInfo{},
	}

	allTimestamps := make(map[int64]int)

	for _, filePath := range files {
		fileInfo, timestamps, err := s.analyzeFile(filePath)
		if err != nil {
			continue
		}
		info.Files = append(info.Files, *fileInfo)

		for ts, count := range timestamps {
			allTimestamps[ts] += count
		}
	}

	if len(allTimestamps) == 0 {
		return info, nil
	}

	var sortedTs []int64
	for ts := range allTimestamps {
		sortedTs = append(sortedTs, ts)
	}
	sort.Slice(sortedTs, func(i, j int) bool { return sortedTs[i] < sortedTs[j] })

	info.MinTime = sortedTs[0]
	info.MaxTime = sortedTs[len(sortedTs)-1]
	info.TotalPoints = len(sortedTs)

	info.Segments = s.computeSegments(sortedTs, allTimestamps)

	info.GapCount = 0
	for i := 1; i < len(sortedTs); i++ {
		if sortedTs[i]-sortedTs[i-1] > GapThresholdSeconds {
			info.GapCount++
		}
	}

	return info, nil
}

func (s *PrometheusStorage) analyzeFile(filePath string) (*MetricFileInfo, map[int64]int, error) {
	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, nil, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	timestamps := make(map[int64]int)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) >= 2 {
			tsStr := parts[len(parts)-1]
			var ts int64
			if _, err := fmt.Sscanf(tsStr, "%d", &ts); err == nil {
				tsMs := ts
				if tsMs > 1e12 {
					tsMs = tsMs / 1000
				}
				timestamps[tsMs]++
			}
		}
	}

	if len(timestamps) == 0 {
		return &MetricFileInfo{
			Path: filePath,
			Size: stat.Size(),
		}, timestamps, nil
	}

	var sortedTs []int64
	for ts := range timestamps {
		sortedTs = append(sortedTs, ts)
	}
	sort.Slice(sortedTs, func(i, j int) bool { return sortedTs[i] < sortedTs[j] })

	minTime := sortedTs[0]
	maxTime := sortedTs[len(sortedTs)-1]

	return &MetricFileInfo{
		Path:      filePath,
		Size:      stat.Size(),
		MinTime:   minTime,
		MaxTime:   maxTime,
		TimeRange: formatTimeRangeForFilename(minTime, maxTime),
	}, timestamps, nil
}

func (s *PrometheusStorage) computeSegments(sortedTs []int64, tsCounts map[int64]int) []TimeSegment {
	if len(sortedTs) == 0 {
		return []TimeSegment{}
	}

	segments := []TimeSegment{}
	segmentStart := sortedTs[0]
	segmentEnd := sortedTs[0]
	segmentCount := tsCounts[segmentStart]

	for i := 1; i < len(sortedTs); i++ {
		ts := sortedTs[i]
		count := tsCounts[ts]

		if ts-sortedTs[i-1] > GapThresholdSeconds {
			segments = append(segments, TimeSegment{
				Start:      segmentStart,
				End:        segmentEnd,
				PointCount: segmentCount,
			})
			segmentStart = ts
			segmentCount = count
		} else {
			segmentCount += count
		}
		segmentEnd = ts
	}

	segments = append(segments, TimeSegment{
		Start:      segmentStart,
		End:        segmentEnd,
		PointCount: segmentCount,
	})

	return segments
}

func (s *PrometheusStorage) GetExistingTimeRanges(clusterID, metricName string) ([]TimeSegment, error) {
	info, err := s.AnalyzeMetric(clusterID, metricName)
	if err != nil {
		if os.IsNotExist(err) {
			return []TimeSegment{}, nil
		}
		return nil, err
	}
	return info.Segments, nil
}

func (s *PrometheusStorage) CalculateNonOverlappingRanges(startTS, endTS int64, existingRanges []TimeSegment) [][2]int64 {
	if len(existingRanges) == 0 {
		return [][2]int64{{startTS, endTS}}
	}

	type interval struct {
		start, end int64
	}

	current := []interval{{startTS, endTS}}

	for _, existing := range existingRanges {
		var next []interval
		for _, c := range current {
			if c.end <= existing.Start || c.start >= existing.End {
				next = append(next, c)
			} else {
				if c.start < existing.Start {
					next = append(next, interval{c.start, existing.Start})
				}
				if c.end > existing.End {
					next = append(next, interval{existing.End, c.end})
				}
			}
		}
		current = next
		if len(current) == 0 {
			break
		}
	}

	var result [][2]int64
	for _, c := range current {
		if c.end > c.start {
			result = append(result, [2]int64{c.start, c.end})
		}
	}

	return result
}

func (s *PrometheusStorage) SaveMetricData(clusterID, metricName string, data map[string]interface{}, startTS, endTS int64) (string, error) {
	metricDir := s.GetMetricDir(clusterID, metricName)

	dataMap, ok := data["data"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid data format: missing 'data' field")
	}
	results, ok := dataMap["result"].([]interface{})
	if !ok {
		return "", fmt.Errorf("invalid data format: missing 'result' field")
	}

	if len(results) == 0 {
		return "", fmt.Errorf("no data points to save")
	}

	if err := os.MkdirAll(metricDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create metric directory: %w", err)
	}

	filename := formatTimeRangeForFilename(startTS, endTS) + ".prom"
	filePath := filepath.Join(metricDir, filename)
	tmpPath := filePath + ".tmp"

	lines := s.buildPromLines(metricName, results)
	content := strings.Join(lines, "\n")

	if err := os.WriteFile(tmpPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("failed to write prometheus file: %w", err)
	}

	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed to rename prometheus file: %w", err)
	}

	return filePath, nil
}

func (s *PrometheusStorage) buildPromLines(metricName string, results []interface{}) []string {
	var lines []string
	lines = append(lines, fmt.Sprintf("# TYPE %s gauge", metricName))
	lines = append(lines, "")

	seriesLines := make(map[string][]string)

	for _, r := range results {
		series, ok := r.(map[string]interface{})
		if !ok {
			continue
		}

		labels := make([]string, 0)
		if metric, ok := series["metric"].(map[string]interface{}); ok {
			for k, v := range metric {
				if k == "__name__" {
					continue
				}
				if sv, ok := v.(string); ok {
					labels = append(labels, fmt.Sprintf(`%s="%s"`, k, sv))
				}
			}
		}
		sort.Strings(labels)

		values, ok := series["values"].([]interface{})
		if !ok {
			continue
		}

		for _, v := range values {
			arr, ok := v.([]interface{})
			if !ok || len(arr) < 2 {
				continue
			}

			timestamp, ok1 := arr[0].(float64)
			value, ok2 := arr[1].(string)
			if !ok1 || !ok2 {
				continue
			}

			var line string
			if len(labels) > 0 {
				line = fmt.Sprintf("%s{%s} %s %d", metricName, strings.Join(labels, ","), value, int64(timestamp*1000))
			} else {
				line = fmt.Sprintf("%s %s %d", metricName, value, int64(timestamp*1000))
			}

			seriesKey := strings.Join(labels, ",")
			seriesLines[seriesKey] = append(seriesLines[seriesKey], line)
		}
	}

	var sortedKeys []string
	for k := range seriesLines {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		for _, line := range seriesLines[key] {
			lines = append(lines, line)
		}
		lines = append(lines, "")
	}

	return lines
}

func (s *PrometheusStorage) MergeAdjacentFiles(clusterID, metricName string) error {
	files, err := s.ListMetricFiles(clusterID, metricName)
	if err != nil {
		return err
	}

	if len(files) <= 1 {
		return nil
	}

	type fileWithRange struct {
		path    string
		minTime int64
		maxTime int64
		size    int64
	}

	var fileInfos []fileWithRange
	for _, f := range files {
		info, _, err := s.analyzeFile(f)
		if err != nil || info.MinTime == 0 {
			continue
		}
		fileInfos = append(fileInfos, fileWithRange{
			path:    f,
			minTime: info.MinTime,
			maxTime: info.MaxTime,
			size:    info.Size,
		})
	}

	sort.Slice(fileInfos, func(i, j int) bool { return fileInfos[i].minTime < fileInfos[j].minTime })

	for i := 0; i < len(fileInfos)-1; i++ {
		curr := fileInfos[i]
		next := fileInfos[i+1]

		if next.minTime-curr.maxTime <= GapThresholdSeconds {
			combinedSize := curr.size + next.size
			if combinedSize <= MergeThresholdBytes {
				mergedPath, err := s.mergeTwoFiles(metricName, curr.path, next.path, curr.minTime, next.maxTime)
				if err != nil {
					continue
				}

				_ = os.Remove(curr.path)
				_ = os.Remove(next.path)

				fileInfos[i+1].path = mergedPath
				fileInfos[i+1].minTime = curr.minTime
				fileInfos[i+1].size = combinedSize

				fileInfos = append(fileInfos[:i], fileInfos[i+1:]...)
				i--
			}
		}
	}

	return nil
}

func (s *PrometheusStorage) mergeTwoFiles(metricName, path1, path2 string, minTime, maxTime int64) (string, error) {
	timestamps := make(map[string]map[int64]string)

	for _, path := range []string{path1, path2} {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 1024*1024)
		scanner.Buffer(buf, 10*1024*1024)

		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
				continue
			}

			parts := strings.Fields(line)
			if len(parts) >= 2 {
				tsStr := parts[len(parts)-1]
				var ts int64
				if _, err := fmt.Sscanf(tsStr, "%d", &ts); err == nil {
					tsMs := ts
					if tsMs > 1e12 {
						tsMs = tsMs / 1000
					}

					labelPart := ""
					if idx := strings.Index(line, "{"); idx >= 0 {
						endIdx := strings.Index(line, "}")
						if endIdx > idx {
							labelPart = line[idx+1 : endIdx]
						}
					}

					value := parts[len(parts)-2]

					if timestamps[labelPart] == nil {
						timestamps[labelPart] = make(map[int64]string)
					}
					timestamps[labelPart][tsMs] = value
				}
			}
		}
		file.Close()
	}

	metricDir := filepath.Dir(path1)
	filename := formatTimeRangeForFilename(minTime, maxTime) + ".prom"
	filePath := filepath.Join(metricDir, filename)
	tmpPath := filePath + ".tmp"

	var lines []string
	lines = append(lines, fmt.Sprintf("# TYPE %s gauge", metricName))
	lines = append(lines, "")

	var labelKeys []string
	for k := range timestamps {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)

	for _, labelKey := range labelKeys {
		tsMap := timestamps[labelKey]
		var tsList []int64
		for ts := range tsMap {
			tsList = append(tsList, ts)
		}
		sort.Slice(tsList, func(i, j int) bool { return tsList[i] < tsList[j] })

		for _, ts := range tsList {
			value := tsMap[ts]
			var line string
			if labelKey != "" {
				line = fmt.Sprintf("%s{%s} %s %d", metricName, labelKey, value, ts*1000)
			} else {
				line = fmt.Sprintf("%s %s %d", metricName, value, ts*1000)
			}
			lines = append(lines, line)
		}
		lines = append(lines, "")
	}

	content := strings.Join(lines, "\n")
	if err := os.WriteFile(tmpPath, []byte(content), 0644); err != nil {
		return "", err
	}

	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}

	return filePath, nil
}

func formatTimeRangeForFilename(startTS, endTS int64) string {
	start := time.Unix(startTS, 0).Format("2006-01-02_15-04-05")
	end := time.Unix(endTS, 0).Format("2006-01-02_15-04-05")
	return start + "_" + end
}

func (m *MetricInfo) FormatFileSize() string {
	var total int64
	for _, f := range m.Files {
		total += f.Size
	}
	const unit = 1024
	if total < unit {
		return fmt.Sprintf("%d B", total)
	}
	div, exp := int64(unit), 0
	for n := total / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(total)/float64(div), "KMGTPE"[exp])
}

func (m *MetricInfo) FormatDuration() string {
	if len(m.Segments) == 0 {
		return "N/A"
	}
	duration := m.MaxTime - m.MinTime
	d := time.Duration(duration) * time.Second
	return d.String()
}

func (s *TimeSegment) FormatStart() string {
	return time.Unix(s.Start, 0).Format("2006-01-02 15:04:05")
}

func (s *TimeSegment) FormatEnd() string {
	return time.Unix(s.End, 0).Format("2006-01-02 15:04:05")
}

func (s *TimeSegment) FormatDuration() string {
	d := time.Duration(s.End-s.Start) * time.Second
	return d.String()
}

func (f *MetricFileInfo) FormatSize() string {
	const unit = 1024
	if f.Size < unit {
		return fmt.Sprintf("%d B", f.Size)
	}
	div, exp := int64(unit), 0
	for n := f.Size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(f.Size)/float64(div), "KMGTPE"[exp])
}
