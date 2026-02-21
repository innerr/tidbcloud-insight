package timerange

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type TimeRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type TimeRangeList []TimeRange

func (l TimeRangeList) Len() int           { return len(l) }
func (l TimeRangeList) Less(i, j int) bool { return l[i].Start < l[j].Start }
func (l TimeRangeList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

type ClusterMetricRanges struct {
	ClusterID string                   `json:"cluster_id"`
	Metrics   map[string]TimeRangeList `json:"metrics"`
}

type TimeRangeManager struct {
	mu       sync.RWMutex
	cacheDir string
	data     map[string]*ClusterMetricRanges
}

func NewTimeRangeManager(cacheDir string) *TimeRangeManager {
	return &TimeRangeManager{
		cacheDir: cacheDir,
		data:     make(map[string]*ClusterMetricRanges),
	}
}

func (m *TimeRangeManager) Load(clusterID string) (*ClusterMetricRanges, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data[clusterID] != nil {
		return m.data[clusterID], nil
	}

	filePath := m.getFilePath(clusterID)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			ranges := &ClusterMetricRanges{
				ClusterID: clusterID,
				Metrics:   make(map[string]TimeRangeList),
			}
			m.data[clusterID] = ranges
			return ranges, nil
		}
		return nil, fmt.Errorf("failed to read time range file: %w", err)
	}

	var ranges ClusterMetricRanges
	if err := json.Unmarshal(data, &ranges); err != nil {
		return nil, fmt.Errorf("failed to parse time range file: %w", err)
	}

	if ranges.Metrics == nil {
		ranges.Metrics = make(map[string]TimeRangeList)
	}

	m.data[clusterID] = &ranges
	return &ranges, nil
}

func (m *TimeRangeManager) Save(clusterID string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ranges, ok := m.data[clusterID]
	if !ok {
		return nil
	}

	filePath := m.getFilePath(clusterID)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := json.MarshalIndent(ranges, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal time ranges: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write time range file: %w", err)
	}

	return nil
}

func (m *TimeRangeManager) GetNonOverlappingRanges(clusterID, metric string, start, end int64) []TimeRange {
	ranges, err := m.Load(clusterID)
	if err != nil {
		return []TimeRange{{Start: start, End: end}}
	}

	existing := ranges.Metrics[metric]

	requested := TimeRange{Start: start, End: end}

	return subtractTimeRanges(requested, existing)
}

func (m *TimeRangeManager) AddRange(clusterID, metric string, start, end int64) error {
	ranges, err := m.Load(clusterID)
	if err != nil {
		return err
	}

	existing := ranges.Metrics[metric]
	newRange := TimeRange{Start: start, End: end}

	merged := mergeTimeRanges(append(existing, newRange))
	ranges.Metrics[metric] = merged

	return m.Save(clusterID)
}

func (m *TimeRangeManager) GetTotalCoverage(clusterID, metric string) int64 {
	ranges, err := m.Load(clusterID)
	if err != nil {
		return 0
	}

	existing := ranges.Metrics[metric]
	var total int64
	for _, r := range existing {
		total += r.End - r.Start
	}
	return total
}

func (m *TimeRangeManager) GetCoveragePercentage(clusterID, metric string, start, end int64) float64 {
	ranges, err := m.Load(clusterID)
	if err != nil {
		return 0
	}

	existing := ranges.Metrics[metric]
	covered := calculateCoverage(existing, start, end)
	total := end - start
	if total <= 0 {
		return 0
	}
	return float64(covered) / float64(total) * 100
}

func (m *TimeRangeManager) getFilePath(clusterID string) string {
	return filepath.Join(m.cacheDir, "timeranges", clusterID+".json")
}

func subtractTimeRanges(requested TimeRange, existing TimeRangeList) []TimeRange {
	if len(existing) == 0 {
		return []TimeRange{requested}
	}

	sort.Sort(TimeRangeList(existing))

	result := []TimeRange{{Start: requested.Start, End: requested.End}}

	for _, ex := range existing {
		var newResult []TimeRange
		for _, r := range result {
			subtracted := subtractSingle(r, ex)
			newResult = append(newResult, subtracted...)
		}
		result = newResult
		if len(result) == 0 {
			break
		}
	}

	return result
}

func subtractSingle(r, ex TimeRange) []TimeRange {
	if ex.End <= r.Start || ex.Start >= r.End {
		return []TimeRange{r}
	}

	var result []TimeRange

	if r.Start < ex.Start {
		result = append(result, TimeRange{Start: r.Start, End: ex.Start})
	}

	if r.End > ex.End {
		result = append(result, TimeRange{Start: ex.End, End: r.End})
	}

	return result
}

func mergeTimeRanges(ranges TimeRangeList) TimeRangeList {
	if len(ranges) == 0 {
		return ranges
	}

	sort.Sort(TimeRangeList(ranges))

	merged := TimeRangeList{ranges[0]}
	for i := 1; i < len(ranges); i++ {
		last := &merged[len(merged)-1]
		curr := ranges[i]

		if curr.Start <= last.End {
			if curr.End > last.End {
				last.End = curr.End
			}
		} else {
			merged = append(merged, curr)
		}
	}

	return merged
}

func calculateCoverage(ranges TimeRangeList, start, end int64) int64 {
	if len(ranges) == 0 {
		return 0
	}

	var covered int64
	for _, r := range ranges {
		overlapStart := max(r.Start, start)
		overlapEnd := min(r.End, end)
		if overlapEnd > overlapStart {
			covered += overlapEnd - overlapStart
		}
	}
	return covered
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
