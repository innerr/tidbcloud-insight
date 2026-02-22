package analysis

import (
	"math"
	"sort"
	"strings"
)

type AnomalyMergerConfig struct {
	BaseWindowSeconds int
	MaxWindowSeconds  int
	MinWindowSeconds  int
	AlgorithmWeights  map[string]float64
	MinConfidence     float64
}

func DefaultAnomalyMergerConfig() AnomalyMergerConfig {
	return AnomalyMergerConfig{
		BaseWindowSeconds: 300,
		MaxWindowSeconds:  900,
		MinWindowSeconds:  60,
		AlgorithmWeights: map[string]float64{
			"MAD":             0.20,
			"ESD":             0.20,
			"IsolationForest": 0.20,
			"SHESD":           0.25,
			"RCF":             0.15,
		},
		MinConfidence: 0.0,
	}
}

type AnomalyMerger struct {
	config AnomalyMergerConfig
}

type MergedAnomalyResult struct {
	Anomalies []DetectedAnomaly
	Summary   MergeSummary
}

type MergeSummary struct {
	TotalAnomalies int
	CriticalCount  int
	HighCount      int
	MediumCount    int
	LowCount       int
	AlgorithmsUsed []string
	MergeWindowSec int
}

func NewAnomalyMerger(config AnomalyMergerConfig) *AnomalyMerger {
	return &AnomalyMerger{config: config}
}

func (m *AnomalyMerger) Merge(algorithmResults map[string][]DetectedAnomaly) MergedAnomalyResult {
	var allAnomalies []DetectedAnomaly
	algorithmsUsed := make(map[string]bool)

	for algoName, anomalies := range algorithmResults {
		algorithmsUsed[algoName] = true
		for _, a := range anomalies {
			a.DetectedBy = []string{algoName}
			a.Confidence = m.calculateConfidence([]string{algoName})
			allAnomalies = append(allAnomalies, a)
		}
	}

	if len(allAnomalies) == 0 {
		return MergedAnomalyResult{
			Summary: MergeSummary{
				AlgorithmsUsed: mapKeys(algorithmsUsed),
			},
		}
	}

	windowSec := m.calculateAdaptiveWindow(allAnomalies)

	merged := m.mergeWithWindow(allAnomalies, windowSec)

	merged = m.filterByConfidence(merged)

	summary := m.buildSummary(merged, algorithmsUsed, windowSec)

	return MergedAnomalyResult{
		Anomalies: merged,
		Summary:   summary,
	}
}

func (m *AnomalyMerger) calculateAdaptiveWindow(anomalies []DetectedAnomaly) int {
	if len(anomalies) <= 1 {
		return m.config.BaseWindowSeconds
	}

	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].Timestamp < anomalies[j].Timestamp
	})

	var intervals []int
	for i := 1; i < len(anomalies); i++ {
		gap := int(anomalies[i].Timestamp - anomalies[i-1].Timestamp)
		if gap > 0 {
			intervals = append(intervals, gap)
		}
	}

	if len(intervals) == 0 {
		return m.config.BaseWindowSeconds
	}

	sort.Ints(intervals)
	medianGap := intervals[len(intervals)/2]

	switch {
	case medianGap < m.config.MinWindowSeconds:
		return m.config.MinWindowSeconds
	case medianGap > m.config.BaseWindowSeconds:
		return min(medianGap, m.config.MaxWindowSeconds)
	default:
		return m.config.BaseWindowSeconds
	}
}

func (m *AnomalyMerger) mergeWithWindow(anomalies []DetectedAnomaly, windowSec int) []DetectedAnomaly {
	if len(anomalies) == 0 {
		return nil
	}

	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].Timestamp < anomalies[j].Timestamp
	})

	var events []DetectedAnomaly
	var currentEvent *DetectedAnomaly
	eventAlgos := make(map[string]bool)

	for _, a := range anomalies {
		if currentEvent == nil {
			currentEvent = &a
			currentEvent.EndTime = a.Timestamp
			eventAlgos = make(map[string]bool)
			for _, algo := range a.DetectedBy {
				eventAlgos[algo] = true
			}
			continue
		}

		if a.Timestamp <= currentEvent.EndTime+int64(windowSec) {
			m.mergeIntoEvent(currentEvent, a, eventAlgos)
		} else {
			finalizeEvent(currentEvent, eventAlgos)
			currentEvent.Confidence = m.calculateConfidence(mapKeys(eventAlgos))
			currentEvent.Severity = m.upgradeSeverity(currentEvent.Confidence, currentEvent.Severity)
			events = append(events, *currentEvent)

			currentEvent = &a
			currentEvent.EndTime = a.Timestamp
			eventAlgos = make(map[string]bool)
			for _, algo := range a.DetectedBy {
				eventAlgos[algo] = true
			}
		}
	}

	if currentEvent != nil {
		finalizeEvent(currentEvent, eventAlgos)
		currentEvent.Confidence = m.calculateConfidence(mapKeys(eventAlgos))
		currentEvent.Severity = m.upgradeSeverity(currentEvent.Confidence, currentEvent.Severity)
		events = append(events, *currentEvent)
	}

	return events
}

func (m *AnomalyMerger) mergeIntoEvent(event *DetectedAnomaly, a DetectedAnomaly, eventAlgos map[string]bool) {
	if a.Timestamp > event.EndTime {
		event.EndTime = a.Timestamp
	}
	if a.Timestamp < event.Timestamp {
		event.Timestamp = a.Timestamp
	}
	event.Duration = int(event.EndTime - event.Timestamp)

	if a.Severity == SeverityCritical {
		event.Severity = SeverityCritical
	} else if a.Severity == SeverityHigh && event.Severity != SeverityCritical {
		event.Severity = SeverityHigh
	}

	for _, algo := range a.DetectedBy {
		eventAlgos[algo] = true
	}

	if math.Abs(a.ZScore) > math.Abs(event.ZScore) {
		event.ZScore = a.ZScore
	}
}

func finalizeEvent(event *DetectedAnomaly, eventAlgos map[string]bool) {
	event.DetectedBy = mapKeys(eventAlgos)
	sort.Strings(event.DetectedBy)

	if event.Duration > 0 && len(event.DetectedBy) > 1 {
		algosStr := strings.Join(event.DetectedBy, ",")
		event.Detail = formatMergeDetail(event, algosStr)
	}
}

func formatMergeDetail(event *DetectedAnomaly, algosStr string) string {
	durationStr := formatDurationShort(event.Duration)
	return string(event.Type) + " (" + algosStr + ", " + durationStr + ")"
}

func formatDurationShort(seconds int) string {
	if seconds < 60 {
		return "<1m"
	}
	minutes := seconds / 60
	if minutes < 60 {
		return intToStr(minutes) + "m"
	}
	hours := minutes / 60
	remainingMinutes := minutes % 60
	if remainingMinutes == 0 {
		return intToStr(hours) + "h"
	}
	return intToStr(hours) + "h" + intToStr(remainingMinutes) + "m"
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	var neg bool
	if n < 0 {
		neg = true
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}

func (m *AnomalyMerger) calculateConfidence(algorithms []string) float64 {
	if len(algorithms) == 0 || len(m.config.AlgorithmWeights) == 0 {
		return 0.0
	}

	var totalWeight float64
	for _, algo := range algorithms {
		if w, ok := m.config.AlgorithmWeights[algo]; ok {
			totalWeight += w
		} else {
			totalWeight += 0.2
		}
	}

	return math.Min(totalWeight, 1.0)
}

func (m *AnomalyMerger) upgradeSeverity(confidence float64, current Severity) Severity {
	if confidence >= 0.8 {
		return SeverityCritical
	} else if confidence >= 0.6 {
		if current == SeverityCritical {
			return SeverityCritical
		}
		return SeverityHigh
	} else if confidence >= 0.4 {
		if current == SeverityCritical || current == SeverityHigh {
			return current
		}
		return SeverityMedium
	}
	return current
}

func (m *AnomalyMerger) filterByConfidence(anomalies []DetectedAnomaly) []DetectedAnomaly {
	if m.config.MinConfidence <= 0 {
		return anomalies
	}

	var filtered []DetectedAnomaly
	for _, a := range anomalies {
		if a.Confidence >= m.config.MinConfidence {
			filtered = append(filtered, a)
		}
	}
	return filtered
}

func (m *AnomalyMerger) buildSummary(anomalies []DetectedAnomaly, algorithmsUsed map[string]bool, windowSec int) MergeSummary {
	summary := MergeSummary{
		TotalAnomalies: len(anomalies),
		AlgorithmsUsed: mapKeys(algorithmsUsed),
		MergeWindowSec: windowSec,
	}

	for _, a := range anomalies {
		switch a.Severity {
		case SeverityCritical:
			summary.CriticalCount++
		case SeverityHigh:
			summary.HighCount++
		case SeverityMedium:
			summary.MediumCount++
		case SeverityLow:
			summary.LowCount++
		}
	}

	sort.Strings(summary.AlgorithmsUsed)

	return summary
}

func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
