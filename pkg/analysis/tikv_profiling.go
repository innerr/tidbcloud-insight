// Package analysis implements TiKV operation profiling and bottleneck detection.
// This file provides per-operation analysis, category-wise summaries,
// write amplification detection, and TiKV-specific optimization recommendations.
package analysis

import (
	"math"
	"sort"
	"strings"
)

type TiKVOperationProfile struct {
	Operation           string   `json:"operation"`
	Category            string   `json:"category"`
	Count               int64    `json:"count"`
	Percent             float64  `json:"percent"`
	Rate                float64  `json:"rate"`
	MeanLatencyMs       float64  `json:"mean_latency_ms"`
	P99LatencyMs        float64  `json:"p99_latency_ms"`
	MaxLatencyMs        float64  `json:"max_latency_ms"`
	LatencyCV           float64  `json:"latency_cv"`
	ResourceIntensity   float64  `json:"resource_intensity"`
	BottleneckIndicator string   `json:"bottleneck_indicator"`
	Trend               string   `json:"trend"`
	IsHotspot           bool     `json:"is_hotspot"`
	Recommendations     []string `json:"recommendations"`
}

type TiKVOperationAnalysis struct {
	Operations               []TiKVOperationProfile `json:"operations"`
	TopOperations            []TiKVOperationProfile `json:"top_operations"`
	LatencyHotspots          []TiKVOperationProfile `json:"latency_hotspots"`
	VolumeHotspots           []TiKVOperationProfile `json:"volume_hotspots"`
	ReadOperations           TiKVOperationSummary   `json:"read_operations"`
	WriteOperations          TiKVOperationSummary   `json:"write_operations"`
	TransactionOperations    TiKVOperationSummary   `json:"transaction_operations"`
	CoprocessorOperations    TiKVOperationSummary   `json:"coprocessor_operations"`
	CategoryDistribution     map[string]float64     `json:"category_distribution"`
	OverallBottleneck        string                 `json:"overall_bottleneck"`
	WriteAmplificationFactor float64                `json:"write_amplification_factor"`
	ReadWriteRatio           float64                `json:"read_write_ratio"`
	TransactionIntensity     float64                `json:"transaction_intensity"`
	Recommendations          []string               `json:"recommendations"`
}

type TiKVOperationSummary struct {
	TotalCount     int64   `json:"total_count"`
	Percent        float64 `json:"percent"`
	AvgLatencyMs   float64 `json:"avg_latency_ms"`
	P99LatencyMs   float64 `json:"p99_latency_ms"`
	MaxLatencyMs   float64 `json:"max_latency_ms"`
	OperationTypes int     `json:"operation_types"`
	IsBottleneck   bool    `json:"is_bottleneck"`
}

func AnalyzeTiKVOperations(tikvOpData map[string][]TimeSeriesPoint, tikvLatencyData map[string][]TimeSeriesPoint, durationHours float64) *TiKVOperationAnalysis {
	analysis := &TiKVOperationAnalysis{
		Operations:           []TiKVOperationProfile{},
		CategoryDistribution: make(map[string]float64),
	}

	var totalCount int64
	countByCategory := make(map[string]int64)
	latencyByCategory := make(map[string][]float64)

	for op, data := range tikvOpData {
		if len(data) == 0 {
			continue
		}

		profile := TiKVOperationProfile{
			Operation: op,
			Category:  classifyTiKVOperation(op),
		}

		last := data[len(data)-1].Value
		first := data[0].Value
		delta := last - first
		if delta > 0 {
			profile.Count = int64(delta)
			totalCount += profile.Count
			countByCategory[profile.Category] += profile.Count
		}

		if durationHours > 0 {
			profile.Rate = float64(profile.Count) / durationHours / 3600
		}

		if latencyData, ok := tikvLatencyData[op]; ok && len(latencyData) > 0 {
			vals := extractValuesFromTSP(latencyData)
			for i := range vals {
				vals[i] *= 1000
			}

			profile.MeanLatencyMs = mean(vals)

			sorted := make([]float64, len(vals))
			copy(sorted, vals)
			sort.Float64s(sorted)

			profile.MaxLatencyMs = sorted[len(sorted)-1]
			profile.P99LatencyMs = percentile(sorted, 0.99)

			if profile.MeanLatencyMs > 0 {
				profile.LatencyCV = stdDev(vals) / profile.MeanLatencyMs
			}

			latencyByCategory[profile.Category] = append(latencyByCategory[profile.Category], profile.MeanLatencyMs)
		}

		profile.ResourceIntensity = calculateTiKVResourceIntensity(profile)
		profile.BottleneckIndicator = identifyOperationBottleneck(profile)
		profile.Trend = calculateOperationTrend(data)
		profile.IsHotspot = profile.ResourceIntensity > 0.7
		profile.Recommendations = generateTiKVRecommendations(profile)

		analysis.Operations = append(analysis.Operations, profile)
	}

	for i := range analysis.Operations {
		if totalCount > 0 {
			analysis.Operations[i].Percent = float64(analysis.Operations[i].Count) / float64(totalCount) * 100
		}
	}

	sort.Slice(analysis.Operations, func(i, j int) bool {
		return analysis.Operations[i].Count > analysis.Operations[j].Count
	})

	if len(analysis.Operations) > 10 {
		analysis.TopOperations = analysis.Operations[:10]
	} else {
		analysis.TopOperations = analysis.Operations
	}

	sort.Slice(analysis.Operations, func(i, j int) bool {
		return analysis.Operations[i].P99LatencyMs > analysis.Operations[j].P99LatencyMs
	})

	if len(analysis.Operations) > 5 {
		analysis.LatencyHotspots = analysis.Operations[:5]
	} else {
		analysis.LatencyHotspots = analysis.Operations
	}

	sort.Slice(analysis.Operations, func(i, j int) bool {
		return analysis.Operations[i].ResourceIntensity > analysis.Operations[j].ResourceIntensity
	})

	if len(analysis.Operations) > 5 {
		analysis.VolumeHotspots = analysis.Operations[:5]
	} else {
		analysis.VolumeHotspots = analysis.Operations
	}

	for cat, count := range countByCategory {
		if totalCount > 0 {
			analysis.CategoryDistribution[cat] = float64(count) / float64(totalCount) * 100
		}
	}

	analysis.ReadOperations = summarizeCategory(analysis.Operations, "read", countByCategory, latencyByCategory)
	analysis.WriteOperations = summarizeCategory(analysis.Operations, "write", countByCategory, latencyByCategory)
	analysis.TransactionOperations = summarizeCategory(analysis.Operations, "transaction", countByCategory, latencyByCategory)
	analysis.CoprocessorOperations = summarizeCategory(analysis.Operations, "coprocessor", countByCategory, latencyByCategory)

	readCount := countByCategory["read"]
	writeCount := countByCategory["write"]

	if readCount > 0 {
		analysis.ReadWriteRatio = float64(readCount) / float64(readCount+writeCount)
	}

	if writeCount > 0 && readCount > 0 {
		analysis.WriteAmplificationFactor = float64(writeCount) / float64(readCount)
	} else if writeCount > 0 {
		analysis.WriteAmplificationFactor = 10.0
	}

	txnCount := countByCategory["transaction"]
	if totalCount > 0 {
		analysis.TransactionIntensity = float64(txnCount) / float64(totalCount)
	}

	analysis.OverallBottleneck = identifyOverallBottleneck(analysis)
	analysis.Recommendations = generateTiKVAnalysisRecommendations(analysis)

	return analysis
}

func classifyTiKVOperation(op string) string {
	op = strings.ToLower(op)

	switch {
	case containsAny(op, []string{"kv_get", "kv_batch_get", "kv_scan", "kv_seek"}):
		return "read"
	case containsAny(op, []string{"kv_prewrite", "kv_commit", "kv_rollback"}):
		return "transaction"
	case containsAny(op, []string{"coprocessor", "copr"}):
		return "coprocessor"
	case containsAny(op, []string{"kv_put", "kv_delete", "kv_batch_put", "kv_batch_delete"}):
		return "write"
	default:
		return "other"
	}
}

func calculateTiKVResourceIntensity(profile TiKVOperationProfile) float64 {
	intensity := 0.0

	intensity += profile.Percent / 100.0 * 0.4

	if profile.MeanLatencyMs > 50 {
		intensity += 0.4
	} else if profile.MeanLatencyMs > 20 {
		intensity += 0.2
	}

	if profile.LatencyCV > 0.5 {
		intensity += 0.2
	}

	return math.Min(1.0, intensity)
}

func identifyOperationBottleneck(profile TiKVOperationProfile) string {
	if profile.MeanLatencyMs > 100 {
		return "high_latency"
	}

	if profile.LatencyCV > 0.8 {
		return "variable_latency"
	}

	if profile.Rate > 10000 {
		return "high_volume"
	}

	return "none"
}

func calculateOperationTrend(data []TimeSeriesPoint) string {
	if len(data) < 20 {
		return "unknown"
	}

	n := len(data)
	firstHalf := data[:n/2]
	secondHalf := data[n/2:]

	if len(firstHalf) < 2 || len(secondHalf) < 2 {
		return "unknown"
	}

	firstDelta := firstHalf[len(firstHalf)-1].Value - firstHalf[0].Value
	secondDelta := secondHalf[len(secondHalf)-1].Value - secondHalf[0].Value

	if firstDelta == 0 {
		return "stable"
	}

	change := (secondDelta - firstDelta) / firstDelta

	if change > 0.2 {
		return "increasing"
	} else if change < -0.2 {
		return "decreasing"
	}

	return "stable"
}

func generateTiKVRecommendations(profile TiKVOperationProfile) []string {
	var recs []string

	if profile.MeanLatencyMs > 100 {
		recs = append(recs, "Investigate latency optimization for this operation")
	}

	if profile.LatencyCV > 0.5 {
		recs = append(recs, "High latency variability - check for resource contention")
	}

	if profile.BottleneckIndicator == "high_volume" {
		recs = append(recs, "Consider batching or rate limiting")
	}

	if profile.Category == "coprocessor" && profile.Percent > 30 {
		recs = append(recs, "High coprocessor usage - review query patterns")
	}

	return recs
}

func summarizeCategory(operations []TiKVOperationProfile, category string, countByCategory map[string]int64, latencyByCategory map[string][]float64) TiKVOperationSummary {
	summary := TiKVOperationSummary{}

	summary.TotalCount = countByCategory[category]
	if len(latencyByCategory[category]) > 0 {
		summary.AvgLatencyMs = mean(latencyByCategory[category])
		sorted := make([]float64, len(latencyByCategory[category]))
		copy(sorted, latencyByCategory[category])
		sort.Float64s(sorted)
		summary.P99LatencyMs = percentile(sorted, 0.99)
		summary.MaxLatencyMs = sorted[len(sorted)-1]
	}

	for _, op := range operations {
		if op.Category == category {
			summary.OperationTypes++
		}
	}

	summary.IsBottleneck = summary.AvgLatencyMs > 50 || summary.TotalCount > 1000000

	return summary
}

func identifyOverallBottleneck(analysis *TiKVOperationAnalysis) string {
	if analysis.WriteAmplificationFactor > 3 {
		return "write_amplification"
	}

	if analysis.CoprocessorOperations.Percent > 30 {
		return "coprocessor_heavy"
	}

	if analysis.TransactionOperations.Percent > 50 {
		return "transaction_heavy"
	}

	if analysis.ReadOperations.IsBottleneck && analysis.ReadOperations.AvgLatencyMs > analysis.WriteOperations.AvgLatencyMs {
		return "read_latency"
	}

	if analysis.WriteOperations.IsBottleneck {
		return "write_latency"
	}

	return "none"
}

func generateTiKVAnalysisRecommendations(analysis *TiKVOperationAnalysis) []string {
	var recs []string

	if analysis.WriteAmplificationFactor > 3 {
		recs = append(recs, "High write amplification - review write patterns and consider batch operations")
	}

	if analysis.OverallBottleneck == "coprocessor_heavy" {
		recs = append(recs, "High coprocessor usage - optimize analytical queries")
	}

	if analysis.OverallBottleneck == "transaction_heavy" {
		recs = append(recs, "High transaction volume - consider connection pooling and batch commits")
	}

	if len(analysis.LatencyHotspots) > 0 {
		recs = append(recs, "Investigate high-latency TiKV operations")
	}

	if len(recs) == 0 {
		recs = append(recs, "TiKV operations are well balanced")
	}

	return recs
}
