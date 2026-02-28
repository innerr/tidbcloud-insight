package analysis

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// FormatMultiDimensionProfile generates a human-readable summary of the multi-dimension analysis.
// This function provides detailed explanations for each metric to make the output more accessible.
// Note: For a more integrated report, use FormatLoadProfile instead.
func FormatMultiDimensionProfile(profile *MultiDimensionProfile) string {
	if profile == nil {
		return "No multi-dimension profile data available"
	}

	return formatIntegratedMultiDimension(profile)
}

func formatSQLDimension(profile *SQLDimensionProfile) string {
	var sb strings.Builder

	if profile.DDLRatio > 0 || profile.DMLRatio > 0 || profile.DQLRatio > 0 || profile.TCLRatio > 0 {
		sb.WriteString("\n\nQuery Type Distribution:")
		if profile.DDLRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  DDL (Schema Changes): %.1f%%", profile.DDLRatio*100))
			sb.WriteString(" - CREATE, ALTER, DROP, etc.")
		}
		if profile.DMLRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  DML (Data Modifications): %.1f%%", profile.DMLRatio*100))
			sb.WriteString(" - INSERT, UPDATE, DELETE, etc.")
		}
		if profile.DQLRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  DQL (Queries): %.1f%%", profile.DQLRatio*100))
			sb.WriteString(" - SELECT, SHOW, etc.")
		}
		if profile.TCLRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  TCL (Transactions): %.1f%%", profile.TCLRatio*100))
			sb.WriteString(" - BEGIN, COMMIT, ROLLBACK")
		}
	}

	if profile.TransactionalIntensity > 0 || profile.AnalyticalIntensity > 0 {
		sb.WriteString("\n\nWorkload Characteristics:")
		if profile.TransactionalIntensity > 0 {
			sb.WriteString(fmt.Sprintf("\n  Transactional Intensity: %.1f%%", profile.TransactionalIntensity*100))
			sb.WriteString(" - OLTP operations (INSERT, UPDATE, DELETE)")
		}
		if profile.AnalyticalIntensity > 0 {
			sb.WriteString(fmt.Sprintf("\n  Analytical Intensity: %.1f%%", profile.AnalyticalIntensity*100))
			sb.WriteString(" - OLAP operations (complex SELECTs, aggregations)")
		}
		if profile.BatchOperationRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Batch Operations: %.1f%%", profile.BatchOperationRatio*100))
			sb.WriteString(" - Bulk operations (LOAD, IMPORT)")
		}
		if profile.InteractiveRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Interactive Queries: %.1f%%", profile.InteractiveRatio*100))
			sb.WriteString(" - Short, frequent queries")
		}
	}

	if profile.ComplexityIndex > 0 || profile.ResourceHotspotScore > 0 {
		sb.WriteString("\n\nQuery Analysis:")
		if profile.ComplexityIndex > 0 {
			sb.WriteString(fmt.Sprintf("\n  Complexity Index: %.2f", profile.ComplexityIndex))
			sb.WriteString(" [0-1, higher = more complex queries]")
		}
		if profile.ResourceHotspotScore > 0 {
			sb.WriteString(fmt.Sprintf("\n  Resource Hotspot Score: %.2f", profile.ResourceHotspotScore))
			sb.WriteString(" [0-1, higher = more resource-intensive patterns]")
		}
		if profile.PatternStability >= 0.5 {
			sb.WriteString(fmt.Sprintf("\n  Pattern Stability: %.2f", profile.PatternStability))
			sb.WriteString(" [0-1, higher = more consistent query patterns]")
		}
	}

	if len(profile.DominantTypes) > 0 {
		sb.WriteString(fmt.Sprintf("\n\nDominant Query Types: %v", profile.DominantTypes))
	}

	if len(profile.AnomalyTypes) > 0 {
		sb.WriteString(fmt.Sprintf("\nAnomaly Indicators: %v", profile.AnomalyTypes))
	}

	return sb.String()
}

func formatTiKVDimension(profile *TiKVDimensionProfile) string {
	var sb strings.Builder

	if profile.ReadOpRatio > 0 || profile.WriteOpRatio > 0 {
		sb.WriteString("\n\nOperation Distribution:")
		if profile.ReadOpRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Read Operations: %.1f%%", profile.ReadOpRatio*100))
			sb.WriteString(" - Point lookups, range scans")
		}
		if profile.WriteOpRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Write Operations: %.1f%%", profile.WriteOpRatio*100))
			sb.WriteString(" - PUT, DELETE operations")
		}
		if profile.TransactionOpRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Transaction Operations: %.1f%%", profile.TransactionOpRatio*100))
			sb.WriteString(" - PREWRITE, COMMIT, ROLLBACK")
		}
		if profile.CoprocessorRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Coprocessor: %.1f%%", profile.CoprocessorRatio*100))
			sb.WriteString(" - Distributed computation (SELECT with WHERE/GROUP BY)")
		}
	}

	if profile.AvgLatencyMs > 0 {
		sb.WriteString("\n\nPerformance Metrics:")
		sb.WriteString(fmt.Sprintf("\n  Average Latency: %.1f ms", profile.AvgLatencyMs))
		if profile.P99LatencyMs > 0 {
			sb.WriteString(fmt.Sprintf("\n  P99 Latency: %.1f ms", profile.P99LatencyMs))
			sb.WriteString(" - 99th percentile (worst 1% of requests)")
		}
		if profile.LatencyVariability > 0 {
			sb.WriteString(fmt.Sprintf("\n  Latency Variability: %.2f", profile.LatencyVariability))
			sb.WriteString(" [0-1, higher = less predictable]")
		}
	}

	if profile.ReadWriteBalance > 0 {
		sb.WriteString("\n\nStorage Efficiency:")
		if profile.WriteAmplification > 0 {
			sb.WriteString(fmt.Sprintf("\n  Write Amplification: %.2fx", profile.WriteAmplification))
			sb.WriteString(" [lower is better, ideal < 3]")
			if profile.WriteAmplification > 3 {
				sb.WriteString(" ⚠️  HIGH - may indicate inefficient write patterns")
			}
		}
		sb.WriteString(fmt.Sprintf("\n  Read/Write Balance: %.2f", profile.ReadWriteBalance))
		sb.WriteString(" [0-1, 1 = perfectly balanced]")
	}

	if profile.BottleneckIndicator != "" && profile.BottleneckIndicator != "none" {
		sb.WriteString(fmt.Sprintf("\n\n⚠️  Bottleneck Detected: %s", profile.BottleneckIndicator))
		if profile.BottleneckScore > 0 {
			sb.WriteString(fmt.Sprintf("\n  Bottleneck Score: %.2f", profile.BottleneckScore))
		}
	}

	if len(profile.HotOpTypes) > 0 {
		sb.WriteString(fmt.Sprintf("\n\nHot Operations (resource-intensive): %v", profile.HotOpTypes))
	}

	return sb.String()
}

func formatLatencyDimension(profile *LatencyDimensionProfile) string {
	var sb strings.Builder

	if profile.TiDBLatency.MeanMs > 0 {
		sb.WriteString("\n\nTiDB Latency (Query Processing):")
		sb.WriteString(formatLatencyDetailProfile(&profile.TiDBLatency, "TiDB"))
	}

	if profile.TiKVLatency.MeanMs > 0 {
		sb.WriteString("\n\nTiKV Latency (Storage Operations):")
		sb.WriteString(formatLatencyDetailProfile(&profile.TiKVLatency, "TiKV"))
	}

	if profile.TailLatencyRatio > 0 {
		sb.WriteString("\n\nTail Latency Analysis:")
		sb.WriteString(fmt.Sprintf("\n  Tail Latency Ratio (P99/P50): %.2fx", profile.TailLatencyRatio))
		sb.WriteString(" [ideal < 5, concerning > 10]")
		if profile.LongTailIndicator != "" {
			sb.WriteString(fmt.Sprintf("\n  Long Tail Indicator: %s", profile.LongTailIndicator))
			sb.WriteString(getLongTailDescription(profile.TailLatencyRatio))
		}
	}

	if profile.SpikeCount > 0 {
		sb.WriteString(fmt.Sprintf("\n\nLatency Spikes: %d detected", profile.SpikeCount))
		if profile.SpikePattern != "" {
			sb.WriteString(fmt.Sprintf("\n  Spike Pattern: %s", profile.SpikePattern))
		}
		if profile.P99Stability >= 0.5 {
			sb.WriteString(fmt.Sprintf("\n  P99 Stability: %.2f", profile.P99Stability))
			sb.WriteString(" [0-1, higher = more stable tail latency]")
		}
	}

	if profile.LatencyTrend != "" {
		sb.WriteString(fmt.Sprintf("\n\nTrend: %s", profile.LatencyTrend))
		if profile.PeriodicLatency {
			sb.WriteString("\n  ⚠️  Periodic latency pattern detected")
			sb.WriteString(" - latency varies predictably over time")
		}
	}

	if profile.BusinessHourLatency > 0 && profile.OffPeakLatency > 0 {
		sb.WriteString(fmt.Sprintf("\n\nBusiness Hours Latency: %.1f ms (9AM-6PM)", profile.BusinessHourLatency))
		sb.WriteString(fmt.Sprintf("\nOff-Peak Latency: %.1f ms (nights/weekends)", profile.OffPeakLatency))
	}

	return sb.String()
}

func formatLatencyDetailProfile(profile *LatencyDetailProfile, component string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("\n  P50 (Median): %.1f ms", profile.P50Ms))
	sb.WriteString(" - 50% of requests faster than this")
	sb.WriteString(fmt.Sprintf("\n  P90: %.1f ms", profile.P90Ms))
	sb.WriteString(" - 90% of requests faster than this")
	sb.WriteString(fmt.Sprintf("\n  P99: %.1f ms", profile.P99Ms))
	sb.WriteString(" - 99% of requests faster than this")
	sb.WriteString(fmt.Sprintf("\n  P99.9: %.1f ms", profile.P999Ms))
	sb.WriteString(" - 99.9% of requests faster than this")
	sb.WriteString(fmt.Sprintf("\n  Max: %.1f ms", profile.MaxMs))
	sb.WriteString(" - slowest request observed")

	sb.WriteString(fmt.Sprintf("\n  Mean: %.1f ms", profile.MeanMs))
	sb.WriteString(fmt.Sprintf("\n  Std Dev: %.1f ms", profile.StdDevMs))
	sb.WriteString(fmt.Sprintf("\n  CV (Variability): %.2f", profile.CV))
	sb.WriteString(" [coefficient of variation, lower = more predictable]")

	sb.WriteString(fmt.Sprintf("\n  Skewness: %.2f", profile.Skewness))
	sb.WriteString(" [>0 = right-skewed (occasional slow requests)]")
	sb.WriteString(fmt.Sprintf("\n  Kurtosis: %.2f", profile.Kurtosis))
	sb.WriteString(" [>3 = heavy tails (more extreme outliers)]")

	sb.WriteString(fmt.Sprintf("\n  Outlier Ratio: %.1f%%", profile.OutlierRatio*100))
	sb.WriteString(" - percentage of requests significantly slower than normal")

	return sb.String()
}

func formatBalanceDimension(profile *BalanceDimensionProfile) string {
	var sb strings.Builder

	sb.WriteString("\n\nTiDB Instances:")
	sb.WriteString(formatInstanceBalance(&profile.TiDBBalance, "TiDB"))

	sb.WriteString("\n\nTiKV Instances:")
	sb.WriteString(formatInstanceBalance(&profile.TiKVBalance, "TiKV"))

	sb.WriteString(fmt.Sprintf("\n\nOverall Imbalance Score: %.2f", profile.OverallImbalanceScore))
	sb.WriteString(" [0-1, lower = more balanced, < 0.3 is good]")
	sb.WriteString(fmt.Sprintf("\nHotspot Risk Level: %s", profile.HotspotRiskLevel))
	sb.WriteString(getHotspotRiskDescription(profile.HotspotRiskLevel))

	if len(profile.HotInstances) > 0 {
		sb.WriteString(fmt.Sprintf("\n\n🔥 Hot Instances (overloaded): %v", profile.HotInstances))
		sb.WriteString(" - handling significantly more load than average")
	}
	if len(profile.ColdInstances) > 0 {
		sb.WriteString(fmt.Sprintf("\n❄️  Cold Instances (underutilized): %v", profile.ColdInstances))
		sb.WriteString(" - handling significantly less load than average")
	}

	sb.WriteString(fmt.Sprintf("\n\nLoad Distribution CV: %.2f", profile.LoadDistributionCV))
	sb.WriteString(" [coefficient of variation across instances]")
	sb.WriteString(fmt.Sprintf("\nQPS Imbalance Ratio: %.2f", profile.QPSImbalanceRatio))
	sb.WriteString(fmt.Sprintf("\nLatency Imbalance Ratio: %.2f", profile.LatencyImbalanceRatio))

	if profile.Recommendation != "" {
		sb.WriteString(fmt.Sprintf("\n\n💡 Recommendation: %s", profile.Recommendation))
	}

	return sb.String()
}

func formatInstanceBalance(detail *InstanceBalanceDetail, component string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("\n  Instance Count: %d", detail.InstanceCount))
	sb.WriteString(fmt.Sprintf("\n  QPS Skew Coefficient: %.2f", detail.QPSSkewCoefficient))
	sb.WriteString(" [0 = perfectly balanced, > 0.5 = significant skew]")
	sb.WriteString(fmt.Sprintf("\n  Latency Skew Coefficient: %.2f", detail.LatencySkewCoefficient))
	sb.WriteString(fmt.Sprintf("\n  Max/Min Ratio: %.2fx", detail.MaxMinRatio))
	sb.WriteString(" - ratio of busiest to least busy instance")
	sb.WriteString(fmt.Sprintf("\n  Balance Score: %.2f", detail.BalanceScore))
	sb.WriteString(" [0-1, 1 = perfectly balanced]")

	return sb.String()
}

func formatQPSDimension(profile *QPSDimensionProfile) string {
	var sb strings.Builder

	if profile.MeanQPS > 0 {
		sb.WriteString("\n\nTraffic Statistics:")
		sb.WriteString(fmt.Sprintf("\n  Mean QPS: %.1f", profile.MeanQPS))
		sb.WriteString(" - average queries per second")
		if profile.PeakQPS > 0 {
			sb.WriteString(fmt.Sprintf("\n  Peak QPS: %.1f", profile.PeakQPS))
			sb.WriteString(" - maximum observed QPS")
		}
		if profile.MinQPS > 0 {
			sb.WriteString(fmt.Sprintf("\n  Minimum QPS: %.1f", profile.MinQPS))
		}
		if profile.PeakToAvgRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Peak-to-Average Ratio: %.2fx", profile.PeakToAvgRatio))
			sb.WriteString(" [> 5 = bursty traffic, > 10 = extreme bursts]")
		}
	}

	if profile.CV > 0 || profile.Burstiness > 0 {
		sb.WriteString("\n\nTraffic Characteristics:")
		if profile.CV > 0 {
			sb.WriteString(fmt.Sprintf("\n  Coefficient of Variation: %.2f", profile.CV))
			sb.WriteString(" [std deviation / mean, lower = more stable]")
		}
		if profile.Burstiness > 0 {
			sb.WriteString(fmt.Sprintf("\n  Burstiness Score: %.2f", profile.Burstiness))
			sb.WriteString(" [0-1, higher = more unpredictable]")
		}
		if profile.QPSPattern != "" {
			sb.WriteString(fmt.Sprintf("\n  Traffic Pattern: %s", profile.QPSPattern))
			sb.WriteString(getQPSPatternDescription(profile.QPSPattern))
		}
	}

	if profile.DailyPatternStrength > 0.3 || profile.WeeklyPatternStrength > 0.2 {
		sb.WriteString("\n\nSeasonality:")
		if profile.DailyPatternStrength > 0.3 {
			sb.WriteString(fmt.Sprintf("\n  Daily Pattern Strength: %.2f", profile.DailyPatternStrength))
			sb.WriteString(" [0-1, > 0.3 = significant daily cycles]")
		}
		if profile.WeeklyPatternStrength > 0.2 {
			sb.WriteString(fmt.Sprintf("\n  Weekly Pattern Strength: %.2f", profile.WeeklyPatternStrength))
			sb.WriteString(" [0-1, > 0.2 = significant weekly cycles]")
		}
		sb.WriteString(fmt.Sprintf("\n  Has Seasonality: %v", profile.HasSeasonality))
	}

	if len(profile.PeakHours) > 0 {
		sb.WriteString(fmt.Sprintf("\n\nPeak Hours: %v", profile.PeakHours))
		sb.WriteString(" - hours with highest QPS (0-23)")
	}
	if profile.BusinessHourQPS > 0 {
		sb.WriteString(fmt.Sprintf("\nBusiness Hour QPS: %.1f", profile.BusinessHourQPS))
		sb.WriteString(" (9AM-6PM)")
	}
	if profile.OffPeakQPS > 0 {
		sb.WriteString(fmt.Sprintf("\nOff-Peak QPS: %.1f", profile.OffPeakQPS))
		sb.WriteString(" (nights/weekends)")
	}

	if profile.TrendDirection != "" {
		sb.WriteString("\n\nTrend Analysis:")
		sb.WriteString(fmt.Sprintf("\n  Trend Direction: %s", profile.TrendDirection))
		if profile.TrendStrength > 0 {
			sb.WriteString(fmt.Sprintf("\n  Trend Strength: %.2f", profile.TrendStrength))
			sb.WriteString(" [0-1, higher = stronger trend]")
		}
		if profile.ForecastNext24H > 0 && profile.ForecastConfidence >= 0.5 {
			sb.WriteString(fmt.Sprintf("\n  24h Forecast: %.1f QPS", profile.ForecastNext24H))
			sb.WriteString(fmt.Sprintf("\n  Forecast Confidence: %.2f", profile.ForecastConfidence))
			sb.WriteString(" [0-1, higher = more reliable prediction]")
		}
	}

	if profile.CriticalLoadIndicator != "" && profile.CriticalLoadIndicator != "normal" {
		sb.WriteString(fmt.Sprintf("\n\nCritical Load Indicator: %s", profile.CriticalLoadIndicator))
		sb.WriteString(getCriticalLoadDescription(profile.CriticalLoadIndicator))
	}

	return sb.String()
}

func formatTiKVVolumeDimension(profile *TiKVVolumeDimensionProfile) string {
	var sb strings.Builder

	if profile.TotalRequests > 0 || profile.MeanRPS > 0 {
		sb.WriteString("\n\nRequest Volume:")
		if profile.TotalRequests > 0 {
			sb.WriteString(fmt.Sprintf("\n  Total Requests: %.0f", profile.TotalRequests))
		}
		if profile.MeanRPS > 0 {
			sb.WriteString(fmt.Sprintf("\n  Mean RPS: %.1f", profile.MeanRPS))
			sb.WriteString(" - requests per second")
		}
		if profile.PeakRPS > 0 {
			sb.WriteString(fmt.Sprintf("\n  Peak RPS: %.1f", profile.PeakRPS))
		}
		if profile.PeakToAvgRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Peak-to-Average Ratio: %.2fx", profile.PeakToAvgRatio))
		}
	}

	if profile.ReadRPS > 0 || profile.WriteRPS > 0 {
		sb.WriteString("\n\nRead/Write Breakdown:")
		if profile.ReadRPS > 0 {
			sb.WriteString(fmt.Sprintf("\n  Read RPS: %.1f", profile.ReadRPS))
		}
		if profile.WriteRPS > 0 {
			sb.WriteString(fmt.Sprintf("\n  Write RPS: %.1f", profile.WriteRPS))
		}
		if profile.ReadWriteRatio > 0 {
			sb.WriteString(fmt.Sprintf("\n  Read/Write Ratio: %.2f", profile.ReadWriteRatio))
		}
		if profile.WriteAmplification > 0 {
			sb.WriteString(fmt.Sprintf("\n  Write Amplification: %.2fx", profile.WriteAmplification))
		}
	}

	if profile.TransactionVolume > 0 || profile.CoprocessorVolume > 0 {
		sb.WriteString("\n\nOperation Types:")
		if profile.TransactionVolume > 0 {
			sb.WriteString(fmt.Sprintf("\n  Transaction Volume: %.0f", profile.TransactionVolume))
		}
		if profile.CoprocessorVolume > 0 {
			sb.WriteString(fmt.Sprintf("\n  Coprocessor Volume: %.0f", profile.CoprocessorVolume))
		}
		if profile.DailyVolumePattern > 0 {
			sb.WriteString(fmt.Sprintf("\n  Daily Volume Pattern: %.2f", profile.DailyVolumePattern))
		}
	}

	if profile.BottleneckType != "" && profile.BottleneckType != "none" {
		sb.WriteString(fmt.Sprintf("\n\nBottleneck: %s", profile.BottleneckType))
		if profile.BottleneckScore > 0 {
			sb.WriteString(fmt.Sprintf("\n  Bottleneck Score: %.2f", profile.BottleneckScore))
		}
	}

	if profile.HotRegionIndicator {
		sb.WriteString("\n\n⚠️  Hot region detected - some regions receiving disproportionate traffic")
	}

	if profile.VolumeTrend != "" {
		sb.WriteString(fmt.Sprintf("\n\nVolume Trend: %s", profile.VolumeTrend))
	}
	if profile.SaturationRisk != "" && profile.SaturationRisk != "low" {
		sb.WriteString(fmt.Sprintf("\nSaturation Risk: %s", profile.SaturationRisk))
		sb.WriteString(getSaturationRiskDescription(profile.SaturationRisk))
	}

	return sb.String()
}

func formatCrossDimensionInsights(insights *CrossDimensionInsights) string {
	var sb strings.Builder

	sb.WriteString("\n\nCorrelation Analysis:")
	sb.WriteString(fmt.Sprintf("\n  SQL → Latency: %.2f", insights.SQLToLatencyCorr))
	sb.WriteString(" [correlation between SQL patterns and latency]")
	sb.WriteString(fmt.Sprintf("\n  TiKV → Latency: %.2f", insights.TiKVToLatencyCorr))
	sb.WriteString(" [correlation between TiKV ops and latency]")
	sb.WriteString(fmt.Sprintf("\n  QPS → Latency: %.2f", insights.QPSToLatencyCorr))
	sb.WriteString(" [correlation between load and latency]")
	sb.WriteString(fmt.Sprintf("\n  Volume → Latency: %.2f", insights.VolumeToLatencyCorr))
	sb.WriteString(fmt.Sprintf("\n  Balance → Performance: %.2f", insights.BalanceToPerformance))

	sb.WriteString(fmt.Sprintf("\n\nCross-Correlation Score: %.2f", insights.CrossCorrelationScore))
	sb.WriteString(" [average correlation strength]")

	sb.WriteString("\n\nBottleneck Analysis:")
	sb.WriteString(fmt.Sprintf("\n  Primary Bottleneck: %s", insights.DominantBottleneck))
	sb.WriteString(getBottleneckDescription(insights.DominantBottleneck))
	if insights.SecondaryBottleneck != "none" {
		sb.WriteString(fmt.Sprintf("\n  Secondary Bottleneck: %s", insights.SecondaryBottleneck))
	}

	sb.WriteString(fmt.Sprintf("\n\nResource Efficiency: %.2f", insights.ResourceEfficiency))
	sb.WriteString(" [0-1, higher = more efficient resource usage]")

	if len(insights.OptimizationPriority) > 0 {
		sb.WriteString("\n\nOptimization Priority:")
		for i, opt := range insights.OptimizationPriority {
			sb.WriteString(fmt.Sprintf("\n  %d. %s", i+1, opt))
		}
	}

	if len(insights.AnomalyIndicators) > 0 {
		sb.WriteString(fmt.Sprintf("\n\n⚠️  Anomaly Indicators: %v", insights.AnomalyIndicators))
	}

	sb.WriteString(fmt.Sprintf("\n\nPerformance Degradation Risk: %s", insights.PerformanceDegradationRisk))
	sb.WriteString(getDegradationRiskDescription(insights.PerformanceDegradationRisk))

	return sb.String()
}

// Helper functions for descriptions

func getHealthScoreDescription(score float64) string {
	switch {
	case score >= 90:
		return "Excellent - system performing optimally"
	case score >= 80:
		return "Good - minor optimization opportunities"
	case score >= 70:
		return "Fair - some performance concerns"
	case score >= 60:
		return "Degraded - performance issues detected"
	default:
		return "Critical - immediate attention required"
	}
}

func getRiskLevelDescription(level string) string {
	switch level {
	case "low":
		return "System is stable with no significant risks"
	case "medium":
		return "Some risks detected, monitor closely"
	case "high":
		return "Significant risks present, take action soon"
	case "critical":
		return "Critical risks, immediate action required"
	default:
		return ""
	}
}

func getLongTailDescription(ratio float64) string {
	switch {
	case ratio < 3:
		return " [Good - latency is predictable]"
	case ratio < 5:
		return " [Acceptable - some outliers]"
	case ratio < 10:
		return " [Warning - significant tail latency]"
	default:
		return " [Critical - severe tail latency issues]"
	}
}

func getHotspotRiskDescription(level string) string {
	switch level {
	case "low":
		return " - Load is well balanced"
	case "medium":
		return " - Some instances are hotter than others"
	case "high":
		return " - Significant imbalance, some instances overloaded"
	default:
		return ""
	}
}

func getQPSPatternDescription(pattern string) string {
	descriptions := map[string]string{
		"multi_periodic": " [Multiple cycles (daily + weekly)]",
		"daily_periodic": " [Predictable daily cycles]",
		"bursty":         " [Unpredictable traffic spikes]",
		"stable":         " [Consistent, predictable traffic]",
		"variable":       " [Moderate variability]",
	}
	return descriptions[pattern]
}

func getCriticalLoadDescription(indicator string) string {
	descriptions := map[string]string{
		"critical_burst":       " ⚠️  Extreme traffic bursts detected",
		"critical_variability": " ⚠️  Highly unpredictable traffic",
		"critical_high_load":   " ⚠️  Very high sustained load",
		"warning_burst":        " ⚡ Traffic bursts above normal",
		"normal":               " ✓ Load is within normal parameters",
	}
	return descriptions[indicator]
}

func getSaturationRiskDescription(risk string) string {
	switch risk {
	case "low":
		return " - Plenty of capacity available"
	case "medium":
		return " - Approaching capacity limits"
	case "high":
		return " ⚠️  Near or at capacity, consider scaling"
	default:
		return ""
	}
}

func getBottleneckDescription(bottleneck string) string {
	descriptions := map[string]string{
		"latency":     " - High latency is limiting performance",
		"balance":     " - Uneven load distribution",
		"tikv":        " - TiKV operations are limiting",
		"sql_hotspot": " - Resource-intensive SQL patterns",
		"qps_burst":   " - Traffic spikes causing strain",
		"volume":      " - High request volume",
		"none":        " ✓ No significant bottleneck",
	}
	return descriptions[bottleneck]
}

func getDegradationRiskDescription(risk string) string {
	switch risk {
	case "low":
		return " - Performance likely to remain stable"
	case "medium":
		return " - Some performance degradation possible"
	case "high":
		return " ⚠️  High risk of performance degradation"
	default:
		return ""
	}
}

// FormatLoadProfile generates a comprehensive human-readable report including
// multi-dimension analysis, daily/weekly patterns, and instance skew analysis.
func FormatLoadProfile(profile *LoadProfile) string {
	if profile == nil {
		return "No load profile data available"
	}

	var sb strings.Builder

	sb.WriteString("\n" + strings.Repeat("=", 80))
	sb.WriteString("\nCOMPREHENSIVE LOAD PROFILE REPORT")
	sb.WriteString("\n" + strings.Repeat("=", 80))

	sb.WriteString(fmt.Sprintf("\n\nCluster: %s", profile.ClusterID))
	sb.WriteString(fmt.Sprintf("\nAnalysis Duration: %.1f hours (%.1f days)", profile.DurationHours, profile.DurationHours/24))
	sb.WriteString(fmt.Sprintf("\nData Points: %d", profile.Samples))

	if profile.MultiDimension != nil {
		sb.WriteString(formatIntegratedMultiDimension(profile.MultiDimension))
	}

	if hasSignificantDailyPattern(&profile.DailyPattern) {
		sb.WriteString(formatDailyPattern(&profile.DailyPattern))
	}

	if hasSignificantWeeklyPattern(&profile.WeeklyPattern) {
		sb.WriteString(formatWeeklyPattern(&profile.WeeklyPattern))
	}

	if profile.InstanceSkew != nil {
		sb.WriteString(formatInstanceSkew(profile.InstanceSkew))
	}

	if profile.Insights != nil {
		sb.WriteString(formatInsights(profile.Insights))
	}

	sb.WriteString("\n")

	return sb.String()
}

func formatIntegratedMultiDimension(profile *MultiDimensionProfile) string {
	var sb strings.Builder

	if profile.SQLDimension.PatternStability >= 0.5 {
		sb.WriteString("\n\n" + strings.Repeat("-", 80))
		sb.WriteString("\nSQL Query Pattern Analysis")
		sb.WriteString("\n" + strings.Repeat("-", 80))
		sb.WriteString(formatSQLDimension(&profile.SQLDimension))
	}

	if hasTiKVData(&profile.TiKVDimension) {
		sb.WriteString("\n\n" + strings.Repeat("-", 80))
		sb.WriteString("\nTiKV Storage Layer Analysis")
		sb.WriteString("\n" + strings.Repeat("-", 80))
		sb.WriteString(formatTiKVDimension(&profile.TiKVDimension))
	}

	if hasLatencyData(&profile.LatencyDimension) {
		sb.WriteString("\n\n" + strings.Repeat("-", 80))
		sb.WriteString("\nLatency Performance Analysis")
		sb.WriteString("\n" + strings.Repeat("-", 80))
		sb.WriteString(formatLatencyDimension(&profile.LatencyDimension))
	}

	if hasBalanceData(&profile.BalanceDimension) {
		sb.WriteString("\n\n" + strings.Repeat("-", 80))
		sb.WriteString("\nLoad Balance Analysis")
		sb.WriteString("\n" + strings.Repeat("-", 80))
		sb.WriteString(formatBalanceDimension(&profile.BalanceDimension))
	}

	if hasQPSData(&profile.QPSDimension) {
		sb.WriteString("\n\n" + strings.Repeat("-", 80))
		sb.WriteString("\nTraffic Pattern Analysis")
		sb.WriteString("\n" + strings.Repeat("-", 80))
		sb.WriteString(formatQPSDimension(&profile.QPSDimension))
	}

	if hasTiKVVolumeData(&profile.TiKVVolumeDimension) {
		sb.WriteString("\n\n" + strings.Repeat("-", 80))
		sb.WriteString("\nTiKV Request Volume Analysis")
		sb.WriteString("\n" + strings.Repeat("-", 80))
		sb.WriteString(formatTiKVVolumeDimension(&profile.TiKVVolumeDimension))
	}

	sb.WriteString("\n\n" + strings.Repeat("-", 80))
	sb.WriteString("\nOverall Assessment")
	sb.WriteString("\n" + strings.Repeat("-", 80))
	sb.WriteString(fmt.Sprintf("\n  Health Score: %.1f/100", profile.OverallHealthScore))
	sb.WriteString(fmt.Sprintf(" [%s]", getHealthScoreDescription(profile.OverallHealthScore)))
	sb.WriteString(fmt.Sprintf("\n  Risk Level: %s", profile.OverallRiskLevel))
	sb.WriteString(fmt.Sprintf(" - %s", getRiskLevelDescription(profile.OverallRiskLevel)))

	if len(profile.TopRecommendations) > 0 {
		sb.WriteString("\n\n  Recommendations:")
		for i, rec := range profile.TopRecommendations {
			sb.WriteString(fmt.Sprintf("\n    %d. %s", i+1, rec))
		}
	}

	return sb.String()
}

func hasTiKVData(profile *TiKVDimensionProfile) bool {
	return profile.ReadOpRatio > 0 || profile.WriteOpRatio > 0 || profile.AvgLatencyMs > 0
}

func hasLatencyData(profile *LatencyDimensionProfile) bool {
	return profile.TiDBLatency.MeanMs > 0 || profile.TiKVLatency.MeanMs > 0
}

func hasBalanceData(profile *BalanceDimensionProfile) bool {
	return profile.TiDBBalance.InstanceCount > 0 || profile.TiKVBalance.InstanceCount > 0
}

func hasQPSData(profile *QPSDimensionProfile) bool {
	return profile.MeanQPS > 0
}

func hasTiKVVolumeData(profile *TiKVVolumeDimensionProfile) bool {
	return profile.MeanRPS > 0 || profile.TotalRequests > 0
}

func hasSignificantDailyPattern(pattern *DailyPattern) bool {
	return pattern.PeakToOffPeak > 1.5 || pattern.NightDrop > 0.15
}

func formatDailyPattern(pattern *DailyPattern) string {
	var sb strings.Builder

	sb.WriteString("\n\n" + strings.Repeat("=", 80))
	sb.WriteString("\nDAILY PATTERN ANALYSIS - Hourly Traffic Distribution")
	sb.WriteString("\n" + strings.Repeat("=", 80))
	sb.WriteString("\nAnalyzes hourly patterns to identify peak and off-peak periods")

	sb.WriteString("\n\nHourly QPS Distribution:")

	maxAvg := 0.0
	for _, v := range pattern.HourlyAvg {
		if v > maxAvg {
			maxAvg = v
		}
	}

	if maxAvg > 0 {
		sb.WriteString("\n")
		for h := 0; h < 24; h++ {
			avg := pattern.HourlyAvg[h]
			if avg == 0 {
				continue
			}

			marker := "  "
			isPeak := false
			isOffPeak := false
			for _, ph := range pattern.PeakHours {
				if ph == h {
					isPeak = true
					break
				}
			}
			for _, oh := range pattern.OffPeakHours {
				if oh == h {
					isOffPeak = true
					break
				}
			}

			if isPeak {
				marker = "▲ "
			} else if isOffPeak {
				marker = "▼ "
			}

			barWidth := 30
			bars := int(avg / maxAvg * float64(barWidth))
			bar := strings.Repeat("█", bars) + strings.Repeat("░", barWidth-bars)

			sb.WriteString(fmt.Sprintf("\n  %s%02d:00 |%s| %.1f QPS", marker, h, bar, avg))
		}
		sb.WriteString("\n\n  Legend: ▲ Peak hour  ▼ Off-peak hour")
	}

	sb.WriteString(fmt.Sprintf("\n\nPeak Analysis:"))
	sb.WriteString(fmt.Sprintf("\n  Peak Hours: %v", formatHours(pattern.PeakHours)))
	sb.WriteString(" - hours with highest traffic")
	sb.WriteString(fmt.Sprintf("\n  Off-Peak Hours: %v", formatHours(pattern.OffPeakHours)))
	sb.WriteString(" - hours with lowest traffic")
	sb.WriteString(fmt.Sprintf("\n  Peak/Off-Peak Ratio: %.2fx", pattern.PeakToOffPeak))
	sb.WriteString(" - [> 1.5 = significant daily pattern]")

	if pattern.NightDrop > 0 && len(pattern.NightDropHours) > 0 {
		sb.WriteString(fmt.Sprintf("\n  Night Traffic Drop: %.1f%%", pattern.NightDrop*100))
		sb.WriteString(fmt.Sprintf(" (%02d:00-%02d:00)", pattern.NightDropHours[0], pattern.NightDropHours[len(pattern.NightDropHours)-1]))
	}

	if pattern.BusinessHours.Ratio > 0 {
		sb.WriteString(fmt.Sprintf("\n\nBusiness Hours Analysis:"))
		sb.WriteString(fmt.Sprintf("\n  Business Hours: %02d:00-%02d:00", pattern.BusinessHours.StartHour, pattern.BusinessHours.EndHour))
		sb.WriteString(fmt.Sprintf("\n  Business Hours QPS: %.1f", pattern.BusinessHours.AvgQPS))
		changePercent := (pattern.BusinessHours.Ratio - 1) * 100
		if changePercent < 0 {
			changePercent = -changePercent
		}
		sb.WriteString(fmt.Sprintf("\n  Business Hours vs Average: %.1f%% %s",
			changePercent,
			getChangeDirection(pattern.BusinessHours.Ratio-1)))
	}

	sb.WriteString(fmt.Sprintf("\n\nPattern Quality Metrics:"))
	sb.WriteString(fmt.Sprintf("\n  Daily Pattern Strength: %.0f%%", (pattern.PeakToOffPeak-1)*50))
	sb.WriteString(" - [based on peak/off-peak ratio]")
	sb.WriteString(fmt.Sprintf("\n  Pattern Consistency: %.0f%%", pattern.ConsistencyScore*100))
	sb.WriteString(" - [how consistent across days]")
	sb.WriteString(fmt.Sprintf("\n  Periodicity Score: %.0f%%", pattern.PeriodicityScore*100))
	sb.WriteString(" - [strength of daily cycle]")

	return sb.String()
}

func hasSignificantWeeklyPattern(pattern *WeeklyPattern) bool {
	return len(pattern.DailyAvg) >= 3
}

func formatWeeklyPattern(pattern *WeeklyPattern) string {
	var sb strings.Builder

	sb.WriteString("\n\n" + strings.Repeat("=", 80))
	sb.WriteString("\nWEEKLY PATTERN ANALYSIS - Day-of-Week Traffic Patterns")
	sb.WriteString("\n" + strings.Repeat("=", 80))
	sb.WriteString("\nAnalyzes weekly patterns to identify weekday/weekend differences")

	weekendDiff := pattern.WeekendDrop
	if weekendDiff < 0 {
		weekendDiff = -weekendDiff
	}
	isPeriodic := weekendDiff > 0.10

	sb.WriteString("\n\nDaily QPS Comparison:")

	days := make([]string, 0, len(pattern.DailyAvg))
	for d := range pattern.DailyAvg {
		days = append(days, d)
	}
	sort.Strings(days)

	maxAvg := 0.0
	for _, avg := range pattern.DailyAvg {
		if avg > maxAvg {
			maxAvg = avg
		}
	}

	if maxAvg > 0 {
		sb.WriteString("\n")
		barWidth := 25
		for _, d := range days {
			t, _ := time.Parse("2006-01-02", d)
			dayName := t.Weekday().String()[:3]
			avg := pattern.DailyAvg[d]

			bars := int(avg / maxAvg * float64(barWidth))
			bar := strings.Repeat("█", bars) + strings.Repeat("░", barWidth-bars)

			marker := "  "
			weekday := t.Weekday()
			if weekday == time.Saturday || weekday == time.Sunday {
				marker = "W "
			}

			sb.WriteString(fmt.Sprintf("\n  %s%s (%s) |%s| %.1f QPS", marker, d, dayName, bar, avg))
		}
		sb.WriteString("\n\n  Legend: W = Weekend")
	}

	if isPeriodic {
		sb.WriteString("\n\nWeekday vs Weekend Comparison:")

		weekdayMax := pattern.WeekdayAvg
		weekendMax := pattern.WeekendAvg
		maxVal := weekdayMax
		if weekendMax > maxVal {
			maxVal = weekendMax
		}

		weekdayBars := 0
		weekendBars := 0
		if maxVal > 0 {
			weekdayBars = int(weekdayMax / maxVal * 25)
			weekendBars = int(weekendMax / maxVal * 25)
		}

		weekdayBar := strings.Repeat("█", weekdayBars) + strings.Repeat("░", 25-weekdayBars)
		weekendBar := strings.Repeat("█", weekendBars) + strings.Repeat("░", 25-weekendBars)

		sb.WriteString(fmt.Sprintf("\n  Weekdays:  |%s| %.1f QPS", weekdayBar, weekdayMax))
		sb.WriteString(fmt.Sprintf("\n  Weekends:  |%s| %.1f QPS", weekendBar, weekendMax))
	}

	sb.WriteString("\n\nWeekly Statistics:")
	sb.WriteString(fmt.Sprintf("\n  Weekday Average: %.1f QPS", pattern.WeekdayAvg))
	sb.WriteString(fmt.Sprintf("\n  Weekend Average: %.1f QPS", pattern.WeekendAvg))

	if pattern.WeekendDrop > 0 {
		sb.WriteString(fmt.Sprintf("\n  Weekend Traffic Drop: %.1f%%", pattern.WeekendDrop*100))
		sb.WriteString(" - weekends have less traffic")
	} else if pattern.WeekendDrop < 0 {
		sb.WriteString(fmt.Sprintf("\n  Weekend Traffic Increase: %.1f%%", -pattern.WeekendDrop*100))
		sb.WriteString(" - weekends have more traffic")
	}

	if pattern.IsWeekdayHeavy {
		sb.WriteString("\n  Pattern: Weekday-heavy traffic (typical business workload)")
	} else if pattern.WeekendDrop < 0 {
		sb.WriteString("\n  Pattern: Weekend-heavy traffic (unusual)")
	} else {
		sb.WriteString("\n  Pattern: Similar weekday/weekend traffic (continuous workload)")
	}

	if isPeriodic {
		dropPct := pattern.WeekendDrop
		if dropPct < 0 {
			dropPct = -dropPct
		}
		sb.WriteString(fmt.Sprintf("\n  Weekly Pattern Strength: %.0f%%", dropPct*100))
		sb.WriteString(" - [significant weekly cycle]")
	}

	sb.WriteString(fmt.Sprintf("\n  Pattern Consistency: %.0f%%", pattern.ConsistencyScore*100))
	sb.WriteString(" - [how consistent across weeks]")

	return sb.String()
}

func formatInstanceSkew(skew *InstanceSkewProfile) string {
	var sb strings.Builder

	sb.WriteString("\n\n" + strings.Repeat("=", 80))
	sb.WriteString("\nDATA SKEW ANALYSIS - Instance Load Distribution")
	sb.WriteString("\n" + strings.Repeat("=", 80))
	sb.WriteString("\nAnalyzes load distribution across TiDB and TiKV instances to detect hotspots")

	sb.WriteString(fmt.Sprintf("\n\nOverall Skew Assessment:"))
	sb.WriteString(fmt.Sprintf("\n  Risk Level: %s", skew.SkewRiskLevel))
	sb.WriteString(getSkewRiskDescription(skew.SkewRiskLevel))
	sb.WriteString(fmt.Sprintf("\n  Has QPS Imbalance: %v", skew.HasQPSImbalance))
	sb.WriteString(fmt.Sprintf("\n  Has Latency Imbalance: %v", skew.HasLatencyImbalance))
	sb.WriteString(fmt.Sprintf("\n  Hot Instance Count: %d", skew.HotInstanceCount))
	if skew.HotInstanceCount > 0 {
		sb.WriteString(" ⚠️  - instances handling significantly more load")
	}

	if skew.TiDBSkew.InstanceCount > 0 {
		sb.WriteString("\n\nTiDB Instances:")
		sb.WriteString(formatInstanceSkewDetail(&skew.TiDBSkew, "TiDB"))
	}

	if skew.TiKVSkew.InstanceCount > 0 {
		sb.WriteString("\n\nTiKV Instances:")
		sb.WriteString(formatInstanceSkewDetail(&skew.TiKVSkew, "TiKV"))
	}

	if skew.Recommendation != "" {
		sb.WriteString(fmt.Sprintf("\n\n💡 Recommendation: %s", skew.Recommendation))
	}

	return sb.String()
}

func formatInstanceSkewDetail(detail *InstanceSkewDetail, component string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("\n  Instance Count: %d", detail.InstanceCount))
	sb.WriteString(fmt.Sprintf("\n  QPS Skew Coefficient: %.2f", detail.QPSSkewCoefficient))
	sb.WriteString(" [0 = perfectly balanced, > 0.5 = significant imbalance]")
	sb.WriteString(fmt.Sprintf("\n  Latency Skew Coefficient: %.2f", detail.LatencySkewCoefficient))
	sb.WriteString(" [0 = consistent latency, > 0.5 = significant variation]")
	sb.WriteString(fmt.Sprintf("\n  Max/Min QPS Ratio: %.2fx", detail.MaxQPSRatio))
	sb.WriteString(" - ratio between busiest and least busy instance")

	if len(detail.HotInstances) > 0 {
		sb.WriteString(fmt.Sprintf("\n\n  🔥 Hot Instances (overloaded): %v", detail.HotInstances))
		sb.WriteString("\n    - handling significantly more load than average")
		sb.WriteString("\n    - consider redistributing load or adding resources")
	}

	if len(detail.ColdInstances) > 0 {
		sb.WriteString(fmt.Sprintf("\n\n  ❄️  Cold Instances (underutilized): %v", detail.ColdInstances))
		sb.WriteString("\n    - handling significantly less load than average")
		sb.WriteString("\n    - may indicate inefficient load balancing")
	}

	if len(detail.QPSDistribution) > 0 && detail.InstanceCount <= 10 {
		sb.WriteString("\n\n  QPS Distribution by Instance:")
		instances := make([]string, 0, len(detail.QPSDistribution))
		for inst := range detail.QPSDistribution {
			instances = append(instances, inst)
		}
		sort.Strings(instances)

		maxQPS := 0.0
		for _, qps := range detail.QPSDistribution {
			if qps > maxQPS {
				maxQPS = qps
			}
		}

		for _, inst := range instances {
			qps := detail.QPSDistribution[inst]
			barWidth := 20
			bars := 0
			if maxQPS > 0 {
				bars = int(qps / maxQPS * float64(barWidth))
			}
			bar := strings.Repeat("█", bars) + strings.Repeat("░", barWidth-bars)
			sb.WriteString(fmt.Sprintf("\n    %s |%s| %.1f QPS", inst, bar, qps))
		}
	}

	return sb.String()
}

func formatInsights(insights *ClusterInsights) string {
	var sb strings.Builder

	sb.WriteString("\n\n" + strings.Repeat("=", 80))
	sb.WriteString("\nCLUSTER INSIGHTS & RECOMMENDATIONS")
	sb.WriteString("\n" + strings.Repeat("=", 80))

	sb.WriteString(fmt.Sprintf("\n\nOverall Health: %s", insights.OverallHealth))
	sb.WriteString(fmt.Sprintf("\n  Performance Score: %.1f/100", insights.PerformanceScore))
	sb.WriteString(fmt.Sprintf("\n  Stability Score: %.1f/100", insights.StabilityScore))
	sb.WriteString(fmt.Sprintf("\n  Efficiency Score: %.1f/100", insights.EfficiencyScore))
	sb.WriteString(fmt.Sprintf("\n  Pattern Type: %s", insights.PatternType))

	if len(insights.RiskFactors) > 0 {
		sb.WriteString("\n\n⚠️  Risk Factors:")
		for i, risk := range insights.RiskFactors {
			sb.WriteString(fmt.Sprintf("\n  %d. %s", i+1, risk))
		}
	}

	if len(insights.AnomalyIndicators) > 0 {
		sb.WriteString("\n\n🔍 Anomaly Indicators:")
		for i, anomaly := range insights.AnomalyIndicators {
			sb.WriteString(fmt.Sprintf("\n  %d. %s", i+1, anomaly))
		}
	}

	if len(insights.OptimizationOpportunities) > 0 {
		sb.WriteString("\n\n💡 Optimization Opportunities:")
		for i, opt := range insights.OptimizationOpportunities {
			sb.WriteString(fmt.Sprintf("\n  %d. %s", i+1, opt))
		}
	}

	if len(insights.RecommendedActions) > 0 {
		sb.WriteString("\n\n✅ Recommended Actions:")
		for i, action := range insights.RecommendedActions {
			sb.WriteString(fmt.Sprintf("\n  %d. %s", i+1, action))
		}
	}

	return sb.String()
}

func getSkewRiskDescription(level string) string {
	switch level {
	case "low":
		return " - Load is well balanced across instances"
	case "medium":
		return " - Some instances have noticeably higher load"
	case "high":
		return " ⚠️  Significant imbalance detected"
	default:
		return ""
	}
}

func getChangeDirection(change float64) string {
	if change > 0 {
		return "above average"
	} else if change < 0 {
		return "below average"
	}
	return "at average"
}
