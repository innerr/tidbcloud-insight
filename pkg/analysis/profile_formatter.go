package analysis

import (
	"fmt"
	"strings"
)

// FormatMultiDimensionProfile generates a human-readable summary of the multi-dimension analysis.
// This function provides detailed explanations for each metric to make the output more accessible.
func FormatMultiDimensionProfile(profile *MultiDimensionProfile) string {
	if profile == nil {
		return "No multi-dimension profile data available"
	}

	var sb strings.Builder

	sb.WriteString("\n" + strings.Repeat("=", 80))
	sb.WriteString("\nMULTI-DIMENSION PROFILE ANALYSIS")
	sb.WriteString("\n" + strings.Repeat("=", 80))

	sb.WriteString(fmt.Sprintf("\n\nCluster: %s", profile.ClusterID))
	sb.WriteString(fmt.Sprintf("\nAnalysis Duration: %.1f hours (%d data points)", profile.DurationHours, profile.Samples))

	sb.WriteString("\n\n" + strings.Repeat("-", 80))
	sb.WriteString("\n[1/6] SQL DIMENSION - Query Pattern Analysis")
	sb.WriteString("\n" + strings.Repeat("-", 80))
	sb.WriteString("\nAnalyzes SQL query patterns to understand workload characteristics")
	sb.WriteString(formatSQLDimension(&profile.SQLDimension))

	sb.WriteString("\n\n" + strings.Repeat("-", 80))
	sb.WriteString("\n[2/6] TiKV DIMENSION - Storage Layer Operations")
	sb.WriteString("\n" + strings.Repeat("-", 80))
	sb.WriteString("\nAnalyzes TiKV operations to identify storage bottlenecks")
	sb.WriteString(formatTiKVDimension(&profile.TiKVDimension))

	sb.WriteString("\n\n" + strings.Repeat("-", 80))
	sb.WriteString("\n[3/6] LATENCY DIMENSION - Performance Characteristics")
	sb.WriteString("\n" + strings.Repeat("-", 80))
	sb.WriteString("\nMeasures tail latency and distribution to assess user experience")
	sb.WriteString(formatLatencyDimension(&profile.LatencyDimension))

	sb.WriteString("\n\n" + strings.Repeat("-", 80))
	sb.WriteString("\n[4/6] BALANCE DIMENSION - Load Distribution")
	sb.WriteString("\n" + strings.Repeat("-", 80))
	sb.WriteString("\nChecks if load is evenly distributed across instances")
	sb.WriteString(formatBalanceDimension(&profile.BalanceDimension))

	sb.WriteString("\n\n" + strings.Repeat("-", 80))
	sb.WriteString("\n[5/6] QPS DIMENSION - Traffic Patterns")
	sb.WriteString("\n" + strings.Repeat("-", 80))
	sb.WriteString("\nAnalyzes query rate patterns and predicts future traffic")
	sb.WriteString(formatQPSDimension(&profile.QPSDimension))

	sb.WriteString("\n\n" + strings.Repeat("-", 80))
	sb.WriteString("\n[6/6] TiKV VOLUME DIMENSION - Request Volume Analysis")
	sb.WriteString("\n" + strings.Repeat("-", 80))
	sb.WriteString("\nMeasures TiKV request volume to detect saturation")
	sb.WriteString(formatTiKVVolumeDimension(&profile.TiKVVolumeDimension))

	sb.WriteString("\n\n" + strings.Repeat("=", 80))
	sb.WriteString("\nCROSS-DIMENSION INSIGHTS")
	sb.WriteString("\n" + strings.Repeat("=", 80))
	sb.WriteString("\nCorrelates metrics across dimensions to identify root causes")
	sb.WriteString(formatCrossDimensionInsights(&profile.CrossDimensionInsights))

	sb.WriteString("\n\n" + strings.Repeat("=", 80))
	sb.WriteString("\nOVERALL ASSESSMENT")
	sb.WriteString("\n" + strings.Repeat("=", 80))
	sb.WriteString(fmt.Sprintf("\n\nHealth Score: %.1f/100", profile.OverallHealthScore))
	sb.WriteString(fmt.Sprintf(" [%s]", getHealthScoreDescription(profile.OverallHealthScore)))
	sb.WriteString(fmt.Sprintf("\nRisk Level: %s", profile.OverallRiskLevel))
	sb.WriteString(fmt.Sprintf(" - %s", getRiskLevelDescription(profile.OverallRiskLevel)))

	if len(profile.TopRecommendations) > 0 {
		sb.WriteString("\n\nTop Recommendations:")
		for i, rec := range profile.TopRecommendations {
			sb.WriteString(fmt.Sprintf("\n  %d. %s", i+1, rec))
		}
	}

	sb.WriteString("\n")

	return sb.String()
}

func formatSQLDimension(profile *SQLDimensionProfile) string {
	var sb strings.Builder

	sb.WriteString("\n\nQuery Type Distribution:")
	sb.WriteString(fmt.Sprintf("\n  DDL (Schema Changes): %.1f%%", profile.DDLRatio*100))
	sb.WriteString(" - CREATE, ALTER, DROP, etc.")
	sb.WriteString(fmt.Sprintf("\n  DML (Data Modifications): %.1f%%", profile.DMLRatio*100))
	sb.WriteString(" - INSERT, UPDATE, DELETE, etc.")
	sb.WriteString(fmt.Sprintf("\n  DQL (Queries): %.1f%%", profile.DQLRatio*100))
	sb.WriteString(" - SELECT, SHOW, etc.")
	sb.WriteString(fmt.Sprintf("\n  TCL (Transactions): %.1f%%", profile.TCLRatio*100))
	sb.WriteString(" - BEGIN, COMMIT, ROLLBACK")

	sb.WriteString("\n\nWorkload Characteristics:")
	sb.WriteString(fmt.Sprintf("\n  Transactional Intensity: %.1f%%", profile.TransactionalIntensity*100))
	sb.WriteString(" - OLTP operations (INSERT, UPDATE, DELETE)")
	sb.WriteString(fmt.Sprintf("\n  Analytical Intensity: %.1f%%", profile.AnalyticalIntensity*100))
	sb.WriteString(" - OLAP operations (complex SELECTs, aggregations)")
	sb.WriteString(fmt.Sprintf("\n  Batch Operations: %.1f%%", profile.BatchOperationRatio*100))
	sb.WriteString(" - Bulk operations (LOAD, IMPORT)")
	sb.WriteString(fmt.Sprintf("\n  Interactive Queries: %.1f%%", profile.InteractiveRatio*100))
	sb.WriteString(" - Short, frequent queries")

	sb.WriteString(fmt.Sprintf("\n\nComplexity Index: %.2f", profile.ComplexityIndex))
	sb.WriteString(" [0-1, higher = more complex queries]")
	sb.WriteString(fmt.Sprintf("\nResource Hotspot Score: %.2f", profile.ResourceHotspotScore))
	sb.WriteString(" [0-1, higher = more resource-intensive patterns]")
	sb.WriteString(fmt.Sprintf("\nPattern Stability: %.2f", profile.PatternStability))
	sb.WriteString(" [0-1, higher = more consistent query patterns]")

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

	sb.WriteString("\n\nOperation Distribution:")
	sb.WriteString(fmt.Sprintf("\n  Read Operations: %.1f%%", profile.ReadOpRatio*100))
	sb.WriteString(" - Point lookups, range scans")
	sb.WriteString(fmt.Sprintf("\n  Write Operations: %.1f%%", profile.WriteOpRatio*100))
	sb.WriteString(" - PUT, DELETE operations")
	sb.WriteString(fmt.Sprintf("\n  Transaction Operations: %.1f%%", profile.TransactionOpRatio*100))
	sb.WriteString(" - PREWRITE, COMMIT, ROLLBACK")
	sb.WriteString(fmt.Sprintf("\n  Coprocessor: %.1f%%", profile.CoprocessorRatio*100))
	sb.WriteString(" - Distributed computation (SELECT with WHERE/GROUP BY)")

	sb.WriteString("\n\nPerformance Metrics:")
	sb.WriteString(fmt.Sprintf("\n  Average Latency: %.1f ms", profile.AvgLatencyMs))
	sb.WriteString(fmt.Sprintf("\n  P99 Latency: %.1f ms", profile.P99LatencyMs))
	sb.WriteString(" - 99th percentile (worst 1% of requests)")
	sb.WriteString(fmt.Sprintf("\n  Latency Variability: %.2f", profile.LatencyVariability))
	sb.WriteString(" [0-1, higher = less predictable]")

	sb.WriteString("\n\nStorage Efficiency:")
	sb.WriteString(fmt.Sprintf("\n  Write Amplification: %.2fx", profile.WriteAmplification))
	sb.WriteString(" [lower is better, ideal < 3]")
	if profile.WriteAmplification > 3 {
		sb.WriteString(" ⚠️  HIGH - may indicate inefficient write patterns")
	}
	sb.WriteString(fmt.Sprintf("\n  Read/Write Balance: %.2f", profile.ReadWriteBalance))
	sb.WriteString(" [0-1, 1 = perfectly balanced]")

	if profile.BottleneckIndicator != "none" {
		sb.WriteString(fmt.Sprintf("\n\n⚠️  Bottleneck Detected: %s", profile.BottleneckIndicator))
		sb.WriteString(fmt.Sprintf("\n  Bottleneck Score: %.2f", profile.BottleneckScore))
	}

	if len(profile.HotOpTypes) > 0 {
		sb.WriteString(fmt.Sprintf("\n\nHot Operations (resource-intensive): %v", profile.HotOpTypes))
	}

	return sb.String()
}

func formatLatencyDimension(profile *LatencyDimensionProfile) string {
	var sb strings.Builder

	sb.WriteString("\n\nTiDB Latency (Query Processing):")
	sb.WriteString(formatLatencyDetailProfile(&profile.TiDBLatency, "TiDB"))

	sb.WriteString("\n\nTiKV Latency (Storage Operations):")
	sb.WriteString(formatLatencyDetailProfile(&profile.TiKVLatency, "TiKV"))

	sb.WriteString("\n\nTail Latency Analysis:")
	sb.WriteString(fmt.Sprintf("\n  Tail Latency Ratio (P99/P50): %.2fx", profile.TailLatencyRatio))
	sb.WriteString(" [ideal < 5, concerning > 10]")
	sb.WriteString(fmt.Sprintf("\n  Long Tail Indicator: %s", profile.LongTailIndicator))
	sb.WriteString(getLongTailDescription(profile.TailLatencyRatio))

	sb.WriteString(fmt.Sprintf("\n\nLatency Spikes: %d detected", profile.SpikeCount))
	sb.WriteString(fmt.Sprintf("\n  Spike Pattern: %s", profile.SpikePattern))
	sb.WriteString(fmt.Sprintf("\n  P99 Stability: %.2f", profile.P99Stability))
	sb.WriteString(" [0-1, higher = more stable tail latency]")

	sb.WriteString(fmt.Sprintf("\n\nTrend: %s", profile.LatencyTrend))
	if profile.PeriodicLatency {
		sb.WriteString("\n  ⚠️  Periodic latency pattern detected")
		sb.WriteString(" - latency varies predictably over time")
	}

	if profile.BusinessHourLatency > 0 {
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

	sb.WriteString("\n\nTraffic Statistics:")
	sb.WriteString(fmt.Sprintf("\n  Mean QPS: %.1f", profile.MeanQPS))
	sb.WriteString(" - average queries per second")
	sb.WriteString(fmt.Sprintf("\n  Peak QPS: %.1f", profile.PeakQPS))
	sb.WriteString(" - maximum observed QPS")
	sb.WriteString(fmt.Sprintf("\n  Minimum QPS: %.1f", profile.MinQPS))
	sb.WriteString(fmt.Sprintf("\n  Peak-to-Average Ratio: %.2fx", profile.PeakToAvgRatio))
	sb.WriteString(" [> 5 = bursty traffic, > 10 = extreme bursts]")

	sb.WriteString("\n\nTraffic Characteristics:")
	sb.WriteString(fmt.Sprintf("\n  Coefficient of Variation: %.2f", profile.CV))
	sb.WriteString(" [std deviation / mean, lower = more stable]")
	sb.WriteString(fmt.Sprintf("\n  Burstiness Score: %.2f", profile.Burstiness))
	sb.WriteString(" [0-1, higher = more unpredictable]")
	sb.WriteString(fmt.Sprintf("\n  Traffic Pattern: %s", profile.QPSPattern))
	sb.WriteString(getQPSPatternDescription(profile.QPSPattern))

	sb.WriteString("\n\nSeasonality:")
	sb.WriteString(fmt.Sprintf("\n  Daily Pattern Strength: %.2f", profile.DailyPatternStrength))
	sb.WriteString(" [0-1, > 0.3 = significant daily cycles]")
	sb.WriteString(fmt.Sprintf("\n  Weekly Pattern Strength: %.2f", profile.WeeklyPatternStrength))
	sb.WriteString(" [0-1, > 0.2 = significant weekly cycles]")
	sb.WriteString(fmt.Sprintf("\n  Has Seasonality: %v", profile.HasSeasonality))

	if len(profile.PeakHours) > 0 {
		sb.WriteString(fmt.Sprintf("\n\nPeak Hours: %v", profile.PeakHours))
		sb.WriteString(" - hours with highest QPS (0-23)")
	}
	sb.WriteString(fmt.Sprintf("\nBusiness Hour QPS: %.1f", profile.BusinessHourQPS))
	sb.WriteString(" (9AM-6PM)")
	sb.WriteString(fmt.Sprintf("\nOff-Peak QPS: %.1f", profile.OffPeakQPS))
	sb.WriteString(" (nights/weekends)")

	sb.WriteString("\n\nTrend Analysis:")
	sb.WriteString(fmt.Sprintf("\n  Trend Direction: %s", profile.TrendDirection))
	sb.WriteString(fmt.Sprintf("\n  Trend Strength: %.2f", profile.TrendStrength))
	sb.WriteString(" [0-1, higher = stronger trend]")
	sb.WriteString(fmt.Sprintf("\n  24h Forecast: %.1f QPS", profile.ForecastNext24H))
	sb.WriteString(fmt.Sprintf("\n  Forecast Confidence: %.2f", profile.ForecastConfidence))
	sb.WriteString(" [0-1, higher = more reliable prediction]")

	sb.WriteString(fmt.Sprintf("\n\nCritical Load Indicator: %s", profile.CriticalLoadIndicator))
	sb.WriteString(getCriticalLoadDescription(profile.CriticalLoadIndicator))

	return sb.String()
}

func formatTiKVVolumeDimension(profile *TiKVVolumeDimensionProfile) string {
	var sb strings.Builder

	sb.WriteString("\n\nRequest Volume:")
	sb.WriteString(fmt.Sprintf("\n  Total Requests: %.0f", profile.TotalRequests))
	sb.WriteString(fmt.Sprintf("\n  Mean RPS: %.1f", profile.MeanRPS))
	sb.WriteString(" - requests per second")
	sb.WriteString(fmt.Sprintf("\n  Peak RPS: %.1f", profile.PeakRPS))
	sb.WriteString(fmt.Sprintf("\n  Peak-to-Average Ratio: %.2fx", profile.PeakToAvgRatio))

	sb.WriteString("\n\nRead/Write Breakdown:")
	sb.WriteString(fmt.Sprintf("\n  Read RPS: %.1f", profile.ReadRPS))
	sb.WriteString(fmt.Sprintf("\n  Write RPS: %.1f", profile.WriteRPS))
	sb.WriteString(fmt.Sprintf("\n  Read/Write Ratio: %.2f", profile.ReadWriteRatio))
	sb.WriteString(fmt.Sprintf("\n  Write Amplification: %.2fx", profile.WriteAmplification))

	sb.WriteString("\n\nOperation Types:")
	sb.WriteString(fmt.Sprintf("\n  Transaction Volume: %.0f", profile.TransactionVolume))
	sb.WriteString(fmt.Sprintf("\n  Coprocessor Volume: %.0f", profile.CoprocessorVolume))
	sb.WriteString(fmt.Sprintf("\n  Daily Volume Pattern: %.2f", profile.DailyVolumePattern))

	sb.WriteString(fmt.Sprintf("\n\nBottleneck: %s", profile.BottleneckType))
	if profile.BottleneckType != "none" {
		sb.WriteString(fmt.Sprintf("\n  Bottleneck Score: %.2f", profile.BottleneckScore))
	}

	if profile.HotRegionIndicator {
		sb.WriteString("\n\n⚠️  Hot region detected - some regions receiving disproportionate traffic")
	}

	sb.WriteString(fmt.Sprintf("\n\nVolume Trend: %s", profile.VolumeTrend))
	sb.WriteString(fmt.Sprintf("\nSaturation Risk: %s", profile.SaturationRisk))
	sb.WriteString(getSaturationRiskDescription(profile.SaturationRisk))

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
