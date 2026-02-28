// Package analysis provides helper functions for multi-dimension profiling.
// This file contains utility functions for SQL classification, TiKV operation
// analysis, latency profiling, and instance balance calculations.
package analysis

import (
	"math"
	"sort"
	"strings"
	"time"
)

func classifySQLType(sqlType string) string {
	sqlType = strings.ToLower(sqlType)
	switch {
	case containsAny(sqlType, []string{"create", "alter", "drop", "truncate", "rename"}):
		return "DDL"
	case containsAny(sqlType, []string{"insert", "update", "delete", "replace"}):
		return "DML"
	case containsAny(sqlType, []string{"select", "show", "explain", "describe"}):
		return "DQL"
	case containsAny(sqlType, []string{"begin", "commit", "rollback", "savepoint"}):
		return "TCL"
	default:
		return "Other"
	}
}

func classifySQLPattern(sqlType string) string {
	sqlType = strings.ToLower(sqlType)
	switch {
	case containsAny(sqlType, []string{"begin", "commit", "rollback", "insert", "update", "delete"}):
		return "transactional"
	case containsAny(sqlType, []string{"select", "show"}):
		if containsAny(sqlType, []string{"join", "group", "order", "having"}) {
			return "analytical"
		}
		return "interactive"
	case containsAny(sqlType, []string{"load", "import", "batch"}):
		return "batch"
	default:
		return "interactive"
	}
}

func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

func calculateSQLResourceScore(sqlType string, count, meanLatencyMs float64) float64 {
	baseCost := 1.0
	sqlType = strings.ToLower(sqlType)

	switch {
	case containsAny(sqlType, []string{"create", "alter", "drop"}):
		baseCost = 5.0
	case containsAny(sqlType, []string{"insert", "update", "delete"}):
		baseCost = 2.0
	case containsAny(sqlType, []string{"select"}):
		baseCost = 1.5
	case containsAny(sqlType, []string{"begin", "commit"}):
		baseCost = 0.5
	}

	latencyFactor := 1.0
	if meanLatencyMs > 100 {
		latencyFactor = 1.5
	} else if meanLatencyMs > 50 {
		latencyFactor = 1.2
	}

	return baseCost * latencyFactor * (count / 1000)
}

func findDominantTypes(typeDistribution map[string]SQLTypeDetail, topN int) []string {
	type typeScore struct {
		name  string
		score float64
	}

	var scores []typeScore
	for name, detail := range typeDistribution {
		scores = append(scores, typeScore{name: name, score: detail.Percent})
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	var result []string
	for i := 0; i < len(scores) && i < topN; i++ {
		result = append(result, scores[i].name)
	}
	return result
}

func calculateResourceHotspotScore(typeDistribution map[string]SQLTypeDetail) float64 {
	if len(typeDistribution) == 0 {
		return 0
	}

	var totalResourceScore float64
	var maxResourceScore float64

	for _, detail := range typeDistribution {
		totalResourceScore += detail.ResourceScore
		if detail.ResourceScore > maxResourceScore {
			maxResourceScore = detail.ResourceScore
		}
	}

	if totalResourceScore == 0 {
		return 0
	}

	return maxResourceScore / totalResourceScore
}

func calculateComplexityIndex(profile SQLDimensionProfile) float64 {
	complexity := 0.0

	complexity += profile.DDLRatio * 5
	complexity += profile.AnalyticalIntensity * 3
	complexity += profile.BatchOperationRatio * 2
	complexity += (1 - profile.PatternStability) * 2

	return math.Min(1.0, complexity/10)
}

func calculatePatternStability(sqlTypeData map[string][]TimeSeriesPoint) float64 {
	if len(sqlTypeData) == 0 {
		return 1.0
	}

	var totalCV float64
	var count int

	for _, data := range sqlTypeData {
		if len(data) < 10 {
			continue
		}
		vals := extractValuesFromTSP(data)
		avg := mean(vals)
		if avg > 0 {
			cv := stdDev(vals) / avg
			totalCV += cv
			count++
		}
	}

	if count == 0 {
		return 1.0
	}

	avgCV := totalCV / float64(count)
	return math.Max(0, 1.0-avgCV)
}

func detectSQLAnomalies(profile SQLDimensionProfile) []string {
	var anomalies []string

	if profile.DDLRatio > 0.3 {
		anomalies = append(anomalies, "high_ddl_ratio")
	}
	if profile.ResourceHotspotScore > 0.7 {
		anomalies = append(anomalies, "resource_hotspot")
	}
	if profile.PatternStability < 0.5 {
		anomalies = append(anomalies, "unstable_pattern")
	}
	if profile.ComplexityIndex > 0.7 {
		anomalies = append(anomalies, "high_complexity")
	}

	return anomalies
}

func classifyTiKVOp(op string) string {
	op = strings.ToLower(op)
	switch {
	case containsAny(op, []string{"get", "batchget", "scan", "seek"}):
		return "read"
	case containsAny(op, []string{"put", "delete", "batchput", "batchdelete", "prenext", "acquirepessimisticlock"}):
		return "write"
	case containsAny(op, []string{"prewrite", "commit", "rollback", "cleanup"}):
		return "transaction"
	case containsAny(op, []string{"coprocessor", "copr"}):
		return "coprocessor"
	default:
		return "other"
	}
}

func findHotOperations(opDistribution map[string]TiKVOpDetail) []string {
	type opScore struct {
		name  string
		score float64
	}

	var scores []opScore
	for name, detail := range opDistribution {
		scores = append(scores, opScore{
			name:  name,
			score: detail.Percent + detail.LatencyCV*10,
		})
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	var result []string
	for i := 0; i < len(scores) && i < 3; i++ {
		result = append(result, scores[i].name)
	}
	return result
}

func calculateLatencyVariability(opDistribution map[string]TiKVOpDetail) float64 {
	if len(opDistribution) == 0 {
		return 0
	}

	var totalCV float64
	var count int

	for _, detail := range opDistribution {
		if detail.LatencyCV > 0 {
			totalCV += detail.LatencyCV
			count++
		}
	}

	if count == 0 {
		return 0
	}

	return totalCV / float64(count)
}

func identifyTiKVBottleneck(profile TiKVDimensionProfile) string {
	if profile.WriteAmplification > 3 {
		return "write_amplification"
	}
	if profile.ScanOperationRatio > 0.3 {
		return "scan_heavy"
	}
	if profile.TransactionIntensity > 0.5 {
		return "transaction_heavy"
	}
	if profile.AvgLatencyMs > 50 {
		return "high_latency"
	}
	if profile.LatencyVariability > 0.5 {
		return "latency_variability"
	}
	return "none"
}

func calculateTiKVWriteAmplification(readCount, writeCount float64) float64 {
	if readCount == 0 {
		if writeCount > 0 {
			return 10.0
		}
		return 0
	}
	return writeCount / readCount
}

func calculateReadWriteBalance(readCount, writeCount float64) float64 {
	total := readCount + writeCount
	if total == 0 {
		return 0.5
	}
	return 1.0 - math.Abs(readCount-writeCount)/total
}

func analyzeHourlyLatency(latencyData []TimeSeriesPoint) map[int]float64 {
	hourlyLatency := make(map[int]float64)
	hourlyCount := make(map[int]int)

	for _, p := range latencyData {
		hour := time.Unix(p.Timestamp/1000, 0).Hour()
		hourlyLatency[hour] += p.Value * 1000
		hourlyCount[hour]++
	}

	for hour, total := range hourlyLatency {
		if hourlyCount[hour] > 0 {
			hourlyLatency[hour] = total / float64(hourlyCount[hour])
		}
	}

	return hourlyLatency
}

func calculateBusinessHourLatency(hourlyLatency map[int]float64) float64 {
	businessHours := []int{9, 10, 11, 12, 13, 14, 15, 16, 17}
	var total float64
	var count int

	for _, h := range businessHours {
		if lat, ok := hourlyLatency[h]; ok {
			total += lat
			count++
		}
	}

	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func calculateOffPeakLatency(hourlyLatency map[int]float64) float64 {
	offPeakHours := []int{0, 1, 2, 3, 4, 5, 22, 23}
	var total float64
	var count int

	for _, h := range offPeakHours {
		if lat, ok := hourlyLatency[h]; ok {
			total += lat
			count++
		}
	}

	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func detectLatencySpikes(latencyData []TimeSeriesPoint) int {
	if len(latencyData) < 10 {
		return 0
	}

	vals := extractValuesFromTSP(latencyData)
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	p99 := percentile(sorted, 0.99) * 1000
	p50 := percentile(sorted, 0.50) * 1000

	if p50 == 0 {
		return 0
	}

	threshold := p99 * 1.5
	spikeCount := 0

	for _, v := range vals {
		if v*1000 > threshold {
			spikeCount++
		}
	}

	return spikeCount
}

func classifySpikePattern(spikeCount int, cv float64) string {
	if spikeCount == 0 {
		return "none"
	}
	if spikeCount > 20 && cv > 0.5 {
		return "frequent_bursty"
	}
	if spikeCount > 10 {
		return "periodic"
	}
	return "occasional"
}

func detectPeriodicLatency(hourlyLatency map[int]float64) bool {
	if len(hourlyLatency) < 12 {
		return false
	}

	var vals []float64
	for i := 0; i < 24; i++ {
		if v, ok := hourlyLatency[i]; ok {
			vals = append(vals, v)
		}
	}

	if len(vals) < 8 {
		return false
	}

	avg := mean(vals)
	stdDev := stdDev(vals)

	if avg == 0 {
		return false
	}

	cv := stdDev / avg
	return cv > 0.3
}

func analyzeLatencyTrend(latencyData []TimeSeriesPoint) string {
	if len(latencyData) < 20 {
		return "unknown"
	}

	n := len(latencyData)
	firstHalf := latencyData[:n/2]
	secondHalf := latencyData[n/2:]

	firstMean := mean(extractValuesFromTSP(firstHalf))
	secondMean := mean(extractValuesFromTSP(secondHalf))

	if firstMean == 0 {
		return "unknown"
	}

	change := (secondMean - firstMean) / firstMean

	if change > 0.2 {
		return "increasing"
	} else if change < -0.2 {
		return "decreasing"
	}
	return "stable"
}

func calculateP99Stability(latencyData []TimeSeriesPoint) float64 {
	if len(latencyData) < 100 {
		return 1.0
	}

	windowSize := 50
	var p99Values []float64

	for i := 0; i <= len(latencyData)-windowSize; i += windowSize / 2 {
		window := latencyData[i : i+windowSize]
		vals := extractValuesFromTSP(window)
		sorted := make([]float64, len(vals))
		copy(sorted, vals)
		sort.Float64s(sorted)
		p99Values = append(p99Values, percentile(sorted, 0.99))
	}

	if len(p99Values) < 2 {
		return 1.0
	}

	avg := mean(p99Values)
	if avg == 0 {
		return 1.0
	}

	cv := stdDev(p99Values) / avg
	return math.Max(0, 1.0-cv)
}

func classifyLongTail(tailRatio, kurtosis float64) string {
	if tailRatio > 10 || kurtosis > 10 {
		return "severe"
	} else if tailRatio > 5 || kurtosis > 5 {
		return "moderate"
	} else if tailRatio > 3 || kurtosis > 3 {
		return "mild"
	}
	return "none"
}

func createLatencyBuckets(sortedVals []float64) map[float64]int {
	buckets := map[float64]int{
		1:    0,
		5:    0,
		10:   0,
		25:   0,
		50:   0,
		100:  0,
		250:  0,
		500:  0,
		1000: 0,
		2500: 0,
		5000: 0,
	}

	for _, v := range sortedVals {
		switch {
		case v <= 1:
			buckets[1]++
		case v <= 5:
			buckets[5]++
		case v <= 10:
			buckets[10]++
		case v <= 25:
			buckets[25]++
		case v <= 50:
			buckets[50]++
		case v <= 100:
			buckets[100]++
		case v <= 250:
			buckets[250]++
		case v <= 500:
			buckets[500]++
		case v <= 1000:
			buckets[1000]++
		case v <= 2500:
			buckets[2500]++
		case v <= 5000:
			buckets[5000]++
		default:
			buckets[5000]++
		}
	}

	return buckets
}

func calculateLatencyOutlierRatio(sortedVals []float64, p99 float64) float64 {
	if len(sortedVals) == 0 || p99 == 0 {
		return 0
	}

	outlierThreshold := p99 * 1.5
	outlierCount := 0

	for _, v := range sortedVals {
		if v > outlierThreshold {
			outlierCount++
		}
	}

	return float64(outlierCount) / float64(len(sortedVals))
}

func classifyHotspotRisk(imbalanceScore float64) string {
	if imbalanceScore > 0.6 {
		return "high"
	} else if imbalanceScore > 0.3 {
		return "medium"
	}
	return "low"
}

func calculateLoadDistributionCV(tidbQPS, tikvQPS map[string][]TimeSeriesPoint) float64 {
	var allMeans []float64

	for _, data := range tidbQPS {
		if len(data) > 0 {
			allMeans = append(allMeans, mean(extractValuesFromTSP(data)))
		}
	}

	for _, data := range tikvQPS {
		if len(data) > 0 {
			allMeans = append(allMeans, mean(extractValuesFromTSP(data)))
		}
	}

	if len(allMeans) < 2 {
		return 0
	}

	avg := mean(allMeans)
	if avg == 0 {
		return 0
	}

	return stdDev(allMeans) / avg
}

func generateBalanceRecommendation(profile BalanceDimensionProfile) string {
	if profile.HotspotRiskLevel == "high" {
		if len(profile.HotInstances) > 0 {
			return "Rebalance load from hot instances: " + strings.Join(profile.HotInstances, ", ")
		}
		return "Consider load balancing or adding instances"
	} else if profile.HotspotRiskLevel == "medium" {
		return "Monitor instance load distribution"
	}
	return "Load distribution is balanced"
}

func calculateOffPeakQPS(dailyPattern DailyPattern) float64 {
	offPeakHours := []int{0, 1, 2, 3, 4, 5, 22, 23}
	var total float64
	var count int

	for _, h := range offPeakHours {
		if qps, ok := dailyPattern.HourlyAvg[h]; ok {
			total += qps
			count++
		}
	}

	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func classifyQPSPattern(profile QPSDimensionProfile) string {
	if profile.DailyPatternStrength > 0.5 && profile.WeeklyPatternStrength > 0.2 {
		return "multi_periodic"
	}
	if profile.DailyPatternStrength > 0.5 {
		return "daily_periodic"
	}
	if profile.Burstiness > 0.5 {
		return "bursty"
	}
	if profile.CV < 0.2 {
		return "stable"
	}
	return "variable"
}

func classifyCriticalLoad(profile QPSDimensionProfile) string {
	if profile.PeakToAvgRatio > 10 {
		return "critical_burst"
	}
	if profile.CV > 1.0 {
		return "critical_variability"
	}
	if profile.PeakQPS > 50000 {
		return "critical_high_load"
	}
	if profile.PeakToAvgRatio > 5 {
		return "warning_burst"
	}
	return "normal"
}

func forecastQPS(vals []float64, hoursAhead float64) float64 {
	if len(vals) < 10 {
		return mean(vals)
	}

	n := float64(len(vals))
	sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0

	for i, v := range vals {
		x := float64(i)
		sumX += x
		sumY += v
		sumXY += x * v
		sumX2 += x * x
	}

	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return vals[len(vals)-1]
	}

	slope := (n*sumXY - sumX*sumY) / denominator
	intercept := (sumY - slope*sumX) / n

	lastIdx := float64(len(vals) - 1)
	stepsAhead := hoursAhead * 18

	forecast := intercept + slope*(lastIdx+stepsAhead)
	return math.Max(0, forecast)
}

func calculateForecastConfidence(cv, burstiness float64) float64 {
	stabilityFactor := 1.0 - cv
	burstinessPenalty := burstiness * 0.5

	confidence := stabilityFactor - burstinessPenalty
	return math.Max(0.1, math.Min(0.95, confidence))
}

func identifyVolumeBottleneck(profile TiKVVolumeDimensionProfile) string {
	if profile.WriteAmplification > 3 {
		return "write_bottleneck"
	}
	if profile.CoprocessorVolume > profile.TotalRequests*0.3 {
		return "coprocessor_heavy"
	}
	if profile.TransactionVolume > profile.TotalRequests*0.5 {
		return "transaction_heavy"
	}
	if profile.PeakToAvgRatio > 5 {
		return "burst_pattern"
	}
	return "none"
}

func calculateBottleneckScore(profile TiKVVolumeDimensionProfile) float64 {
	score := 0.0

	if profile.PeakToAvgRatio > 3 {
		score += 0.3
	}
	if profile.ReadWriteRatio < 0.5 || profile.ReadWriteRatio > 10 {
		score += 0.2
	}
	if profile.TransactionVolume > profile.TotalRequests*0.5 {
		score += 0.3
	}

	return math.Min(1.0, score)
}

func analyzeVolumeTrend(rates []float64) string {
	if len(rates) < 20 {
		return "unknown"
	}

	n := len(rates)
	firstHalf := rates[:n/2]
	secondHalf := rates[n/2:]

	firstMean := mean(firstHalf)
	secondMean := mean(secondHalf)

	if firstMean == 0 {
		return "unknown"
	}

	change := (secondMean - firstMean) / firstMean

	if change > 0.1 {
		return "increasing"
	} else if change < -0.1 {
		return "decreasing"
	}
	return "stable"
}

func calculateDailyVolumePattern(tikvOpData map[string][]TimeSeriesPoint) float64 {
	return 0.0
}

func classifySaturationRisk(profile TiKVVolumeDimensionProfile) string {
	if profile.PeakRPS > 100000 {
		return "high"
	} else if profile.PeakRPS > 50000 {
		return "medium"
	}
	return "low"
}

func correlateSQLToLatency(sql SQLDimensionProfile, latency LatencyDimensionProfile) float64 {
	if len(sql.TypeDistribution) == 0 {
		return 0
	}

	var highLatencyRatio float64
	for _, detail := range sql.TypeDistribution {
		if detail.P99LatencyMs > latency.TiDBLatency.P99Ms*0.8 {
			highLatencyRatio += detail.Percent
		}
	}

	return math.Min(1.0, highLatencyRatio/100)
}

func correlateTiKVToLatency(tikv TiKVDimensionProfile, latency LatencyDimensionProfile) float64 {
	if tikv.AvgLatencyMs == 0 || latency.TiKVLatency.MeanMs == 0 {
		return 0
	}

	ratio := tikv.AvgLatencyMs / latency.TiKVLatency.MeanMs
	return math.Min(1.0, ratio)
}

func correlateQPSToLatency(qps QPSDimensionProfile, latency LatencyDimensionProfile) float64 {
	if qps.CV > 0.5 && latency.TiDBLatency.CV > 0.5 {
		return 0.7
	}
	if qps.Burstiness > 0.5 && latency.SpikeCount > 5 {
		return 0.6
	}
	return qps.CV * 0.5
}

func correlateVolumeToLatency(volume TiKVVolumeDimensionProfile, latency LatencyDimensionProfile) float64 {
	if volume.PeakToAvgRatio > 3 && latency.TailLatencyRatio > 5 {
		return 0.7
	}
	return volume.PeakToAvgRatio / 10.0
}

func correlateBalanceToPerformance(balance BalanceDimensionProfile, latency LatencyDimensionProfile) float64 {
	return balance.OverallImbalanceScore
}

func identifyDominantBottleneck(profile *MultiDimensionProfile) string {
	scores := map[string]float64{
		"latency":     profile.LatencyDimension.TailLatencyRatio / 10.0,
		"balance":     profile.BalanceDimension.OverallImbalanceScore,
		"tikv":        profile.TiKVDimension.BottleneckScore,
		"sql_hotspot": profile.SQLDimension.ResourceHotspotScore,
		"qps_burst":   profile.QPSDimension.PeakToAvgRatio / 10.0,
		"volume":      profile.TiKVVolumeDimension.BottleneckScore,
	}

	maxScore := 0.0
	bottleneck := "none"

	for name, score := range scores {
		if score > maxScore {
			maxScore = score
			bottleneck = name
		}
	}

	if maxScore < 0.3 {
		return "none"
	}

	return bottleneck
}

func identifySecondaryBottleneck(profile *MultiDimensionProfile) string {
	scores := map[string]float64{
		"latency":     profile.LatencyDimension.TailLatencyRatio / 10.0,
		"balance":     profile.BalanceDimension.OverallImbalanceScore,
		"tikv":        profile.TiKVDimension.BottleneckScore,
		"sql_hotspot": profile.SQLDimension.ResourceHotspotScore,
		"qps_burst":   profile.QPSDimension.PeakToAvgRatio / 10.0,
		"volume":      profile.TiKVVolumeDimension.BottleneckScore,
	}

	dominant := identifyDominantBottleneck(profile)

	maxScore := 0.0
	secondary := "none"

	for name, score := range scores {
		if name == dominant {
			continue
		}
		if score > maxScore {
			maxScore = score
			secondary = name
		}
	}

	if maxScore < 0.2 {
		return "none"
	}

	return secondary
}

func calculateResourceEfficiency(profile *MultiDimensionProfile) float64 {
	efficiency := 1.0

	efficiency -= profile.BalanceDimension.OverallImbalanceScore * 0.3
	efficiency -= profile.TiKVDimension.WriteAmplification / 10.0 * 0.2
	efficiency -= profile.LatencyDimension.TailLatencyRatio / 20.0 * 0.2
	efficiency -= profile.QPSDimension.PeakToAvgRatio / 20.0 * 0.2
	efficiency -= profile.SQLDimension.ResourceHotspotScore * 0.1

	return math.Max(0, math.Min(1, efficiency))
}

func prioritizeOptimizations(profile *MultiDimensionProfile) []string {
	var priorities []string

	if profile.LatencyDimension.TiDBLatency.P99Ms > 500 {
		priorities = append(priorities, "latency_optimization")
	}
	if profile.BalanceDimension.OverallImbalanceScore > 0.4 {
		priorities = append(priorities, "load_balancing")
	}
	if profile.TiKVDimension.WriteAmplification > 3 {
		priorities = append(priorities, "write_optimization")
	}
	if profile.SQLDimension.ResourceHotspotScore > 0.6 {
		priorities = append(priorities, "sql_tuning")
	}
	if profile.QPSDimension.PeakToAvgRatio > 5 {
		priorities = append(priorities, "autoscaling")
	}
	if profile.TiKVVolumeDimension.SaturationRisk == "high" {
		priorities = append(priorities, "capacity_expansion")
	}

	if len(priorities) == 0 {
		priorities = append(priorities, "maintenance")
	}

	return priorities
}

func calculateCrossCorrelationScore(insights CrossDimensionInsights) float64 {
	correlations := []float64{
		insights.SQLToLatencyCorr,
		insights.TiKVToLatencyCorr,
		insights.QPSToLatencyCorr,
		insights.VolumeToLatencyCorr,
		insights.BalanceToPerformance,
	}

	return mean(correlations)
}

func detectCrossDimensionAnomalies(profile *MultiDimensionProfile) []string {
	var anomalies []string

	if profile.CrossDimensionInsights.SQLToLatencyCorr > 0.7 {
		anomalies = append(anomalies, "sql_latency_correlation")
	}
	if profile.CrossDimensionInsights.QPSToLatencyCorr > 0.7 {
		anomalies = append(anomalies, "qps_latency_correlation")
	}
	if profile.BalanceDimension.OverallImbalanceScore > 0.5 {
		anomalies = append(anomalies, "severe_imbalance")
	}
	if profile.LatencyDimension.TailLatencyRatio > 10 {
		anomalies = append(anomalies, "extreme_tail_latency")
	}

	return anomalies
}

func assessDegradationRisk(profile *MultiDimensionProfile) string {
	riskScore := 0.0

	if profile.LatencyDimension.LatencyTrend == "increasing" {
		riskScore += 0.3
	}
	if profile.TiKVVolumeDimension.VolumeTrend == "increasing" {
		riskScore += 0.2
	}
	if profile.QPSDimension.TrendDirection == "upward" {
		riskScore += 0.2
	}
	if profile.OverallHealthScore < 60 {
		riskScore += 0.3
	}

	if riskScore >= 0.7 {
		return "high"
	} else if riskScore >= 0.4 {
		return "medium"
	}
	return "low"
}
