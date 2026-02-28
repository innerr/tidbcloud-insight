// Package analysis implements instance-level profiling and hotspot analysis.
// This file provides per-instance health monitoring, comparative ranking,
// and temperature-based hotspot detection for TiDB and TiKV instances.
package analysis

import (
	"math"
	"sort"
	"strings"
)

type InstanceProfile struct {
	InstanceID          string                 `json:"instance_id"`
	ComponentType       string                 `json:"component_type"`
	HealthStatus        string                 `json:"health_status"`
	QPSProfile          InstanceQPSProfile     `json:"qps_profile"`
	LatencyProfile      InstanceLatencyProfile `json:"latency_profile"`
	ResourceUtilization InstanceResourceUtil   `json:"resource_utilization"`
	AnomalyIndicators   []InstanceAnomaly      `json:"anomaly_indicators"`
	ComparativeRank     int                    `json:"comparative_rank"`
	RelativePerformance float64                `json:"relative_performance"`
	Recommendation      string                 `json:"recommendation"`
}

type InstanceQPSProfile struct {
	MeanQPS            float64         `json:"mean_qps"`
	PeakQPS            float64         `json:"peak_qps"`
	MinQPS             float64         `json:"min_qps"`
	QPSCV              float64         `json:"qps_cv"`
	QPSTrend           string          `json:"qps_trend"`
	PercentOfTotal     float64         `json:"percent_of_total"`
	HourlyDistribution map[int]float64 `json:"hourly_distribution"`
	BurstinessScore    float64         `json:"burstiness_score"`
}

type InstanceLatencyProfile struct {
	MeanLatencyMs    float64 `json:"mean_latency_ms"`
	P99LatencyMs     float64 `json:"p99_latency_ms"`
	MaxLatencyMs     float64 `json:"max_latency_ms"`
	LatencyCV        float64 `json:"latency_cv"`
	TailLatencyRatio float64 `json:"tail_latency_ratio"`
	SpikeCount       int     `json:"spike_count"`
	LatencyTrend     string  `json:"latency_trend"`
}

type InstanceResourceUtil struct {
	UtilizationScore float64 `json:"utilization_score"`
	IsOverloaded     bool    `json:"is_overloaded"`
	IsUnderutilized  bool    `json:"is_underutilized"`
	LoadLevel        string  `json:"load_level"`
	HeadroomPercent  float64 `json:"headroom_percent"`
}

type InstanceAnomaly struct {
	Type        string  `json:"type"`
	Severity    string  `json:"severity"`
	Description string  `json:"description"`
	Score       float64 `json:"score"`
}

type InstanceClusterAnalysis struct {
	Instances               []InstanceProfile    `json:"instances"`
	ClusterHealthSummary    ClusterHealthSummary `json:"cluster_health_summary"`
	HotspotAnalysis         HotspotAnalysis      `json:"hotspot_analysis"`
	RebalanceRecommendation string               `json:"rebalance_recommendation"`
}

type ClusterHealthSummary struct {
	TotalInstances     int     `json:"total_instances"`
	HealthyCount       int     `json:"healthy_count"`
	DegradedCount      int     `json:"degraded_count"`
	CriticalCount      int     `json:"critical_count"`
	AverageLoad        float64 `json:"average_load"`
	LoadDistributionCV float64 `json:"load_distribution_cv"`
	ClusterHealthScore float64 `json:"cluster_health_score"`
}

type HotspotAnalysis struct {
	HasHotspot              bool               `json:"has_hotspot"`
	HotInstances            []string           `json:"hot_instances"`
	ColdInstances           []string           `json:"cold_instances"`
	HotspotSeverity         string             `json:"hotspot_severity"`
	HotspotRatio            float64            `json:"hotspot_ratio"`
	TemperatureDistribution map[string]float64 `json:"temperature_distribution"`
}

func AnalyzeInstances(
	tidbInstanceQPS map[string][]TimeSeriesPoint,
	tidbInstanceLatency map[string][]TimeSeriesPoint,
	tikvInstanceQPS map[string][]TimeSeriesPoint,
	tikvInstanceLatency map[string][]TimeSeriesPoint,
) *InstanceClusterAnalysis {
	analysis := &InstanceClusterAnalysis{
		Instances: []InstanceProfile{},
	}

	allInstanceQPS := make(map[string][]TimeSeriesPoint)
	for k, v := range tidbInstanceQPS {
		allInstanceQPS["tidb-"+k] = v
	}
	for k, v := range tikvInstanceQPS {
		allInstanceQPS["tikv-"+k] = v
	}

	allInstanceLatency := make(map[string][]TimeSeriesPoint)
	for k, v := range tidbInstanceLatency {
		allInstanceLatency["tidb-"+k] = v
	}
	for k, v := range tikvInstanceLatency {
		allInstanceLatency["tikv-"+k] = v
	}

	totalQPS := 0.0
	qpsByInstance := make(map[string]float64)

	for instanceID, qpsData := range allInstanceQPS {
		if len(qpsData) == 0 {
			continue
		}

		profile := analyzeSingleInstance(instanceID, qpsData, allInstanceLatency[instanceID])

		totalQPS += profile.QPSProfile.MeanQPS
		qpsByInstance[instanceID] = profile.QPSProfile.MeanQPS

		analysis.Instances = append(analysis.Instances, profile)
	}

	for i := range analysis.Instances {
		if totalQPS > 0 {
			analysis.Instances[i].QPSProfile.PercentOfTotal = analysis.Instances[i].QPSProfile.MeanQPS / totalQPS * 100
		}
	}

	sort.Slice(analysis.Instances, func(i, j int) bool {
		return analysis.Instances[i].QPSProfile.MeanQPS > analysis.Instances[j].QPSProfile.MeanQPS
	})

	for i := range analysis.Instances {
		analysis.Instances[i].ComparativeRank = i + 1
		if len(analysis.Instances) > 0 {
			bestQPS := analysis.Instances[0].QPSProfile.MeanQPS
			if bestQPS > 0 {
				analysis.Instances[i].RelativePerformance = analysis.Instances[i].QPSProfile.MeanQPS / bestQPS
			}
		}
	}

	analysis.ClusterHealthSummary = calculateClusterHealthSummary(analysis.Instances)
	analysis.HotspotAnalysis = analyzeHotspots(analysis.Instances, qpsByInstance)
	analysis.RebalanceRecommendation = generateRebalanceRecommendation(analysis)

	return analysis
}

func analyzeSingleInstance(instanceID string, qpsData []TimeSeriesPoint, latencyData []TimeSeriesPoint) InstanceProfile {
	profile := InstanceProfile{
		InstanceID:        instanceID,
		AnomalyIndicators: []InstanceAnomaly{},
	}

	if strings.HasPrefix(instanceID, "tidb-") {
		profile.ComponentType = "tidb"
		instanceID = strings.TrimPrefix(instanceID, "tidb-")
	} else if strings.HasPrefix(instanceID, "tikv-") {
		profile.ComponentType = "tikv"
		instanceID = strings.TrimPrefix(instanceID, "tikv-")
	}

	profile.QPSProfile = analyzeInstanceQPS(qpsData)
	profile.LatencyProfile = analyzeInstanceLatency(latencyData)
	profile.ResourceUtilization = calculateInstanceUtilization(profile.QPSProfile, profile.LatencyProfile)

	profile.HealthStatus = determineInstanceHealth(profile)
	profile.AnomalyIndicators = detectInstanceAnomalies(profile)
	profile.Recommendation = generateInstanceRecommendation(profile)

	return profile
}

func analyzeInstanceQPS(data []TimeSeriesPoint) InstanceQPSProfile {
	profile := InstanceQPSProfile{
		HourlyDistribution: make(map[int]float64),
	}

	if len(data) == 0 {
		return profile
	}

	vals := extractValuesFromTSP(data)
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	profile.MeanQPS = mean(vals)
	profile.PeakQPS = sorted[len(sorted)-1]
	profile.MinQPS = sorted[0]

	if profile.MeanQPS > 0 {
		profile.QPSCV = stdDev(vals) / profile.MeanQPS
	}

	profile.QPSTrend = calculateInstanceTrend(data)
	profile.BurstinessScore = calculateBurstiness(vals)

	hourlySum := make(map[int]float64)
	hourlyCount := make(map[int]int)

	for _, p := range data {
		hour := int((p.Timestamp / 1000 / 3600) % 24)
		hourlySum[hour] += p.Value
		hourlyCount[hour]++
	}

	for hour, sum := range hourlySum {
		if hourlyCount[hour] > 0 {
			profile.HourlyDistribution[hour] = sum / float64(hourlyCount[hour])
		}
	}

	return profile
}

func analyzeInstanceLatency(data []TimeSeriesPoint) InstanceLatencyProfile {
	profile := InstanceLatencyProfile{}

	if len(data) == 0 {
		return profile
	}

	vals := extractValuesFromTSP(data)
	for i := range vals {
		vals[i] *= 1000
	}

	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	profile.MeanLatencyMs = mean(vals)
	profile.MaxLatencyMs = sorted[len(sorted)-1]
	profile.P99LatencyMs = percentile(sorted, 0.99)

	if profile.MeanLatencyMs > 0 {
		profile.LatencyCV = stdDev(vals) / profile.MeanLatencyMs
	}

	if profile.P99LatencyMs > 0 {
		p50 := percentile(sorted, 0.50)
		if p50 > 0 {
			profile.TailLatencyRatio = profile.P99LatencyMs / p50
		}
	}

	profile.SpikeCount = countLatencySpikes(vals, profile.MeanLatencyMs)
	profile.LatencyTrend = calculateInstanceTrend(data)

	return profile
}

func calculateInstanceUtilization(qpsProfile InstanceQPSProfile, latencyProfile InstanceLatencyProfile) InstanceResourceUtil {
	util := InstanceResourceUtil{}

	utilScore := 0.0

	if qpsProfile.MeanQPS > 0 {
		utilScore += math.Min(1.0, qpsProfile.MeanQPS/10000) * 0.4
	}

	if qpsProfile.QPSCV < 0.3 {
		utilScore += 0.3
	}

	if latencyProfile.MeanLatencyMs > 0 && latencyProfile.MeanLatencyMs < 100 {
		utilScore += 0.3
	}

	util.UtilizationScore = math.Min(1.0, utilScore)

	util.IsOverloaded = latencyProfile.P99LatencyMs > 500 || qpsProfile.QPSCV > 0.8
	util.IsUnderutilized = util.UtilizationScore < 0.3

	util.HeadroomPercent = (1.0 - util.UtilizationScore) * 100

	switch {
	case util.UtilizationScore > 0.8:
		util.LoadLevel = "high"
	case util.UtilizationScore > 0.5:
		util.LoadLevel = "medium"
	default:
		util.LoadLevel = "low"
	}

	return util
}

func determineInstanceHealth(profile InstanceProfile) string {
	score := 100.0

	if profile.LatencyProfile.P99LatencyMs > 500 {
		score -= 30
	} else if profile.LatencyProfile.P99LatencyMs > 200 {
		score -= 15
	}

	if profile.QPSProfile.QPSCV > 0.8 {
		score -= 20
	} else if profile.QPSProfile.QPSCV > 0.5 {
		score -= 10
	}

	if profile.ResourceUtilization.IsOverloaded {
		score -= 20
	}

	for _, anomaly := range profile.AnomalyIndicators {
		if anomaly.Severity == "critical" {
			score -= 15
		} else if anomaly.Severity == "warning" {
			score -= 8
		}
	}

	switch {
	case score >= 80:
		return "healthy"
	case score >= 60:
		return "degraded"
	case score >= 40:
		return "unhealthy"
	default:
		return "critical"
	}
}

func detectInstanceAnomalies(profile InstanceProfile) []InstanceAnomaly {
	var anomalies []InstanceAnomaly

	if profile.LatencyProfile.TailLatencyRatio > 10 {
		anomalies = append(anomalies, InstanceAnomaly{
			Type:        "tail_latency",
			Severity:    "critical",
			Description: "Extreme tail latency detected",
			Score:       profile.LatencyProfile.TailLatencyRatio / 10.0,
		})
	} else if profile.LatencyProfile.TailLatencyRatio > 5 {
		anomalies = append(anomalies, InstanceAnomaly{
			Type:        "tail_latency",
			Severity:    "warning",
			Description: "Elevated tail latency",
			Score:       profile.LatencyProfile.TailLatencyRatio / 10.0,
		})
	}

	if profile.QPSProfile.BurstinessScore > 0.7 {
		anomalies = append(anomalies, InstanceAnomaly{
			Type:        "burst_traffic",
			Severity:    "warning",
			Description: "High traffic burstiness detected",
			Score:       profile.QPSProfile.BurstinessScore,
		})
	}

	if profile.LatencyProfile.SpikeCount > 10 {
		anomalies = append(anomalies, InstanceAnomaly{
			Type:        "latency_spikes",
			Severity:    "warning",
			Description: "Frequent latency spikes detected",
			Score:       float64(profile.LatencyProfile.SpikeCount) / 20.0,
		})
	}

	return anomalies
}

func generateInstanceRecommendation(profile InstanceProfile) string {
	if profile.ResourceUtilization.IsOverloaded {
		return "Consider scaling or load balancing"
	}

	if profile.ResourceUtilization.IsUnderutilized {
		return "Potential resource consolidation candidate"
	}

	if profile.LatencyProfile.P99LatencyMs > 200 {
		return "Investigate latency optimization"
	}

	if profile.QPSProfile.QPSCV > 0.5 {
		return "Monitor traffic variability"
	}

	return "Instance operating normally"
}

func calculateClusterHealthSummary(instances []InstanceProfile) ClusterHealthSummary {
	summary := ClusterHealthSummary{}

	summary.TotalInstances = len(instances)

	var loadSum float64
	var loads []float64

	for _, inst := range instances {
		loadSum += inst.QPSProfile.MeanQPS
		loads = append(loads, inst.QPSProfile.MeanQPS)

		switch inst.HealthStatus {
		case "healthy":
			summary.HealthyCount++
		case "degraded":
			summary.DegradedCount++
		default:
			summary.CriticalCount++
		}
	}

	summary.AverageLoad = loadSum / float64(len(instances))

	if len(loads) > 0 {
		summary.LoadDistributionCV = stdDev(loads) / mean(loads)
	}

	summary.ClusterHealthScore = calculateClusterHealthScore(summary)

	return summary
}

func calculateClusterHealthScore(summary ClusterHealthSummary) float64 {
	if summary.TotalInstances == 0 {
		return 100
	}

	healthyRatio := float64(summary.HealthyCount) / float64(summary.TotalInstances)
	degradedRatio := float64(summary.DegradedCount) / float64(summary.TotalInstances)

	score := healthyRatio*100 + degradedRatio*50

	cvPenalty := summary.LoadDistributionCV * 20

	return math.Max(0, math.Min(100, score-cvPenalty))
}

func analyzeHotspots(instances []InstanceProfile, qpsByInstance map[string]float64) HotspotAnalysis {
	analysis := HotspotAnalysis{
		TemperatureDistribution: make(map[string]float64),
	}

	if len(instances) == 0 {
		return analysis
	}

	var qpsValues []float64
	for _, qps := range qpsByInstance {
		qpsValues = append(qpsValues, qps)
	}

	if len(qpsValues) == 0 {
		return analysis
	}

	avgQPS := mean(qpsValues)

	for _, inst := range instances {
		temp := inst.QPSProfile.MeanQPS / avgQPS
		analysis.TemperatureDistribution[inst.InstanceID] = temp

		if temp > 1.5 {
			analysis.HotInstances = append(analysis.HotInstances, inst.InstanceID)
		} else if temp < 0.5 {
			analysis.ColdInstances = append(analysis.ColdInstances, inst.InstanceID)
		}
	}

	analysis.HasHotspot = len(analysis.HotInstances) > 0

	if len(analysis.HotInstances) > 0 {
		hotQPS := 0.0
		for _, inst := range instances {
			for _, hotID := range analysis.HotInstances {
				if inst.InstanceID == hotID {
					hotQPS += inst.QPSProfile.MeanQPS
					break
				}
			}
		}
		analysis.HotspotRatio = hotQPS / (avgQPS * float64(len(instances)))
	}

	switch {
	case analysis.HotspotRatio > 2:
		analysis.HotspotSeverity = "severe"
	case analysis.HotspotRatio > 1.5:
		analysis.HotspotSeverity = "moderate"
	case analysis.HotspotRatio > 1.2:
		analysis.HotspotSeverity = "mild"
	default:
		analysis.HotspotSeverity = "none"
	}

	return analysis
}

func generateRebalanceRecommendation(analysis *InstanceClusterAnalysis) string {
	if !analysis.HotspotAnalysis.HasHotspot {
		return "Load is balanced across instances"
	}

	if analysis.HotspotAnalysis.HotspotSeverity == "severe" {
		return "Urgent: Implement load balancing or add capacity to hot instances"
	} else if analysis.HotspotAnalysis.HotspotSeverity == "moderate" {
		return "Consider redistributing load from hot instances"
	}

	return "Monitor hot instance load distribution"
}

func calculateInstanceTrend(data []TimeSeriesPoint) string {
	if len(data) < 20 {
		return "unknown"
	}

	n := len(data)
	firstHalf := data[:n/2]
	secondHalf := data[n/2:]

	firstMean := mean(extractValuesFromTSP(firstHalf))
	secondMean := mean(extractValuesFromTSP(secondHalf))

	if firstMean == 0 {
		return "unknown"
	}

	change := (secondMean - firstMean) / firstMean

	if change > 0.15 {
		return "upward"
	} else if change < -0.15 {
		return "downward"
	}
	return "stable"
}

func countLatencySpikes(vals []float64, meanLatency float64) int {
	if meanLatency == 0 {
		return 0
	}

	threshold := meanLatency * 2
	count := 0

	for _, v := range vals {
		if v > threshold {
			count++
		}
	}

	return count
}
