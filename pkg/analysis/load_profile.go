// Package analysis provides comprehensive load profiling for TiDB/TiKV clusters.
// This file implements the core LoadProfile analysis with QPS patterns, latency
// distribution, periodicity detection, and workload characterization.
// It integrates multi-dimension analysis, advanced insights, and correlation analysis.
package analysis

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type LoadProfile struct {
	ClusterID             string                 `json:"cluster_id"`
	DurationHours         float64                `json:"duration_hours"`
	Samples               int                    `json:"samples"`
	QPSProfile            QPSProfile             `json:"qps_profile"`
	LatencyProfile        LatencyProfile         `json:"latency_profile"`
	DailyPattern          DailyPattern           `json:"daily_pattern"`
	WeeklyPattern         WeeklyPattern          `json:"weekly_pattern"`
	MonthlyPattern        MonthlyPattern         `json:"monthly_pattern"`
	Periodicity           PeriodicityProfile     `json:"periodicity"`
	Characteristics       Characteristics        `json:"characteristics"`
	Workload              *WorkloadProfile       `json:"workload,omitempty"`
	InstanceSkew          *InstanceSkewProfile   `json:"instance_skew,omitempty"`
	Correlation           CorrelationAnalysis    `json:"correlation"`
	TrendAnalysis         TrendAnalysis          `json:"trend_analysis"`
	ResourceEfficiency    ResourceEfficiency     `json:"resource_efficiency"`
	Insights              *ClusterInsights       `json:"insights,omitempty"`
	MultiDimension        *MultiDimensionProfile `json:"multi_dimension,omitempty"`
	AdvancedInsights      *AdvancedInsights      `json:"advanced_insights,omitempty"`
	TimeSeriesCorrelation *TimeSeriesCorrelation `json:"time_series_correlation,omitempty"`
}

type ClusterInsights struct {
	OverallHealth             string   `json:"overall_health"`
	RiskFactors               []string `json:"risk_factors"`
	OptimizationOpportunities []string `json:"optimization_opportunities"`
	PatternType               string   `json:"pattern_type"`
	RecommendedActions        []string `json:"recommended_actions"`
	AnomalyIndicators         []string `json:"anomaly_indicators"`
	PerformanceScore          float64  `json:"performance_score"`
	StabilityScore            float64  `json:"stability_score"`
	EfficiencyScore           float64  `json:"efficiency_score"`
}

type MonthlyPattern struct {
	HasMonthlyPattern    bool            `json:"has_monthly_pattern"`
	PatternStrength      float64         `json:"pattern_strength"`
	WeekOfMonthVariation map[int]float64 `json:"week_of_month_variation"`
	FirstWeekRatio       float64         `json:"first_week_ratio"`
	MidMonthRatio        float64         `json:"mid_month_ratio"`
	LastWeekRatio        float64         `json:"last_week_ratio"`
	ConsistencyScore     float64         `json:"consistency_score"`
}

type PeriodicityProfile struct {
	DailyStrength      float64 `json:"daily_strength"`
	WeeklyStrength     float64 `json:"weekly_strength"`
	MonthlyStrength    float64 `json:"monthly_strength"`
	DominantPeriod     string  `json:"dominant_period"`
	PeriodicityScore   float64 `json:"periodicity_score"`
	HasMultiplePeriods bool    `json:"has_multiple_periods"`
}

type InstanceSkewProfile struct {
	TiDBSkew            InstanceSkewDetail `json:"tidb_skew"`
	TiKVSkew            InstanceSkewDetail `json:"tikv_skew"`
	HasQPSImbalance     bool               `json:"has_qps_imbalance"`
	HasLatencyImbalance bool               `json:"has_latency_imbalance"`
	SkewRiskLevel       string             `json:"skew_risk_level"`
	HotInstanceCount    int                `json:"hot_instance_count"`
	Recommendation      string             `json:"recommendation"`
}

type InstanceSkewDetail struct {
	InstanceCount          int                `json:"instance_count"`
	QPSDistribution        map[string]float64 `json:"qps_distribution"`
	LatencyDistribution    map[string]float64 `json:"latency_distribution"`
	QPSSkewCoefficient     float64            `json:"qps_skew_coefficient"`
	LatencySkewCoefficient float64            `json:"latency_skew_coefficient"`
	HotInstances           []string           `json:"hot_instances"`
	ColdInstances          []string           `json:"cold_instances"`
	MaxQPSRatio            float64            `json:"max_qps_ratio"`
	MinQPSRatio            float64            `json:"min_qps_ratio"`
}

type CorrelationAnalysis struct {
	QPSLatencyCorr   float64 `json:"qps_latency_corr"`
	QPSLatencyLagged float64 `json:"qps_latency_lagged"`
	IsLoadSensitive  bool    `json:"is_load_sensitive"`
	LoadSensitivity  string  `json:"load_sensitivity"`
	CorrelationTrend string  `json:"correlation_trend"`
}

type TrendAnalysis struct {
	ShortTermEMA      float64 `json:"short_term_ema"`
	LongTermEMA       float64 `json:"long_term_ema"`
	EMACrossSignal    string  `json:"ema_cross_signal"`
	TrendDirection    string  `json:"trend_direction"`
	TrendAcceleration float64 `json:"trend_acceleration"`
	ChangePointCount  int     `json:"change_point_count"`
	RecentTrendSlope  float64 `json:"recent_trend_slope"`
	ForecastNext24H   float64 `json:"forecast_next_24h"`
}

type ResourceEfficiency struct {
	QPSEfficiency   float64 `json:"qps_efficiency"`
	PeakUtilization float64 `json:"peak_utilization"`
	AvgUtilization  float64 `json:"avg_utilization"`
	EfficiencyScore float64 `json:"efficiency_score"`
	Recommendation  string  `json:"recommendation"`
}

type QPSProfile struct {
	Mean         float64 `json:"mean"`
	Median       float64 `json:"median"`
	StdDev       float64 `json:"std_dev"`
	Min          float64 `json:"min"`
	Max          float64 `json:"max"`
	P10          float64 `json:"p10"`
	P90          float64 `json:"p90"`
	P99          float64 `json:"p99"`
	PeakToAvg    float64 `json:"peak_to_avg"`
	CV           float64 `json:"cv"`
	Skewness     float64 `json:"skewness"`
	IQR          float64 `json:"iqr"`
	Kurtosis     float64 `json:"kurtosis"`
	OutlierRatio float64 `json:"outlier_ratio"`
	MAD          float64 `json:"mad"`
	Entropy      float64 `json:"entropy"`
}

type LatencyProfile struct {
	P50Ms      float64 `json:"p50_ms"`
	P90Ms      float64 `json:"p90_ms"`
	P99Ms      float64 `json:"p99_ms"`
	MaxMs      float64 `json:"max_ms"`
	P99toP50   float64 `json:"p99_to_p50"`
	TailRatio  float64 `json:"tail_ratio"`
	StdDevMs   float64 `json:"std_dev_ms"`
	IQRMs      float64 `json:"iqr_ms"`
	CV         float64 `json:"cv"`
	SpikeRatio float64 `json:"spike_ratio"`
	Stability  float64 `json:"stability"`
	P95Ms      float64 `json:"p95_ms"`
	MedianMs   float64 `json:"median_ms"`
}

type DailyPattern struct {
	HourlyAvg        map[int]float64 `json:"hourly_avg"`
	HourlyPeak       map[int]float64 `json:"hourly_peak"`
	HourlyStdDev     map[int]float64 `json:"hourly_std_dev"`
	PeakHours        []int           `json:"peak_hours"`
	OffPeakHours     []int           `json:"off_peak_hours"`
	PeakToOffPeak    float64         `json:"peak_to_off_peak"`
	NightDrop        float64         `json:"night_drop"`
	NightDropHours   []int           `json:"night_drop_hours"`
	BusinessHours    BusinessHours   `json:"business_hours"`
	PatternStrength  float64         `json:"pattern_strength"`
	ConsistencyScore float64         `json:"consistency_score"`
	PeriodicityScore float64         `json:"periodicity_score"`
}

type BusinessHours struct {
	StartHour int     `json:"start_hour"`
	EndHour   int     `json:"end_hour"`
	AvgQPS    float64 `json:"avg_qps"`
	Ratio     float64 `json:"ratio"`
}

type WeeklyPattern struct {
	DailyAvg           map[string]float64 `json:"daily_avg"`
	WeekdayAvg         float64            `json:"weekday_avg"`
	WeekendAvg         float64            `json:"weekend_avg"`
	WeekendDrop        float64            `json:"weekend_drop"`
	IsWeekdayHeavy     bool               `json:"is_weekday_heavy"`
	PatternStrength    float64            `json:"pattern_strength"`
	DayOfWeekVariation map[int]float64    `json:"day_of_week_variation"`
	ConsistencyScore   float64            `json:"consistency_score"`
}

type Characteristics struct {
	Burstiness      float64 `json:"burstiness"`
	Predictability  float64 `json:"predictability"`
	TrendSlope      float64 `json:"trend_slope"`
	IsGrowing       bool    `json:"is_growing"`
	StabilityClass  string  `json:"stability_class"`
	LoadClass       string  `json:"load_class"`
	TrafficType     string  `json:"traffic_type"`
	Seasonality     float64 `json:"seasonality"`
	TrendStrength   float64 `json:"trend_strength"`
	NoiseLevel      float64 `json:"noise_level"`
	AnomalyScore    float64 `json:"anomaly_score"`
	EMATrend        string  `json:"ema_trend"`
	ChangePoints    int     `json:"change_points"`
	Autocorrelation float64 `json:"autocorrelation"`
}

func AnalyzeLoadProfile(clusterID string, qpsData []TimeSeriesPoint, latencyData []TimeSeriesPoint) *LoadProfile {
	return AnalyzeLoadProfileWithWorkload(clusterID, qpsData, latencyData, nil, nil, nil, nil)
}

func AnalyzeLoadProfileWithWorkload(
	clusterID string,
	qpsData []TimeSeriesPoint,
	latencyData []TimeSeriesPoint,
	sqlTypeData map[string][]TimeSeriesPoint,
	sqlLatencyData map[string][]TimeSeriesPoint,
	tikvOpData map[string][]TimeSeriesPoint,
	tikvLatencyData map[string][]TimeSeriesPoint,
) *LoadProfile {
	return AnalyzeLoadProfileFull(clusterID, qpsData, latencyData, sqlTypeData, sqlLatencyData,
		tikvOpData, tikvLatencyData, nil, nil, nil, nil)
}

func AnalyzeLoadProfileFull(
	clusterID string,
	qpsData []TimeSeriesPoint,
	latencyData []TimeSeriesPoint,
	sqlTypeData map[string][]TimeSeriesPoint,
	sqlLatencyData map[string][]TimeSeriesPoint,
	tikvOpData map[string][]TimeSeriesPoint,
	tikvLatencyData map[string][]TimeSeriesPoint,
	tidbInstanceQPS map[string][]TimeSeriesPoint,
	tidbInstanceLatency map[string][]TimeSeriesPoint,
	tikvInstanceQPS map[string][]TimeSeriesPoint,
	tikvInstanceLatency map[string][]TimeSeriesPoint,
) *LoadProfile {
	if len(qpsData) == 0 {
		return nil
	}

	sort.Slice(qpsData, func(i, j int) bool {
		return qpsData[i].Timestamp < qpsData[j].Timestamp
	})

	profile := &LoadProfile{
		ClusterID: clusterID,
	}

	if len(qpsData) > 0 {
		first := qpsData[0].Timestamp
		last := qpsData[len(qpsData)-1].Timestamp
		profile.DurationHours = float64(last-first) / 3600000
		profile.Samples = len(qpsData)
	}

	profile.QPSProfile = analyzeQPSProfile(qpsData)
	profile.LatencyProfile = analyzeLatencyProfile(latencyData)
	profile.DailyPattern = analyzeDailyPattern(qpsData)
	profile.WeeklyPattern = analyzeWeeklyPattern(qpsData)
	profile.MonthlyPattern = analyzeMonthlyPattern(qpsData)
	profile.Periodicity = analyzePeriodicity(qpsData, profile.DailyPattern, profile.WeeklyPattern, profile.MonthlyPattern)
	profile.Characteristics = analyzeCharacteristics(qpsData, profile)
	profile.Correlation = analyzeCorrelation(qpsData, latencyData)
	profile.TrendAnalysis = analyzeTrend(qpsData)
	profile.ResourceEfficiency = analyzeResourceEfficiency(profile)

	if len(sqlTypeData) > 0 || len(tikvOpData) > 0 {
		profile.Workload = AnalyzeWorkloadProfile(sqlTypeData, sqlLatencyData, tikvOpData, tikvLatencyData)
	}

	if len(tidbInstanceQPS) > 0 || len(tikvInstanceQPS) > 0 {
		profile.InstanceSkew = analyzeInstanceSkew(tidbInstanceQPS, tidbInstanceLatency, tikvInstanceQPS, tikvInstanceLatency)
	}

	profile.Insights = generateClusterInsights(profile)

	profile.MultiDimension = AnalyzeMultiDimensionProfile(
		clusterID,
		qpsData,
		latencyData,
		sqlTypeData,
		sqlLatencyData,
		tikvOpData,
		tikvLatencyData,
		tidbInstanceQPS,
		tidbInstanceLatency,
		tikvInstanceQPS,
		tikvInstanceLatency,
	)

	if profile.MultiDimension != nil {
		profile.AdvancedInsights = GenerateAdvancedInsights(profile.MultiDimension)
		profile.TimeSeriesCorrelation = AnalyzeTimeSeriesCorrelation(profile.MultiDimension)
	}

	return profile
}

func analyzeInstanceSkew(
	tidbQPS map[string][]TimeSeriesPoint,
	tidbLatency map[string][]TimeSeriesPoint,
	tikvQPS map[string][]TimeSeriesPoint,
	tikvLatency map[string][]TimeSeriesPoint,
) *InstanceSkewProfile {
	profile := &InstanceSkewProfile{
		TiDBSkew: InstanceSkewDetail{
			QPSDistribution:     make(map[string]float64),
			LatencyDistribution: make(map[string]float64),
		},
		TiKVSkew: InstanceSkewDetail{
			QPSDistribution:     make(map[string]float64),
			LatencyDistribution: make(map[string]float64),
		},
	}

	if len(tidbQPS) > 0 {
		profile.TiDBSkew = analyzeSingleComponentSkew(tidbQPS, tidbLatency)
	}

	if len(tikvQPS) > 0 {
		profile.TiKVSkew = analyzeSingleComponentSkew(tikvQPS, tikvLatency)
	}

	profile.HasQPSImbalance = profile.TiDBSkew.QPSSkewCoefficient > 0.3 || profile.TiKVSkew.QPSSkewCoefficient > 0.3
	profile.HasLatencyImbalance = profile.TiDBSkew.LatencySkewCoefficient > 0.3 || profile.TiKVSkew.LatencySkewCoefficient > 0.3

	profile.HotInstanceCount = len(profile.TiDBSkew.HotInstances) + len(profile.TiKVSkew.HotInstances)

	maxSkew := math.Max(profile.TiDBSkew.QPSSkewCoefficient, profile.TiKVSkew.QPSSkewCoefficient)
	if maxSkew > 0.5 {
		profile.SkewRiskLevel = "high"
		profile.Recommendation = "Consider load balancing or scaling hot instances"
	} else if maxSkew > 0.3 {
		profile.SkewRiskLevel = "medium"
		profile.Recommendation = "Monitor instance load distribution"
	} else {
		profile.SkewRiskLevel = "low"
		profile.Recommendation = "Load distribution is balanced"
	}

	return profile
}

func analyzeSingleComponentSkew(
	qpsData map[string][]TimeSeriesPoint,
	latencyData map[string][]TimeSeriesPoint,
) InstanceSkewDetail {
	detail := InstanceSkewDetail{
		QPSDistribution:     make(map[string]float64),
		LatencyDistribution: make(map[string]float64),
	}

	detail.InstanceCount = len(qpsData)
	if detail.InstanceCount == 0 {
		return detail
	}

	var qpsMeans []float64
	for instance, points := range qpsData {
		if len(points) == 0 {
			continue
		}
		vals := extractValuesFromTSP(points)
		meanQPS := mean(vals)
		detail.QPSDistribution[instance] = meanQPS
		qpsMeans = append(qpsMeans, meanQPS)
	}

	if len(qpsMeans) > 0 {
		overallMean := mean(qpsMeans)
		if overallMean > 0 {
			detail.QPSSkewCoefficient = stdDev(qpsMeans) / overallMean

			var maxQPS, minQPS float64
			for _, qps := range detail.QPSDistribution {
				if maxQPS == 0 || qps > maxQPS {
					maxQPS = qps
				}
				if minQPS == 0 || qps < minQPS {
					minQPS = qps
				}
			}
			detail.MaxQPSRatio = maxQPS / overallMean
			detail.MinQPSRatio = minQPS / overallMean

			for instance, qps := range detail.QPSDistribution {
				if qps > overallMean*1.3 {
					detail.HotInstances = append(detail.HotInstances, instance)
				} else if qps < overallMean*0.7 {
					detail.ColdInstances = append(detail.ColdInstances, instance)
				}
			}
		}
	}

	var latMeans []float64
	for instance, points := range latencyData {
		if len(points) == 0 {
			continue
		}
		vals := extractValuesFromTSP(points)
		meanLat := mean(vals)
		detail.LatencyDistribution[instance] = meanLat
		latMeans = append(latMeans, meanLat)
	}

	if len(latMeans) > 0 {
		overallMean := mean(latMeans)
		if overallMean > 0 {
			detail.LatencySkewCoefficient = stdDev(latMeans) / overallMean
		}
	}

	return detail
}

func extractValuesFromTSP(points []TimeSeriesPoint) []float64 {
	vals := make([]float64, len(points))
	for i, p := range points {
		vals[i] = p.Value
	}
	return vals
}

func generateClusterInsights(profile *LoadProfile) *ClusterInsights {
	insights := &ClusterInsights{
		RiskFactors:               []string{},
		OptimizationOpportunities: []string{},
		RecommendedActions:        []string{},
		AnomalyIndicators:         []string{},
	}

	insights.PerformanceScore = calculatePerformanceScore(profile)
	insights.StabilityScore = calculateStabilityScore(profile)
	insights.EfficiencyScore = calculateEfficiencyScore(profile)

	insights.PatternType = classifyOverallPattern(profile)

	identifyRiskFactors(profile, insights)
	identifyOptimizations(profile, insights)
	identifyAnomalies(profile, insights)

	overallScore := (insights.PerformanceScore + insights.StabilityScore + insights.EfficiencyScore) / 3
	if overallScore >= 0.7 {
		insights.OverallHealth = "healthy"
	} else if overallScore >= 0.5 {
		insights.OverallHealth = "moderate"
	} else {
		insights.OverallHealth = "needs_attention"
	}

	if len(insights.RiskFactors) > 2 {
		insights.OverallHealth = "needs_attention"
	}

	return insights
}

func calculatePerformanceScore(profile *LoadProfile) float64 {
	score := 1.0

	if profile.LatencyProfile.P99Ms > 100 {
		score -= 0.2
	}
	if profile.LatencyProfile.P99Ms > 500 {
		score -= 0.3
	}
	if profile.LatencyProfile.CV > 0.5 {
		score -= 0.1
	}
	if profile.QPSProfile.CV > 0.7 {
		score -= 0.1
	}

	return math.Max(0, math.Min(1, score))
}

func calculateStabilityScore(profile *LoadProfile) float64 {
	score := 1.0

	stabilityClass := profile.Characteristics.StabilityClass
	switch stabilityClass {
	case "highly_variable":
		score -= 0.4
	case "variable":
		score -= 0.2
	case "moderate":
		score -= 0.1
	}

	if profile.Characteristics.ChangePoints > 5 {
		changePointPenalty := math.Min(0.3, 0.05*float64(profile.Characteristics.ChangePoints-5))
		score -= changePointPenalty
	}

	if profile.Characteristics.NoiseLevel > 0.5 {
		score -= 0.1
	}

	if profile.Periodicity.PeriodicityScore > 0.5 {
		score += 0.15
	} else if profile.Periodicity.PeriodicityScore > 0.3 {
		score += 0.1
	}

	return math.Max(0, math.Min(1, score))
}

func calculateEfficiencyScore(profile *LoadProfile) float64 {
	score := 1.0

	if profile.InstanceSkew != nil && profile.InstanceSkew.SkewRiskLevel == "high" {
		score -= 0.2
	}

	if profile.QPSProfile.PeakToAvg > 5 {
		score -= 0.1
	}
	if profile.QPSProfile.PeakToAvg > 10 {
		score -= 0.2
	}

	if profile.ResourceEfficiency.EfficiencyScore < 0.5 {
		score -= 0.1
	}

	return math.Max(0, math.Min(1, score))
}

func classifyOverallPattern(profile *LoadProfile) string {
	if profile.Periodicity.DominantPeriod == "none" {
		if profile.Characteristics.Burstiness > 0.5 {
			return "unpredictable"
		}
		return "flat"
	}

	var patterns []string
	if profile.Periodicity.DailyStrength > 0.3 {
		patterns = append(patterns, "daily")
	}
	if profile.Periodicity.WeeklyStrength > 0.2 {
		patterns = append(patterns, "weekly")
	}
	if profile.Periodicity.MonthlyStrength > 0.15 {
		patterns = append(patterns, "monthly")
	}

	if len(patterns) >= 2 {
		return "multi_periodic"
	} else if len(patterns) == 1 {
		return patterns[0] + "_periodic"
	}

	return "mixed"
}

func identifyRiskFactors(profile *LoadProfile, insights *ClusterInsights) {
	if profile.LatencyProfile.P99Ms > 500 {
		insights.RiskFactors = append(insights.RiskFactors, "high_latency_p99")
	} else if profile.LatencyProfile.P99Ms > 200 {
		insights.RiskFactors = append(insights.RiskFactors, "elevated_latency_p99")
	}

	if profile.Characteristics.StabilityClass == "highly_variable" {
		insights.RiskFactors = append(insights.RiskFactors, "highly_variable_load")
	} else if profile.Characteristics.StabilityClass == "variable" {
		insights.RiskFactors = append(insights.RiskFactors, "variable_load")
	}

	if profile.InstanceSkew != nil && profile.InstanceSkew.SkewRiskLevel == "high" {
		insights.RiskFactors = append(insights.RiskFactors, "instance_imbalance")
	}

	if profile.Characteristics.ChangePoints > 5 {
		insights.RiskFactors = append(insights.RiskFactors, "frequent_regime_changes")
	}

	if profile.Characteristics.AnomalyScore > 0.5 {
		insights.RiskFactors = append(insights.RiskFactors, "anomaly_prone")
	}

	if profile.Workload != nil && profile.Workload.HotspotRisk == "high" {
		insights.RiskFactors = append(insights.RiskFactors, "hotspot_risk")
	}

	if profile.QPSProfile.PeakToAvg > 10 {
		insights.RiskFactors = append(insights.RiskFactors, "extreme_peak_to_avg_ratio")
	}

	if profile.Characteristics.IsGrowing && profile.TrendAnalysis.TrendAcceleration > 0.01 {
		insights.RiskFactors = append(insights.RiskFactors, "accelerating_growth")
	}

	if profile.QPSProfile.CV > 0.8 {
		insights.RiskFactors = append(insights.RiskFactors, "high_variability")
	}
}

func identifyOptimizations(profile *LoadProfile, insights *ClusterInsights) {
	if profile.QPSProfile.PeakToAvg > 3 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"consider_autoscaling_for_peak_load")
	}

	if profile.QPSProfile.PeakToAvg > 5 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"burst_capacity_planning_recommended")
	}

	if profile.InstanceSkew != nil && len(profile.InstanceSkew.TiDBSkew.HotInstances) > 0 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"balance_load_across_instances")
	}

	if profile.Periodicity.DailyStrength > 0.5 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"scheduled_scaling_based_on_daily_pattern")
	}

	if profile.Periodicity.WeeklyStrength > 0.3 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"weekly_capacity_planning")
	}

	if profile.Workload != nil && profile.Workload.WriteAmplification > 3 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"optimize_write_pattern_to_reduce_amplification")
	}

	if profile.LatencyProfile.TailRatio > 3 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"investigate_latency_tail_issues")
	}

	if profile.LatencyProfile.P99Ms > 200 {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"latency_optimization_recommended")
	}

	if profile.ResourceEfficiency.EfficiencyScore < 0.4 && !profile.Characteristics.IsGrowing {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"resource_consolidation_candidate")
	}

	if profile.Characteristics.IsGrowing {
		insights.OptimizationOpportunities = append(insights.OptimizationOpportunities,
			"capacity_expansion_planning")
	}
}

func identifyAnomalies(profile *LoadProfile, insights *ClusterInsights) {
	if profile.QPSProfile.CV > 1.0 {
		insights.AnomalyIndicators = append(insights.AnomalyIndicators, "extreme_qps_variability")
	}

	if profile.LatencyProfile.SpikeRatio > 0.3 {
		insights.AnomalyIndicators = append(insights.AnomalyIndicators, "latency_spikes_detected")
	}

	if profile.Characteristics.AnomalyScore > 0.7 {
		insights.AnomalyIndicators = append(insights.AnomalyIndicators, "high_anomaly_score")
	}

	if profile.WeeklyPattern.WeekendDrop < -0.3 {
		insights.AnomalyIndicators = append(insights.AnomalyIndicators, "significant_weekend_drop")
	}

	if profile.MonthlyPattern.HasMonthlyPattern && profile.MonthlyPattern.PatternStrength > 0.3 {
		insights.AnomalyIndicators = append(insights.AnomalyIndicators, "monthly_cycle_detected")
	}
}

func analyzeCorrelation(qpsData, latencyData []TimeSeriesPoint) CorrelationAnalysis {
	corr := CorrelationAnalysis{}

	if len(qpsData) < 10 || len(latencyData) < 10 {
		return corr
	}

	qpsByTS := make(map[int64]float64)
	for _, p := range qpsData {
		qpsByTS[p.Timestamp] = p.Value
	}

	latByTS := make(map[int64]float64)
	for _, p := range latencyData {
		latByTS[p.Timestamp] = p.Value
	}

	var commonQPS, commonLat []float64
	for ts, qps := range qpsByTS {
		if lat, ok := latByTS[ts]; ok {
			commonQPS = append(commonQPS, qps)
			commonLat = append(commonLat, lat)
		}
	}

	if len(commonQPS) >= 10 {
		corr.QPSLatencyCorr = robustCorrelation(commonQPS, commonLat)
	}

	if len(commonQPS) >= 20 {
		var laggedQPS, laggedLat []float64
		for i := 1; i < len(commonQPS); i++ {
			laggedQPS = append(laggedQPS, commonQPS[i-1])
			laggedLat = append(laggedLat, commonLat[i])
		}
		corr.QPSLatencyLagged = robustCorrelation(laggedQPS, laggedLat)
	}

	absCorr := math.Abs(corr.QPSLatencyCorr)
	if absCorr > 0.5 {
		corr.IsLoadSensitive = true
		corr.LoadSensitivity = "high"
	} else if absCorr > 0.3 {
		corr.IsLoadSensitive = true
		corr.LoadSensitivity = "moderate"
	} else {
		corr.IsLoadSensitive = false
		corr.LoadSensitivity = "low"
	}

	if corr.QPSLatencyCorr > 0 {
		corr.CorrelationTrend = "positive"
	} else if corr.QPSLatencyCorr < 0 {
		corr.CorrelationTrend = "negative"
	} else {
		corr.CorrelationTrend = "none"
	}

	return corr
}

func robustCorrelation(x, y []float64) float64 {
	if len(x) != len(y) || len(x) < 5 {
		return 0
	}

	sortedX := make([]float64, len(x))
	sortedY := make([]float64, len(y))
	copy(sortedX, x)
	copy(sortedY, y)
	sort.Float64s(sortedX)
	sort.Float64s(sortedY)

	q1X := percentile(sortedX, 0.25)
	q3X := percentile(sortedX, 0.75)
	q1Y := percentile(sortedY, 0.25)
	q3Y := percentile(sortedY, 0.75)

	iqrX := q3X - q1X
	iqrY := q3Y - q1Y

	var filteredX, filteredY []float64
	for i := 0; i < len(x); i++ {
		if x[i] >= q1X-1.5*iqrX && x[i] <= q3X+1.5*iqrX &&
			y[i] >= q1Y-1.5*iqrY && y[i] <= q3Y+1.5*iqrY {
			filteredX = append(filteredX, x[i])
			filteredY = append(filteredY, y[i])
		}
	}

	if len(filteredX) < 5 {
		return PearsonCorrelation(x, y)
	}

	n := len(filteredX)
	sumX, sumY := 0.0, 0.0
	for i := 0; i < n; i++ {
		sumX += filteredX[i]
		sumY += filteredY[i]
	}
	meanX := sumX / float64(n)
	meanY := sumY / float64(n)

	cov := 0.0
	stdX, stdY := 0.0, 0.0
	for i := 0; i < n; i++ {
		cov += (filteredX[i] - meanX) * (filteredY[i] - meanY)
		stdX += (filteredX[i] - meanX) * (filteredX[i] - meanX)
		stdY += (filteredY[i] - meanY) * (filteredY[i] - meanY)
	}
	stdX = math.Sqrt(stdX)
	stdY = math.Sqrt(stdY)

	if stdX == 0 || stdY == 0 {
		return 0
	}

	return cov / (stdX * stdY)
}

func analyzeTrend(data []TimeSeriesPoint) TrendAnalysis {
	trend := TrendAnalysis{}

	if len(data) < 20 {
		return trend
	}

	vals := make([]float64, len(data))
	for i, p := range data {
		vals[i] = p.Value
	}

	shortAlpha := 0.2
	longAlpha := 0.05

	shortEMA := calculateEMA(vals, shortAlpha)
	longEMA := calculateEMA(vals, longAlpha)

	trend.ShortTermEMA = shortEMA
	trend.LongTermEMA = longEMA

	if shortEMA > longEMA*1.05 {
		trend.EMACrossSignal = "bullish"
		trend.TrendDirection = "upward"
	} else if shortEMA < longEMA*0.95 {
		trend.EMACrossSignal = "bearish"
		trend.TrendDirection = "downward"
	} else {
		trend.EMACrossSignal = "neutral"
		trend.TrendDirection = "stable"
	}

	n := len(vals)
	if n >= 20 {
		recent := vals[n-10:]
		earlier := vals[n-20 : n-10]
		recentMean := mean(recent)
		earlierMean := mean(earlier)
		if earlierMean > 0 {
			trend.RecentTrendSlope = (recentMean - earlierMean) / earlierMean
		}
	}

	if len(vals) >= 20 {
		firstHalf := vals[:len(vals)/2]
		secondHalf := vals[len(vals)/2:]
		firstSlope := calculateSlopeSimple(firstHalf)
		secondSlope := calculateSlopeSimple(secondHalf)
		trend.TrendAcceleration = secondSlope - firstSlope
	}

	trend.ChangePointCount = detectChangePoints(data)

	overallSlope := calculateTrendSlope(data)
	lastValue := vals[len(vals)-1]
	if overallSlope != 0 {
		trend.ForecastNext24H = lastValue * (1 + overallSlope*24)
	} else {
		trend.ForecastNext24H = lastValue
	}

	return trend
}

func calculateSlopeSimple(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}

	n := float64(len(vals))
	sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0

	for i, v := range vals {
		x := float64(i)
		y := v
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return 0
	}

	return (n*sumXY - sumX*sumY) / denominator
}

func analyzeResourceEfficiency(profile *LoadProfile) ResourceEfficiency {
	eff := ResourceEfficiency{}

	qps := profile.QPSProfile
	char := profile.Characteristics

	if qps.Max > 0 {
		eff.QPSEfficiency = qps.Mean / qps.Max
		if eff.QPSEfficiency > 1 {
			eff.QPSEfficiency = 1
		}
	}

	peakUtil := 0.0
	if qps.PeakToAvg > 1 {
		peakUtil = (qps.PeakToAvg - 1) / 4.0
	}
	eff.PeakUtilization = math.Min(1.0, peakUtil)

	eff.AvgUtilization = math.Max(0, 1.0-qps.CV)

	periodicityBonus := 0.0
	if profile.Periodicity.PeriodicityScore > 0.3 {
		periodicityBonus = 0.1
	}

	eff.EfficiencyScore = (eff.QPSEfficiency*0.3 + (1-eff.PeakUtilization)*0.25 + eff.AvgUtilization*0.25 + periodicityBonus)
	eff.EfficiencyScore = math.Min(1.0, eff.EfficiencyScore)

	if eff.EfficiencyScore > 0.7 {
		eff.Recommendation = "well_optimized"
	} else if eff.EfficiencyScore > 0.5 {
		eff.Recommendation = "room_for_improvement"
	} else if char.IsGrowing {
		eff.Recommendation = "scale_up_needed"
	} else {
		eff.Recommendation = "scale_down_candidate"
	}

	return eff
}

func analyzeQPSProfile(data []TimeSeriesPoint) QPSProfile {
	if len(data) == 0 {
		return QPSProfile{}
	}

	vals := make([]float64, len(data))
	for i, p := range data {
		vals[i] = p.Value
	}

	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	var filteredVals []float64
	if len(sorted) >= 10 {
		q1 := percentile(sorted, 0.25)
		q3 := percentile(sorted, 0.75)
		iqr := q3 - q1
		lowerBound := q1 - 3*iqr
		upperBound := q3 + 3*iqr

		for _, v := range vals {
			if v >= lowerBound && v <= upperBound {
				filteredVals = append(filteredVals, v)
			}
		}

		if len(filteredVals) < len(vals)/2 {
			filteredVals = vals
		}
	} else {
		filteredVals = vals
	}

	filteredSorted := make([]float64, len(filteredVals))
	copy(filteredSorted, filteredVals)
	sort.Float64s(filteredSorted)

	profile := QPSProfile{
		Min:    filteredSorted[0],
		Max:    filteredSorted[len(filteredSorted)-1],
		Mean:   mean(filteredVals),
		Median: median(filteredSorted),
		StdDev: stdDev(filteredVals),
	}

	if len(filteredSorted) >= 10 {
		profile.P10 = percentile(filteredSorted, 0.10)
		profile.P90 = percentile(filteredSorted, 0.90)
		profile.P99 = percentile(filteredSorted, 0.99)
		profile.IQR = percentile(filteredSorted, 0.75) - percentile(filteredSorted, 0.25)
	}

	if profile.Mean > 0 {
		if profile.P99 > 0 {
			profile.PeakToAvg = profile.P99 / profile.Mean
		} else {
			profile.PeakToAvg = profile.Max / profile.Mean
		}
		if profile.PeakToAvg > 100 {
			profile.PeakToAvg = 100
		}
		profile.CV = profile.StdDev / profile.Mean
		if profile.CV > 10 {
			profile.CV = 10
		}
	}

	profile.Skewness = skewness(vals)
	profile.Kurtosis = kurtosis(vals)
	profile.MAD = mad(vals)
	profile.Entropy = calculateEntropy(vals)
	profile.OutlierRatio = calculateOutlierRatio(sorted, profile.IQR)

	return profile
}

func analyzeLatencyProfile(data []TimeSeriesPoint) LatencyProfile {
	if len(data) == 0 {
		return LatencyProfile{}
	}

	vals := make([]float64, len(data))
	for i, p := range data {
		vals[i] = p.Value
	}

	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	profile := LatencyProfile{
		MaxMs: sorted[len(sorted)-1] * 1000,
	}

	if len(sorted) >= 10 {
		profile.P50Ms = percentile(sorted, 0.50) * 1000
		profile.P90Ms = percentile(sorted, 0.90) * 1000
		profile.P95Ms = percentile(sorted, 0.95) * 1000
		profile.P99Ms = percentile(sorted, 0.99) * 1000
		profile.MedianMs = profile.P50Ms
	}

	latencyMean := mean(vals)
	latencyStd := stdDev(vals)
	profile.StdDevMs = latencyStd * 1000

	if latencyMean > 0 {
		profile.CV = latencyStd / latencyMean
	}

	if len(sorted) >= 10 {
		profile.IQRMs = (percentile(sorted, 0.75) - percentile(sorted, 0.25)) * 1000
	}

	if profile.P50Ms > 0 {
		profile.P99toP50 = profile.P99Ms / profile.P50Ms
		profile.TailRatio = (profile.P99Ms - profile.P50Ms) / profile.P50Ms
	}

	if latencyMean > 0 {
		profile.SpikeRatio = profile.MaxMs / (latencyMean * 1000)
	}

	if profile.StdDevMs > 0 && latencyMean > 0 {
		profile.Stability = 1.0 - math.Min(1.0, profile.CV)
	}

	return profile
}

func analyzeDailyPattern(data []TimeSeriesPoint) DailyPattern {
	if len(data) == 0 {
		return DailyPattern{}
	}

	hourlyData := make(map[int][]float64)
	for _, p := range data {
		hour := time.Unix(p.Timestamp/1000, 0).Hour()
		hourlyData[hour] = append(hourlyData[hour], p.Value)
	}

	pattern := DailyPattern{
		HourlyAvg:    make(map[int]float64),
		HourlyPeak:   make(map[int]float64),
		HourlyStdDev: make(map[int]float64),
	}

	for hour, vals := range hourlyData {
		pattern.HourlyAvg[hour] = mean(vals)
		pattern.HourlyPeak[hour] = max(vals)
		pattern.HourlyStdDev[hour] = stdDev(vals)
	}

	var allAvgs []float64
	for _, avg := range pattern.HourlyAvg {
		allAvgs = append(allAvgs, avg)
	}

	overallMedian := median(allAvgs)
	overallMean := mean(allAvgs)
	overallStd := stdDev(allAvgs)

	var peakAvg, offPeakAvg float64
	var peakHours, offPeakHours []int

	dynamicThreshold := overallStd * 0.5
	peakThreshold := overallMedian + dynamicThreshold
	offPeakThreshold := overallMedian - dynamicThreshold

	if peakThreshold < overallMedian*1.2 {
		peakThreshold = overallMedian * 1.2
	}
	if offPeakThreshold > overallMedian*0.8 {
		offPeakThreshold = overallMedian * 0.8
	}

	for hour, avg := range pattern.HourlyAvg {
		if avg > peakThreshold {
			peakHours = append(peakHours, hour)
			peakAvg += avg
		} else if avg < offPeakThreshold {
			offPeakHours = append(offPeakHours, hour)
			offPeakAvg += avg
		}
	}

	sort.Ints(peakHours)
	sort.Ints(offPeakHours)
	pattern.PeakHours = peakHours
	pattern.OffPeakHours = offPeakHours

	if len(peakHours) > 0 && len(offPeakHours) > 0 {
		peakAvg /= float64(len(peakHours))
		offPeakAvg /= float64(len(offPeakHours))
		if offPeakAvg > 0 {
			pattern.PeakToOffPeak = peakAvg / offPeakAvg
		}
	}

	nightHours := []int{0, 1, 2, 3, 4, 5, 22, 23}
	var nightAvg, dayAvg float64
	var nightCount, dayCount int

	for hour, avg := range pattern.HourlyAvg {
		isNight := false
		for _, nh := range nightHours {
			if hour == nh {
				isNight = true
				break
			}
		}
		if isNight {
			nightAvg += avg
			nightCount++
		} else {
			dayAvg += avg
			dayCount++
		}
	}

	if nightCount > 0 && dayCount > 0 {
		nightAvg /= float64(nightCount)
		dayAvg /= float64(dayCount)
		if dayAvg > 0 {
			pattern.NightDrop = 1 - (nightAvg / dayAvg)
		}
	}
	pattern.NightDropHours = nightHours

	businessHours := []int{9, 10, 11, 12, 13, 14, 15, 16, 17}
	var bizAvg float64
	var bizCount int

	for _, h := range businessHours {
		if avg, ok := pattern.HourlyAvg[h]; ok {
			bizAvg += avg
			bizCount++
		}
	}

	if bizCount > 0 {
		bizAvg /= float64(bizCount)
		pattern.BusinessHours.StartHour = 9
		pattern.BusinessHours.EndHour = 17
		pattern.BusinessHours.AvgQPS = bizAvg
		if len(allAvgs) > 0 {
			if overallMean > 0 {
				pattern.BusinessHours.Ratio = bizAvg / overallMean
			}
		}
	}

	if pattern.PeakToOffPeak > 1 {
		pattern.PatternStrength = math.Min(1.0, (pattern.PeakToOffPeak-1)/2)
	}

	pattern.ConsistencyScore = calculateHourlyConsistency(hourlyData, pattern.HourlyAvg)
	pattern.PeriodicityScore = calculatePeriodicityScore(pattern.HourlyAvg, overallMean)

	return pattern
}

func calculateHourlyConsistency(hourlyData map[int][]float64, hourlyAvg map[int]float64) float64 {
	if len(hourlyData) == 0 {
		return 0
	}

	totalCV := 0.0
	count := 0

	for hour, vals := range hourlyData {
		if len(vals) > 1 {
			avg := hourlyAvg[hour]
			if avg > 0 {
				cv := stdDev(vals) / avg
				totalCV += cv
				count++
			}
		}
	}

	if count == 0 {
		return 1.0
	}

	avgCV := totalCV / float64(count)
	return math.Max(0, 1.0-avgCV)
}

func calculatePeriodicityScore(hourlyAvg map[int]float64, overallMean float64) float64 {
	if len(hourlyAvg) < 12 || overallMean == 0 {
		return 0
	}

	var sumSquaredDiff float64
	for _, avg := range hourlyAvg {
		normalizedDiff := (avg - overallMean) / overallMean
		sumSquaredDiff += normalizedDiff * normalizedDiff
	}

	variance := sumSquaredDiff / float64(len(hourlyAvg))
	return math.Min(1.0, variance*2)
}

func analyzeWeeklyPattern(data []TimeSeriesPoint) WeeklyPattern {
	if len(data) == 0 {
		return WeeklyPattern{}
	}

	dailyData := make(map[string][]float64)
	dayOfWeekData := make(map[int][]float64)
	var weekdaySum, weekendSum float64
	var weekdayCount, weekendCount int

	for _, p := range data {
		t := time.Unix(p.Timestamp/1000, 0)
		dayKey := t.Format("2006-01-02")
		dailyData[dayKey] = append(dailyData[dayKey], p.Value)

		weekday := int(t.Weekday())
		dayOfWeekData[weekday] = append(dayOfWeekData[weekday], p.Value)

		if weekday == 0 || weekday == 6 {
			weekendSum += p.Value
			weekendCount++
		} else {
			weekdaySum += p.Value
			weekdayCount++
		}
	}

	pattern := WeeklyPattern{
		DailyAvg:           make(map[string]float64),
		DayOfWeekVariation: make(map[int]float64),
	}

	for day, vals := range dailyData {
		pattern.DailyAvg[day] = mean(vals)
	}

	allDailyAvgs := make([]float64, 0, len(pattern.DailyAvg))
	for _, avg := range pattern.DailyAvg {
		allDailyAvgs = append(allDailyAvgs, avg)
	}

	for dow, vals := range dayOfWeekData {
		pattern.DayOfWeekVariation[dow] = mean(vals)
	}

	if weekdayCount > 0 {
		pattern.WeekdayAvg = weekdaySum / float64(weekdayCount)
	}
	if weekendCount > 0 {
		pattern.WeekendAvg = weekendSum / float64(weekendCount)
	}

	if pattern.WeekdayAvg > 0 && pattern.WeekendAvg > 0 {
		pattern.WeekendDrop = 1 - (pattern.WeekendAvg / pattern.WeekdayAvg)
		pattern.IsWeekdayHeavy = pattern.WeekdayAvg > pattern.WeekendAvg*1.2
	}

	if pattern.WeekdayAvg > 0 && pattern.WeekendAvg > 0 {
		absDiff := math.Abs(pattern.WeekdayAvg - pattern.WeekendAvg)
		pattern.PatternStrength = absDiff / pattern.WeekdayAvg
	}

	pattern.ConsistencyScore = calculateWeeklyConsistency(dayOfWeekData)

	return pattern
}

func calculateWeeklyConsistency(dayOfWeekData map[int][]float64) float64 {
	if len(dayOfWeekData) < 2 {
		return 0
	}

	dayMeans := make(map[int]float64)
	for dow, vals := range dayOfWeekData {
		dayMeans[dow] = mean(vals)
	}

	var totalCV float64
	count := 0

	for dow, vals := range dayOfWeekData {
		if len(vals) > 1 {
			avg := dayMeans[dow]
			if avg > 0 {
				cv := stdDev(vals) / avg
				totalCV += cv
				count++
			}
		}
	}

	if count == 0 {
		return 1.0
	}

	avgCV := totalCV / float64(count)
	return math.Max(0, 1.0-avgCV)
}

func analyzeMonthlyPattern(data []TimeSeriesPoint) MonthlyPattern {
	pattern := MonthlyPattern{
		WeekOfMonthVariation: make(map[int]float64),
	}

	if len(data) < 672 {
		return pattern
	}

	weekData := make(map[int][]float64)
	for _, p := range data {
		t := time.Unix(p.Timestamp/1000, 0)
		day := t.Day()
		weekOfMonth := (day - 1) / 7
		if weekOfMonth > 4 {
			weekOfMonth = 4
		}
		weekData[weekOfMonth] = append(weekData[weekOfMonth], p.Value)
	}

	if len(weekData) < 3 {
		return pattern
	}

	for week, vals := range weekData {
		pattern.WeekOfMonthVariation[week] = mean(vals)
	}

	var weekAvgs []float64
	for week := 0; week <= 4; week++ {
		if avg, ok := pattern.WeekOfMonthVariation[week]; ok {
			weekAvgs = append(weekAvgs, avg)
		}
	}

	if len(weekAvgs) < 3 {
		return pattern
	}

	overallMean := mean(weekAvgs)
	if overallMean == 0 {
		return pattern
	}

	if firstWeek, ok := pattern.WeekOfMonthVariation[0]; ok {
		pattern.FirstWeekRatio = firstWeek / overallMean
	}
	if midWeek, ok := pattern.WeekOfMonthVariation[2]; ok {
		pattern.MidMonthRatio = midWeek / overallMean
	}
	if lastWeek, ok := pattern.WeekOfMonthVariation[4]; ok {
		pattern.LastWeekRatio = lastWeek / overallMean
	}

	var variance float64
	for _, avg := range weekAvgs {
		diff := (avg - overallMean) / overallMean
		variance += diff * diff
	}
	pattern.PatternStrength = math.Min(1.0, variance/float64(len(weekAvgs))*2)

	pattern.HasMonthlyPattern = pattern.PatternStrength > 0.15

	cv := stdDev(weekAvgs) / overallMean
	pattern.ConsistencyScore = math.Max(0, 1.0-cv)

	return pattern
}

func analyzePeriodicity(data []TimeSeriesPoint, daily DailyPattern, weekly WeeklyPattern, monthly MonthlyPattern) PeriodicityProfile {
	profile := PeriodicityProfile{}

	profile.DailyStrength = calculateDailyStrength(daily)
	profile.WeeklyStrength = calculateWeeklyStrength(weekly)
	profile.MonthlyStrength = monthly.PatternStrength

	profile.PeriodicityScore = (profile.DailyStrength*0.5 + profile.WeeklyStrength*0.3 + profile.MonthlyStrength*0.2)

	profile.HasMultiplePeriods = (profile.DailyStrength > 0.3 && profile.WeeklyStrength > 0.2) ||
		(profile.DailyStrength > 0.3 && profile.MonthlyStrength > 0.15) ||
		(profile.WeeklyStrength > 0.2 && profile.MonthlyStrength > 0.15)

	if profile.DailyStrength >= profile.WeeklyStrength && profile.DailyStrength >= profile.MonthlyStrength {
		profile.DominantPeriod = "daily"
	} else if profile.WeeklyStrength >= profile.MonthlyStrength {
		profile.DominantPeriod = "weekly"
	} else {
		profile.DominantPeriod = "monthly"
	}

	if profile.PeriodicityScore < 0.1 {
		profile.DominantPeriod = "none"
	}

	return profile
}

func calculateDailyStrength(daily DailyPattern) float64 {
	if daily.PeakToOffPeak <= 1 {
		return 0
	}

	strength := (daily.PeakToOffPeak - 1) / 3.0
	strength = math.Min(1.0, strength)

	if daily.PatternStrength > 0 {
		strength = strength*0.6 + daily.PatternStrength*0.4
	}

	return strength
}

func calculateWeeklyStrength(weekly WeeklyPattern) float64 {
	if weekly.PatternStrength > 0 {
		return weekly.PatternStrength
	}

	if math.Abs(weekly.WeekendDrop) < 0.05 {
		return 0
	}

	strength := math.Abs(weekly.WeekendDrop) * 2
	return math.Min(1.0, strength)
}

func analyzeCharacteristics(data []TimeSeriesPoint, profile *LoadProfile) Characteristics {
	if len(data) < 10 {
		return Characteristics{}
	}

	char := Characteristics{}

	vals := make([]float64, len(data))
	for i, p := range data {
		vals[i] = p.Value
	}

	char.Burstiness = calculateBurstiness(vals)
	char.Predictability = 1 - char.Burstiness
	char.TrendSlope = calculateTrendSlope(data)
	char.IsGrowing = char.TrendSlope > 0.01
	char.Seasonality = calculateSeasonality(data)
	char.TrendStrength = calculateTrendStrength(data, char.TrendSlope)
	char.NoiseLevel = calculateNoiseLevel(vals)
	char.Autocorrelation = calculateAutocorrelation(vals)
	char.ChangePoints = detectChangePoints(data)
	char.AnomalyScore = calculateAnomalyScore(vals, profile)

	char.StabilityClass = classifyStabilityMulti(profile.QPSProfile.CV, char.TrendStrength, char.Autocorrelation, char.ChangePoints, profile.Periodicity.PeriodicityScore)
	char.LoadClass = classifyLoad(profile.QPSProfile.Mean)
	char.TrafficType = classifyTraffic(profile)

	shortEMA := calculateEMA(vals, 0.1)
	longEMA := calculateEMA(vals, 0.02)
	if shortEMA > longEMA*1.05 {
		char.EMATrend = "upward"
	} else if shortEMA < longEMA*0.95 {
		char.EMATrend = "downward"
	} else {
		char.EMATrend = "stable"
	}

	return char
}

func calculateSeasonality(data []TimeSeriesPoint) float64 {
	if len(data) < 48 {
		return 0
	}

	hourlyAvg := make(map[int][]float64)
	for _, p := range data {
		hour := time.Unix(p.Timestamp/1000, 0).Hour()
		hourlyAvg[hour] = append(hourlyAvg[hour], p.Value)
	}

	if len(hourlyAvg) < 12 {
		return 0
	}

	hourlyMeansArr := make([]float64, 24)
	for i := 0; i < 24; i++ {
		if vals, ok := hourlyAvg[i]; ok {
			hourlyMeansArr[i] = mean(vals)
		}
	}

	overallMean := mean(hourlyMeansArr)
	if overallMean == 0 {
		return 0
	}

	var variance float64
	for _, m := range hourlyMeansArr {
		diff := (m - overallMean) / overallMean
		variance += diff * diff
	}
	variance /= 24.0

	peakHour := 0
	troughHour := 0
	maxVal := hourlyMeansArr[0]
	minVal := hourlyMeansArr[0]
	for i, v := range hourlyMeansArr {
		if v > maxVal {
			maxVal = v
			peakHour = i
		}
		if v < minVal {
			minVal = v
			troughHour = i
		}
	}

	amplitude := 0.0
	if minVal > 0 {
		amplitude = (maxVal - minVal) / minVal
	}

	_ = peakHour
	_ = troughHour

	seasonalityScore := math.Min(1.0, variance*2)*0.6 + math.Min(1.0, amplitude/3)*0.4

	return seasonalityScore
}

func calculateTrendStrength(data []TimeSeriesPoint, slope float64) float64 {
	if len(data) < 10 {
		return 0
	}

	n := float64(len(data))
	if n == 0 {
		return 0
	}

	vals := make([]float64, len(data))
	for i, p := range data {
		vals[i] = p.Value
	}

	detrended := make([]float64, len(vals))
	avgVal := mean(vals)
	for i, v := range vals {
		detrended[i] = v - avgVal
	}

	var sumSquaredResiduals float64
	for _, d := range detrended {
		sumSquaredResiduals += d * d
	}

	var sumSquaredTotal float64
	for _, v := range vals {
		diff := v - avgVal
		sumSquaredTotal += diff * diff
	}

	var r2 float64
	if sumSquaredTotal > 0 {
		r2 = 1 - sumSquaredResiduals/sumSquaredTotal
	}

	trendComponent := math.Min(1.0, math.Abs(slope)*math.Sqrt(n)*5)
	consistencyComponent := math.Max(0, r2)

	return trendComponent*0.5 + consistencyComponent*0.5
}

func calculateNoiseLevel(vals []float64) float64 {
	if len(vals) < 5 {
		return 0
	}

	diffs := make([]float64, len(vals)-1)
	for i := 0; i < len(vals)-1; i++ {
		diffs[i] = math.Abs(vals[i+1] - vals[i])
	}

	sortedVals := make([]float64, len(vals))
	copy(sortedVals, vals)
	sort.Float64s(sortedVals)
	iqr := percentile(sortedVals, 0.75) - percentile(sortedVals, 0.25)

	avgDiff := mean(diffs)
	avgVal := mean(vals)

	if avgVal == 0 {
		return 0
	}

	noiseFromDiffs := math.Min(1.0, avgDiff/avgVal)

	noiseFromIQR := 0.0
	if avgVal > 0 {
		noiseFromIQR = math.Min(1.0, iqr/avgVal)
	}

	return (noiseFromDiffs + noiseFromIQR) / 2
}

func calculateAutocorrelation(vals []float64) float64 {
	if len(vals) < 10 {
		return 0
	}

	n := len(vals)
	avg := mean(vals)
	if avg == 0 {
		return 0
	}

	var denominator float64
	for i := 0; i < n; i++ {
		denominator += (vals[i] - avg) * (vals[i] - avg)
	}

	if denominator == 0 {
		return 0
	}

	var autocorr1, autocorr2, autocorr3 float64
	for i := 1; i < n; i++ {
		autocorr1 += (vals[i] - avg) * (vals[i-1] - avg)
	}
	for i := 2; i < n; i++ {
		autocorr2 += (vals[i] - avg) * (vals[i-2] - avg)
	}
	for i := 3; i < n; i++ {
		autocorr3 += (vals[i] - avg) * (vals[i-3] - avg)
	}

	autocorr1 /= denominator
	autocorr2 /= denominator
	autocorr3 /= denominator

	weightedAutocorr := (autocorr1*0.5 + autocorr2*0.3 + autocorr3*0.2)

	return math.Max(-1, math.Min(1, weightedAutocorr))
}

func detectChangePoints(data []TimeSeriesPoint) int {
	if len(data) < 30 {
		return 0
	}

	vals := make([]float64, len(data))
	for i, p := range data {
		vals[i] = p.Value
	}

	changePoints := 0
	windowSize := 15
	threshold := 3.5
	minGap := 20
	lastChangePoint := -minGap - 1

	for i := windowSize; i < len(vals)-windowSize; i++ {
		if i-lastChangePoint < minGap {
			continue
		}

		before := vals[i-windowSize : i]
		after := vals[i : i+windowSize]

		beforeMean := mean(before)
		afterMean := mean(after)
		beforeStd := stdDev(before)
		afterStd := stdDev(after)

		pooledStd := math.Sqrt((beforeStd*beforeStd + afterStd*afterStd) / 2)
		if pooledStd == 0 {
			continue
		}

		tStat := math.Abs(afterMean-beforeMean) / pooledStd / math.Sqrt(2.0/float64(windowSize))

		overallMean := mean(vals)
		if overallMean > 0 {
			relativeChange := math.Abs(afterMean-beforeMean) / overallMean
			if relativeChange < 0.2 {
				continue
			}
		}

		if tStat > threshold {
			changePoints++
			lastChangePoint = i
		}
	}

	return changePoints
}

func calculateAnomalyScore(vals []float64, profile *LoadProfile) float64 {
	if len(vals) < 10 || profile == nil {
		return 0
	}

	qps := profile.QPSProfile
	lat := profile.LatencyProfile

	score := 0.0

	if qps.CV > 0.6 {
		score += 0.25
	} else if qps.CV > 0.4 {
		score += 0.15
	}

	if qps.OutlierRatio > 0.15 {
		score += 0.20
	} else if qps.OutlierRatio > 0.08 {
		score += 0.10
	}

	if lat.P99toP50 > 8 {
		score += 0.25
	} else if lat.P99toP50 > 5 {
		score += 0.15
	}

	if lat.CV > 0.6 {
		score += 0.20
	} else if lat.CV > 0.4 {
		score += 0.10
	}

	if profile.Characteristics.ChangePoints > 5 {
		score += 0.10
	}

	return math.Min(1.0, score)
}

func calculateEMA(vals []float64, alpha float64) float64 {
	if len(vals) == 0 {
		return 0
	}

	ema := vals[0]
	for i := 1; i < len(vals); i++ {
		ema = alpha*vals[i] + (1-alpha)*ema
	}

	return ema
}

func calculateBurstiness(vals []float64) float64 {
	if len(vals) < 5 {
		return 0
	}

	avgVal := mean(vals)
	if avgVal == 0 {
		return 0
	}

	varianceRatio := variance(vals) / avgVal
	if varianceRatio < 0 {
		varianceRatio = 0
	}

	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	p90 := percentile(sorted, 0.90)
	p50 := percentile(sorted, 0.50)
	p10 := percentile(sorted, 0.10)

	var peakRatio float64
	if p50 > 0 {
		peakRatio = (p90 - p10) / p50
	}

	windowSize := 5
	if len(vals) < windowSize {
		windowSize = len(vals)
	}
	rollingVar := make([]float64, 0)
	for i := windowSize; i < len(vals); i++ {
		window := vals[i-windowSize : i]
		rollingVar = append(rollingVar, variance(window))
	}
	var variabilityOfVariability float64
	if len(rollingVar) > 1 {
		variabilityOfVariability = stdDev(rollingVar) / (mean(rollingVar) + 1e-10)
	}

	spikeCount := 0
	threshold := p90
	for _, v := range vals {
		if v > threshold {
			spikeCount++
		}
	}
	spikeFrequency := float64(spikeCount) / float64(len(vals))

	burstiness := 0.0
	burstiness += math.Min(1.0, varianceRatio/avgVal) * 0.25
	burstiness += math.Min(1.0, peakRatio/3.0) * 0.30
	burstiness += math.Min(1.0, variabilityOfVariability) * 0.25
	burstiness += spikeFrequency * 0.20

	return math.Min(1.0, burstiness)
}

func calculateTrendSlope(data []TimeSeriesPoint) float64 {
	if len(data) < 10 {
		return 0
	}

	n := float64(len(data))
	sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0

	for i, p := range data {
		x := float64(i)
		y := p.Value
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return 0
	}

	slope := (n*sumXY - sumX*sumY) / denominator

	meanY := sumY / n
	if meanY == 0 {
		return 0
	}

	return slope / meanY
}

func classifyStability(cv float64) string {
	if cv < 0.1 {
		return "very_stable"
	} else if cv < 0.25 {
		return "stable"
	} else if cv < 0.45 {
		return "moderate"
	} else if cv < 0.7 {
		return "variable"
	}
	return "highly_variable"
}

func classifyStabilityMulti(cv, trendStrength, autocorr float64, changePoints int, periodicityScore float64) string {
	stabilityScore := 0.0

	if cv < 0.1 {
		stabilityScore += 2.0
	} else if cv < 0.25 {
		stabilityScore += 1.5
	} else if cv < 0.45 {
		stabilityScore += 1.0
	} else if cv < 0.7 {
		stabilityScore += 0.5
	}

	if trendStrength < 0.1 {
		stabilityScore += 1.0
	} else if trendStrength < 0.3 {
		stabilityScore += 0.5
	}

	if autocorr > 0.7 {
		stabilityScore += 0.5
	}

	if changePoints == 0 {
		stabilityScore += 0.5
	} else if changePoints > 3 {
		stabilityScore -= 0.5
	}

	if periodicityScore > 0.5 {
		stabilityScore += 0.5
	} else if periodicityScore > 0.3 {
		stabilityScore += 0.25
	}

	if stabilityScore >= 3.5 {
		return "very_stable"
	} else if stabilityScore >= 2.5 {
		return "stable"
	} else if stabilityScore >= 1.5 {
		return "moderate"
	} else if stabilityScore >= 0.5 {
		return "variable"
	}
	return "highly_variable"
}

func classifyLoad(meanQPS float64) string {
	if meanQPS < 50 {
		return "very_light"
	} else if meanQPS < 200 {
		return "light"
	} else if meanQPS < 1000 {
		return "moderate"
	} else if meanQPS < 5000 {
		return "heavy"
	} else if meanQPS < 20000 {
		return "very_heavy"
	}
	return "extreme"
}

func classifyTraffic(profile *LoadProfile) string {
	pattern := profile.DailyPattern
	weekly := profile.WeeklyPattern
	char := profile.Characteristics
	qps := profile.QPSProfile

	hasDailyPattern := pattern.PeakToOffPeak > 1.4
	hasStrongDailyPattern := pattern.PeakToOffPeak > 2.0
	hasWeeklyPattern := math.Abs(weekly.WeekendDrop) > 0.15
	isBusinessHours := pattern.BusinessHours.Ratio > 1.15
	isHighlyVariable := qps.CV > 0.5
	isBursty := qps.PeakToAvg > 4 || char.Burstiness > 0.4
	isConstant := qps.CV < 0.15 && qps.PeakToAvg < 2
	hasMultimodal := char.Seasonality > 0.3 && pattern.PatternStrength > 0.2

	if hasStrongDailyPattern && hasWeeklyPattern && isBusinessHours {
		return "business_traffic"
	}

	if hasDailyPattern && hasWeeklyPattern {
		return "periodic_with_weekly_variation"
	}

	if hasStrongDailyPattern {
		return "daily_periodic"
	}

	if hasDailyPattern && !hasWeeklyPattern {
		return "daily_periodic"
	}

	if isConstant {
		return "constant"
	}

	if isBursty && isHighlyVariable {
		return "highly_bursty"
	}

	if isBursty {
		return "bursty"
	}

	if isHighlyVariable && char.Burstiness > 0.5 {
		return "unpredictable"
	}

	if hasMultimodal {
		return "multimodal"
	}

	if isHighlyVariable {
		return "variable"
	}

	return "mixed"
}

func percentile(sortedVals []float64, p float64) float64 {
	if len(sortedVals) == 0 {
		return 0
	}

	idx := p * float64(len(sortedVals)-1)
	lower := int(idx)
	upper := lower + 1

	if upper >= len(sortedVals) {
		return sortedVals[len(sortedVals)-1]
	}

	frac := idx - float64(lower)
	return sortedVals[lower] + frac*(sortedVals[upper]-sortedVals[lower])
}

func skewness(vals []float64) float64 {
	if len(vals) < 3 {
		return 0
	}

	m := mean(vals)
	s := stdDev(vals)

	if s == 0 {
		return 0
	}

	var sum float64
	for _, v := range vals {
		sum += math.Pow((v-m)/s, 3)
	}

	return sum / float64(len(vals))
}

func kurtosis(vals []float64) float64 {
	if len(vals) < 4 {
		return 0
	}

	m := mean(vals)
	s := stdDev(vals)

	if s == 0 {
		return 0
	}

	var sum float64
	for _, v := range vals {
		sum += math.Pow((v-m)/s, 4)
	}

	return sum/float64(len(vals)) - 3
}

func calculateEntropy(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}

	bins := 10
	minVal, maxVal := vals[0], vals[0]
	for _, v := range vals {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	if maxVal == minVal {
		return 0
	}

	counts := make([]int, bins)
	for _, v := range vals {
		idx := int((v - minVal) / (maxVal - minVal) * float64(bins-1))
		if idx >= bins {
			idx = bins - 1
		}
		counts[idx]++
	}

	entropy := 0.0
	n := float64(len(vals))
	for _, c := range counts {
		if c > 0 {
			p := float64(c) / n
			entropy -= p * math.Log2(p)
		}
	}

	return entropy / math.Log2(float64(bins))
}

func calculateOutlierRatio(sortedVals []float64, iqr float64) float64 {
	if len(sortedVals) == 0 || iqr == 0 {
		return 0
	}

	q1 := percentile(sortedVals, 0.25)
	q3 := percentile(sortedVals, 0.75)
	lowerBound := q1 - 1.5*iqr
	upperBound := q3 + 1.5*iqr

	outlierCount := 0
	for _, v := range sortedVals {
		if v < lowerBound || v > upperBound {
			outlierCount++
		}
	}

	return float64(outlierCount) / float64(len(sortedVals))
}

func PrintLoadProfile(profile *LoadProfile, jsonOutput bool) {
	if jsonOutput {
		fmt.Println(mustMarshalJSON(profile))
		return
	}

	fmt.Println(stringsRepeat("=", 60))
	fmt.Printf("LOAD PROFILE REPORT - %s\n", profile.ClusterID)
	fmt.Println(stringsRepeat("=", 60))
	fmt.Printf("Analysis Period: %.1f hours (%.1f days)\n", profile.DurationHours, profile.DurationHours/24)
	fmt.Printf("Data Points: %d\n\n", profile.Samples)

	printQPSProfile(profile)
	printLatencyProfile(profile)
	PrintDailyPattern(profile)
	PrintWeeklyPattern(profile)
	printCharacteristics(profile)
	if profile.Workload != nil {
		PrintWorkloadProfile(profile.Workload)
	}
	if profile.MultiDimension != nil {
		fmt.Println(FormatMultiDimensionProfile(profile.MultiDimension))
	}
	printSummary(profile)
}

func printQPSProfile(profile *LoadProfile) {
	qps := profile.QPSProfile

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("QPS PROFILE")
	fmt.Println(stringsRepeat("-", 60))

	trafficClass := classifyTrafficLevel(qps.Mean)

	fmt.Printf("  Mean:        %s QPS", formatNum(qps.Mean))
	if trafficClass == "very_low" || trafficClass == "low" {
		fmt.Printf(" (%s traffic)", trafficClass)
	}
	fmt.Println()

	fmt.Printf("  Median:      %s QPS\n", formatNum(qps.Median))
	fmt.Printf("  Std Dev:     %s QPS (CV: %.2f)\n", formatNum(qps.StdDev), qps.CV)
	fmt.Println()

	if qps.PeakToAvg < 10 || qps.Mean > 100 {
		fmt.Printf("  Min:         %s QPS\n", formatNum(qps.Min))
		fmt.Printf("  Max:         %s QPS\n", formatNum(qps.Max))
		fmt.Printf("  Peak/Avg:    %.2fx\n", qps.PeakToAvg)
		fmt.Println()
	} else {
		fmt.Printf("  Peak/Avg:    %.2fx (highly bursty)\n", qps.PeakToAvg)
		fmt.Println()
	}

	fmt.Printf("  P10:         %s QPS\n", formatNum(qps.P10))
	fmt.Printf("  P90:         %s QPS\n", formatNum(qps.P90))
	fmt.Printf("  P99:         %s QPS\n", formatNum(qps.P99))
	fmt.Println()

	if qps.Skewness > 1 {
		fmt.Println("  Distribution: Right-skewed (occasional traffic spikes)")
	} else if qps.Skewness < -1 {
		fmt.Println("  Distribution: Left-skewed (occasional traffic drops)")
	} else {
		fmt.Println("  Distribution: Approximately symmetric")
	}
	fmt.Println()
}

func printLatencyProfile(profile *LoadProfile) {
	lat := profile.LatencyProfile

	if lat.P50Ms == 0 && lat.P99Ms == 0 {
		return
	}

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("LATENCY PROFILE")
	fmt.Println(stringsRepeat("-", 60))
	fmt.Printf("  P50:         %.2f ms\n", lat.P50Ms)
	fmt.Printf("  P90:         %.2f ms\n", lat.P90Ms)
	fmt.Printf("  P99:         %.2f ms\n", lat.P99Ms)
	fmt.Printf("  Max:         %.2f ms\n", lat.MaxMs)
	fmt.Println()
	fmt.Printf("  P99/P50:     %.2fx\n", lat.P99toP50)

	if lat.P99toP50 > 10 {
		fmt.Println("  Tail Latency: HIGH - significant long-tail latency issues")
	} else if lat.P99toP50 > 5 {
		fmt.Println("  Tail Latency: MODERATE - some long-tail latency")
	} else {
		fmt.Println("  Tail Latency: LOW - consistent response times")
	}
	fmt.Println()
}

func PrintDailyPattern(profile *LoadProfile) {
	pattern := profile.DailyPattern

	isPeriodic := pattern.PeakToOffPeak > 1.5 || pattern.NightDrop > 0.15
	if !isPeriodic {
		return
	}

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("DAILY PATTERN")
	fmt.Println(stringsRepeat("-", 60))

	fmt.Println("\n  Hourly QPS Distribution (significant daily pattern detected):")
	fmt.Println()

	maxAvg := 0.0
	for _, v := range pattern.HourlyAvg {
		if v > maxAvg {
			maxAvg = v
		}
	}

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
		bar := ""
		for i := 0; i < bars; i++ {
			bar += "█"
		}
		for i := bars; i < barWidth; i++ {
			bar += "░"
		}

		fmt.Printf("  %s%02d:00 |%s| %s\n", marker, h, bar, formatNum(avg))
	}
	fmt.Println()
	fmt.Println("  Legend: ▲ Peak hour  ▼ Off-peak hour")

	fmt.Printf("\n  Peak Hours:      %v\n", formatHours(pattern.PeakHours))
	fmt.Printf("  Off-Peak Hours:  %v\n", formatHours(pattern.OffPeakHours))
	fmt.Printf("  Peak/Off-Peak:   %.2fx\n", pattern.PeakToOffPeak)

	if pattern.NightDrop > 0 {
		fmt.Printf("  Night Drop:      %.1f%% (%02d:00-%02d:00)\n",
			pattern.NightDrop*100,
			pattern.NightDropHours[0],
			pattern.NightDropHours[len(pattern.NightDropHours)-1])
	}

	if pattern.BusinessHours.Ratio > 0 {
		fmt.Printf("  Business Hours:  %02d:00-%02d:00 (%.1f%% above average)\n",
			pattern.BusinessHours.StartHour,
			pattern.BusinessHours.EndHour,
			(pattern.BusinessHours.Ratio-1)*100)
	}

	fmt.Printf("\n  Daily Pattern Strength: %.0f%% (strong periodic behavior)\n",
		(pattern.PeakToOffPeak-1)*50)

	if pattern.PatternStrength > 0 {
		fmt.Printf("  Pattern Consistency:    %.0f%%\n", pattern.ConsistencyScore*100)
		fmt.Printf("  Periodicity Score:      %.0f%%\n", pattern.PeriodicityScore*100)
	}
	fmt.Println()
}

func PrintWeeklyPattern(profile *LoadProfile) {
	weekly := profile.WeeklyPattern

	if len(weekly.DailyAvg) < 3 {
		return
	}

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("WEEKLY PATTERN")
	fmt.Println(stringsRepeat("-", 60))

	weekendDiff := weekly.WeekendDrop
	if weekendDiff < 0 {
		weekendDiff = -weekendDiff
	}
	isPeriodic := weekendDiff > 0.10

	fmt.Println("\n  Daily QPS Comparison:")
	fmt.Println()

	days := make([]string, 0, len(weekly.DailyAvg))
	for d := range weekly.DailyAvg {
		days = append(days, d)
	}
	sort.Strings(days)

	maxAvg := 0.0
	for _, avg := range weekly.DailyAvg {
		if avg > maxAvg {
			maxAvg = avg
		}
	}

	barWidth := 25
	for _, d := range days {
		t, _ := time.Parse("2006-01-02", d)
		dayName := t.Weekday().String()[:3]
		avg := weekly.DailyAvg[d]

		bars := int(avg / maxAvg * float64(barWidth))
		bar := ""
		for i := 0; i < bars; i++ {
			bar += "█"
		}
		for i := bars; i < barWidth; i++ {
			bar += "░"
		}

		marker := "  "
		weekday := t.Weekday()
		if weekday == time.Saturday || weekday == time.Sunday {
			marker = "W "
		}

		fmt.Printf("  %s%s (%s) |%s| %s\n", marker, d, dayName, bar, formatNum(avg))
	}
	fmt.Println()
	fmt.Println("  Legend: W = Weekend")

	if isPeriodic {
		fmt.Println("\n  Weekday vs Weekend Comparison:")

		weekdayMax := weekly.WeekdayAvg
		weekendMax := weekly.WeekendAvg
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

		weekdayBar := ""
		for i := 0; i < weekdayBars; i++ {
			weekdayBar += "█"
		}
		for i := weekdayBars; i < 25; i++ {
			weekdayBar += "░"
		}
		weekendBar := ""
		for i := 0; i < weekendBars; i++ {
			weekendBar += "█"
		}
		for i := weekendBars; i < 25; i++ {
			weekendBar += "░"
		}

		fmt.Printf("    Weekdays:  |%s| %s\n", weekdayBar, formatNum(weekdayMax))
		fmt.Printf("    Weekends:  |%s| %s\n", weekendBar, formatNum(weekendMax))
	}

	fmt.Println()
	fmt.Printf("  Weekday Average:  %s\n", formatNum(weekly.WeekdayAvg))
	fmt.Printf("  Weekend Average:  %s\n", formatNum(weekly.WeekendAvg))

	if weekly.WeekendDrop > 0 {
		fmt.Printf("  Weekend Drop:     %.1f%%\n", weekly.WeekendDrop*100)
	} else if weekly.WeekendDrop < 0 {
		fmt.Printf("  Weekend Increase: %.1f%%\n", -weekly.WeekendDrop*100)
	}

	if weekly.IsWeekdayHeavy {
		fmt.Println("  Pattern:          Weekday-heavy traffic")
	} else if weekly.WeekendDrop < 0 {
		fmt.Println("  Pattern:          Weekend-heavy traffic")
	} else {
		fmt.Println("  Pattern:          Similar weekday/weekend traffic")
	}

	if isPeriodic {
		dropPct := weekly.WeekendDrop
		if dropPct < 0 {
			dropPct = -dropPct
		}
		fmt.Printf("  Weekly Pattern Strength: %.0f%%\n", dropPct*100)
	}
	fmt.Println()
}

func printCharacteristics(profile *LoadProfile) {
	char := profile.Characteristics

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("TRAFFIC CHARACTERISTICS")
	fmt.Println(stringsRepeat("-", 60))
	fmt.Printf("  Burstiness:      %.2f (0=steady, 1=very bursty)\n", char.Burstiness)
	fmt.Printf("  Predictability:  %.2f (0=random, 1=predictable)\n", char.Predictability)

	trend := "stable"
	if char.IsGrowing {
		trend = "growing"
	} else if char.TrendSlope < -0.01 {
		trend = "declining"
	}
	fmt.Printf("  Trend:           %s (%.1f%%/period)\n", trend, char.TrendSlope*100)

	fmt.Println()
	fmt.Printf("  Stability Class: %s\n", char.StabilityClass)
	fmt.Printf("  Load Class:      %s\n", char.LoadClass)
	fmt.Printf("  Traffic Type:    %s\n", char.TrafficType)

	fmt.Println()
	fmt.Println("  Advanced Metrics:")
	fmt.Printf("  Seasonality:     %.2f\n", char.Seasonality)
	fmt.Printf("  Noise Level:     %.2f\n", char.NoiseLevel)
	fmt.Printf("  Autocorrelation: %.2f\n", char.Autocorrelation)
	fmt.Printf("  Change Points:   %d\n", char.ChangePoints)
	fmt.Printf("  EMA Trend:       %s\n", char.EMATrend)

	if char.AnomalyScore > 0 {
		fmt.Printf("  Anomaly Score:   %.2f\n", char.AnomalyScore)
	}
	fmt.Println()
}

func printSummary(profile *LoadProfile) {
	fmt.Println(stringsRepeat("=", 60))
	fmt.Println("SUMMARY")
	fmt.Println(stringsRepeat("=", 60))

	qps := profile.QPSProfile
	char := profile.Characteristics

	fmt.Printf(`
  Cluster:        %s
  Avg QPS:        %s (Peak: %s, %.1fx)
  Latency P99:    %.1fms

  Traffic Type:   %s
  Stability:      %s
  Load Level:     %s
`,
		profile.ClusterID,
		formatNum(qps.Mean), formatNum(qps.Max), qps.PeakToAvg,
		profile.LatencyProfile.P99Ms,
		char.TrafficType,
		char.StabilityClass,
		char.LoadClass,
	)

	fmt.Println("\n  Recommendations:")

	if qps.CV > 0.5 {
		fmt.Println("    - High QPS variability: consider implementing rate limiting or auto-scaling")
	}
	if profile.LatencyProfile.P99toP50 > 10 {
		fmt.Println("    - High tail latency: investigate slow queries or resource contention")
	}
	if char.Burstiness > 0.5 {
		fmt.Println("    - Bursty traffic: ensure adequate capacity headroom for peak handling")
	}
	if profile.DailyPattern.PeakToOffPeak > 3 {
		fmt.Println("    - Strong daily pattern: consider time-based auto-scaling")
	}
	if profile.WeeklyPattern.IsWeekdayHeavy {
		fmt.Println("    - Weekday-heavy: weekend maintenance windows may be appropriate")
	}
	if char.IsGrowing {
		fmt.Println("    - Growing trend: plan for capacity expansion")
	}
	if qps.Mean < 100 && profile.DurationHours > 24 {
		fmt.Println("    - Light traffic: consider cluster consolidation for cost optimization")
	}

	fmt.Println()
}

func formatNum(n float64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", n/1000000)
	} else if n >= 1000 {
		return fmt.Sprintf("%.1fK", n/1000)
	}
	return fmt.Sprintf("%.0f", n)
}

func formatHours(hours []int) string {
	if len(hours) == 0 {
		return "none"
	}

	sorted := make([]int, len(hours))
	copy(sorted, hours)
	sort.Ints(sorted)

	var ranges []string
	start := sorted[0]
	end := sorted[0]

	for i := 1; i < len(sorted); i++ {
		if sorted[i] == end+1 {
			end = sorted[i]
		} else {
			if start == end {
				ranges = append(ranges, fmt.Sprintf("%02d:00", start))
			} else {
				ranges = append(ranges, fmt.Sprintf("%02d:00-%02d:00", start, end))
			}
			start = sorted[i]
			end = sorted[i]
		}
	}

	if start == end {
		ranges = append(ranges, fmt.Sprintf("%02d:00", start))
	} else {
		ranges = append(ranges, fmt.Sprintf("%02d:00-%02d:00", start, end))
	}

	return strings.Join(ranges, ", ")
}

func stringsRepeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}

func mustMarshalJSON(v interface{}) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	return string(data)
}
