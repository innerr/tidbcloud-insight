// Package analysis provides comprehensive multi-dimension profiling for TiDB/TiKV clusters.
// This file implements the 6-dimension analysis framework that combines:
// 1. SQL Dimension - Fine-grained SQL type classification and resource consumption
// 2. TiKV Dimension - Request type analysis with latency profiling
// 3. Latency Dimension - Tail latency, periodicity, and distribution analysis
// 4. Balance Dimension - Cross-instance load balancing analysis
// 5. QPS Dimension - TiDB QPS pattern analysis with forecasting
// 6. TiKV Volume Dimension - Request volume analysis with bottleneck detection
package analysis

import (
	"fmt"
	"math"
	"sort"
)

// MultiDimensionProfile represents a comprehensive analysis across 6 dimensions.
// It provides holistic insights by correlating metrics from different dimensions
// to identify bottlenecks, predict issues, and generate recommendations.
type MultiDimensionProfile struct {
	ClusterID              string                     `json:"cluster_id"`
	DurationHours          float64                    `json:"duration_hours"`
	Samples                int                        `json:"samples"`
	SQLDimension           SQLDimensionProfile        `json:"sql_dimension"`
	TiKVDimension          TiKVDimensionProfile       `json:"tikv_dimension"`
	LatencyDimension       LatencyDimensionProfile    `json:"latency_dimension"`
	BalanceDimension       BalanceDimensionProfile    `json:"balance_dimension"`
	QPSDimension           QPSDimensionProfile        `json:"qps_dimension"`
	TiKVVolumeDimension    TiKVVolumeDimensionProfile `json:"tikv_volume_dimension"`
	CrossDimensionInsights CrossDimensionInsights     `json:"cross_dimension_insights"`
	OverallHealthScore     float64                    `json:"overall_health_score"`
	OverallRiskLevel       string                     `json:"overall_risk_level"`
	TopRecommendations     []string                   `json:"top_recommendations"`
}

// SQLDimensionProfile provides fine-grained analysis of SQL query patterns.
// It classifies queries by type (DDL/DML/DQL/TCL), identifies resource hotspots,
// and measures workload characteristics like transactional vs analytical intensity.
type SQLDimensionProfile struct {
	TypeDistribution       map[string]SQLTypeDetail `json:"type_distribution"`
	DDLRatio               float64                  `json:"ddl_ratio"`
	DMLRatio               float64                  `json:"dml_ratio"`
	DQLRatio               float64                  `json:"dql_ratio"`
	TCLRatio               float64                  `json:"tcl_ratio"`
	ResourceHotspotScore   float64                  `json:"resource_hotspot_score"`
	DominantTypes          []string                 `json:"dominant_types"`
	ComplexityIndex        float64                  `json:"complexity_index"`
	TransactionalIntensity float64                  `json:"transactional_intensity"`
	AnalyticalIntensity    float64                  `json:"analytical_intensity"`
	BatchOperationRatio    float64                  `json:"batch_operation_ratio"`
	InteractiveRatio       float64                  `json:"interactive_ratio"`
	PatternStability       float64                  `json:"pattern_stability"`
	AnomalyTypes           []string                 `json:"anomaly_types"`
}

// SQLTypeDetail contains detailed metrics for a specific SQL type.
type SQLTypeDetail struct {
	Count              float64 `json:"count"`
	Percent            float64 `json:"percent"`
	MeanLatencyMs      float64 `json:"mean_latency_ms"`
	P99LatencyMs       float64 `json:"p99_latency_ms"`
	ResourceScore      float64 `json:"resource_score"`
	FrequencyPerSecond float64 `json:"frequency_per_second"`
	PeriodicityScore   float64 `json:"periodicity_score"`
}

// TiKVDimensionProfile analyzes TiKV operation patterns and performance.
// It categorizes operations by type (read/write/transaction/coprocessor),
// identifies bottlenecks, and measures write amplification and balance.
type TiKVDimensionProfile struct {
	OpDistribution       map[string]TiKVOpDetail `json:"op_distribution"`
	ReadOpRatio          float64                 `json:"read_op_ratio"`
	WriteOpRatio         float64                 `json:"write_op_ratio"`
	TransactionOpRatio   float64                 `json:"transaction_op_ratio"`
	CoprocessorRatio     float64                 `json:"coprocessor_ratio"`
	HotOpTypes           []string                `json:"hot_op_types"`
	AvgLatencyMs         float64                 `json:"avg_latency_ms"`
	P99LatencyMs         float64                 `json:"p99_latency_ms"`
	LatencyVariability   float64                 `json:"latency_variability"`
	BottleneckIndicator  string                  `json:"bottleneck_indicator"`
	BottleneckScore      float64                 `json:"bottleneck_score"`
	WriteAmplification   float64                 `json:"write_amplification"`
	ReadWriteBalance     float64                 `json:"read_write_balance"`
	TransactionIntensity float64                 `json:"transaction_intensity"`
	ScanOperationRatio   float64                 `json:"scan_operation_ratio"`
	PointLookupRatio     float64                 `json:"point_lookup_ratio"`
}

// TiKVOpDetail contains detailed metrics for a specific TiKV operation type.
type TiKVOpDetail struct {
	Count           float64 `json:"count"`
	Percent         float64 `json:"percent"`
	MeanLatencyMs   float64 `json:"mean_latency_ms"`
	P99LatencyMs    float64 `json:"p99_latency_ms"`
	OperationPerSec float64 `json:"operation_per_sec"`
	LatencyCV       float64 `json:"latency_cv"`
}

// LatencyDimensionProfile provides comprehensive latency analysis including
// tail latency, periodicity, distribution, and correlation metrics.
type LatencyDimensionProfile struct {
	TiDBLatency         LatencyDetailProfile `json:"tidb_latency"`
	TiKVLatency         LatencyDetailProfile `json:"tikv_latency"`
	TailLatencyRatio    float64              `json:"tail_latency_ratio"`
	LatencyCorrelation  float64              `json:"latency_correlation"`
	SpikeCount          int                  `json:"spike_count"`
	SpikePattern        string               `json:"spike_pattern"`
	PeriodicLatency     bool                 `json:"periodic_latency"`
	LatencyTrend        string               `json:"latency_trend"`
	P99Stability        float64              `json:"p99_stability"`
	LongTailIndicator   string               `json:"long_tail_indicator"`
	HourlyLatencyMap    map[int]float64      `json:"hourly_latency_map"`
	BusinessHourLatency float64              `json:"business_hour_latency"`
	OffPeakLatency      float64              `json:"off_peak_latency"`
}

// LatencyDetailProfile contains detailed latency distribution metrics
// including percentiles, skewness, kurtosis, and bucket distribution.
type LatencyDetailProfile struct {
	P50Ms              float64         `json:"p50_ms"`
	P90Ms              float64         `json:"p90_ms"`
	P99Ms              float64         `json:"p99_ms"`
	P999Ms             float64         `json:"p99_9_ms"`
	MaxMs              float64         `json:"max_ms"`
	MeanMs             float64         `json:"mean_ms"`
	StdDevMs           float64         `json:"std_dev_ms"`
	CV                 float64         `json:"cv"`
	Skewness           float64         `json:"skewness"`
	Kurtosis           float64         `json:"kurtosis"`
	BucketDistribution map[float64]int `json:"bucket_distribution"`
	OutlierRatio       float64         `json:"outlier_ratio"`
}

// BalanceDimensionProfile analyzes load distribution across instances.
// It identifies hot/cold instances, calculates imbalance scores, and
// provides recommendations for load balancing.
type BalanceDimensionProfile struct {
	TiDBBalance           InstanceBalanceDetail `json:"tidb_balance"`
	TiKVBalance           InstanceBalanceDetail `json:"tikv_balance"`
	CrossComponentBalance float64               `json:"cross_component_balance"`
	OverallImbalanceScore float64               `json:"overall_imbalance_score"`
	HotInstances          []string              `json:"hot_instances"`
	ColdInstances         []string              `json:"cold_instances"`
	HotspotRiskLevel      string                `json:"hotspot_risk_level"`
	LoadDistributionCV    float64               `json:"load_distribution_cv"`
	QPSImbalanceRatio     float64               `json:"qps_imbalance_ratio"`
	LatencyImbalanceRatio float64               `json:"latency_imbalance_ratio"`
	Recommendation        string                `json:"recommendation"`
}

// InstanceBalanceDetail provides per-instance balance metrics.
type InstanceBalanceDetail struct {
	InstanceCount          int                `json:"instance_count"`
	QPSDistribution        map[string]float64 `json:"qps_distribution"`
	LatencyDistribution    map[string]float64 `json:"latency_distribution"`
	QPSSkewCoefficient     float64            `json:"qps_skew_coefficient"`
	LatencySkewCoefficient float64            `json:"latency_skew_coefficient"`
	HotInstances           []string           `json:"hot_instances"`
	ColdInstances          []string           `json:"cold_instances"`
	MaxMinRatio            float64            `json:"max_min_ratio"`
	BalanceScore           float64            `json:"balance_score"`
}

type QPSDimensionProfile struct {
	MeanQPS               float64 `json:"mean_qps"`
	PeakQPS               float64 `json:"peak_qps"`
	MinQPS                float64 `json:"min_qps"`
	PeakToAvgRatio        float64 `json:"peak_to_avg_ratio"`
	CV                    float64 `json:"cv"`
	Burstiness            float64 `json:"burstiness"`
	DailyPatternStrength  float64 `json:"daily_pattern_strength"`
	WeeklyPatternStrength float64 `json:"weekly_pattern_strength"`
	HasSeasonality        bool    `json:"has_seasonality"`
	TrendDirection        string  `json:"trend_direction"`
	TrendStrength         float64 `json:"trend_strength"`
	ForecastNext24H       float64 `json:"forecast_next_24h"`
	ForecastConfidence    float64 `json:"forecast_confidence"`
	PeakHours             []int   `json:"peak_hours"`
	BusinessHourQPS       float64 `json:"business_hour_qps"`
	OffPeakQPS            float64 `json:"off_peak_qps"`
	QPSPattern            string  `json:"qps_pattern"`
	CriticalLoadIndicator string  `json:"critical_load_indicator"`
}

type TiKVVolumeDimensionProfile struct {
	TotalRequests      float64 `json:"total_requests"`
	MeanRPS            float64 `json:"mean_rps"`
	PeakRPS            float64 `json:"peak_rps"`
	PeakToAvgRatio     float64 `json:"peak_to_avg_ratio"`
	ReadRPS            float64 `json:"read_rps"`
	WriteRPS           float64 `json:"write_rps"`
	ReadWriteRatio     float64 `json:"read_write_ratio"`
	WriteAmplification float64 `json:"write_amplification"`
	BottleneckType     string  `json:"bottleneck_type"`
	BottleneckScore    float64 `json:"bottleneck_score"`
	HotRegionIndicator bool    `json:"hot_region_indicator"`
	VolumeTrend        string  `json:"volume_trend"`
	TransactionVolume  float64 `json:"transaction_volume"`
	CoprocessorVolume  float64 `json:"coprocessor_volume"`
	DailyVolumePattern float64 `json:"daily_volume_pattern"`
	SaturationRisk     string  `json:"saturation_risk"`
}

type CrossDimensionInsights struct {
	SQLToLatencyCorr           float64  `json:"sql_to_latency_corr"`
	TiKVToLatencyCorr          float64  `json:"tikv_to_latency_corr"`
	QPSToLatencyCorr           float64  `json:"qps_to_latency_corr"`
	VolumeToLatencyCorr        float64  `json:"volume_to_latency_corr"`
	BalanceToPerformance       float64  `json:"balance_to_performance"`
	DominantBottleneck         string   `json:"dominant_bottleneck"`
	SecondaryBottleneck        string   `json:"secondary_bottleneck"`
	ResourceEfficiency         float64  `json:"resource_efficiency"`
	OptimizationPriority       []string `json:"optimization_priority"`
	CrossCorrelationScore      float64  `json:"cross_correlation_score"`
	AnomalyIndicators          []string `json:"anomaly_indicators"`
	PerformanceDegradationRisk string   `json:"performance_degradation_risk"`
}

// AnalyzeMultiDimensionProfile performs comprehensive 6-dimension analysis on TiDB/TiKV metrics.
// This is the main entry point for multi-dimension profiling that correlates data from:
// - SQL query patterns (DDL/DML/DQL/TCL classification)
// - TiKV operations (read/write/transaction patterns)
// - Latency distribution (tail latency, periodicity)
// - Instance balance (load distribution across nodes)
// - QPS patterns (burstiness, trends, forecasting)
// - TiKV volume (request rates, bottlenecks)
//
// Parameters:
//   - clusterID: Unique identifier for the cluster
//   - qpsData: Time series of QPS measurements
//   - latencyData: Time series of latency measurements
//   - sqlTypeData: SQL type counts grouped by SQL type
//   - sqlLatencyData: Latency measurements grouped by SQL type
//   - tikvOpData: TiKV operation counts grouped by operation type
//   - tikvLatencyData: TiKV latency measurements grouped by operation type
//   - tidbInstanceQPS: QPS data grouped by TiDB instance
//   - tidbInstanceLatency: Latency data grouped by TiDB instance
//   - tikvInstanceQPS: QPS data grouped by TiKV instance
//   - tikvInstanceLatency: Latency data grouped by TiKV instance
//
// Returns a MultiDimensionProfile with comprehensive analysis across all dimensions,
// including cross-dimension correlations, health scores, and recommendations.
func AnalyzeMultiDimensionProfile(
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
) *MultiDimensionProfile {
	if len(qpsData) == 0 {
		return nil
	}

	profile := &MultiDimensionProfile{
		ClusterID: clusterID,
	}

	sort.Slice(qpsData, func(i, j int) bool {
		return qpsData[i].Timestamp < qpsData[j].Timestamp
	})

	if len(qpsData) > 0 {
		first := qpsData[0].Timestamp
		last := qpsData[len(qpsData)-1].Timestamp
		profile.DurationHours = float64(last-first) / 3600000
		profile.Samples = len(qpsData)
	}

	profile.SQLDimension = analyzeSQLDimension(sqlTypeData, sqlLatencyData, profile.DurationHours)
	profile.TiKVDimension = analyzeTiKVDimension(tikvOpData, tikvLatencyData, profile.DurationHours)
	profile.LatencyDimension = analyzeLatencyDimension(latencyData, sqlLatencyData, tikvLatencyData)
	profile.BalanceDimension = analyzeBalanceDimension(tidbInstanceQPS, tidbInstanceLatency, tikvInstanceQPS, tikvInstanceLatency)
	profile.QPSDimension = analyzeQPSDimension(qpsData)
	profile.TiKVVolumeDimension = analyzeTiKVVolumeDimension(tikvOpData, profile.DurationHours)

	profile.CrossDimensionInsights = analyzeCrossDimensionInsights(profile)

	profile.OverallHealthScore = calculateOverallHealthScore(profile)
	profile.OverallRiskLevel = classifyOverallRisk(profile.OverallHealthScore)
	profile.TopRecommendations = generateTopRecommendations(profile)

	return profile
}

func analyzeSQLDimension(sqlTypeData map[string][]TimeSeriesPoint, sqlLatencyData map[string][]TimeSeriesPoint, durationHours float64) SQLDimensionProfile {
	profile := SQLDimensionProfile{
		TypeDistribution: make(map[string]SQLTypeDetail),
	}

	if len(sqlTypeData) == 0 {
		return profile
	}

	var totalCount float64
	for _, data := range sqlTypeData {
		if len(data) > 0 {
			last := data[len(data)-1].Value
			first := data[0].Value
			totalCount += (last - first)
		}
	}

	if totalCount == 0 {
		return profile
	}

	var ddlCount, dmlCount, dqlCount, tclCount float64
	var transactionalCount, analyticalCount, batchCount, interactiveCount float64

	for sqlType, data := range sqlTypeData {
		if len(data) == 0 {
			continue
		}

		detail := SQLTypeDetail{}
		last := data[len(data)-1].Value
		first := data[0].Value
		detail.Count = last - first
		if detail.Count < 0 {
			detail.Count = 0
		}
		detail.Percent = detail.Count / totalCount * 100

		if durationHours > 0 {
			detail.FrequencyPerSecond = detail.Count / (durationHours * 3600)
		}

		if latencyData, ok := sqlLatencyData[sqlType]; ok && len(latencyData) > 0 {
			vals := extractValues(latencyData)
			detail.MeanLatencyMs = mean(vals) * 1000
			sorted := make([]float64, len(vals))
			copy(sorted, vals)
			sort.Float64s(sorted)
			detail.P99LatencyMs = percentile(sorted, 0.99) * 1000
		}

		detail.ResourceScore = calculateSQLResourceScore(sqlType, detail.Count, detail.MeanLatencyMs)
		profile.TypeDistribution[sqlType] = detail

		typeCategory := classifySQLType(sqlType)
		switch typeCategory {
		case "DDL":
			ddlCount += detail.Count
		case "DML":
			dmlCount += detail.Count
		case "DQL":
			dqlCount += detail.Count
		case "TCL":
			tclCount += detail.Count
		}

		pattern := classifySQLPattern(sqlType)
		switch pattern {
		case "transactional":
			transactionalCount += detail.Count
		case "analytical":
			analyticalCount += detail.Count
		case "batch":
			batchCount += detail.Count
		case "interactive":
			interactiveCount += detail.Count
		}
	}

	profile.DDLRatio = ddlCount / totalCount
	profile.DMLRatio = dmlCount / totalCount
	profile.DQLRatio = dqlCount / totalCount
	profile.TCLRatio = tclCount / totalCount
	profile.TransactionalIntensity = transactionalCount / totalCount
	profile.AnalyticalIntensity = analyticalCount / totalCount
	profile.BatchOperationRatio = batchCount / totalCount
	profile.InteractiveRatio = interactiveCount / totalCount

	profile.DominantTypes = findDominantTypes(profile.TypeDistribution, 3)
	profile.ResourceHotspotScore = calculateResourceHotspotScore(profile.TypeDistribution)
	profile.ComplexityIndex = calculateComplexityIndex(profile)
	profile.PatternStability = calculatePatternStability(sqlTypeData)
	profile.AnomalyTypes = detectSQLAnomalies(profile)

	return profile
}

func analyzeTiKVDimension(tikvOpData map[string][]TimeSeriesPoint, tikvLatencyData map[string][]TimeSeriesPoint, durationHours float64) TiKVDimensionProfile {
	profile := TiKVDimensionProfile{
		OpDistribution: make(map[string]TiKVOpDetail),
	}

	if len(tikvOpData) == 0 {
		return profile
	}

	var totalCount float64
	var readCount, writeCount, txnCount, copCount float64
	var totalLatency float64
	var latencyCount float64

	for op, data := range tikvOpData {
		if len(data) == 0 {
			continue
		}

		detail := TiKVOpDetail{}
		last := data[len(data)-1].Value
		first := data[0].Value
		detail.Count = last - first
		if detail.Count < 0 {
			detail.Count = 0
		}
		totalCount += detail.Count

		if durationHours > 0 {
			detail.OperationPerSec = detail.Count / (durationHours * 3600)
		}

		if latencyData, ok := tikvLatencyData[op]; ok && len(latencyData) > 0 {
			vals := extractValues(latencyData)
			detail.MeanLatencyMs = mean(vals) * 1000
			sorted := make([]float64, len(vals))
			copy(sorted, vals)
			sort.Float64s(sorted)
			detail.P99LatencyMs = percentile(sorted, 0.99) * 1000
			if detail.MeanLatencyMs > 0 {
				detail.LatencyCV = stdDev(vals) / mean(vals)
			}
			totalLatency += detail.MeanLatencyMs * detail.Count
			latencyCount += detail.Count
		}

		profile.OpDistribution[op] = detail

		opCategory := classifyTiKVOp(op)
		switch opCategory {
		case "read":
			readCount += detail.Count
		case "write":
			writeCount += detail.Count
		case "transaction":
			txnCount += detail.Count
		case "coprocessor":
			copCount += detail.Count
		}
	}

	if totalCount > 0 {
		profile.ReadOpRatio = readCount / totalCount
		profile.WriteOpRatio = writeCount / totalCount
		profile.TransactionOpRatio = txnCount / totalCount
		profile.CoprocessorRatio = copCount / totalCount
		profile.PointLookupRatio = readCount / totalCount
		profile.ScanOperationRatio = copCount / totalCount
	}

	if latencyCount > 0 {
		profile.AvgLatencyMs = totalLatency / latencyCount
	}

	profile.HotOpTypes = findHotOperations(profile.OpDistribution)
	profile.LatencyVariability = calculateLatencyVariability(profile.OpDistribution)
	profile.BottleneckIndicator = identifyTiKVBottleneck(profile)
	profile.WriteAmplification = calculateTiKVWriteAmplification(readCount, writeCount)
	profile.ReadWriteBalance = calculateReadWriteBalance(readCount, writeCount)
	profile.TransactionIntensity = txnCount / totalCount

	return profile
}

func analyzeLatencyDimension(latencyData []TimeSeriesPoint, sqlLatencyData map[string][]TimeSeriesPoint, tikvLatencyData map[string][]TimeSeriesPoint) LatencyDimensionProfile {
	profile := LatencyDimensionProfile{
		TiDBLatency:      LatencyDetailProfile{BucketDistribution: make(map[float64]int)},
		TiKVLatency:      LatencyDetailProfile{BucketDistribution: make(map[float64]int)},
		HourlyLatencyMap: make(map[int]float64),
	}

	if len(latencyData) > 0 {
		profile.TiDBLatency = analyzeLatencyDetail(latencyData)
		profile.HourlyLatencyMap = analyzeHourlyLatency(latencyData)
		profile.BusinessHourLatency = calculateBusinessHourLatency(profile.HourlyLatencyMap)
		profile.OffPeakLatency = calculateOffPeakLatency(profile.HourlyLatencyMap)
	}

	var allTiKVLatency []TimeSeriesPoint
	for _, data := range tikvLatencyData {
		allTiKVLatency = append(allTiKVLatency, data...)
	}
	if len(allTiKVLatency) > 0 {
		profile.TiKVLatency = analyzeLatencyDetail(allTiKVLatency)
	}

	if profile.TiDBLatency.P99Ms > 0 && profile.TiDBLatency.P50Ms > 0 {
		profile.TailLatencyRatio = profile.TiDBLatency.P99Ms / profile.TiDBLatency.P50Ms
	}

	profile.SpikeCount = detectLatencySpikes(latencyData)
	profile.SpikePattern = classifySpikePattern(profile.SpikeCount, profile.TiDBLatency.CV)
	profile.PeriodicLatency = detectPeriodicLatency(profile.HourlyLatencyMap)
	profile.LatencyTrend = analyzeLatencyTrend(latencyData)
	profile.P99Stability = calculateP99Stability(latencyData)
	profile.LongTailIndicator = classifyLongTail(profile.TailLatencyRatio, profile.TiDBLatency.Kurtosis)

	return profile
}

func analyzeLatencyDetail(data []TimeSeriesPoint) LatencyDetailProfile {
	profile := LatencyDetailProfile{
		BucketDistribution: make(map[float64]int),
	}

	if len(data) == 0 {
		return profile
	}

	vals := extractValues(data)
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	for i := range vals {
		vals[i] *= 1000
	}
	for i := range sorted {
		sorted[i] *= 1000
	}

	profile.P50Ms = percentile(sorted, 0.50)
	profile.P90Ms = percentile(sorted, 0.90)
	profile.P99Ms = percentile(sorted, 0.99)
	profile.P999Ms = percentile(sorted, 0.999)
	profile.MaxMs = sorted[len(sorted)-1]
	profile.MeanMs = mean(vals)
	profile.StdDevMs = stdDev(vals)

	if profile.MeanMs > 0 {
		profile.CV = profile.StdDevMs / profile.MeanMs
	}

	profile.Skewness = skewness(vals)
	profile.Kurtosis = kurtosis(vals)
	profile.BucketDistribution = createLatencyBuckets(sorted)
	profile.OutlierRatio = calculateOutlierRatio(sorted, profile.P99Ms)

	return profile
}

func analyzeBalanceDimension(tidbQPS, tidbLatency, tikvQPS, tikvLatency map[string][]TimeSeriesPoint) BalanceDimensionProfile {
	profile := BalanceDimensionProfile{}

	profile.TiDBBalance = analyzeInstanceBalance(tidbQPS, tidbLatency)
	profile.TiKVBalance = analyzeInstanceBalance(tikvQPS, tikvLatency)

	var imbalanceScores []float64
	if profile.TiDBBalance.InstanceCount > 0 {
		imbalanceScores = append(imbalanceScores, profile.TiDBBalance.QPSSkewCoefficient)
	}
	if profile.TiKVBalance.InstanceCount > 0 {
		imbalanceScores = append(imbalanceScores, profile.TiKVBalance.QPSSkewCoefficient)
	}

	if len(imbalanceScores) > 0 {
		profile.OverallImbalanceScore = mean(imbalanceScores)
	}

	profile.HotInstances = append(profile.TiDBBalance.HotInstances, profile.TiKVBalance.HotInstances...)
	profile.ColdInstances = append(profile.TiDBBalance.ColdInstances, profile.TiKVBalance.ColdInstances...)

	profile.HotspotRiskLevel = classifyHotspotRisk(profile.OverallImbalanceScore)
	profile.LoadDistributionCV = calculateLoadDistributionCV(tidbQPS, tikvQPS)

	if profile.TiDBBalance.MaxMinRatio > 0 {
		profile.QPSImbalanceRatio = profile.TiDBBalance.MaxMinRatio - 1
	}
	if profile.TiDBBalance.LatencySkewCoefficient > 0 {
		profile.LatencyImbalanceRatio = profile.TiDBBalance.LatencySkewCoefficient
	}

	profile.Recommendation = generateBalanceRecommendation(profile)

	return profile
}

func analyzeInstanceBalance(qpsData, latencyData map[string][]TimeSeriesPoint) InstanceBalanceDetail {
	detail := InstanceBalanceDetail{
		QPSDistribution:     make(map[string]float64),
		LatencyDistribution: make(map[string]float64),
	}

	detail.InstanceCount = len(qpsData)
	if detail.InstanceCount == 0 {
		return detail
	}

	var qpsMeans []float64
	for instance, data := range qpsData {
		if len(data) == 0 {
			continue
		}
		vals := extractValues(data)
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
			if minQPS > 0 {
				detail.MaxMinRatio = maxQPS / minQPS
			}

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
	for instance, data := range latencyData {
		if len(data) == 0 {
			continue
		}
		vals := extractValues(data)
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

	detail.BalanceScore = 1.0 - math.Min(1.0, detail.QPSSkewCoefficient)

	return detail
}

func analyzeQPSDimension(qpsData []TimeSeriesPoint) QPSDimensionProfile {
	profile := QPSDimensionProfile{}

	if len(qpsData) == 0 {
		return profile
	}

	vals := extractValues(qpsData)
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	profile.MeanQPS = mean(vals)
	profile.PeakQPS = sorted[len(sorted)-1]
	profile.MinQPS = sorted[0]

	if profile.MeanQPS > 0 {
		profile.PeakToAvgRatio = profile.PeakQPS / profile.MeanQPS
		profile.CV = stdDev(vals) / profile.MeanQPS
	}

	profile.Burstiness = calculateBurstiness(vals)

	dailyPattern := analyzeDailyPattern(qpsData)
	profile.DailyPatternStrength = dailyPattern.PatternStrength
	profile.PeakHours = dailyPattern.PeakHours
	profile.BusinessHourQPS = dailyPattern.BusinessHours.AvgQPS

	weeklyPattern := analyzeWeeklyPattern(qpsData)
	profile.WeeklyPatternStrength = weeklyPattern.PatternStrength

	profile.HasSeasonality = profile.DailyPatternStrength > 0.3 || profile.WeeklyPatternStrength > 0.2

	trend := analyzeTrend(qpsData)
	profile.TrendDirection = trend.TrendDirection
	profile.TrendStrength = calculateTrendStrength(qpsData, trend.RecentTrendSlope)
	profile.ForecastNext24H = forecastQPS(vals, 24)
	profile.ForecastConfidence = calculateForecastConfidence(profile.CV, profile.Burstiness)

	profile.OffPeakQPS = calculateOffPeakQPS(dailyPattern)
	profile.QPSPattern = classifyQPSPattern(profile)
	profile.CriticalLoadIndicator = classifyCriticalLoad(profile)

	return profile
}

func analyzeTiKVVolumeDimension(tikvOpData map[string][]TimeSeriesPoint, durationHours float64) TiKVVolumeDimensionProfile {
	profile := TiKVVolumeDimensionProfile{}

	if len(tikvOpData) == 0 {
		return profile
	}

	var readOps, writeOps, txnOps, copOps []float64
	var totalOps float64
	var allRates []float64

	for op, data := range tikvOpData {
		if len(data) < 2 {
			continue
		}

		for i := 1; i < len(data); i++ {
			rate := (data[i].Value - data[i-1].Value) / 200
			if rate > 0 {
				allRates = append(allRates, rate)
				totalOps += rate

				category := classifyTiKVOp(op)
				switch category {
				case "read":
					readOps = append(readOps, rate)
				case "write":
					writeOps = append(writeOps, rate)
				case "transaction":
					txnOps = append(txnOps, rate)
				case "coprocessor":
					copOps = append(copOps, rate)
				}
			}
		}
	}

	profile.TotalRequests = totalOps
	if len(allRates) > 0 {
		profile.MeanRPS = mean(allRates)
		sort.Float64s(allRates)
		profile.PeakRPS = percentile(allRates, 0.99)
		if profile.MeanRPS > 0 {
			profile.PeakToAvgRatio = profile.PeakRPS / profile.MeanRPS
		}
	}

	if len(readOps) > 0 {
		profile.ReadRPS = mean(readOps)
	}
	if len(writeOps) > 0 {
		profile.WriteRPS = mean(writeOps)
	}
	if profile.WriteRPS > 0 {
		profile.ReadWriteRatio = profile.ReadRPS / profile.WriteRPS
	}

	profile.BottleneckType = identifyVolumeBottleneck(profile)
	profile.BottleneckScore = calculateBottleneckScore(profile)
	profile.HotRegionIndicator = profile.PeakToAvgRatio > 3
	profile.VolumeTrend = analyzeVolumeTrend(allRates)
	profile.TransactionVolume = sum(txnOps)
	profile.CoprocessorVolume = sum(copOps)
	profile.DailyVolumePattern = calculateDailyVolumePattern(tikvOpData)
	profile.SaturationRisk = classifySaturationRisk(profile)

	return profile
}

func analyzeCrossDimensionInsights(profile *MultiDimensionProfile) CrossDimensionInsights {
	insights := CrossDimensionInsights{}

	insights.SQLToLatencyCorr = correlateSQLToLatency(profile.SQLDimension, profile.LatencyDimension)
	insights.TiKVToLatencyCorr = correlateTiKVToLatency(profile.TiKVDimension, profile.LatencyDimension)
	insights.QPSToLatencyCorr = correlateQPSToLatency(profile.QPSDimension, profile.LatencyDimension)
	insights.VolumeToLatencyCorr = correlateVolumeToLatency(profile.TiKVVolumeDimension, profile.LatencyDimension)
	insights.BalanceToPerformance = correlateBalanceToPerformance(profile.BalanceDimension, profile.LatencyDimension)

	insights.DominantBottleneck = identifyDominantBottleneck(profile)
	insights.SecondaryBottleneck = identifySecondaryBottleneck(profile)

	insights.ResourceEfficiency = calculateResourceEfficiency(profile)
	insights.OptimizationPriority = prioritizeOptimizations(profile)
	insights.CrossCorrelationScore = calculateCrossCorrelationScore(insights)
	insights.AnomalyIndicators = detectCrossDimensionAnomalies(profile)
	insights.PerformanceDegradationRisk = assessDegradationRisk(profile)

	return insights
}

func calculateOverallHealthScore(profile *MultiDimensionProfile) float64 {
	score := 100.0

	if profile.LatencyDimension.TiDBLatency.P99Ms > 500 {
		score -= 15
	} else if profile.LatencyDimension.TiDBLatency.P99Ms > 200 {
		score -= 8
	}

	if profile.LatencyDimension.TailLatencyRatio > 10 {
		score -= 10
	} else if profile.LatencyDimension.TailLatencyRatio > 5 {
		score -= 5
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.5 {
		score -= 12
	} else if profile.BalanceDimension.OverallImbalanceScore > 0.3 {
		score -= 6
	}

	if profile.QPSDimension.CV > 0.8 {
		score -= 8
	} else if profile.QPSDimension.CV > 0.5 {
		score -= 4
	}

	if profile.TiKVDimension.BottleneckIndicator != "none" {
		score -= 8
	}

	if profile.TiKVVolumeDimension.SaturationRisk == "high" {
		score -= 10
	} else if profile.TiKVVolumeDimension.SaturationRisk == "medium" {
		score -= 5
	}

	if profile.SQLDimension.ResourceHotspotScore > 0.7 {
		score -= 8
	}

	if profile.LatencyDimension.SpikeCount > 10 {
		score -= 6
	}

	if profile.CrossDimensionInsights.CrossCorrelationScore > 0.6 {
		score += 5
	}

	return math.Max(0, math.Min(100, score))
}

func classifyOverallRisk(score float64) string {
	if score >= 80 {
		return "low"
	} else if score >= 60 {
		return "medium"
	} else if score >= 40 {
		return "high"
	}
	return "critical"
}

func generateTopRecommendations(profile *MultiDimensionProfile) []string {
	var recommendations []string

	if profile.LatencyDimension.TiDBLatency.P99Ms > 500 {
		recommendations = append(recommendations, "Critical: Investigate high P99 latency (>500ms)")
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.4 {
		recommendations = append(recommendations, "High: Address load imbalance across instances")
	}

	if profile.LatencyDimension.TailLatencyRatio > 8 {
		recommendations = append(recommendations, "High: Investigate long tail latency issues")
	}

	if profile.TiKVDimension.BottleneckIndicator != "none" {
		recommendations = append(recommendations, fmt.Sprintf("Medium: TiKV bottleneck detected: %s", profile.TiKVDimension.BottleneckIndicator))
	}

	if profile.QPSDimension.PeakToAvgRatio > 5 {
		recommendations = append(recommendations, "Medium: Consider auto-scaling for peak load")
	}

	if profile.SQLDimension.ResourceHotspotScore > 0.6 {
		recommendations = append(recommendations, "Medium: Optimize resource-intensive SQL patterns")
	}

	if len(recommendations) == 0 {
		recommendations = append(recommendations, "System performance is healthy")
	}

	if len(recommendations) > 5 {
		return recommendations[:5]
	}
	return recommendations
}
