package analysis

import (
	"strings"
	"testing"
)

func TestFormatMultiDimensionProfile(t *testing.T) {
	profile := &MultiDimensionProfile{
		ClusterID:     "test-cluster",
		DurationHours: 24.0,
		Samples:       1440,
		SQLDimension: SQLDimensionProfile{
			DDLRatio:               0.01,
			DMLRatio:               0.40,
			DQLRatio:               0.55,
			TCLRatio:               0.04,
			TransactionalIntensity: 0.70,
			AnalyticalIntensity:    0.30,
			BatchOperationRatio:    0.10,
			InteractiveRatio:       0.80,
			ComplexityIndex:        0.45,
			ResourceHotspotScore:   0.25,
			PatternStability:       0.85,
		},
		TiKVDimension: TiKVDimensionProfile{
			ReadOpRatio:         0.60,
			WriteOpRatio:        0.30,
			TransactionOpRatio:  0.08,
			CoprocessorRatio:    0.02,
			AvgLatencyMs:        15.5,
			P99LatencyMs:        85.2,
			LatencyVariability:  0.35,
			WriteAmplification:  2.5,
			ReadWriteBalance:    0.70,
			BottleneckIndicator: "none",
			BottleneckScore:     0.0,
		},
		LatencyDimension: LatencyDimensionProfile{
			TiDBLatency: LatencyDetailProfile{
				P50Ms:        10.5,
				P90Ms:        25.3,
				P99Ms:        85.2,
				P999Ms:       150.0,
				MaxMs:        250.0,
				MeanMs:       18.5,
				StdDevMs:     22.3,
				CV:           1.2,
				Skewness:     2.5,
				Kurtosis:     8.3,
				OutlierRatio: 0.02,
			},
			TiKVLatency: LatencyDetailProfile{
				P50Ms:        8.2,
				P90Ms:        18.5,
				P99Ms:        55.0,
				P999Ms:       120.0,
				MaxMs:        180.0,
				MeanMs:       12.3,
				StdDevMs:     15.8,
				CV:           1.3,
				Skewness:     2.1,
				Kurtosis:     7.5,
				OutlierRatio: 0.015,
			},
			TailLatencyRatio:    8.1,
			LatencyCorrelation:  0.85,
			SpikeCount:          5,
			SpikePattern:        "periodic",
			PeriodicLatency:     true,
			LatencyTrend:        "stable",
			P99Stability:        0.75,
			LongTailIndicator:   "moderate",
			BusinessHourLatency: 22.5,
			OffPeakLatency:      15.2,
		},
		BalanceDimension: BalanceDimensionProfile{
			TiDBBalance: InstanceBalanceDetail{
				InstanceCount:          3,
				QPSSkewCoefficient:     0.15,
				LatencySkewCoefficient: 0.10,
				MaxMinRatio:            1.5,
				BalanceScore:           0.85,
			},
			TiKVBalance: InstanceBalanceDetail{
				InstanceCount:          5,
				QPSSkewCoefficient:     0.20,
				LatencySkewCoefficient: 0.12,
				MaxMinRatio:            1.8,
				BalanceScore:           0.80,
			},
			CrossComponentBalance: 0.82,
			OverallImbalanceScore: 0.18,
			HotspotRiskLevel:      "low",
			HotInstances:          []string{},
			ColdInstances:         []string{},
			LoadDistributionCV:    0.15,
			QPSImbalanceRatio:     0.18,
			LatencyImbalanceRatio: 0.12,
			Recommendation:        "Load is well balanced across instances",
		},
		QPSDimension: QPSDimensionProfile{
			MeanQPS:               1250.5,
			PeakQPS:               3500.0,
			MinQPS:                150.0,
			PeakToAvgRatio:        2.8,
			CV:                    0.45,
			Burstiness:            0.25,
			QPSPattern:            "daily_periodic",
			DailyPatternStrength:  0.65,
			WeeklyPatternStrength: 0.45,
			HasSeasonality:        true,
			PeakHours:             []int{10, 11, 14, 15, 16},
			BusinessHourQPS:       1800.5,
			OffPeakQPS:            450.2,
			TrendDirection:        "stable",
			TrendStrength:         0.15,
			ForecastNext24H:       1280.0,
			ForecastConfidence:    0.82,
			CriticalLoadIndicator: "normal",
		},
		TiKVVolumeDimension: TiKVVolumeDimensionProfile{
			TotalRequests:      125000000,
			MeanRPS:            1446.8,
			PeakRPS:            4200.0,
			PeakToAvgRatio:     2.9,
			ReadRPS:            868.1,
			WriteRPS:           578.7,
			ReadWriteRatio:     1.5,
			WriteAmplification: 2.5,
			TransactionVolume:  125000000,
			CoprocessorVolume:  15000000,
			DailyVolumePattern: 0.65,
			BottleneckType:     "none",
			BottleneckScore:    0.0,
			HotRegionIndicator: false,
			VolumeTrend:        "stable",
			SaturationRisk:     "low",
		},
		CrossDimensionInsights: CrossDimensionInsights{
			SQLToLatencyCorr:           0.45,
			TiKVToLatencyCorr:          0.65,
			QPSToLatencyCorr:           0.72,
			VolumeToLatencyCorr:        0.68,
			BalanceToPerformance:       0.55,
			CrossCorrelationScore:      0.61,
			DominantBottleneck:         "none",
			SecondaryBottleneck:        "none",
			ResourceEfficiency:         0.78,
			OptimizationPriority:       []string{"monitor_tail_latency", "optimize_slow_queries"},
			AnomalyIndicators:          []string{},
			PerformanceDegradationRisk: "low",
		},
		OverallHealthScore: 85.5,
		OverallRiskLevel:   "low",
		TopRecommendations: []string{
			"Continue monitoring tail latency",
			"Optimize slow queries",
			"Maintain current configuration",
		},
	}

	output := FormatMultiDimensionProfile(profile)

	if !strings.Contains(output, "SQL Query Pattern Analysis") {
		t.Error("Output should contain SQL dimension section")
	}

	if !strings.Contains(output, "DDL (Schema Changes)") {
		t.Error("Output should explain DDL")
	}

	if !strings.Contains(output, "TiKV Storage Layer Analysis") {
		t.Error("Output should contain TiKV dimension section")
	}

	if !strings.Contains(output, "Latency Performance Analysis") {
		t.Error("Output should contain Latency dimension section")
	}

	if !strings.Contains(output, "P50 (Median)") {
		t.Error("Output should explain P50")
	}

	if !strings.Contains(output, "Load Balance Analysis") {
		t.Error("Output should contain Balance dimension section")
	}

	if !strings.Contains(output, "Traffic Pattern Analysis") {
		t.Error("Output should contain QPS dimension section")
	}

	if !strings.Contains(output, "TiKV Request Volume Analysis") {
		t.Error("Output should contain TiKV Volume dimension section")
	}

	if !strings.Contains(output, "Overall Assessment") {
		t.Error("Output should contain overall assessment section")
	}

	if !strings.Contains(output, "Health Score: 85.5/100") {
		t.Error("Output should contain health score")
	}

	if !strings.Contains(output, "Good - minor optimization opportunities") {
		t.Error("Output should explain health score")
	}

	if !strings.Contains(output, "Risk Level: low") {
		t.Error("Output should contain risk level")
	}

	if !strings.Contains(output, "System is stable with no significant risks") {
		t.Error("Output should explain risk level")
	}

	if !strings.Contains(output, "Continue monitoring tail latency") {
		t.Error("Output should contain recommendations")
	}

	t.Logf("\n%s", output)
}

func TestFormatLoadProfile(t *testing.T) {
	profile := &LoadProfile{
		ClusterID:     "test-cluster",
		DurationHours: 168.0,
		Samples:       10080,
		DailyPattern: DailyPattern{
			HourlyAvg: map[int]float64{
				0:  100.0,
				1:  80.0,
				2:  60.0,
				9:  500.0,
				10: 600.0,
				11: 550.0,
				14: 520.0,
				15: 580.0,
				16: 540.0,
			},
			PeakHours:        []int{10, 11, 14, 15, 16},
			OffPeakHours:     []int{0, 1, 2, 3, 4},
			PeakToOffPeak:    6.0,
			NightDrop:        0.20,
			NightDropHours:   []int{0, 1, 2, 3, 4, 5},
			PatternStrength:  0.75,
			ConsistencyScore: 0.80,
			PeriodicityScore: 0.85,
			BusinessHours: BusinessHours{
				StartHour: 9,
				EndHour:   18,
				AvgQPS:    550.0,
				Ratio:     1.5,
			},
		},
		WeeklyPattern: WeeklyPattern{
			DailyAvg: map[string]float64{
				"2024-01-01": 500.0,
				"2024-01-02": 520.0,
				"2024-01-03": 510.0,
				"2024-01-04": 505.0,
				"2024-01-05": 515.0,
				"2024-01-06": 350.0,
				"2024-01-07": 340.0,
			},
			WeekdayAvg:       510.0,
			WeekendAvg:       345.0,
			WeekendDrop:      0.32,
			IsWeekdayHeavy:   true,
			PatternStrength:  0.70,
			ConsistencyScore: 0.75,
		},
		InstanceSkew: &InstanceSkewProfile{
			TiDBSkew: InstanceSkewDetail{
				InstanceCount:          3,
				QPSSkewCoefficient:     0.25,
				LatencySkewCoefficient: 0.15,
				MaxQPSRatio:            1.8,
				MinQPSRatio:            0.6,
				HotInstances:           []string{"tidb-1"},
				ColdInstances:          []string{"tidb-3"},
				QPSDistribution: map[string]float64{
					"tidb-1": 600.0,
					"tidb-2": 450.0,
					"tidb-3": 300.0,
				},
			},
			TiKVSkew: InstanceSkewDetail{
				InstanceCount:          5,
				QPSSkewCoefficient:     0.18,
				LatencySkewCoefficient: 0.12,
				MaxQPSRatio:            1.5,
				MinQPSRatio:            0.7,
				HotInstances:           []string{},
				ColdInstances:          []string{},
			},
			HasQPSImbalance:     true,
			HasLatencyImbalance: false,
			SkewRiskLevel:       "medium",
			HotInstanceCount:    1,
			Recommendation:      "Consider redistributing load from hot instances",
		},
		Insights: &ClusterInsights{
			OverallHealth:             "Good",
			PerformanceScore:          85.0,
			StabilityScore:            80.0,
			EfficiencyScore:           82.0,
			PatternType:               "Business hours pattern",
			RiskFactors:               []string{"Moderate instance skew"},
			AnomalyIndicators:         []string{},
			OptimizationOpportunities: []string{"Optimize instance load balancing"},
			RecommendedActions:        []string{"Monitor hot instances", "Consider scaling"},
		},
	}

	output := FormatLoadProfile(profile)

	if !strings.Contains(output, "COMPREHENSIVE LOAD PROFILE REPORT") {
		t.Error("Output should contain main header")
	}

	if !strings.Contains(output, "test-cluster") {
		t.Error("Output should contain cluster ID")
	}

	if !strings.Contains(output, "DAILY PATTERN ANALYSIS") {
		t.Error("Output should contain daily pattern section")
	}

	if !strings.Contains(output, "Hourly QPS Distribution") {
		t.Error("Output should contain hourly distribution")
	}

	if !strings.Contains(output, "Peak Hours") {
		t.Error("Output should identify peak hours")
	}

	if !strings.Contains(output, "WEEKLY PATTERN ANALYSIS") {
		t.Error("Output should contain weekly pattern section")
	}

	if !strings.Contains(output, "Weekday vs Weekend") {
		t.Error("Output should compare weekday and weekend")
	}

	if !strings.Contains(output, "Weekend Traffic Drop") {
		t.Error("Output should show weekend drop")
	}

	if !strings.Contains(output, "DATA SKEW ANALYSIS") {
		t.Error("Output should contain data skew section")
	}

	if !strings.Contains(output, "Instance Load Distribution") {
		t.Error("Output should describe instance load distribution")
	}

	if !strings.Contains(output, "Hot Instance Count") {
		t.Error("Output should show hot instance count")
	}

	if !strings.Contains(output, "tidb-1") {
		t.Error("Output should identify hot instance")
	}

	if !strings.Contains(output, "CLUSTER INSIGHTS") {
		t.Error("Output should contain insights section")
	}

	if !strings.Contains(output, "Performance Score") {
		t.Error("Output should show performance score")
	}

	if !strings.Contains(output, "Risk Factors") {
		t.Error("Output should list risk factors")
	}

	t.Logf("\n%s", output)
}
