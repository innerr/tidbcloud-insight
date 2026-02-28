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

	if !strings.Contains(output, "MULTI-DIMENSION PROFILE ANALYSIS") {
		t.Error("Output should contain header")
	}

	if !strings.Contains(output, "test-cluster") {
		t.Error("Output should contain cluster ID")
	}

	if !strings.Contains(output, "[1/6] SQL DIMENSION") {
		t.Error("Output should contain SQL dimension section")
	}

	if !strings.Contains(output, "DDL (Schema Changes)") {
		t.Error("Output should explain DDL")
	}

	if !strings.Contains(output, "[2/6] TiKV DIMENSION") {
		t.Error("Output should contain TiKV dimension section")
	}

	if !strings.Contains(output, "[3/6] LATENCY DIMENSION") {
		t.Error("Output should contain Latency dimension section")
	}

	if !strings.Contains(output, "P50 (Median)") {
		t.Error("Output should explain P50")
	}

	if !strings.Contains(output, "[4/6] BALANCE DIMENSION") {
		t.Error("Output should contain Balance dimension section")
	}

	if !strings.Contains(output, "[5/6] QPS DIMENSION") {
		t.Error("Output should contain QPS dimension section")
	}

	if !strings.Contains(output, "[6/6] TiKV VOLUME DIMENSION") {
		t.Error("Output should contain TiKV Volume dimension section")
	}

	if !strings.Contains(output, "CROSS-DIMENSION INSIGHTS") {
		t.Error("Output should contain cross-dimension insights section")
	}

	if !strings.Contains(output, "OVERALL ASSESSMENT") {
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
