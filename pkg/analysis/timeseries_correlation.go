package analysis

import (
	"math"
	"sort"
)

type TimeSeriesCorrelation struct {
	MetricPairs         []MetricCorrelation           `json:"metric_pairs"`
	LaggedCorrelations  []LaggedCorrelation           `json:"lagged_correlations"`
	CausalityIndicators []CausalityIndicator          `json:"causality_indicators"`
	AnomalyPropagation  AnomalyPropagationAnalysis    `json:"anomaly_propagation"`
	CorrelationMatrix   map[string]map[string]float64 `json:"correlation_matrix"`
}

type MetricCorrelation struct {
	Metric1         string  `json:"metric1"`
	Metric2         string  `json:"metric2"`
	Correlation     float64 `json:"correlation"`
	CorrelationType string  `json:"correlation_type"`
	Significance    float64 `json:"significance"`
}

type LaggedCorrelation struct {
	Metric1        string  `json:"metric1"`
	Metric2        string  `json:"metric2"`
	OptimalLag     int     `json:"optimal_lag"`
	LagCorrelation float64 `json:"lag_correlation"`
	LagDirection   string  `json:"lag_direction"`
}

type CausalityIndicator struct {
	Cause        string  `json:"cause"`
	Effect       string  `json:"effect"`
	GrangerScore float64 `json:"granger_score"`
	Confidence   float64 `json:"confidence"`
	LagMinutes   int     `json:"lag_minutes"`
}

type AnomalyPropagationAnalysis struct {
	PropagationChains    []PropagationChain `json:"propagation_chains"`
	SourceAnomalies      []string           `json:"source_anomalies"`
	AmplificationFactors map[string]float64 `json:"amplification_factors"`
}

type PropagationChain struct {
	Source          string   `json:"source"`
	Path            []string `json:"path"`
	Strength        float64  `json:"strength"`
	PropagationTime int      `json:"propagation_time_minutes"`
}

func AnalyzeTimeSeriesCorrelation(profile *MultiDimensionProfile) *TimeSeriesCorrelation {
	tsc := &TimeSeriesCorrelation{
		MetricPairs:         []MetricCorrelation{},
		LaggedCorrelations:  []LaggedCorrelation{},
		CausalityIndicators: []CausalityIndicator{},
		CorrelationMatrix:   make(map[string]map[string]float64),
	}

	tsc.analyzeMetricCorrelations(profile)
	tsc.analyzeLaggedCorrelations(profile)
	tsc.analyzeCausality(profile)
	tsc.analyzeAnomalyPropagation(profile)

	return tsc
}

func (tsc *TimeSeriesCorrelation) analyzeMetricCorrelations(profile *MultiDimensionProfile) {
	metrics := map[string]float64{
		"qps_mean":           profile.QPSDimension.MeanQPS,
		"qps_burstiness":     profile.QPSDimension.Burstiness,
		"latency_p99":        profile.LatencyDimension.TiDBLatency.P99Ms,
		"latency_tail_ratio": profile.LatencyDimension.TailLatencyRatio,
		"balance_imbalance":  profile.BalanceDimension.OverallImbalanceScore,
		"volume_bottleneck":  profile.TiKVVolumeDimension.BottleneckScore,
		"sql_hotspot":        profile.SQLDimension.ResourceHotspotScore,
	}

	for name := range metrics {
		tsc.CorrelationMatrix[name] = make(map[string]float64)
	}

	for name1, val1 := range metrics {
		for name2, val2 := range metrics {
			if name1 == name2 {
				tsc.CorrelationMatrix[name1][name2] = 1.0
				continue
			}

			corr := calculateMetricCorrelation(val1, val2)
			tsc.CorrelationMatrix[name1][name2] = corr
		}
	}

	correlationPairs := []struct {
		m1, m2 string
		corr   float64
	}{
		{"qps_mean", "latency_p99", profile.CrossDimensionInsights.QPSToLatencyCorr},
		{"volume_bottleneck", "latency_p99", profile.CrossDimensionInsights.VolumeToLatencyCorr},
		{"balance_imbalance", "latency_p99", profile.CrossDimensionInsights.BalanceToPerformance},
		{"sql_hotspot", "latency_p99", profile.CrossDimensionInsights.SQLToLatencyCorr},
		{"qps_burstiness", "latency_tail_ratio", profile.QPSDimension.Burstiness * profile.LatencyDimension.TailLatencyRatio / 10},
	}

	for _, pair := range correlationPairs {
		corrType := "positive"
		if pair.corr < 0 {
			corrType = "negative"
		} else if pair.corr < 0.1 {
			corrType = "weak"
		}

		significance := calculateSignificance(pair.corr, profile.Samples)

		tsc.MetricPairs = append(tsc.MetricPairs, MetricCorrelation{
			Metric1:         pair.m1,
			Metric2:         pair.m2,
			Correlation:     pair.corr,
			CorrelationType: corrType,
			Significance:    significance,
		})
	}

	sort.Slice(tsc.MetricPairs, func(i, j int) bool {
		return math.Abs(tsc.MetricPairs[i].Correlation) > math.Abs(tsc.MetricPairs[j].Correlation)
	})
}

func (tsc *TimeSeriesCorrelation) analyzeLaggedCorrelations(profile *MultiDimensionProfile) {
	if profile.DurationHours < 1 {
		return
	}

	avgSampleInterval := profile.DurationHours * 3600 / float64(profile.Samples)
	estimatedLag := int(300 / avgSampleInterval)
	if estimatedLag < 1 {
		estimatedLag = 1
	}

	laggedPairs := []struct {
		m1, m2    string
		baseCorr  float64
		likelyLag int
	}{
		{"qps_mean", "latency_p99", profile.CrossDimensionInsights.QPSToLatencyCorr, estimatedLag},
		{"qps_mean", "volume_bottleneck", profile.QPSDimension.MeanQPS / 1000, estimatedLag * 2},
		{"volume_bottleneck", "latency_p99", profile.CrossDimensionInsights.VolumeToLatencyCorr, estimatedLag / 2},
	}

	for _, pair := range laggedPairs {
		if pair.baseCorr > 0.2 {
			lagDirection := "positive"
			if pair.likelyLag > 0 {
				lagDirection = pair.m1 + "_leads"
			}

			lagMinutes := int(float64(pair.likelyLag) * avgSampleInterval / 60)

			tsc.LaggedCorrelations = append(tsc.LaggedCorrelations, LaggedCorrelation{
				Metric1:        pair.m1,
				Metric2:        pair.m2,
				OptimalLag:     pair.likelyLag,
				LagCorrelation: pair.baseCorr * 0.9,
				LagDirection:   lagDirection,
			})

			if pair.baseCorr > 0.3 {
				tsc.CausalityIndicators = append(tsc.CausalityIndicators, CausalityIndicator{
					Cause:        pair.m1,
					Effect:       pair.m2,
					GrangerScore: pair.baseCorr,
					Confidence:   pair.baseCorr * 0.8,
					LagMinutes:   lagMinutes,
				})
			}
		}
	}
}

func (tsc *TimeSeriesCorrelation) analyzeCausality(profile *MultiDimensionProfile) {
	existingCauses := make(map[string]bool)
	for _, ci := range tsc.CausalityIndicators {
		existingCauses[ci.Cause+"->"+ci.Effect] = true
	}

	if profile.SQLDimension.ResourceHotspotScore > 0.4 && !existingCauses["sql_hotspot->latency_p99"] {
		tsc.CausalityIndicators = append(tsc.CausalityIndicators, CausalityIndicator{
			Cause:        "sql_hotspot",
			Effect:       "latency_p99",
			GrangerScore: profile.SQLDimension.ResourceHotspotScore,
			Confidence:   profile.SQLDimension.ResourceHotspotScore * 0.7,
			LagMinutes:   0,
		})
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.3 && !existingCauses["balance_imbalance->qps_burstiness"] {
		tsc.CausalityIndicators = append(tsc.CausalityIndicators, CausalityIndicator{
			Cause:        "balance_imbalance",
			Effect:       "qps_burstiness",
			GrangerScore: profile.BalanceDimension.OverallImbalanceScore,
			Confidence:   profile.BalanceDimension.OverallImbalanceScore * 0.6,
			LagMinutes:   5,
		})
	}

	sort.Slice(tsc.CausalityIndicators, func(i, j int) bool {
		return tsc.CausalityIndicators[i].GrangerScore > tsc.CausalityIndicators[j].GrangerScore
	})
}

func (tsc *TimeSeriesCorrelation) analyzeAnomalyPropagation(profile *MultiDimensionProfile) {
	tsc.AnomalyPropagation = AnomalyPropagationAnalysis{
		PropagationChains:    []PropagationChain{},
		SourceAnomalies:      []string{},
		AmplificationFactors: make(map[string]float64),
	}

	anomalyScores := map[string]float64{
		"qps_burst":       profile.QPSDimension.Burstiness,
		"latency_spike":   profile.LatencyDimension.TailLatencyRatio / 10.0,
		"load_imbalance":  profile.BalanceDimension.OverallImbalanceScore,
		"tikv_bottleneck": profile.TiKVVolumeDimension.BottleneckScore,
		"sql_hotspot":     profile.SQLDimension.ResourceHotspotScore,
	}

	for name, score := range anomalyScores {
		if score > 0.3 {
			tsc.AnomalyPropagation.SourceAnomalies = append(tsc.AnomalyPropagation.SourceAnomalies, name)
		}
	}

	if profile.QPSDimension.Burstiness > 0.4 && profile.LatencyDimension.TailLatencyRatio > 3 {
		tsc.AnomalyPropagation.PropagationChains = append(tsc.AnomalyPropagation.PropagationChains,
			PropagationChain{
				Source:          "qps_burst",
				Path:            []string{"qps_burst", "tikv_bottleneck", "latency_spike"},
				Strength:        profile.CrossDimensionInsights.QPSToLatencyCorr,
				PropagationTime: int(profile.DurationHours * 60 / float64(profile.Samples) * 5),
			})
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.3 {
		tsc.AnomalyPropagation.PropagationChains = append(tsc.AnomalyPropagation.PropagationChains,
			PropagationChain{
				Source:          "load_imbalance",
				Path:            []string{"load_imbalance", "hot_instance", "latency_spike"},
				Strength:        profile.BalanceDimension.OverallImbalanceScore,
				PropagationTime: 5,
			})
	}

	for name, score := range anomalyScores {
		if score > 0.2 {
			amplification := 1.0 + score*0.5
			if profile.CrossDimensionInsights.QPSToLatencyCorr > 0.5 {
				amplification *= 1.2
			}
			tsc.AnomalyPropagation.AmplificationFactors[name] = amplification
		}
	}

	sort.Slice(tsc.AnomalyPropagation.PropagationChains, func(i, j int) bool {
		return tsc.AnomalyPropagation.PropagationChains[i].Strength > tsc.AnomalyPropagation.PropagationChains[j].Strength
	})
}

func calculateMetricCorrelation(v1, v2 float64) float64 {
	if v1 == 0 && v2 == 0 {
		return 1.0
	}
	if v1 == 0 || v2 == 0 {
		return 0.0
	}

	maxVal := math.Max(v1, v2)
	minVal := math.Min(v1, v2)
	return minVal / maxVal
}

func calculateSignificance(corr float64, sampleSize int) float64 {
	if sampleSize < 10 {
		return corr * 0.3
	}

	n := float64(sampleSize)
	tStat := corr * math.Sqrt((n-2)/(1-corr*corr+0.001))

	significance := 1.0 - 1.0/(1.0+math.Abs(tStat)/10)

	return math.Min(0.99, significance)
}
