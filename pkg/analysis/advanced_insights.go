// Package analysis provides advanced insights for TiDB/TiKV performance profiling.
// This file implements root cause analysis, predictive alerts, capacity planning,
// and optimization roadmaps based on multi-dimension profiling data.
// Inspired by Google SRE principles and industry best practices.
package analysis

import (
	"fmt"
	"math"
	"sort"
)

type AdvancedInsights struct {
	RootCauseAnalysis    RootCauseAnalysis    `json:"root_cause_analysis"`
	PredictiveAlerts     PredictiveAlerts     `json:"predictive_alerts"`
	CapacityPlanning     CapacityPlanning     `json:"capacity_planning"`
	PerformanceScorecard PerformanceScorecard `json:"performance_scorecard"`
	OptimizationRoadmap  OptimizationRoadmap  `json:"optimization_roadmap"`
}

type RootCauseAnalysis struct {
	PrimaryCause       string          `json:"primary_cause"`
	SecondaryCauses    []string        `json:"secondary_causes"`
	CausalityChain     []CausalityLink `json:"causality_chain"`
	ConfidenceScore    float64         `json:"confidence_score"`
	SupportingEvidence []string        `json:"supporting_evidence"`
}

type CausalityLink struct {
	Cause       string  `json:"cause"`
	Effect      string  `json:"effect"`
	Strength    float64 `json:"strength"`
	Description string  `json:"description"`
}

type PredictiveAlerts struct {
	ActiveAlerts        []PredictiveAlert `json:"active_alerts"`
	ImminentRisks       []ImminentRisk    `json:"imminent_risks"`
	TrendBasedForecasts []TrendForecast   `json:"trend_based_forecasts"`
	AlertScore          float64           `json:"alert_score"`
}

type PredictiveAlert struct {
	Type             string  `json:"type"`
	Severity         string  `json:"severity"`
	Description      string  `json:"description"`
	TriggerCondition string  `json:"trigger_condition"`
	CurrentValue     float64 `json:"current_value"`
	Threshold        float64 `json:"threshold"`
	TimeToTrigger    string  `json:"time_to_trigger"`
}

type ImminentRisk struct {
	RiskType       string  `json:"risk_type"`
	Probability    float64 `json:"probability"`
	Impact         string  `json:"impact"`
	MitigationStep string  `json:"mitigation_step"`
}

type TrendForecast struct {
	Metric         string  `json:"metric"`
	CurrentValue   float64 `json:"current_value"`
	ForecastValue  float64 `json:"forecast_value"`
	TimeHorizon    string  `json:"time_horizon"`
	Confidence     float64 `json:"confidence"`
	TrendDirection string  `json:"trend_direction"`
}

type CapacityPlanning struct {
	CurrentUtilization     float64      `json:"current_utilization"`
	PeakUtilization        float64      `json:"peak_utilization"`
	Headroom               float64      `json:"headroom"`
	RecommendedCapacity    string       `json:"recommended_capacity"`
	ScalingTrigger         string       `json:"scaling_trigger"`
	CapacityScore          float64      `json:"capacity_score"`
	ScalingRecommendations []ScalingRec `json:"scaling_recommendations"`
}

type ScalingRec struct {
	Type          string  `json:"type"`
	Trigger       string  `json:"trigger"`
	TargetValue   float64 `json:"target_value"`
	Priority      int     `json:"priority"`
	EstimatedCost string  `json:"estimated_cost"`
}

type PerformanceScorecard struct {
	OverallScore         float64            `json:"overall_score"`
	DimensionScores      map[string]float64 `json:"dimension_scores"`
	Grade                string             `json:"grade"`
	Strengths            []string           `json:"strengths"`
	Weaknesses           []string           `json:"weaknesses"`
	ImprovementPotential float64            `json:"improvement_potential"`
}

type OptimizationRoadmap struct {
	Immediate       []OptimizationAction `json:"immediate"`
	ShortTerm       []OptimizationAction `json:"short_term"`
	MediumTerm      []OptimizationAction `json:"medium_term"`
	LongTerm        []OptimizationAction `json:"long_term"`
	EstimatedImpact float64              `json:"estimated_impact"`
}

type OptimizationAction struct {
	Category       string  `json:"category"`
	Action         string  `json:"action"`
	Priority       int     `json:"priority"`
	ExpectedImpact float64 `json:"expected_impact"`
	Effort         string  `json:"effort"`
	Risk           string  `json:"risk"`
}

func GenerateAdvancedInsights(profile *MultiDimensionProfile) *AdvancedInsights {
	insights := &AdvancedInsights{}

	insights.RootCauseAnalysis = analyzeRootCause(profile)
	insights.PredictiveAlerts = generatePredictiveAlerts(profile)
	insights.CapacityPlanning = generateCapacityPlanning(profile)
	insights.PerformanceScorecard = generatePerformanceScorecard(profile)
	insights.OptimizationRoadmap = generateOptimizationRoadmap(profile)

	return insights
}

func analyzeRootCause(profile *MultiDimensionProfile) RootCauseAnalysis {
	rca := RootCauseAnalysis{
		SecondaryCauses:    []string{},
		CausalityChain:     []CausalityLink{},
		SupportingEvidence: []string{},
	}

	var causes []struct {
		cause    string
		score    float64
		evidence string
	}

	if profile.LatencyDimension.TiDBLatency.P99Ms > 200 {
		causes = append(causes, struct {
			cause    string
			score    float64
			evidence string
		}{
			cause:    "high_latency",
			score:    profile.LatencyDimension.TiDBLatency.P99Ms / 500.0,
			evidence: "P99 latency exceeds 200ms threshold",
		})
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.3 {
		causes = append(causes, struct {
			cause    string
			score    float64
			evidence string
		}{
			cause:    "load_imbalance",
			score:    profile.BalanceDimension.OverallImbalanceScore,
			evidence: "Instance load distribution is skewed",
		})
	}

	if profile.QPSDimension.Burstiness > 0.5 {
		causes = append(causes, struct {
			cause    string
			score    float64
			evidence string
		}{
			cause:    "traffic_burstiness",
			score:    profile.QPSDimension.Burstiness,
			evidence: "Traffic exhibits high burstiness",
		})
	}

	if profile.CrossDimensionInsights.QPSToLatencyCorr > 0.5 {
		causes = append(causes, struct {
			cause    string
			score    float64
			evidence string
		}{
			cause:    "load_sensitive_performance",
			score:    profile.CrossDimensionInsights.QPSToLatencyCorr,
			evidence: "Performance degrades under high load",
		})
	}

	if profile.TiKVVolumeDimension.BottleneckScore > 0.3 {
		causes = append(causes, struct {
			cause    string
			score    float64
			evidence string
		}{
			cause:    "tikv_bottleneck",
			score:    profile.TiKVVolumeDimension.BottleneckScore,
			evidence: "TiKV operations show bottleneck patterns",
		})
	}

	if profile.SQLDimension.ResourceHotspotScore > 0.5 {
		causes = append(causes, struct {
			cause    string
			score    float64
			evidence string
		}{
			cause:    "sql_hotspot",
			score:    profile.SQLDimension.ResourceHotspotScore,
			evidence: "Resource-intensive SQL patterns detected",
		})
	}

	sort.Slice(causes, func(i, j int) bool {
		return causes[i].score > causes[j].score
	})

	if len(causes) > 0 {
		rca.PrimaryCause = causes[0].cause
		rca.SupportingEvidence = append(rca.SupportingEvidence, causes[0].evidence)

		for i := 1; i < len(causes) && i < 3; i++ {
			rca.SecondaryCauses = append(rca.SecondaryCauses, causes[i].cause)
			rca.SupportingEvidence = append(rca.SupportingEvidence, causes[i].evidence)
		}
	}

	if len(causes) > 1 {
		for i := 0; i < len(causes)-1; i++ {
			rca.CausalityChain = append(rca.CausalityChain, CausalityLink{
				Cause:       causes[i].cause,
				Effect:      causes[i+1].cause,
				Strength:    math.Min(causes[i].score, causes[i+1].score),
				Description: "Correlated through multi-dimension analysis",
			})
		}
	}

	if len(causes) > 0 {
		rca.ConfidenceScore = math.Min(0.95, causes[0].score*0.8+0.3)
	} else {
		rca.ConfidenceScore = 0.9
		rca.PrimaryCause = "no_significant_issues"
		rca.SupportingEvidence = append(rca.SupportingEvidence, "All metrics within normal parameters")
	}

	return rca
}

func generatePredictiveAlerts(profile *MultiDimensionProfile) PredictiveAlerts {
	alerts := PredictiveAlerts{
		ActiveAlerts:        []PredictiveAlert{},
		ImminentRisks:       []ImminentRisk{},
		TrendBasedForecasts: []TrendForecast{},
	}

	if profile.QPSDimension.PeakToAvgRatio > 5 {
		alerts.ActiveAlerts = append(alerts.ActiveAlerts, PredictiveAlert{
			Type:             "capacity",
			Severity:         "warning",
			Description:      "Peak-to-average ratio indicates bursty traffic",
			TriggerCondition: "peak_to_avg > 5",
			CurrentValue:     profile.QPSDimension.PeakToAvgRatio,
			Threshold:        5.0,
			TimeToTrigger:    "immediate",
		})
	}

	if profile.LatencyDimension.TiDBLatency.P99Ms > 200 {
		alerts.ActiveAlerts = append(alerts.ActiveAlerts, PredictiveAlert{
			Type:             "performance",
			Severity:         "warning",
			Description:      "P99 latency elevated above optimal threshold",
			TriggerCondition: "p99_latency > 200ms",
			CurrentValue:     profile.LatencyDimension.TiDBLatency.P99Ms,
			Threshold:        200.0,
			TimeToTrigger:    "immediate",
		})
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.4 {
		alerts.ImminentRisks = append(alerts.ImminentRisks, ImminentRisk{
			RiskType:       "hotspot_overload",
			Probability:    profile.BalanceDimension.OverallImbalanceScore,
			Impact:         "medium",
			MitigationStep: "Redistribute load across instances",
		})
	}

	if profile.QPSDimension.TrendDirection == "upward" && profile.QPSDimension.TrendStrength > 0.3 {
		alerts.ImminentRisks = append(alerts.ImminentRisks, ImminentRisk{
			RiskType:       "capacity_exhaustion",
			Probability:    profile.QPSDimension.TrendStrength,
			Impact:         "high",
			MitigationStep: "Plan capacity expansion",
		})
	}

	if profile.DurationHours > 24 {
		alerts.TrendBasedForecasts = append(alerts.TrendBasedForecasts, TrendForecast{
			Metric:         "qps",
			CurrentValue:   profile.QPSDimension.MeanQPS,
			ForecastValue:  profile.QPSDimension.ForecastNext24H,
			TimeHorizon:    "24h",
			Confidence:     profile.QPSDimension.ForecastConfidence,
			TrendDirection: profile.QPSDimension.TrendDirection,
		})
	}

	alertScore := 0.0
	for _, alert := range alerts.ActiveAlerts {
		if alert.Severity == "critical" {
			alertScore += 0.3
		} else if alert.Severity == "warning" {
			alertScore += 0.15
		}
	}
	alertScore += float64(len(alerts.ImminentRisks)) * 0.1

	alerts.AlertScore = math.Min(1.0, alertScore)

	return alerts
}

func generateCapacityPlanning(profile *MultiDimensionProfile) CapacityPlanning {
	planning := CapacityPlanning{
		ScalingRecommendations: []ScalingRec{},
	}

	currentUtil := profile.QPSDimension.MeanQPS / profile.QPSDimension.PeakQPS
	planning.CurrentUtilization = currentUtil
	planning.PeakUtilization = 1.0
	planning.Headroom = 1.0 - currentUtil

	if profile.QPSDimension.PeakToAvgRatio > 3 {
		planning.ScalingTrigger = "peak_based"
		planning.RecommendedCapacity = fmt.Sprintf("scale_for_peak_%dx", int(profile.QPSDimension.PeakToAvgRatio))
	} else {
		planning.ScalingTrigger = "growth_based"
		planning.RecommendedCapacity = "scale_for_growth"
	}

	if profile.QPSDimension.PeakToAvgRatio > 5 {
		planning.ScalingRecommendations = append(planning.ScalingRecommendations, ScalingRec{
			Type:          "horizontal",
			Trigger:       "peak_qps_exceeded",
			TargetValue:   profile.QPSDimension.PeakQPS * 1.2,
			Priority:      1,
			EstimatedCost: "medium",
		})
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.3 {
		planning.ScalingRecommendations = append(planning.ScalingRecommendations, ScalingRec{
			Type:          "rebalance",
			Trigger:       "load_imbalance_detected",
			TargetValue:   0.1,
			Priority:      2,
			EstimatedCost: "low",
		})
	}

	capacityScore := 1.0
	capacityScore -= planning.Headroom * 0.3
	capacityScore -= profile.QPSDimension.Burstiness * 0.2
	capacityScore -= profile.QPSDimension.CV * 0.2

	planning.CapacityScore = math.Max(0, math.Min(1, capacityScore))

	return planning
}

func generatePerformanceScorecard(profile *MultiDimensionProfile) PerformanceScorecard {
	scorecard := PerformanceScorecard{
		DimensionScores: make(map[string]float64),
		Strengths:       []string{},
		Weaknesses:      []string{},
	}

	latencyScore := 100.0
	if profile.LatencyDimension.TiDBLatency.P99Ms > 500 {
		latencyScore -= 30
	} else if profile.LatencyDimension.TiDBLatency.P99Ms > 200 {
		latencyScore -= 15
	}
	if profile.LatencyDimension.TailLatencyRatio > 10 {
		latencyScore -= 20
	} else if profile.LatencyDimension.TailLatencyRatio > 5 {
		latencyScore -= 10
	}
	latencyScore = math.Max(0, latencyScore)
	scorecard.DimensionScores["latency"] = latencyScore

	balanceScore := 100.0 - profile.BalanceDimension.OverallImbalanceScore*100
	balanceScore = math.Max(0, balanceScore)
	scorecard.DimensionScores["balance"] = balanceScore

	qpsScore := 100.0 - profile.QPSDimension.Burstiness*50 - profile.QPSDimension.CV*30
	qpsScore = math.Max(0, qpsScore)
	scorecard.DimensionScores["qps_stability"] = qpsScore

	efficiencyScore := profile.CrossDimensionInsights.ResourceEfficiency * 100
	scorecard.DimensionScores["efficiency"] = efficiencyScore

	total := 0.0
	for _, score := range scorecard.DimensionScores {
		total += score
	}
	scorecard.OverallScore = total / float64(len(scorecard.DimensionScores))

	switch {
	case scorecard.OverallScore >= 80:
		scorecard.Grade = "A"
	case scorecard.OverallScore >= 60:
		scorecard.Grade = "B"
	case scorecard.OverallScore >= 40:
		scorecard.Grade = "C"
	default:
		scorecard.Grade = "D"
	}

	for name, score := range scorecard.DimensionScores {
		if score >= 80 {
			scorecard.Strengths = append(scorecard.Strengths, fmt.Sprintf("Strong %s performance", name))
		} else if score < 60 {
			scorecard.Weaknesses = append(scorecard.Weaknesses, fmt.Sprintf("Weak %s performance", name))
		}
	}

	improvementPotential := 0.0
	for _, score := range scorecard.DimensionScores {
		improvementPotential += (100 - score)
	}
	scorecard.ImprovementPotential = improvementPotential / float64(len(scorecard.DimensionScores))

	return scorecard
}

func generateOptimizationRoadmap(profile *MultiDimensionProfile) OptimizationRoadmap {
	roadmap := OptimizationRoadmap{
		Immediate:       []OptimizationAction{},
		ShortTerm:       []OptimizationAction{},
		MediumTerm:      []OptimizationAction{},
		LongTerm:        []OptimizationAction{},
		EstimatedImpact: 0,
	}

	if profile.LatencyDimension.TiDBLatency.P99Ms > 500 {
		roadmap.Immediate = append(roadmap.Immediate, OptimizationAction{
			Category:       "performance",
			Action:         "Investigate and optimize high-latency queries",
			Priority:       1,
			ExpectedImpact: 25,
			Effort:         "medium",
			Risk:           "low",
		})
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.4 {
		roadmap.Immediate = append(roadmap.Immediate, OptimizationAction{
			Category:       "balance",
			Action:         "Rebalance load across hot instances",
			Priority:       2,
			ExpectedImpact: 20,
			Effort:         "low",
			Risk:           "low",
		})
	}

	if profile.QPSDimension.PeakToAvgRatio > 5 {
		roadmap.ShortTerm = append(roadmap.ShortTerm, OptimizationAction{
			Category:       "capacity",
			Action:         "Implement auto-scaling for peak load",
			Priority:       1,
			ExpectedImpact: 30,
			Effort:         "medium",
			Risk:           "medium",
		})
	}

	if profile.SQLDimension.ResourceHotspotScore > 0.5 {
		roadmap.ShortTerm = append(roadmap.ShortTerm, OptimizationAction{
			Category:       "optimization",
			Action:         "Optimize resource-intensive SQL patterns",
			Priority:       2,
			ExpectedImpact: 15,
			Effort:         "medium",
			Risk:           "low",
		})
	}

	if profile.TiKVDimension.WriteAmplification > 3 {
		roadmap.MediumTerm = append(roadmap.MediumTerm, OptimizationAction{
			Category:       "storage",
			Action:         "Optimize write patterns to reduce amplification",
			Priority:       1,
			ExpectedImpact: 20,
			Effort:         "high",
			Risk:           "medium",
		})
	}

	if profile.QPSDimension.DailyPatternStrength > 0.5 {
		roadmap.MediumTerm = append(roadmap.MediumTerm, OptimizationAction{
			Category:       "scheduling",
			Action:         "Implement scheduled scaling based on daily patterns",
			Priority:       2,
			ExpectedImpact: 15,
			Effort:         "medium",
			Risk:           "low",
		})
	}

	roadmap.LongTerm = append(roadmap.LongTerm, OptimizationAction{
		Category:       "architecture",
		Action:         "Implement predictive auto-scaling with ML",
		Priority:       1,
		ExpectedImpact: 25,
		Effort:         "high",
		Risk:           "medium",
	})

	for _, action := range roadmap.Immediate {
		roadmap.EstimatedImpact += action.ExpectedImpact
	}
	for _, action := range roadmap.ShortTerm {
		roadmap.EstimatedImpact += action.ExpectedImpact * 0.7
	}
	for _, action := range roadmap.MediumTerm {
		roadmap.EstimatedImpact += action.ExpectedImpact * 0.5
	}

	return roadmap
}
