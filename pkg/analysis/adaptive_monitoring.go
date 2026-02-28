package analysis

import (
	"math"
	"time"
)

type AdaptiveThresholds struct {
	QPSThresholds        MetricThresholds `json:"qps_thresholds"`
	LatencyThresholds    MetricThresholds `json:"latency_thresholds"`
	BalanceThresholds    MetricThresholds `json:"balance_thresholds"`
	CalibrationTimestamp int64            `json:"calibration_timestamp"`
	AdaptationScore      float64          `json:"adaptation_score"`
}

type MetricThresholds struct {
	Warning    float64 `json:"warning"`
	Critical   float64 `json:"critical"`
	Baseline   float64 `json:"baseline"`
	Variance   float64 `json:"variance"`
	Confidence float64 `json:"confidence"`
}

type DynamicAlerting struct {
	ActiveRules       []AlertRule        `json:"active_rules"`
	SuppressedAlerts  []SuppressedAlert  `json:"suppressed_alerts"`
	AlertFatigueScore float64            `json:"alert_fatigue_score"`
	PriorityQueue     []PrioritizedAlert `json:"priority_queue"`
}

type AlertRule struct {
	Name            string  `json:"name"`
	Condition       string  `json:"condition"`
	Severity        string  `json:"severity"`
	CurrentStatus   string  `json:"current_status"`
	TriggerCount    int     `json:"trigger_count"`
	LastTriggered   int64   `json:"last_triggered"`
	Threshold       float64 `json:"threshold"`
	CurrentValue    float64 `json:"current_value"`
	CooldownMinutes int     `json:"cooldown_minutes"`
	InCooldown      bool    `json:"in_cooldown"`
}

type SuppressedAlert struct {
	AlertName     string `json:"alert_name"`
	Reason        string `json:"reason"`
	SuppressedAt  int64  `json:"suppressed_at"`
	SuppressUntil int64  `json:"suppress_until"`
}

type PrioritizedAlert struct {
	AlertName     string  `json:"alert_name"`
	Priority      int     `json:"priority"`
	Impact        float64 `json:"impact"`
	Urgency       float64 `json:"urgency"`
	CombinedScore float64 `json:"combined_score"`
}

type RealTimeMonitor struct {
	HealthStatus         string             `json:"health_status"`
	CriticalIndicators   []string           `json:"critical_indicators"`
	WarningIndicators    []string           `json:"warning_indicators"`
	TrendingUp           []string           `json:"trending_up"`
	TrendingDown         []string           `json:"trending_down"`
	ScoreHistory         []HealthScorePoint `json:"score_history"`
	PredictedScore       float64            `json:"predicted_score"`
	MonitoringConfidence float64            `json:"monitoring_confidence"`
}

type HealthScorePoint struct {
	Timestamp int64   `json:"timestamp"`
	Score     float64 `json:"score"`
}

func CalculateAdaptiveThresholds(profile *MultiDimensionProfile) *AdaptiveThresholds {
	at := &AdaptiveThresholds{
		CalibrationTimestamp: time.Now().Unix(),
	}

	at.QPSThresholds = calculateQPSThresholds(profile)
	at.LatencyThresholds = calculateLatencyThresholds(profile)
	at.BalanceThresholds = calculateBalanceThresholds(profile)

	at.AdaptationScore = calculateAdaptationScore(profile)

	return at
}

func calculateQPSThresholds(profile *MultiDimensionProfile) MetricThresholds {
	mt := MetricThresholds{}

	meanQPS := profile.QPSDimension.MeanQPS
	stdDev := meanQPS * profile.QPSDimension.CV

	mt.Baseline = meanQPS
	mt.Variance = stdDev * stdDev
	mt.Warning = meanQPS + 2*stdDev
	mt.Critical = meanQPS + 3*stdDev

	if profile.QPSDimension.Burstiness > 0.5 {
		mt.Warning *= 1.2
		mt.Critical *= 1.3
	}

	if profile.QPSDimension.DailyPatternStrength > 0.3 {
		peakMultiplier := 1 + profile.QPSDimension.PeakToAvgRatio*0.5
		mt.Warning = math.Max(mt.Warning, meanQPS*peakMultiplier)
		mt.Critical = math.Max(mt.Critical, meanQPS*peakMultiplier*1.2)
	}

	mt.Confidence = math.Min(0.95, 0.5+profile.QPSDimension.ForecastConfidence*0.5)

	return mt
}

func calculateLatencyThresholds(profile *MultiDimensionProfile) MetricThresholds {
	mt := MetricThresholds{}

	p99 := profile.LatencyDimension.TiDBLatency.P99Ms

	if p99 > 0 {
		mt.Baseline = profile.LatencyDimension.TiDBLatency.MeanMs
		mt.Variance = profile.LatencyDimension.TiDBLatency.StdDevMs * profile.LatencyDimension.TiDBLatency.StdDevMs

		if p99 < 50 {
			mt.Warning = 100
			mt.Critical = 200
		} else if p99 < 100 {
			mt.Warning = 200
			mt.Critical = 500
		} else if p99 < 500 {
			mt.Warning = 500
			mt.Critical = 1000
		} else {
			mt.Warning = p99 * 1.5
			mt.Critical = p99 * 2.0
		}

		if profile.LatencyDimension.P99Stability > 0.7 {
			mt.Confidence = 0.9
		} else {
			mt.Confidence = 0.6
		}
	} else {
		mt.Warning = 100
		mt.Critical = 500
		mt.Confidence = 0.3
	}

	return mt
}

func calculateBalanceThresholds(profile *MultiDimensionProfile) MetricThresholds {
	mt := MetricThresholds{}

	imbalance := profile.BalanceDimension.OverallImbalanceScore

	mt.Baseline = 0
	mt.Variance = imbalance * imbalance * 0.25

	if imbalance < 0.1 {
		mt.Warning = 0.3
		mt.Critical = 0.5
	} else if imbalance < 0.3 {
		mt.Warning = 0.4
		mt.Critical = 0.6
	} else {
		mt.Warning = 0.5
		mt.Critical = 0.7
	}

	if profile.BalanceDimension.TiDBBalance.InstanceCount > 3 {
		mt.Confidence = 0.85
	} else {
		mt.Confidence = 0.6
	}

	return mt
}

func calculateAdaptationScore(profile *MultiDimensionProfile) float64 {
	score := 0.0

	if profile.DurationHours >= 24 {
		score += 0.3
	} else if profile.DurationHours >= 6 {
		score += 0.2
	}

	if profile.Samples >= 1000 {
		score += 0.2
	} else if profile.Samples >= 500 {
		score += 0.1
	}

	if profile.QPSDimension.DailyPatternStrength > 0.3 {
		score += 0.2
	}

	if profile.QPSDimension.ForecastConfidence > 0.5 {
		score += 0.2
	}

	if profile.LatencyDimension.P99Stability > 0.5 {
		score += 0.1
	}

	return math.Min(1.0, score)
}

func GenerateDynamicAlerting(profile *MultiDimensionProfile, thresholds *AdaptiveThresholds) *DynamicAlerting {
	da := &DynamicAlerting{
		ActiveRules:      []AlertRule{},
		SuppressedAlerts: []SuppressedAlert{},
		PriorityQueue:    []PrioritizedAlert{},
	}

	now := time.Now().Unix()

	da.ActiveRules = append(da.ActiveRules, AlertRule{
		Name:            "high_qps",
		Condition:       "qps > threshold",
		Severity:        "warning",
		Threshold:       thresholds.QPSThresholds.Warning,
		CurrentValue:    profile.QPSDimension.PeakQPS,
		CurrentStatus:   getAlertStatus(profile.QPSDimension.PeakQPS, thresholds.QPSThresholds.Warning, thresholds.QPSThresholds.Critical),
		CooldownMinutes: 5,
	})

	da.ActiveRules = append(da.ActiveRules, AlertRule{
		Name:            "high_latency",
		Condition:       "p99_latency > threshold",
		Severity:        "warning",
		Threshold:       thresholds.LatencyThresholds.Warning,
		CurrentValue:    profile.LatencyDimension.TiDBLatency.P99Ms,
		CurrentStatus:   getAlertStatus(profile.LatencyDimension.TiDBLatency.P99Ms, thresholds.LatencyThresholds.Warning, thresholds.LatencyThresholds.Critical),
		CooldownMinutes: 3,
	})

	da.ActiveRules = append(da.ActiveRules, AlertRule{
		Name:            "load_imbalance",
		Condition:       "imbalance_score > threshold",
		Severity:        "warning",
		Threshold:       thresholds.BalanceThresholds.Warning,
		CurrentValue:    profile.BalanceDimension.OverallImbalanceScore,
		CurrentStatus:   getAlertStatus(profile.BalanceDimension.OverallImbalanceScore, thresholds.BalanceThresholds.Warning, thresholds.BalanceThresholds.Critical),
		CooldownMinutes: 10,
	})

	da.ActiveRules = append(da.ActiveRules, AlertRule{
		Name:            "tail_latency",
		Condition:       "tail_ratio > 5",
		Severity:        "warning",
		Threshold:       5.0,
		CurrentValue:    profile.LatencyDimension.TailLatencyRatio,
		CurrentStatus:   getAlertStatus(profile.LatencyDimension.TailLatencyRatio, 5.0, 10.0),
		CooldownMinutes: 5,
	})

	da.ActiveRules = append(da.ActiveRules, AlertRule{
		Name:            "burst_traffic",
		Condition:       "burstiness > 0.6",
		Severity:        "info",
		Threshold:       0.6,
		CurrentValue:    profile.QPSDimension.Burstiness,
		CurrentStatus:   getAlertStatus(profile.QPSDimension.Burstiness, 0.6, 0.8),
		CooldownMinutes: 15,
	})

	for i := range da.ActiveRules {
		if da.ActiveRules[i].CurrentStatus == "firing" {
			da.ActiveRules[i].TriggerCount++
			da.ActiveRules[i].LastTriggered = now
		}
	}

	triggeredCount := 0
	for _, rule := range da.ActiveRules {
		if rule.CurrentStatus == "firing" {
			triggeredCount++
		}
	}

	if triggeredCount > 0 {
		da.AlertFatigueScore = math.Min(1.0, float64(triggeredCount)*0.2)
	}

	da.PriorityQueue = prioritizeAlerts(da.ActiveRules, profile)

	if da.AlertFatigueScore > 0.5 {
		for _, rule := range da.ActiveRules {
			if rule.Severity == "info" && rule.CurrentStatus == "firing" {
				da.SuppressedAlerts = append(da.SuppressedAlerts, SuppressedAlert{
					AlertName:     rule.Name,
					Reason:        "alert_fatigue_mitigation",
					SuppressedAt:  now,
					SuppressUntil: now + 3600,
				})
			}
		}
	}

	return da
}

func getAlertStatus(current, warning, critical float64) string {
	if current >= critical {
		return "firing"
	} else if current >= warning {
		return "warning"
	}
	return "ok"
}

func prioritizeAlerts(rules []AlertRule, profile *MultiDimensionProfile) []PrioritizedAlert {
	var pq []PrioritizedAlert

	for _, rule := range rules {
		if rule.CurrentStatus != "firing" {
			continue
		}

		priority := 0
		switch rule.Severity {
		case "critical":
			priority = 1
		case "warning":
			priority = 2
		case "info":
			priority = 3
		}

		impact := rule.CurrentValue / rule.Threshold
		if impact > 2 {
			impact = 2
		}

		urgency := 1.0
		if rule.Name == "high_latency" {
			urgency = 1.5
		} else if rule.Name == "load_imbalance" {
			urgency = 1.2
		}

		combinedScore := float64(priority)*0.3 + impact*0.4 + urgency*0.3

		pq = append(pq, PrioritizedAlert{
			AlertName:     rule.Name,
			Priority:      priority,
			Impact:        impact,
			Urgency:       urgency,
			CombinedScore: combinedScore,
		})
	}

	for i := 0; i < len(pq)-1; i++ {
		for j := i + 1; j < len(pq); j++ {
			if pq[i].CombinedScore < pq[j].CombinedScore {
				pq[i], pq[j] = pq[j], pq[i]
			}
		}
	}

	return pq
}

func GenerateRealTimeMonitor(profile *MultiDimensionProfile) *RealTimeMonitor {
	rtm := &RealTimeMonitor{
		CriticalIndicators: []string{},
		WarningIndicators:  []string{},
		TrendingUp:         []string{},
		TrendingDown:       []string{},
		ScoreHistory:       []HealthScorePoint{},
	}

	if profile.OverallHealthScore >= 80 {
		rtm.HealthStatus = "healthy"
	} else if profile.OverallHealthScore >= 60 {
		rtm.HealthStatus = "degraded"
	} else if profile.OverallHealthScore >= 40 {
		rtm.HealthStatus = "unhealthy"
	} else {
		rtm.HealthStatus = "critical"
	}

	if profile.LatencyDimension.TiDBLatency.P99Ms > 500 {
		rtm.CriticalIndicators = append(rtm.CriticalIndicators, "latency_p99_critical")
	} else if profile.LatencyDimension.TiDBLatency.P99Ms > 200 {
		rtm.WarningIndicators = append(rtm.WarningIndicators, "latency_p99_elevated")
	}

	if profile.BalanceDimension.OverallImbalanceScore > 0.5 {
		rtm.CriticalIndicators = append(rtm.CriticalIndicators, "load_imbalance_severe")
	} else if profile.BalanceDimension.OverallImbalanceScore > 0.3 {
		rtm.WarningIndicators = append(rtm.WarningIndicators, "load_imbalance_moderate")
	}

	if profile.QPSDimension.PeakToAvgRatio > 10 {
		rtm.CriticalIndicators = append(rtm.CriticalIndicators, "traffic_burst_extreme")
	} else if profile.QPSDimension.PeakToAvgRatio > 5 {
		rtm.WarningIndicators = append(rtm.WarningIndicators, "traffic_burst_high")
	}

	if profile.QPSDimension.TrendDirection == "upward" {
		rtm.TrendingUp = append(rtm.TrendingUp, "qps_increasing")
	} else if profile.QPSDimension.TrendDirection == "downward" {
		rtm.TrendingDown = append(rtm.TrendingDown, "qps_decreasing")
	}

	if profile.LatencyDimension.LatencyTrend == "increasing" {
		rtm.TrendingUp = append(rtm.TrendingUp, "latency_increasing")
	} else if profile.LatencyDimension.LatencyTrend == "decreasing" {
		rtm.TrendingDown = append(rtm.TrendingDown, "latency_decreasing")
	}

	if profile.QPSDimension.ForecastConfidence > 0.5 {
		trendFactor := 1.0
		if profile.QPSDimension.TrendDirection == "upward" {
			trendFactor = 1.0 + profile.QPSDimension.TrendStrength
		} else if profile.QPSDimension.TrendDirection == "downward" {
			trendFactor = 1.0 - profile.QPSDimension.TrendStrength
		}
		rtm.PredictedScore = profile.OverallHealthScore * trendFactor
	} else {
		rtm.PredictedScore = profile.OverallHealthScore
	}

	rtm.PredictedScore = math.Max(0, math.Min(100, rtm.PredictedScore))

	rtm.MonitoringConfidence = calculateMonitoringConfidence(profile)

	return rtm
}

func calculateMonitoringConfidence(profile *MultiDimensionProfile) float64 {
	confidence := 0.5

	if profile.DurationHours >= 24 {
		confidence += 0.2
	}

	if profile.Samples >= 1000 {
		confidence += 0.1
	}

	if profile.QPSDimension.ForecastConfidence > 0.6 {
		confidence += 0.1
	}

	if profile.LatencyDimension.P99Stability > 0.6 {
		confidence += 0.1
	}

	return math.Min(0.95, confidence)
}
