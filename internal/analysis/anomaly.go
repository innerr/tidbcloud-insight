package analysis

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type AnomalyType string

const (
	AnomalyQPSDrop           AnomalyType = "QPS_DROP"
	AnomalyQPSSpike          AnomalyType = "QPS_SPIKE"
	AnomalyLatencySpike      AnomalyType = "LATENCY_SPIKE"
	AnomalyLatencyDegraded   AnomalyType = "LATENCY_DEGRADED"
	AnomalyInstanceImbalance AnomalyType = "INSTANCE_IMBALANCE"
	AnomalySustainedLowQPS   AnomalyType = "SUSTAINED_LOW_QPS"
	AnomalySustainedHighQPS  AnomalyType = "SUSTAINED_HIGH_QPS"
	AnomalyVolatilitySpike   AnomalyType = "VOLATILITY_SPIKE"
)

type Severity string

const (
	SeverityLow      Severity = "LOW"
	SeverityMedium   Severity = "MEDIUM"
	SeverityHigh     Severity = "HIGH"
	SeverityCritical Severity = "CRITICAL"
)

type DetectedAnomaly struct {
	Type      AnomalyType `json:"type"`
	Severity  Severity    `json:"severity"`
	Timestamp int64       `json:"timestamp"`
	TimeStr   string      `json:"time"`
	Value     float64     `json:"value"`
	Baseline  float64     `json:"baseline"`
	ZScore    float64     `json:"z_score,omitempty"`
	Detail    string      `json:"detail"`
	Instance  string      `json:"instance,omitempty"`
	Duration  int         `json:"duration,omitempty"`
	EndTime   int64       `json:"end_timestamp,omitempty"`
}

type AnomalyConfig struct {
	MinBaselineSamples int
}

func DefaultAnomalyConfig() AnomalyConfig {
	return AnomalyConfig{
		MinBaselineSamples: 10,
	}
}

type TimeSeriesPoint struct {
	Timestamp int64
	Value     float64
}

type AnomalyDetector struct {
	config AnomalyConfig
}

func NewAnomalyDetector(config AnomalyConfig) *AnomalyDetector {
	return &AnomalyDetector{config: config}
}

func (d *AnomalyDetector) DetectAll(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinBaselineSamples {
		return nil
	}

	vals := extractValues(values)
	m := median(vals)
	std := stdDev(vals)

	trafficClass := classifyTrafficLevel(m)

	if m < 10 {
		return nil
	}

	adjustFactor := getAdjustFactor(trafficClass)

	if std < m*0.1 {
		return nil
	}

	var anomalies []DetectedAnomaly

	drops := d.detectDrops(values, m, std, adjustFactor)
	anomalies = append(anomalies, drops...)

	spikes := d.detectSpikes(values, m, std, adjustFactor)
	anomalies = append(anomalies, spikes...)

	sustained := d.detectSustained(values, m, adjustFactor)
	anomalies = append(anomalies, sustained...)

	instImbalance := d.DetectInstanceImbalance(nil)
	_ = instImbalance

	return MergeConsecutiveAnomalies(anomalies)
}

func classifyTrafficLevel(meanQPS float64) string {
	if meanQPS < 50 {
		return "very_low"
	} else if meanQPS < 200 {
		return "low"
	} else if meanQPS < 1000 {
		return "medium"
	} else if meanQPS < 10000 {
		return "high"
	}
	return "very_high"
}

func getAdjustFactor(trafficClass string) float64 {
	switch trafficClass {
	case "very_low":
		return 3.0
	case "low":
		return 2.0
	case "medium":
		return 1.5
	case "high":
		return 1.2
	case "very_high":
		return 1.0
	default:
		return 1.5
	}
}

func (d *AnomalyDetector) detectDrops(values []TimeSeriesPoint, m, std, adjustFactor float64) []DetectedAnomaly {
	if len(values) < d.config.MinBaselineSamples || m < 10 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	hasDailyPattern := checkDailyPattern(values)
	nightHours := getNightHours(values, m)

	threshold := m - std*adjustFactor*1.5
	minDropThreshold := m * 0.5

	var rawAnomalies []DetectedAnomaly

	for _, v := range values {
		if v.Value > threshold {
			continue
		}

		dropRatio := 0.0
		if m > 0 {
			dropRatio = (m - v.Value) / m
		}
		if dropRatio < 0.3 {
			continue
		}

		if hasDailyPattern {
			hour := time.Unix(v.Timestamp, 0).Hour()
			isNight := false
			for _, nh := range nightHours {
				if hour == nh {
					isNight = true
					break
				}
			}
			if isNight {
				continue
			}
		}

		absDrop := m - v.Value
		if absDrop < minDropThreshold && m < 500 {
			continue
		}

		severity := SeverityMedium
		if dropRatio > 0.7 {
			severity = SeverityHigh
		}
		if dropRatio > 0.9 || (v.Value == 0 && m > 100) {
			severity = SeverityCritical
		}

		detail := fmt.Sprintf("QPS dropped to %.0f (baseline=%.0f)", v.Value, m)
		if m > 0 {
			detail = fmt.Sprintf("QPS dropped to %.0f (baseline=%.0f, -%.0f%%)", v.Value, m, dropRatio*100)
		}

		rawAnomalies = append(rawAnomalies, DetectedAnomaly{
			Type:      AnomalyQPSDrop,
			Severity:  severity,
			Timestamp: v.Timestamp,
			TimeStr:   time.Unix(v.Timestamp, 0).Format("2006-01-02 15:04"),
			Value:     v.Value,
			Baseline:  m,
			Detail:    detail,
		})
	}

	return rawAnomalies
}

func (d *AnomalyDetector) detectSpikes(values []TimeSeriesPoint, m, std, adjustFactor float64) []DetectedAnomaly {
	if len(values) < d.config.MinBaselineSamples || m < 10 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	threshold := m + std*adjustFactor*2.0
	minSpikeRatio := 1.5 + adjustFactor*0.3

	var rawAnomalies []DetectedAnomaly

	for _, v := range values {
		if v.Value < threshold {
			continue
		}

		spikeRatio := 0.0
		if m > 0 {
			spikeRatio = v.Value / m
		}
		if spikeRatio < minSpikeRatio {
			continue
		}

		absIncrease := v.Value - m
		if absIncrease < m*0.5 && m < 500 {
			continue
		}

		if v.Value < 200 {
			continue
		}

		severity := SeverityMedium
		if spikeRatio > 3 {
			severity = SeverityHigh
		}
		if spikeRatio > 5 {
			severity = SeverityCritical
		}

		detail := fmt.Sprintf("QPS spiked to %.0f (baseline=%.0f)", v.Value, m)
		if spikeRatio > 0 {
			detail = fmt.Sprintf("QPS spiked to %.0f (baseline=%.0f, +%.0f%%)", v.Value, m, (spikeRatio-1)*100)
		}

		rawAnomalies = append(rawAnomalies, DetectedAnomaly{
			Type:      AnomalyQPSSpike,
			Severity:  severity,
			Timestamp: v.Timestamp,
			TimeStr:   time.Unix(v.Timestamp, 0).Format("2006-01-02 15:04"),
			Value:     v.Value,
			Baseline:  m,
			Detail:    detail,
		})
	}

	return rawAnomalies
}

func (d *AnomalyDetector) detectSustained(values []TimeSeriesPoint, m, adjustFactor float64) []DetectedAnomaly {
	if len(values) < 24 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	var anomalies []DetectedAnomaly

	windowSize := 6
	for i := windowSize; i < len(values); i += windowSize / 2 {
		window := values[i-windowSize : i]
		windowMean := mean(extractValues(window))

		lowThreshold := m * 0.4 * adjustFactor
		highThreshold := m * 2.0 * adjustFactor

		var anomalyType AnomalyType
		var severity Severity
		var detail string

		if windowMean < lowThreshold && m > 100 {
			anomalyType = AnomalySustainedLowQPS
			if windowMean < m*0.2 {
				severity = SeverityHigh
			} else {
				severity = SeverityMedium
			}
			detail = fmt.Sprintf("Sustained low QPS: avg=%.0f over %d samples (baseline=%.0f)", windowMean, windowSize, m)
		} else if windowMean > highThreshold {
			anomalyType = AnomalySustainedHighQPS
			if windowMean > m*3 {
				severity = SeverityHigh
			} else {
				severity = SeverityMedium
			}
			detail = fmt.Sprintf("Sustained high QPS: avg=%.0f over %d samples (baseline=%.0f)", windowMean, windowSize, m)
		} else {
			continue
		}

		anomalies = append(anomalies, DetectedAnomaly{
			Type:      anomalyType,
			Severity:  severity,
			Timestamp: values[i-windowSize].Timestamp,
			TimeStr:   time.Unix(values[i-windowSize].Timestamp, 0).Format("2006-01-02 15:04"),
			Value:     windowMean,
			Baseline:  m,
			Duration:  windowSize,
			EndTime:   values[i-1].Timestamp,
			Detail:    detail,
		})
	}

	return anomalies
}

func checkDailyPattern(values []TimeSeriesPoint) bool {
	if len(values) < 48 {
		return false
	}

	hourlyAvg := make(map[int][]float64)
	for _, v := range values {
		hour := time.Unix(v.Timestamp, 0).Hour()
		hourlyAvg[hour] = append(hourlyAvg[hour], v.Value)
	}

	if len(hourlyAvg) < 12 {
		return false
	}

	var daySum, nightSum float64
	var dayCount, nightCount int
	nightHours := map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 22: true, 23: true}

	for hour, vals := range hourlyAvg {
		avg := mean(vals)
		if nightHours[hour] {
			nightSum += avg
			nightCount++
		} else {
			daySum += avg
			dayCount++
		}
	}

	if dayCount == 0 || nightCount == 0 {
		return false
	}

	dayAvg := daySum / float64(dayCount)
	nightAvg := nightSum / float64(nightCount)

	if dayAvg == 0 {
		return false
	}

	ratio := nightAvg / dayAvg
	return ratio < 0.7
}

func getNightHours(values []TimeSeriesPoint, m float64) []int {
	hourlyAvg := make(map[int]float64)
	hourlyCount := make(map[int]int)

	for _, v := range values {
		hour := time.Unix(v.Timestamp, 0).Hour()
		hourlyAvg[hour] += v.Value
		hourlyCount[hour]++
	}

	for hour := range hourlyAvg {
		hourlyAvg[hour] /= float64(hourlyCount[hour])
	}

	var nightHours []int
	for hour, avg := range hourlyAvg {
		if avg < m*0.6 {
			nightHours = append(nightHours, hour)
		}
	}

	return nightHours
}

func MergeConsecutiveAnomalies(anomalies []DetectedAnomaly) []DetectedAnomaly {
	if len(anomalies) == 0 {
		return anomalies
	}

	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].Timestamp < anomalies[j].Timestamp
	})

	var result []DetectedAnomaly
	var current *DetectedAnomaly
	currentTypes := make(map[AnomalyType]bool)

	for _, a := range anomalies {
		if current == nil {
			current = &a
			currentTypes[a.Type] = true
			continue
		}

		if a.Timestamp <= current.Timestamp+1800 {
			if current.EndTime == 0 {
				current.EndTime = current.Timestamp
			}
			if a.Timestamp > current.EndTime {
				current.EndTime = a.Timestamp
			}

			if a.Severity == SeverityCritical {
				current.Severity = SeverityCritical
			} else if a.Severity == SeverityHigh && current.Severity != SeverityCritical {
				current.Severity = SeverityHigh
			}

			if !currentTypes[a.Type] {
				currentTypes[a.Type] = true
			}

			current.Duration = int(current.EndTime - current.Timestamp)
		} else {
			if len(currentTypes) > 1 {
				current.Type = AnomalyQPSSpike
				if current.Severity == SeverityCritical {
					current.Detail = fmt.Sprintf("Multiple anomalies detected (%d types, duration: %dh)",
						len(currentTypes), current.Duration/3600+1)
				} else {
					current.Detail = fmt.Sprintf("Multiple anomalies (%d types, duration: %dh)",
						len(currentTypes), current.Duration/3600+1)
				}
			} else if current.Duration > 0 {
				current.Detail = fmt.Sprintf("%s (duration: %dh)", current.Detail, current.Duration/3600+1)
			}
			result = append(result, *current)
			current = &a
			currentTypes = make(map[AnomalyType]bool)
			currentTypes[a.Type] = true
		}
	}

	if current != nil {
		if len(currentTypes) > 1 {
			current.Type = AnomalyQPSSpike
			if current.Severity == SeverityCritical {
				current.Detail = fmt.Sprintf("Multiple anomalies detected (%d types, duration: %dh)",
					len(currentTypes), current.Duration/3600+1)
			} else {
				current.Detail = fmt.Sprintf("Multiple anomalies (%d types, duration: %dh)",
					len(currentTypes), current.Duration/3600+1)
			}
		} else if current.Duration > 0 {
			current.Detail = fmt.Sprintf("%s (duration: %dh)", current.Detail, current.Duration/3600+1)
		}
		result = append(result, *current)
	}

	return result
}

func (d *AnomalyDetector) DetectLatencyAnomalies(p50Values, p99Values []TimeSeriesPoint) []DetectedAnomaly {
	if len(p99Values) < d.config.MinBaselineSamples {
		return nil
	}

	sort.Slice(p99Values, func(i, j int) bool {
		return p99Values[i].Timestamp < p99Values[j].Timestamp
	})

	p99Vals := extractValues(p99Values)
	p99Median := median(p99Vals)
	p99P90 := percentile(p99Vals, 0.9)

	threshold := p99Median * 2.5
	if p99Median < 0.01 {
		threshold = p99Median * 5
	}

	var rawAnomalies []DetectedAnomaly

	for _, v := range p99Values {
		if v.Value <= threshold {
			continue
		}

		absDiff := v.Value - p99Median
		if absDiff < 0.01 {
			continue
		}

		if v.Value < p99P90*1.2 {
			continue
		}

		severity := SeverityMedium
		if v.Value > p99Median*4 {
			severity = SeverityHigh
		}
		if v.Value > p99Median*10 {
			severity = SeverityCritical
		}

		rawAnomalies = append(rawAnomalies, DetectedAnomaly{
			Type:      AnomalyLatencySpike,
			Severity:  severity,
			Timestamp: v.Timestamp,
			TimeStr:   time.Unix(v.Timestamp, 0).Format("2006-01-02 15:04"),
			Value:     v.Value * 1000,
			Baseline:  p99Median * 1000,
			Detail:    fmt.Sprintf("P99 latency spiked to %.1fms (baseline=%.1fms)", v.Value*1000, p99Median*1000),
		})
	}

	return MergeConsecutiveAnomalies(rawAnomalies)
}

func (d *AnomalyDetector) DetectInstanceImbalance(instanceValues map[string][]TimeSeriesPoint) []DetectedAnomaly {
	if len(instanceValues) < 2 {
		return nil
	}

	var anomalies []DetectedAnomaly

	instanceMeans := make(map[string]float64)
	var allMeans []float64

	for inst, values := range instanceValues {
		if len(values) < d.config.MinBaselineSamples {
			continue
		}
		m := mean(extractValues(values))
		if m < 10 {
			continue
		}
		instanceMeans[inst] = m
		allMeans = append(allMeans, m)
	}

	if len(allMeans) < 2 {
		return nil
	}

	overallMean := mean(allMeans)
	if overallMean < 10 {
		return nil
	}

	for inst, m := range instanceMeans {
		deviation := (m - overallMean) / overallMean

		if math.Abs(deviation) < 0.5 {
			continue
		}

		severity := SeverityMedium
		if math.Abs(deviation) > 0.7 {
			severity = SeverityHigh
		}

		direction := "under"
		if deviation > 0 {
			direction = "over"
		}
		pct := math.Abs(deviation) * 100

		anomalies = append(anomalies, DetectedAnomaly{
			Type:     AnomalyInstanceImbalance,
			Severity: severity,
			Instance: inst,
			Value:    m,
			Baseline: overallMean,
			Detail:   fmt.Sprintf("Instance %s is %.0f%% %s-loaded (avg=%.0f, cluster=%.0f)", inst, pct, direction, m, overallMean),
		})
	}

	return anomalies
}

type CategoryInstanceValues struct {
	Category string
	Values   map[string][]TimeSeriesPoint
}

func DetectDetailedImbalance(
	sqlTypeByInst map[string]map[string][]TimeSeriesPoint,
	latencyByInst map[string][]TimeSeriesPoint,
	tikvOpByInst map[string]map[string][]TimeSeriesPoint,
	tikvLatencyByInst map[string]map[string][]TimeSeriesPoint,
	config AnomalyConfig,
) []DetectedAnomaly {
	var anomalies []DetectedAnomaly

	for sqlType, instData := range sqlTypeByInst {
		typeAnomalies := detectCategoryImbalance(instData, "tidb", sqlType, config, false)
		anomalies = append(anomalies, typeAnomalies...)
	}

	if len(latencyByInst) >= 2 {
		latencyAnomalies := detectLatencyImbalance(latencyByInst, "tidb", config)
		anomalies = append(anomalies, latencyAnomalies...)
	}

	for op, instData := range tikvOpByInst {
		opAnomalies := detectCategoryImbalance(instData, "tikv", op, config, false)
		anomalies = append(anomalies, opAnomalies...)
	}

	for op, instData := range tikvLatencyByInst {
		latencyAnomalies := detectCategoryImbalance(instData, "tikv", op, config, true)
		anomalies = append(anomalies, latencyAnomalies...)
	}

	return anomalies
}

func detectCategoryImbalance(instData map[string][]TimeSeriesPoint, component, category string, config AnomalyConfig, isLatency bool) []DetectedAnomaly {
	if len(instData) < 2 {
		return nil
	}

	instanceMeans := make(map[string]float64)
	var allMeans []float64

	for inst, values := range instData {
		if len(values) < config.MinBaselineSamples {
			continue
		}
		m := mean(extractValues(values))
		minThreshold := float64(10)
		if isLatency {
			minThreshold = 0.001
		}
		if m < minThreshold {
			continue
		}
		instanceMeans[inst] = m
		allMeans = append(allMeans, m)
	}

	if len(allMeans) < 2 {
		return nil
	}

	overallMean := mean(allMeans)
	if overallMean < 0.001 {
		return nil
	}

	var anomalies []DetectedAnomaly
	for inst, m := range instanceMeans {
		deviation := (m - overallMean) / overallMean

		threshold := 0.5
		if isLatency {
			threshold = 0.8
		}
		if math.Abs(deviation) < threshold {
			continue
		}

		severity := SeverityMedium
		if math.Abs(deviation) > 0.7 {
			severity = SeverityHigh
		}
		if math.Abs(deviation) > 1.0 {
			severity = SeverityCritical
		}

		pct := math.Abs(deviation) * 100

		var detail string
		if isLatency {
			if deviation > 0 {
				detail = fmt.Sprintf("%s %s latency on %s is %.1fx cluster avg (inst=%.1fms, cluster=%.1fms)",
					component, category, inst, m/overallMean, m*1000, overallMean*1000)
			} else {
				detail = fmt.Sprintf("%s %s latency on %s is %.0f%% below cluster avg (inst=%.1fms, cluster=%.1fms)",
					component, category, inst, pct, m*1000, overallMean*1000)
			}
		} else {
			if deviation > 0 {
				detail = fmt.Sprintf("%s %s on %s is %.1fx cluster avg (inst=%.0f, cluster=%.0f)",
					component, category, inst, m/overallMean, m, overallMean)
			} else {
				detail = fmt.Sprintf("%s %s on %s is %.0f%% below cluster avg (inst=%.0f, cluster=%.0f)",
					component, category, inst, pct, m, overallMean)
			}
		}

		anomalies = append(anomalies, DetectedAnomaly{
			Type:     AnomalyInstanceImbalance,
			Severity: severity,
			Instance: inst,
			Value:    m,
			Baseline: overallMean,
			Detail:   detail,
		})
	}

	return anomalies
}

func detectLatencyImbalance(latencyByInst map[string][]TimeSeriesPoint, component string, config AnomalyConfig) []DetectedAnomaly {
	if len(latencyByInst) < 2 {
		return nil
	}

	instanceMeans := make(map[string]float64)
	var allMeans []float64

	for inst, values := range latencyByInst {
		if len(values) < config.MinBaselineSamples {
			continue
		}
		m := mean(extractValues(values))
		if m < 0.001 {
			continue
		}
		instanceMeans[inst] = m
		allMeans = append(allMeans, m)
	}

	if len(allMeans) < 2 {
		return nil
	}

	overallMean := mean(allMeans)
	if overallMean < 0.001 {
		return nil
	}

	var anomalies []DetectedAnomaly
	for inst, m := range instanceMeans {
		deviation := (m - overallMean) / overallMean

		if math.Abs(deviation) < 0.8 {
			continue
		}

		severity := SeverityMedium
		if math.Abs(deviation) > 1.5 {
			severity = SeverityHigh
		}
		if math.Abs(deviation) > 2.0 {
			severity = SeverityCritical
		}

		pct := math.Abs(deviation) * 100
		var detail string
		if deviation > 0 {
			detail = fmt.Sprintf("%s P99 latency on %s is %.1fx cluster avg (inst=%.1fms, cluster=%.1fms)",
				component, inst, m/overallMean, m*1000, overallMean*1000)
		} else {
			detail = fmt.Sprintf("%s P99 latency on %s is %.0f%% below cluster avg (inst=%.1fms, cluster=%.1fms)",
				component, inst, pct, m*1000, overallMean*1000)
		}

		anomalies = append(anomalies, DetectedAnomaly{
			Type:     AnomalyInstanceImbalance,
			Severity: severity,
			Instance: inst,
			Value:    m * 1000,
			Baseline: overallMean * 1000,
			Detail:   detail,
		})
	}

	return anomalies
}

func (d *AnomalyDetector) DetectVolatilitySpike(values []TimeSeriesPoint) []DetectedAnomaly {
	return nil
}

func (d *AnomalyDetector) DetectZScoreAnomalies(values []TimeSeriesPoint) []DetectedAnomaly {
	return nil
}

func (d *AnomalyDetector) DetectEMAAnomalies(values []TimeSeriesPoint) []DetectedAnomaly {
	return nil
}

func (d *AnomalyDetector) DetectMADAnomalies(values []TimeSeriesPoint) []DetectedAnomaly {
	return nil
}

func (d *AnomalyDetector) deduplicateAnomalies(anomalies []DetectedAnomaly) []DetectedAnomaly {
	return MergeConsecutiveAnomalies(anomalies)
}

func DetectHourlyBaselineAnomalies(values []TimeSeriesPoint, config AnomalyConfig) []DetectedAnomaly {
	return nil
}

func extractValues(points []TimeSeriesPoint) []float64 {
	vals := make([]float64, len(points))
	for i, p := range points {
		vals[i] = p.Value
	}
	return vals
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

func mad(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}

	m := median(vals)
	deviations := make([]float64, len(vals))
	for i, v := range vals {
		deviations[i] = math.Abs(v - m)
	}
	return median(deviations)
}

func coefficientOfVariation(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := mean(vals)
	if m == 0 {
		return 0
	}
	return stdDev(vals) / m
}

func formatAnomalyDetail(anomalyType AnomalyType, value, baseline, zScore float64) string {
	switch anomalyType {
	case AnomalyQPSDrop:
		return fmt.Sprintf("QPS dropped to %.0f (baseline=%.0f)", value, baseline)
	case AnomalyQPSSpike:
		return fmt.Sprintf("QPS spiked to %.0f (baseline=%.0f)", value, baseline)
	default:
		return fmt.Sprintf("Value=%.0f, baseline=%.0f", value, baseline)
	}
}

func formatSustainedAnomalyDetail(anomalyType AnomalyType, value, baseline float64, duration int) string {
	switch anomalyType {
	case AnomalySustainedLowQPS:
		return fmt.Sprintf("Sustained low QPS for %d samples: avg=%.0f (baseline=%.0f)", duration, value, baseline)
	case AnomalySustainedHighQPS:
		return fmt.Sprintf("Sustained high QPS for %d samples: avg=%.0f (baseline=%.0f)", duration, value, baseline)
	default:
		return fmt.Sprintf("Sustained anomaly for %d samples", duration)
	}
}

func formatVolatilityDetail(cv, baselineCV float64) string {
	return fmt.Sprintf("High volatility detected: CV=%.2f (baseline=%.2f)", cv, baselineCV)
}

func formatLatencyDetail(value, baseline float64) string {
	return fmt.Sprintf("P99 latency spiked to %.1fms (baseline=%.1fms)", value, baseline)
}

func formatLatencyTailDetail(p50, p99 float64) string {
	return fmt.Sprintf("High latency tail: P50=%.1fms, P99=%.1fms (ratio=%.1fx)", p50, p99, p99/p50)
}

func formatImbalanceDetail(instance string, value, baseline, deviation float64) string {
	direction := "under"
	if deviation > 0 {
		direction = "over"
	}
	pct := math.Abs(deviation) * 100
	return fmt.Sprintf("Instance %s is %.0f%% %s-loaded (avg=%.0f, cluster=%.0f)", instance, pct, direction, value, baseline)
}
