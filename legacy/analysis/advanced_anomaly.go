package analysis

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type AdvancedAnomalyDetector struct {
	config AdvancedAnomalyConfig
}

type AdvancedAnomalyConfig struct {
	MinSamples           int
	STLEnable            bool
	MADThreshold         float64
	ZScoreThreshold      float64
	ESDAlpha             float64
	ESDMaxAnomalies      float64
	SeasonalPeriod       int
	TrendWindow          int
	RobustDecomposition  bool
	EnableHourlyBaseline bool
	EnableEMA            bool
	EMAAlpha             float64
	EMAThreshold         float64
	EnablePrediction     bool
	PredictionWindow     int
}

func DefaultAdvancedAnomalyConfig() AdvancedAnomalyConfig {
	return AdvancedAnomalyConfig{
		MinSamples:           10,
		STLEnable:            true,
		MADThreshold:         3.5,
		ZScoreThreshold:      3.0,
		ESDAlpha:             0.05,
		ESDMaxAnomalies:      0.02,
		SeasonalPeriod:       24,
		TrendWindow:          7,
		RobustDecomposition:  true,
		EnableHourlyBaseline: true,
		EnableEMA:            true,
		EMAAlpha:             0.2,
		EMAThreshold:         0.3,
		EnablePrediction:     true,
		PredictionWindow:     6,
	}
}

func NewAdvancedAnomalyDetector(config AdvancedAnomalyConfig) *AdvancedAnomalyDetector {
	return &AdvancedAnomalyDetector{config: config}
}

type DecompositionResult struct {
	Trend    []float64
	Seasonal []float64
	Residual []float64
	Original []float64
}

func minInt2(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt2(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (d *AdvancedAnomalyDetector) DecomposeSTL(values []TimeSeriesPoint, period int) *DecompositionResult {
	if len(values) < period*2 {
		return nil
	}

	n := len(values)
	vals := extractValues(values)

	trend := d.extractTrend(vals, d.config.TrendWindow)
	seasonal := d.extractSeasonal(vals, trend, period)
	residual := make([]float64, n)
	for i := 0; i < n; i++ {
		residual[i] = vals[i] - trend[i] - seasonal[i]
	}

	return &DecompositionResult{
		Trend:    trend,
		Seasonal: seasonal,
		Residual: residual,
		Original: vals,
	}
}

func (d *AdvancedAnomalyDetector) extractTrend(vals []float64, window int) []float64 {
	n := len(vals)
	trend := make([]float64, n)

	halfWindow := window / 2
	for i := 0; i < n; i++ {
		start := maxInt2(0, i-halfWindow)
		end := minInt2(n, i+halfWindow+1)

		if d.config.RobustDecomposition {
			trend[i] = median(vals[start:end])
		} else {
			trend[i] = mean(vals[start:end])
		}
	}

	trend = d.smoothWithLOESS(trend, window)

	return trend
}

func (d *AdvancedAnomalyDetector) smoothWithLOESS(vals []float64, window int) []float64 {
	n := len(vals)
	smoothed := make([]float64, n)

	halfWindow := window / 2
	for i := 0; i < n; i++ {
		weights := make([]float64, 0, window)
		weightedVals := make([]float64, 0, window)

		for j := maxInt2(0, i-halfWindow); j < minInt2(n, i+halfWindow+1); j++ {
			distance := float64(abs(i - j))
			weight := math.Exp(-distance * distance / float64(halfWindow*halfWindow))
			weights = append(weights, weight)
			weightedVals = append(weightedVals, vals[j]*weight)
		}

		sumWeights := sum(weights)
		if sumWeights > 0 {
			smoothed[i] = sum(weightedVals) / sumWeights
		} else {
			smoothed[i] = vals[i]
		}
	}

	return smoothed
}

func (d *AdvancedAnomalyDetector) extractSeasonal(vals, trend []float64, period int) []float64 {
	n := len(vals)
	detrended := make([]float64, n)
	for i := 0; i < n; i++ {
		detrended[i] = vals[i] - trend[i]
	}

	seasonal := make([]float64, n)
	seasonalMeans := make([]float64, period)

	for p := 0; p < period; p++ {
		var sum float64
		var count int
		for i := p; i < n; i += period {
			sum += detrended[i]
			count++
		}
		if count > 0 {
			seasonalMeans[p] = sum / float64(count)
		}
	}

	overallMean := mean(seasonalMeans)
	for p := 0; p < period; p++ {
		seasonalMeans[p] -= overallMean
	}

	for i := 0; i < n; i++ {
		seasonal[i] = seasonalMeans[i%period]
	}

	return seasonal
}

func (d *AdvancedAnomalyDetector) DetectMAD(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples {
		return nil
	}

	vals := extractValues(values)
	m := median(vals)
	deviations := make([]float64, len(vals))
	for i, v := range vals {
		deviations[i] = math.Abs(v - m)
	}
	mad := median(deviations)

	if mad == 0 {
		constant := 1.4826
		mad = stdDev(vals) / constant
	}
	if mad == 0 {
		return nil
	}

	scaledMAD := mad * 1.4826

	var anomalies []DetectedAnomaly
	for _, v := range values {
		zScore := math.Abs(v.Value-m) / scaledMAD
		if zScore > d.config.MADThreshold {
			severity := SeverityMedium
			if zScore > d.config.MADThreshold*1.5 {
				severity = SeverityHigh
			}
			if zScore > d.config.MADThreshold*2 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if v.Value < m {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: v.Timestamp,
				TimeStr:   time.Unix(v.Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     v.Value,
				Baseline:  m,
				ZScore:    zScore,
				Detail:    fmt.Sprintf("MAD anomaly: %.0f (baseline=%.0f, z=%.2f)", v.Value, m, zScore),
			})
		}
	}

	return anomalies
}

func (d *AdvancedAnomalyDetector) DetectESD(values []TimeSeriesPoint, maxAnomalies float64, alpha float64) []DetectedAnomaly {
	if len(values) < d.config.MinSamples {
		return nil
	}

	n := len(values)
	vals := extractValues(values)
	anomalyCount := int(float64(n) * maxAnomalies)
	if anomalyCount < 1 {
		anomalyCount = 1
	}

	var anomalies []DetectedAnomaly
	remaining := make([]int, n)
	for i := 0; i < n; i++ {
		remaining[i] = i
	}

	for k := 0; k < anomalyCount && len(remaining) >= d.config.MinSamples; k++ {
		remainingVals := make([]float64, len(remaining))
		for i, idx := range remaining {
			remainingVals[i] = vals[idx]
		}

		m := mean(remainingVals)
		s := stdDev(remainingVals)

		if s == 0 {
			break
		}

		maxZScore := 0.0
		maxIdx := 0
		for i, idx := range remaining {
			z := math.Abs(vals[idx] - m)
			if z > maxZScore {
				maxZScore = z
				maxIdx = i
			}
		}

		testStatistic := maxZScore / s
		criticalValue := d.calculateGrubbsCriticalValue(len(remaining), alpha)

		if testStatistic <= criticalValue {
			break
		}

		originalIdx := remaining[maxIdx]
		originalValue := values[originalIdx]

		severity := SeverityMedium
		if testStatistic > criticalValue*1.5 {
			severity = SeverityHigh
		}

		anomalyType := AnomalyQPSSpike
		if originalValue.Value < m {
			anomalyType = AnomalyQPSDrop
		}

		anomalies = append(anomalies, DetectedAnomaly{
			Type:      anomalyType,
			Severity:  severity,
			Timestamp: originalValue.Timestamp,
			TimeStr:   time.Unix(originalValue.Timestamp, 0).Format("2006-01-02 15:04"),
			Value:     originalValue.Value,
			Baseline:  m,
			ZScore:    testStatistic,
			Detail:    fmt.Sprintf("ESD anomaly: %.0f (baseline=%.0f, statistic=%.2f)", originalValue.Value, m, testStatistic),
		})

		remaining = append(remaining[:maxIdx], remaining[maxIdx+1:]...)
	}

	return anomalies
}

func (d *AdvancedAnomalyDetector) calculateGrubbsCriticalValue(n int, alpha float64) float64 {
	t := d.inverseTCDF(1-alpha/(2*float64(n)), n-2)
	return (float64(n) - 1) / math.Sqrt(float64(n)) * math.Sqrt(t*t/(float64(n)-2+t*t))
}

func (d *AdvancedAnomalyDetector) inverseTCDF(p float64, df int) float64 {
	if p <= 0 {
		return -3
	}
	if p >= 1 {
		return 3
	}

	low, high := -10.0, 10.0
	for i := 0; i < 100; i++ {
		mid := (low + high) / 2
		cdf := d.tDistributionCDF(mid, df)
		if cdf < p {
			low = mid
		} else {
			high = mid
		}
	}
	return (low + high) / 2
}

func (d *AdvancedAnomalyDetector) tDistributionCDF(x float64, df int) float64 {
	if df <= 0 {
		return 0.5 + 0.5*math.Erf(x/math.Sqrt2)
	}

	coeff := math.Gamma(float64(df+1)/2) / (math.Sqrt(float64(df)*math.Pi) * math.Gamma(float64(df)/2))
	integrand := math.Pow(1+x*x/float64(df), -float64(df+1)/2)

	return 0.5 + 0.5*math.Erf(x/math.Sqrt2)*coeff*integrand
}

func (d *AdvancedAnomalyDetector) DetectWithDecomposition(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	period := d.detectSeasonalPeriod(values)
	if period < 4 {
		period = d.config.SeasonalPeriod
	}

	decomp := d.DecomposeSTL(values, period)
	if decomp == nil {
		return nil
	}

	residualPoints := make([]TimeSeriesPoint, len(values))
	for i, v := range values {
		residualPoints[i] = TimeSeriesPoint{
			Timestamp: v.Timestamp,
			Value:     decomp.Residual[i],
		}
	}

	residualAnomalies := d.DetectMAD(residualPoints)

	var anomalies []DetectedAnomaly
	for _, a := range residualAnomalies {
		originalIdx := -1
		for i, v := range values {
			if v.Timestamp == a.Timestamp {
				originalIdx = i
				break
			}
		}
		if originalIdx >= 0 {
			anomalies = append(anomalies, DetectedAnomaly{
				Type:      a.Type,
				Severity:  a.Severity,
				Timestamp: a.Timestamp,
				TimeStr:   a.TimeStr,
				Value:     values[originalIdx].Value,
				Baseline:  decomp.Trend[originalIdx] + decomp.Seasonal[originalIdx],
				ZScore:    a.ZScore,
				Detail:    fmt.Sprintf("Decomposition anomaly: %.0f (expected=%.0f)", values[originalIdx].Value, a.Baseline),
			})
		}
	}

	return anomalies
}

func (d *AdvancedAnomalyDetector) detectSeasonalPeriod(values []TimeSeriesPoint) int {
	n := len(values)
	if n < 48 {
		return 24
	}

	vals := extractValues(values)
	maxLag := minInt2(n/2, 168)

	bestPeriod := 24
	bestCorr := 0.0

	for lag := 4; lag < maxLag; lag++ {
		corr := d.calculateAutocorrelation(vals, lag)
		if corr > bestCorr {
			bestCorr = corr
			bestPeriod = lag
		}
	}

	return bestPeriod
}

func (d *AdvancedAnomalyDetector) calculateAutocorrelation(vals []float64, lag int) float64 {
	n := len(vals)
	if n <= lag {
		return 0
	}

	mean := mean(vals)
	var numerator, denominator float64

	for i := 0; i < n; i++ {
		denominator += (vals[i] - mean) * (vals[i] - mean)
	}

	if denominator == 0 {
		return 0
	}

	for i := lag; i < n; i++ {
		numerator += (vals[i] - mean) * (vals[i-lag] - mean)
	}

	return numerator / denominator
}

func (d *AdvancedAnomalyDetector) DetectEMAAnomalies(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	ema := make([]float64, n)
	ema[0] = vals[0]
	alpha := d.config.EMAAlpha

	for i := 1; i < n; i++ {
		ema[i] = alpha*vals[i] + (1-alpha)*ema[i-1]
	}

	emaStd := make([]float64, n)
	emaStd[0] = 0
	for i := 1; i < n; i++ {
		diff := vals[i] - ema[i-1]
		emaStd[i] = math.Sqrt(alpha*diff*diff + (1-alpha)*emaStd[i-1]*emaStd[i-1])
	}

	var anomalies []DetectedAnomaly
	for i := d.config.MinSamples; i < n; i++ {
		if emaStd[i] == 0 {
			continue
		}

		deviation := math.Abs(vals[i] - ema[i])
		ratio := deviation / emaStd[i]

		if ratio > d.config.EMAThreshold*3 {
			severity := SeverityMedium
			if ratio > d.config.EMAThreshold*5 {
				severity = SeverityHigh
			}
			if ratio > d.config.EMAThreshold*7 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if vals[i] < ema[i] {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     vals[i],
				Baseline:  ema[i],
				Detail:    fmt.Sprintf("EMA deviation: %.0f (expected=%.0f, deviation=%.1fx)", vals[i], ema[i], ratio),
			})
		}
	}

	return anomalies
}

func (d *AdvancedAnomalyDetector) DetectHourlyBaselineAnomalies(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples*2 {
		return nil
	}

	hourlyData := make(map[int][]float64)
	hourlyTS := make(map[int][]TimeSeriesPoint)

	for _, v := range values {
		hour := time.Unix(v.Timestamp, 0).Hour()
		hourlyData[hour] = append(hourlyData[hour], v.Value)
		hourlyTS[hour] = append(hourlyTS[hour], v)
	}

	if len(hourlyData) < 8 {
		return nil
	}

	hourlyStats := make(map[int]struct {
		mean float64
		std  float64
	})

	for hour, vals := range hourlyData {
		hourlyStats[hour] = struct {
			mean float64
			std  float64
		}{mean(vals), stdDev(vals)}
	}

	var anomalies []DetectedAnomaly
	for hour, points := range hourlyTS {
		stats := hourlyStats[hour]
		if stats.std == 0 {
			continue
		}

		for _, p := range points {
			zScore := math.Abs(p.Value-stats.mean) / stats.std
			if zScore > d.config.ZScoreThreshold {
				severity := SeverityMedium
				if zScore > d.config.ZScoreThreshold*1.5 {
					severity = SeverityHigh
				}

				anomalyType := AnomalyQPSSpike
				if p.Value < stats.mean {
					anomalyType = AnomalyQPSDrop
				}

				anomalies = append(anomalies, DetectedAnomaly{
					Type:      anomalyType,
					Severity:  severity,
					Timestamp: p.Timestamp,
					TimeStr:   time.Unix(p.Timestamp, 0).Format("2006-01-02 15:04"),
					Value:     p.Value,
					Baseline:  stats.mean,
					ZScore:    zScore,
					Detail:    fmt.Sprintf("Hourly baseline anomaly at %02d:00: %.0f (baseline=%.0f)", hour, p.Value, stats.mean),
				})
			}
		}
	}

	return anomalies
}

func (d *AdvancedAnomalyDetector) DetectAllAdvanced(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples {
		return nil
	}

	var allAnomalies []DetectedAnomaly

	if d.config.STLEnable {
		decompAnomalies := d.DetectWithDecomposition(values)
		allAnomalies = append(allAnomalies, decompAnomalies...)
	}

	madAnomalies := d.DetectMAD(values)
	allAnomalies = append(allAnomalies, madAnomalies...)

	esdAnomalies := d.DetectESD(values, d.config.ESDMaxAnomalies, d.config.ESDAlpha)
	allAnomalies = append(allAnomalies, esdAnomalies...)

	if d.config.EnableEMA {
		emaAnomalies := d.DetectEMAAnomalies(values)
		allAnomalies = append(allAnomalies, emaAnomalies...)
	}

	if d.config.EnableHourlyBaseline {
		hourlyAnomalies := d.DetectHourlyBaselineAnomalies(values)
		allAnomalies = append(allAnomalies, hourlyAnomalies...)
	}

	return d.deduplicateAndRankAnomalies(allAnomalies)
}

func (d *AdvancedAnomalyDetector) deduplicateAndRankAnomalies(anomalies []DetectedAnomaly) []DetectedAnomaly {
	if len(anomalies) == 0 {
		return nil
	}

	type anomalyScore struct {
		anomaly DetectedAnomaly
		score   float64
		count   int
	}

	timestampScores := make(map[int64]*anomalyScore)

	for _, a := range anomalies {
		ts := a.Timestamp
		if existing, ok := timestampScores[ts]; ok {
			existing.score += a.ZScore
			existing.count++
			if a.Severity == SeverityCritical {
				existing.anomaly.Severity = SeverityCritical
			} else if a.Severity == SeverityHigh && existing.anomaly.Severity != SeverityCritical {
				existing.anomaly.Severity = SeverityHigh
			}
		} else {
			timestampScores[ts] = &anomalyScore{
				anomaly: a,
				score:   a.ZScore,
				count:   1,
			}
		}
	}

	var result []DetectedAnomaly
	for _, as := range timestampScores {
		as.anomaly.ZScore = as.score / float64(as.count)
		if as.count >= 3 {
			as.anomaly.Detail = fmt.Sprintf("%s (detected by %d methods)", as.anomaly.Detail, as.count)
		}
		result = append(result, as.anomaly)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

func (d *AdvancedAnomalyDetector) PredictNext(values []TimeSeriesPoint, steps int) []float64 {
	if len(values) < d.config.MinSamples*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	period := d.detectSeasonalPeriod(values)
	decomp := d.DecomposeSTL(values, period)
	if decomp == nil {
		return nil
	}

	trendSlope := d.calculateTrendSlope(decomp.Trend)

	predictions := make([]float64, steps)
	for i := 0; i < steps; i++ {
		futureTrend := decomp.Trend[n-1] + trendSlope*float64(i+1)
		futureSeasonal := decomp.Seasonal[(n+i)%period]
		predictions[i] = futureTrend + futureSeasonal
	}

	return predictions
}

func (d *AdvancedAnomalyDetector) calculateTrendSlope(trend []float64) float64 {
	n := len(trend)
	if n < 2 {
		return 0
	}

	recent := trend[maxInt(0, n-10):]
	if len(recent) < 2 {
		return 0
	}

	var sumX, sumY, sumXY, sumX2 float64
	for i, y := range recent {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	m := float64(len(recent))
	denominator := m*sumX2 - sumX*sumX
	if denominator == 0 {
		return 0
	}

	return (m*sumXY - sumX*sumY) / denominator
}

func (d *AdvancedAnomalyDetector) DetectVolatilityAnomalies(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	windowSize := 6
	if n < windowSize*2 {
		return nil
	}

	allCV := make([]float64, 0, n-windowSize+1)
	windowCVs := make([]float64, n-windowSize+1)

	for i := 0; i <= n-windowSize; i++ {
		window := vals[i : i+windowSize]
		m := mean(window)
		s := stdDev(window)
		if m > 0 {
			windowCVs[i] = s / m
			allCV = append(allCV, windowCVs[i])
		}
	}

	if len(allCV) < 3 {
		return nil
	}

	cvMedian := median(allCV)
	cvMAD := mad(allCV)

	var anomalies []DetectedAnomaly
	for i := 0; i <= n-windowSize; i++ {
		if windowCVs[i] == 0 {
			continue
		}

		deviation := windowCVs[i] - cvMedian
		scaledDeviation := deviation / (cvMAD * 1.4826)

		if scaledDeviation > 3 {
			severity := SeverityMedium
			if scaledDeviation > 5 {
				severity = SeverityHigh
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      AnomalyVolatilitySpike,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     windowCVs[i],
				Baseline:  cvMedian,
				ZScore:    scaledDeviation,
				Detail:    fmt.Sprintf("Volatility spike: CV=%.2f (baseline=%.2f)", windowCVs[i], cvMedian),
				Duration:  windowSize,
			})
		}
	}

	return anomalies
}

func (d *AdvancedAnomalyDetector) DetectSustainedAnomaliesAdvanced(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	period := d.detectSeasonalPeriod(values)
	decomp := d.DecomposeSTL(values, period)
	if decomp == nil {
		return nil
	}

	vals := extractValues(values)
	n := len(vals)

	windowSize := period / 2
	if windowSize < 4 {
		windowSize = 4
	}

	var anomalies []DetectedAnomaly

	for i := windowSize; i <= n-windowSize; i += windowSize / 2 {
		window := decomp.Residual[i : i+windowSize]

		windowMean := mean(window)
		windowStd := stdDev(window)

		if windowStd == 0 {
			continue
		}

		overallResidualMean := mean(decomp.Residual)
		overallResidualStd := stdDev(decomp.Residual)

		zScore := (windowMean - overallResidualMean) / overallResidualStd
		if math.Abs(zScore) > 2.5 {
			severity := SeverityMedium
			if math.Abs(zScore) > 3.5 {
				severity = SeverityHigh
			}

			anomalyType := AnomalySustainedHighQPS
			if zScore < 0 {
				anomalyType = AnomalySustainedLowQPS
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     mean(vals[i : i+windowSize]),
				Baseline:  mean(vals),
				ZScore:    zScore,
				Detail:    fmt.Sprintf("Sustained anomaly over %d periods: avg=%.0f (baseline=%.0f)", windowSize, mean(vals[i:i+windowSize]), mean(vals)),
				Duration:  windowSize,
				EndTime:   values[i+windowSize-1].Timestamp,
			})
		}
	}

	return anomalies
}

func sum(vals []float64) float64 {
	var s float64
	for _, v := range vals {
		s += v
	}
	return s
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
