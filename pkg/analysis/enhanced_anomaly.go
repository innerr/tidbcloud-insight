package analysis

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type DynamicThresholdConfig struct {
	WindowSize         int
	WarmupPeriod       int
	LowerBoundFactor   float64
	UpperBoundFactor   float64
	UseMedian          bool
	AdaptiveFactor     bool
	SeasonalAdjustment bool
	Period             int
	SmoothingAlpha     float64
	AnomalyRatio       float64
}

func DefaultDynamicThresholdConfig() DynamicThresholdConfig {
	return DynamicThresholdConfig{
		WindowSize:         24,
		WarmupPeriod:       10,
		LowerBoundFactor:   3.0,
		UpperBoundFactor:   3.0,
		UseMedian:          true,
		AdaptiveFactor:     true,
		SeasonalAdjustment: true,
		Period:             24,
		SmoothingAlpha:     0.2,
		AnomalyRatio:       0.05,
	}
}

type DynamicThresholdDetector struct {
	config DynamicThresholdConfig
}

func NewDynamicThresholdDetector(config DynamicThresholdConfig) *DynamicThresholdDetector {
	return &DynamicThresholdDetector{config: config}
}

type ThresholdState struct {
	Baseline    float64
	LowerBound  float64
	UpperBound  float64
	StdDev      float64
	MAD         float64
	Volatility  float64
	SeasonalIdx int
	WindowMean  float64
	WindowStd   float64
}

func (d *DynamicThresholdDetector) ComputeThresholds(values []TimeSeriesPoint) ([]ThresholdState, []DetectedAnomaly) {
	if len(values) < d.config.WarmupPeriod {
		return nil, nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	n := len(values)
	vals := extractValues(values)

	states := make([]ThresholdState, n)
	var anomalies []DetectedAnomaly

	var seasonalPattern []float64
	if d.config.SeasonalAdjustment && n >= d.config.Period*2 {
		seasonalPattern = d.extractSeasonalPattern(vals, d.config.Period)
	}

	window := make([]float64, 0, d.config.WindowSize)

	for i := 0; i < n; i++ {
		window = append(window, vals[i])
		if len(window) > d.config.WindowSize {
			window = window[1:]
		}

		var baseline, dispersion float64
		if d.config.UseMedian {
			baseline = median(window)
			dispersion = mad(window) * 1.4826
		} else {
			baseline = mean(window)
			dispersion = stdDev(window)
		}

		if dispersion == 0 {
			dispersion = baseline * 0.01
		}

		lowerFactor := d.config.LowerBoundFactor
		upperFactor := d.config.UpperBoundFactor

		if d.config.AdaptiveFactor {
			cv := dispersion / baseline
			if cv > 0.5 {
				lowerFactor *= 1.5
				upperFactor *= 1.5
			} else if cv > 0.3 {
				lowerFactor *= 1.2
				upperFactor *= 1.2
			}
		}

		lowerBound := baseline - lowerFactor*dispersion
		upperBound := baseline + upperFactor*dispersion

		if d.config.SeasonalAdjustment && len(seasonalPattern) > 0 {
			seasonalIdx := i % d.config.Period
			seasonalAdjustment := seasonalPattern[seasonalIdx]
			lowerBound += seasonalAdjustment
			upperBound += seasonalAdjustment
		}

		states[i] = ThresholdState{
			Baseline:    baseline,
			LowerBound:  lowerBound,
			UpperBound:  upperBound,
			StdDev:      dispersion,
			MAD:         mad(window),
			Volatility:  stdDev(window) / baseline,
			SeasonalIdx: i % d.config.Period,
			WindowMean:  mean(window),
			WindowStd:   stdDev(window),
		}

		if i >= d.config.WarmupPeriod {
			if vals[i] < lowerBound || vals[i] > upperBound {
				deviation := 0.0
				if vals[i] < lowerBound {
					deviation = (lowerBound - vals[i]) / dispersion
				} else {
					deviation = (vals[i] - upperBound) / dispersion
				}

				severity := SeverityMedium
				if deviation > 2 {
					severity = SeverityHigh
				}
				if deviation > 3 {
					severity = SeverityCritical
				}

				anomalyType := AnomalyQPSSpike
				if vals[i] < baseline {
					anomalyType = AnomalyQPSDrop
				}

				anomalies = append(anomalies, DetectedAnomaly{
					Type:      anomalyType,
					Severity:  severity,
					Timestamp: values[i].Timestamp,
					TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
					Value:     vals[i],
					Baseline:  baseline,
					ZScore:    deviation,
					Detail:    fmt.Sprintf("Dynamic threshold breach: %.0f (bounds=[%.0f, %.0f])", vals[i], lowerBound, upperBound),
				})
			}
		}
	}

	return states, anomalies
}

func (d *DynamicThresholdDetector) extractSeasonalPattern(vals []float64, period int) []float64 {
	n := len(vals)

	detrended := make([]float64, n)
	trendWindow := period
	if trendWindow < 5 {
		trendWindow = 5
	}

	for i := 0; i < n; i++ {
		start := i - trendWindow/2
		end := i + trendWindow/2 + 1
		if start < 0 {
			start = 0
		}
		if end > n {
			end = n
		}
		detrended[i] = vals[i] - median(vals[start:end])
	}

	pattern := make([]float64, period)
	for p := 0; p < period; p++ {
		var sum float64
		var count int
		for i := p; i < n; i += period {
			sum += detrended[i]
			count++
		}
		if count > 0 {
			pattern[p] = sum / float64(count)
		}
	}

	overallMean := mean(pattern)
	for i := range pattern {
		pattern[i] -= overallMean
	}

	return pattern
}

type ContextualAnomalyDetector struct {
	config ContextualAnomalyConfig
}

type ContextualAnomalyConfig struct {
	HourlyProfiles   bool
	DailyProfiles    bool
	WeeklyProfiles   bool
	MinSamplesPerCtx int
	Threshold        float64
}

func DefaultContextualAnomalyConfig() ContextualAnomalyConfig {
	return ContextualAnomalyConfig{
		HourlyProfiles:   true,
		DailyProfiles:    true,
		WeeklyProfiles:   true,
		MinSamplesPerCtx: 5,
		Threshold:        3.0,
	}
}

func NewContextualAnomalyDetector(config ContextualAnomalyConfig) *ContextualAnomalyDetector {
	return &ContextualAnomalyDetector{config: config}
}

func (d *ContextualAnomalyDetector) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamplesPerCtx*7 {
		return nil
	}

	hourlyStats := make(map[int][]float64)
	dailyStats := make(map[int][]float64)
	weeklyStats := make(map[int][]float64)

	for _, v := range values {
		t := time.Unix(v.Timestamp, 0)
		hour := t.Hour()
		weekday := int(t.Weekday())
		hourOfWeek := weekday*24 + hour

		hourlyStats[hour] = append(hourlyStats[hour], v.Value)
		dailyStats[weekday] = append(dailyStats[weekday], v.Value)
		weeklyStats[hourOfWeek] = append(weeklyStats[hourOfWeek], v.Value)
	}

	hourlyBaseline := make(map[int]struct{ m, s float64 })
	for hour, vals := range hourlyStats {
		if len(vals) >= d.config.MinSamplesPerCtx {
			hourlyBaseline[hour] = struct{ m, s float64 }{median(vals), mad(vals) * 1.4826}
		}
	}

	dailyBaseline := make(map[int]struct{ m, s float64 })
	for day, vals := range dailyStats {
		if len(vals) >= d.config.MinSamplesPerCtx {
			dailyBaseline[day] = struct{ m, s float64 }{median(vals), mad(vals) * 1.4826}
		}
	}

	weeklyBaseline := make(map[int]struct{ m, s float64 })
	for hourOfWeek, vals := range weeklyStats {
		if len(vals) >= d.config.MinSamplesPerCtx {
			weeklyBaseline[hourOfWeek] = struct{ m, s float64 }{median(vals), mad(vals) * 1.4826}
		}
	}

	var anomalies []DetectedAnomaly

	for _, v := range values {
		t := time.Unix(v.Timestamp, 0)
		hour := t.Hour()
		weekday := int(t.Weekday())
		hourOfWeek := weekday*24 + hour

		var scores []float64
		var baselines []float64

		if d.config.HourlyProfiles {
			if stats, ok := hourlyBaseline[hour]; ok && stats.s > 0 {
				z := math.Abs(v.Value-stats.m) / stats.s
				scores = append(scores, z)
				baselines = append(baselines, stats.m)
			}
		}

		if d.config.DailyProfiles {
			if stats, ok := dailyBaseline[weekday]; ok && stats.s > 0 {
				z := math.Abs(v.Value-stats.m) / stats.s
				scores = append(scores, z)
				baselines = append(baselines, stats.m)
			}
		}

		if d.config.WeeklyProfiles {
			if stats, ok := weeklyBaseline[hourOfWeek]; ok && stats.s > 0 {
				z := math.Abs(v.Value-stats.m) / stats.s
				scores = append(scores, z)
				baselines = append(baselines, stats.m)
			}
		}

		if len(scores) == 0 {
			continue
		}

		avgScore := mean(scores)
		avgBaseline := mean(baselines)

		if avgScore > d.config.Threshold {
			severity := SeverityMedium
			if avgScore > d.config.Threshold*1.5 {
				severity = SeverityHigh
			}
			if avgScore > d.config.Threshold*2 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if v.Value < avgBaseline {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: v.Timestamp,
				TimeStr:   time.Unix(v.Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     v.Value,
				Baseline:  avgBaseline,
				ZScore:    avgScore,
				Detail:    fmt.Sprintf("Contextual anomaly: %.0f (hourly/daily/weekly baseline=%.0f)", v.Value, avgBaseline),
			})
		}
	}

	return MergeConsecutiveAnomalies(anomalies)
}

type RobustSTLDecomposer struct {
	config RobustSTLConfig
}

type RobustSTLConfig struct {
	Period         int
	SeasonalWindow int
	TrendWindow    int
	LowPassWindow  int
	SeasonalDegree int
	TrendDegree    int
	InnerLoop      int
	OuterLoop      int
	RobustWeight   float64
}

func DefaultRobustSTLConfig() RobustSTLConfig {
	return RobustSTLConfig{
		Period:         24,
		SeasonalWindow: 7,
		TrendWindow:    7,
		LowPassWindow:  7,
		SeasonalDegree: 1,
		TrendDegree:    1,
		InnerLoop:      2,
		OuterLoop:      1,
		RobustWeight:   0.3,
	}
}

func NewRobustSTLDecomposer(config RobustSTLConfig) *RobustSTLDecomposer {
	return &RobustSTLDecomposer{config: config}
}

func (d *RobustSTLDecomposer) Decompose(values []TimeSeriesPoint) *DecompositionResult {
	if len(values) < d.config.Period*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	n := len(values)
	vals := extractValues(values)

	seasonal := make([]float64, n)
	trend := make([]float64, n)

	weights := make([]float64, n)
	for i := range weights {
		weights[i] = 1.0
	}

	for outer := 0; outer < d.config.OuterLoop; outer++ {
		for inner := 0; inner < d.config.InnerLoop; inner++ {
			detrended := make([]float64, n)
			for i := 0; i < n; i++ {
				detrended[i] = vals[i] - trend[i]
			}

			cycleSubSeries := make([][]float64, d.config.Period)
			for i := 0; i < n; i++ {
				idx := i % d.config.Period
				cycleSubSeries[idx] = append(cycleSubSeries[idx], detrended[i])
			}

			smoothedSubSeries := make([][]float64, d.config.Period)
			for p := 0; p < d.config.Period; p++ {
				smoothedSubSeries[p] = d.loessSmooth(cycleSubSeries[p], d.config.SeasonalWindow, d.config.SeasonalDegree)
			}

			for i := 0; i < n; i++ {
				p := i % d.config.Period
				seriesIdx := i / d.config.Period
				if seriesIdx < len(smoothedSubSeries[p]) {
					seasonal[i] = smoothedSubSeries[p][seriesIdx]
				}
			}

			seasonal = d.lowPassFilter(seasonal, d.config.LowPassWindow)

			seasonallyAdjusted := make([]float64, n)
			for i := 0; i < n; i++ {
				seasonallyAdjusted[i] = vals[i] - seasonal[i]
			}

			trend = d.loessSmooth(seasonallyAdjusted, d.config.TrendWindow, d.config.TrendDegree)
		}

		residuals := make([]float64, n)
		for i := 0; i < n; i++ {
			residuals[i] = vals[i] - trend[i] - seasonal[i]
		}

		m := median(residuals)
		mad := mad(residuals) * 1.4826
		if mad == 0 {
			mad = stdDev(residuals)
		}

		for i := 0; i < n; i++ {
			z := math.Abs(residuals[i]-m) / (6 * mad)
			if z > 1 {
				z = 1
			}
			weights[i] = (1 - z*z) * (1 - z*z)
		}
	}

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

func (d *RobustSTLDecomposer) loessSmooth(data []float64, window, degree int) []float64 {
	n := len(data)
	if n == 0 {
		return data
	}

	smoothed := make([]float64, n)
	halfWindow := window / 2

	for i := 0; i < n; i++ {
		start := i - halfWindow
		end := i + halfWindow + 1
		if start < 0 {
			start = 0
		}
		if end > n {
			end = n
		}

		weights := make([]float64, end-start)
		for j := start; j < end; j++ {
			distance := math.Abs(float64(i - j))
			weights[j-start] = math.Exp(-distance * distance / float64(halfWindow*halfWindow))
		}

		if degree == 0 {
			var sumWeighted, sumWeights float64
			for j := start; j < end; j++ {
				sumWeighted += data[j] * weights[j-start]
				sumWeights += weights[j-start]
			}
			if sumWeights > 0 {
				smoothed[i] = sumWeighted / sumWeights
			} else {
				smoothed[i] = data[i]
			}
		} else {
			var sumX, sumY, sumXY, sumX2, sumW float64
			for j := start; j < end; j++ {
				x := float64(j - i)
				y := data[j]
				w := weights[j-start]
				sumX += x * w
				sumY += y * w
				sumXY += x * y * w
				sumX2 += x * x * w
				sumW += w
			}

			if sumW > 0 {
				denominator := sumW*sumX2 - sumX*sumX
				if denominator != 0 {
					intercept := (sumX2*sumY - sumX*sumXY) / denominator
					smoothed[i] = intercept
				} else {
					smoothed[i] = sumY / sumW
				}
			} else {
				smoothed[i] = data[i]
			}
		}
	}

	return smoothed
}

func (d *RobustSTLDecomposer) lowPassFilter(data []float64, window int) []float64 {
	n := len(data)
	filtered := make([]float64, n)

	halfWindow := window / 2
	for i := 0; i < n; i++ {
		start := i - halfWindow
		end := i + halfWindow + 1
		if start < 0 {
			start = 0
		}
		if end > n {
			end = n
		}
		filtered[i] = mean(data[start:end])
	}

	return filtered
}

type PeakDetector struct {
	config PeakDetectorConfig
}

type PeakDetectorConfig struct {
	MinPeakHeight    float64
	MinPeakDistance  int
	ProminenceFactor float64
	WidthRange       []int
}

func DefaultPeakDetectorConfig() PeakDetectorConfig {
	return PeakDetectorConfig{
		MinPeakHeight:    0,
		MinPeakDistance:  5,
		ProminenceFactor: 0.5,
		WidthRange:       []int{1, 100},
	}
}

func NewPeakDetector(config PeakDetectorConfig) *PeakDetector {
	return &PeakDetector{config: config}
}

type Peak struct {
	Index      int
	Timestamp  int64
	Value      float64
	Prominence float64
	Width      int
	LeftBase   int
	RightBase  int
	IsLocal    bool
}

func (d *PeakDetector) DetectPeaks(values []TimeSeriesPoint) []Peak {
	if len(values) < 5 {
		return nil
	}

	vals := extractValues(values)

	peakIndices := d.findLocalMaxima(vals)

	var peaks []Peak
	for _, idx := range peakIndices {
		prominence, leftBase, rightBase := d.calculateProminence(vals, idx)
		width := d.calculateWidth(vals, idx, prominence/2)

		if prominence < d.config.MinPeakHeight {
			continue
		}

		peaks = append(peaks, Peak{
			Index:      idx,
			Timestamp:  values[idx].Timestamp,
			Value:      vals[idx],
			Prominence: prominence,
			Width:      width,
			LeftBase:   leftBase,
			RightBase:  rightBase,
			IsLocal:    true,
		})
	}

	if d.config.MinPeakDistance > 1 {
		peaks = d.filterByDistance(peaks, d.config.MinPeakDistance)
	}

	return peaks
}

func (d *PeakDetector) findLocalMaxima(vals []float64) []int {
	n := len(vals)
	var peaks []int

	for i := 1; i < n-1; i++ {
		if vals[i] > vals[i-1] && vals[i] > vals[i+1] {
			peaks = append(peaks, i)
		}
	}

	return peaks
}

func (d *PeakDetector) calculateProminence(vals []float64, peakIdx int) (float64, int, int) {
	n := len(vals)
	peakVal := vals[peakIdx]

	leftBase := peakIdx
	leftMin := peakVal
	for i := peakIdx - 1; i >= 0; i-- {
		if vals[i] < leftMin {
			leftMin = vals[i]
			leftBase = i
		}
		if vals[i] > peakVal {
			break
		}
	}

	rightBase := peakIdx
	rightMin := peakVal
	for i := peakIdx + 1; i < n; i++ {
		if vals[i] < rightMin {
			rightMin = vals[i]
			rightBase = i
		}
		if vals[i] > peakVal {
			break
		}
	}

	baseVal := math.Max(leftMin, rightMin)
	prominence := peakVal - baseVal

	return prominence, leftBase, rightBase
}

func (d *PeakDetector) calculateWidth(vals []float64, peakIdx int, heightThreshold float64) int {
	peakVal := vals[peakIdx]
	targetHeight := peakVal - heightThreshold

	leftWidth := 0
	for i := peakIdx - 1; i >= 0; i-- {
		leftWidth++
		if vals[i] < targetHeight {
			break
		}
	}

	rightWidth := 0
	for i := peakIdx + 1; i < len(vals); i++ {
		rightWidth++
		if vals[i] < targetHeight {
			break
		}
	}

	return leftWidth + rightWidth + 1
}

func (d *PeakDetector) filterByDistance(peaks []Peak, minDistance int) []Peak {
	if len(peaks) == 0 {
		return peaks
	}

	sort.Slice(peaks, func(i, j int) bool {
		return peaks[i].Prominence > peaks[j].Prominence
	})

	var filtered []Peak
	usedIndices := make(map[int]bool)

	for _, peak := range peaks {
		canAdd := true
		for usedIdx := range usedIndices {
			if abs(peak.Index-usedIdx) < minDistance {
				canAdd = false
				break
			}
		}

		if canAdd {
			filtered = append(filtered, peak)
			usedIndices[peak.Index] = true
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Index < filtered[j].Index
	})

	return filtered
}

func (d *PeakDetector) DetectPeakAnomalies(values []TimeSeriesPoint) []DetectedAnomaly {
	peaks := d.DetectPeaks(values)
	if len(peaks) == 0 {
		return nil
	}

	vals := extractValues(values)
	m := median(vals)
	std := stdDev(vals)

	var anomalies []DetectedAnomaly

	for _, peak := range peaks {
		zScore := (peak.Value - m) / std

		severity := SeverityMedium
		if peak.Prominence > std*3 {
			severity = SeverityHigh
		}
		if peak.Prominence > std*5 {
			severity = SeverityCritical
		}

		anomalies = append(anomalies, DetectedAnomaly{
			Type:      AnomalyQPSSpike,
			Severity:  severity,
			Timestamp: peak.Timestamp,
			TimeStr:   time.Unix(peak.Timestamp, 0).Format("2006-01-02 15:04"),
			Value:     peak.Value,
			Baseline:  m,
			ZScore:    zScore,
			Detail:    fmt.Sprintf("Peak detected: %.0f (prominence=%.0f, width=%d)", peak.Value, peak.Prominence, peak.Width),
		})
	}

	return anomalies
}
