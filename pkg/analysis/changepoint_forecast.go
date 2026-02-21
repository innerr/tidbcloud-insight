package analysis

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type ChangePoint struct {
	Index       int
	Timestamp   int64
	TimeStr     string
	MeanBefore  float64
	MeanAfter   float64
	StdBefore   float64
	StdAfter    float64
	Confidence  float64
	Type        string
	Description string
}

type ChangePointDetector struct {
	config ChangePointConfig
}

type ChangePointConfig struct {
	MinSegmentSize    int
	Threshold         float64
	MaxChangePoints   int
	UseCUSUM          bool
	UsePelt           bool
	Penalty           float64
	UseBootstrap      bool
	BootstrapSamples  int
	SignificanceLevel float64
}

func DefaultChangePointConfig() ChangePointConfig {
	return ChangePointConfig{
		MinSegmentSize:    10,
		Threshold:         2.0,
		MaxChangePoints:   10,
		UseCUSUM:          true,
		UsePelt:           true,
		Penalty:           3.0,
		UseBootstrap:      false,
		BootstrapSamples:  100,
		SignificanceLevel: 0.05,
	}
}

func NewChangePointDetector(config ChangePointConfig) *ChangePointDetector {
	return &ChangePointDetector{config: config}
}

func (d *ChangePointDetector) DetectCUSUM(values []TimeSeriesPoint) []ChangePoint {
	if len(values) < d.config.MinSegmentSize*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	overallMean := mean(vals)
	overallStd := stdDev(vals)
	if overallStd == 0 {
		return nil
	}

	k := overallStd / 2.0
	h := d.config.Threshold * overallStd

	posSum := 0.0
	negSum := 0.0
	posMax := 0.0
	negMax := 0.0

	var changePoints []ChangePoint
	lastCP := 0

	for i := 0; i < n; i++ {
		posSum = math.Max(0, posSum+vals[i]-overallMean-k)
		negSum = math.Max(0, negSum-overallMean-vals[i]-k)

		if posSum > h || negSum > h {
			if i-lastCP >= d.config.MinSegmentSize {
				beforeMean := mean(vals[lastCP:i])
				afterMean := mean(vals[i:minInt2(n, i+d.config.MinSegmentSize)])

				confidence := math.Abs(afterMean-beforeMean) / overallStd

				cp := ChangePoint{
					Index:      i,
					Timestamp:  values[i].Timestamp,
					TimeStr:    time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
					MeanBefore: beforeMean,
					MeanAfter:  afterMean,
					StdBefore:  stdDev(vals[lastCP:i]),
					StdAfter:   stdDev(vals[i:minInt2(n, i+d.config.MinSegmentSize)]),
					Confidence: confidence,
					Type:       "CUSUM",
					Description: fmt.Sprintf("CUSUM detected change: %.0f -> %.0f (confidence: %.2f)",
						beforeMean, afterMean, confidence),
				}
				changePoints = append(changePoints, cp)
				lastCP = i
				posSum = 0
				negSum = 0
				posMax = 0
				negMax = 0
			}
		}

		if posSum > posMax {
			posMax = posSum
		}
		if negSum > negMax {
			negMax = negSum
		}
	}

	return changePoints
}

func (d *ChangePointDetector) DetectPELT(values []TimeSeriesPoint) []ChangePoint {
	if len(values) < d.config.MinSegmentSize*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	cost := func(start, end int) float64 {
		if end <= start {
			return 0
		}
		segment := vals[start:end]
		m := mean(segment)
		var cost float64
		for _, v := range segment {
			cost += (v - m) * (v - m)
		}
		return cost
	}

	penalty := d.config.Penalty * variance(vals) * math.Log(float64(n))

	dp := make([]float64, n+1)
	dp[0] = 0
	for i := 1; i <= n; i++ {
		dp[i] = math.Inf(1)
	}

	lastChange := make([]int, n+1)

	for i := 1; i <= n; i++ {
		minJ := maxInt2(0, i-2*d.config.MinSegmentSize)
		for j := minJ; j <= i-d.config.MinSegmentSize; j++ {
			if j < 0 {
				continue
			}
			c := dp[j] + cost(j, i) + penalty
			if c < dp[i] {
				dp[i] = c
				lastChange[i] = j
			}
		}
	}

	var changePoints []ChangePoint
	current := n
	var cpIndices []int

	for current > d.config.MinSegmentSize {
		prev := lastChange[current]
		if prev <= 0 {
			break
		}
		cpIndices = append(cpIndices, prev)
		current = prev
	}

	sort.Ints(cpIndices)

	for idx, cpIdx := range cpIndices {
		if cpIdx <= 0 || cpIdx >= n {
			continue
		}

		startIdx := 0
		if idx > 0 {
			startIdx = cpIndices[idx-1]
		}
		endIdx := n
		if idx < len(cpIndices)-1 {
			endIdx = cpIndices[idx+1]
		}

		beforeMean := mean(vals[startIdx:cpIdx])
		afterMean := mean(vals[cpIdx:minInt2(n, endIdx)])
		beforeStd := stdDev(vals[startIdx:cpIdx])
		afterStd := stdDev(vals[cpIdx:minInt2(n, endIdx)])

		confidence := 0.0
		if beforeStd > 0 || afterStd > 0 {
			confidence = math.Abs(afterMean-beforeMean) / math.Max(beforeStd, afterStd)
		}

		cp := ChangePoint{
			Index:      cpIdx,
			Timestamp:  values[cpIdx].Timestamp,
			TimeStr:    time.Unix(values[cpIdx].Timestamp, 0).Format("2006-01-02 15:04"),
			MeanBefore: beforeMean,
			MeanAfter:  afterMean,
			StdBefore:  beforeStd,
			StdAfter:   afterStd,
			Confidence: confidence,
			Type:       "PELT",
			Description: fmt.Sprintf("PELT detected change: %.0f -> %.0f (confidence: %.2f)",
				beforeMean, afterMean, confidence),
		}
		changePoints = append(changePoints, cp)
	}

	return changePoints
}

func (d *ChangePointDetector) DetectSlidingWindow(values []TimeSeriesPoint) []ChangePoint {
	if len(values) < d.config.MinSegmentSize*3 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	windowSize := d.config.MinSegmentSize
	var changePoints []ChangePoint

	for i := windowSize; i < n-windowSize; i++ {
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

		if tStat > d.config.Threshold {
			confidence := tStat / d.config.Threshold

			cp := ChangePoint{
				Index:       i,
				Timestamp:   values[i].Timestamp,
				TimeStr:     time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				MeanBefore:  beforeMean,
				MeanAfter:   afterMean,
				StdBefore:   beforeStd,
				StdAfter:    afterStd,
				Confidence:  confidence,
				Type:        "SlidingWindow",
				Description: fmt.Sprintf("T-stat=%.2f: %.0f -> %.0f", tStat, beforeMean, afterMean),
			}
			changePoints = append(changePoints, cp)
			i += windowSize / 2
		}
	}

	return changePoints
}

func (d *ChangePointDetector) DetectEDivisive(values []TimeSeriesPoint) []ChangePoint {
	if len(values) < d.config.MinSegmentSize*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	var changePoints []ChangePoint
	segments := []struct{ start, end int }{{0, n}}

	for len(segments) > 0 && len(changePoints) < d.config.MaxChangePoints {
		var newSegments []struct{ start, end int }

		for _, seg := range segments {
			if seg.end-seg.start < d.config.MinSegmentSize*2 {
				continue
			}

			bestCP := -1
			bestStat := 0.0

			for i := seg.start + d.config.MinSegmentSize; i < seg.end-d.config.MinSegmentSize; i++ {
				stat := d.calculateEDivisiveStat(vals, seg.start, i, seg.end)
				if stat > bestStat {
					bestStat = stat
					bestCP = i
				}
			}

			threshold := d.calculateEDivisiveThreshold(seg.end-seg.start, d.config.SignificanceLevel)
			if bestStat > threshold && bestCP >= 0 {
				beforeMean := mean(vals[seg.start:bestCP])
				afterMean := mean(vals[bestCP:seg.end])
				beforeStd := stdDev(vals[seg.start:bestCP])
				afterStd := stdDev(vals[bestCP:seg.end])

				confidence := bestStat / threshold

				cp := ChangePoint{
					Index:      bestCP,
					Timestamp:  values[bestCP].Timestamp,
					TimeStr:    time.Unix(values[bestCP].Timestamp, 0).Format("2006-01-02 15:04"),
					MeanBefore: beforeMean,
					MeanAfter:  afterMean,
					StdBefore:  beforeStd,
					StdAfter:   afterStd,
					Confidence: confidence,
					Type:       "EDivisive",
					Description: fmt.Sprintf("E-Divisive: %.0f -> %.0f (stat=%.2f)",
						beforeMean, afterMean, bestStat),
				}
				changePoints = append(changePoints, cp)

				newSegments = append(newSegments,
					struct{ start, end int }{seg.start, bestCP},
					struct{ start, end int }{bestCP, seg.end},
				)
			}
		}

		segments = newSegments
	}

	sort.Slice(changePoints, func(i, j int) bool {
		return changePoints[i].Index < changePoints[j].Index
	})

	return changePoints
}

func (d *ChangePointDetector) calculateEDivisiveStat(vals []float64, start, cp, end int) float64 {
	n1 := cp - start
	n2 := end - cp
	n := n1 + n2

	var sumDiff float64
	for i := start; i < cp; i++ {
		for j := cp; j < end; j++ {
			diff := vals[i] - vals[j]
			sumDiff += diff * diff
		}
	}

	return sumDiff * float64(n1*n2) / float64(n)
}

func (d *ChangePointDetector) calculateEDivisiveThreshold(n int, alpha float64) float64 {
	z := 1.96
	if alpha < 0.01 {
		z = 2.576
	} else if alpha < 0.05 {
		z = 1.96
	} else if alpha < 0.1 {
		z = 1.645
	}

	return z * math.Sqrt(float64(n)) * 0.5
}

func (d *ChangePointDetector) DetectAllChangePoints(values []TimeSeriesPoint) []ChangePoint {
	if len(values) < d.config.MinSegmentSize*2 {
		return nil
	}

	changePointsMap := make(map[int]*ChangePoint)

	if d.config.UseCUSUM {
		cusumCPs := d.DetectCUSUM(values)
		for _, cp := range cusumCPs {
			key := cp.Index / d.config.MinSegmentSize
			if existing, ok := changePointsMap[key]; !ok || cp.Confidence > existing.Confidence {
				changePointsMap[key] = &cp
			}
		}
	}

	if d.config.UsePelt {
		peltCPs := d.DetectPELT(values)
		for _, cp := range peltCPs {
			key := cp.Index / d.config.MinSegmentSize
			if existing, ok := changePointsMap[key]; !ok || cp.Confidence > existing.Confidence {
				changePointsMap[key] = &cp
			}
		}
	}

	swCPs := d.DetectSlidingWindow(values)
	for _, cp := range swCPs {
		key := cp.Index / d.config.MinSegmentSize
		if existing, ok := changePointsMap[key]; !ok || cp.Confidence > existing.Confidence {
			changePointsMap[key] = &cp
		}
	}

	edivCPs := d.DetectEDivisive(values)
	for _, cp := range edivCPs {
		key := cp.Index / d.config.MinSegmentSize
		if existing, ok := changePointsMap[key]; !ok || cp.Confidence > existing.Confidence {
			changePointsMap[key] = &cp
		}
	}

	var changePoints []ChangePoint
	for _, cp := range changePointsMap {
		changePoints = append(changePoints, *cp)
	}

	sort.Slice(changePoints, func(i, j int) bool {
		return changePoints[i].Index < changePoints[j].Index
	})

	if len(changePoints) > d.config.MaxChangePoints {
		sort.Slice(changePoints, func(i, j int) bool {
			return changePoints[i].Confidence > changePoints[j].Confidence
		})
		changePoints = changePoints[:d.config.MaxChangePoints]
		sort.Slice(changePoints, func(i, j int) bool {
			return changePoints[i].Index < changePoints[j].Index
		})
	}

	return changePoints
}

type ForecastResult struct {
	Predictions []float64
	LowerBound  []float64
	UpperBound  []float64
	Method      string
	MAE         float64
	RMSE        float64
	MAPE        float64
	Horizon     int
}

type Forecaster struct {
	config ForecasterConfig
}

type ForecasterConfig struct {
	Method          string
	Alpha           float64
	Beta            float64
	Gamma           float64
	SeasonalPeriod  int
	Horizon         int
	ConfidenceLevel float64
	UseBoxCox       bool
	DampedTrend     bool
}

func DefaultForecasterConfig() ForecasterConfig {
	return ForecasterConfig{
		Method:          "auto",
		Alpha:           0.2,
		Beta:            0.1,
		Gamma:           0.1,
		SeasonalPeriod:  24,
		Horizon:         6,
		ConfidenceLevel: 0.95,
		UseBoxCox:       false,
		DampedTrend:     false,
	}
}

func NewForecaster(config ForecasterConfig) *Forecaster {
	return &Forecaster{config: config}
}

func (f *Forecaster) ForecastSimpleMovingAverage(values []TimeSeriesPoint, window, horizon int) *ForecastResult {
	if len(values) < window {
		return nil
	}

	vals := extractValues(values)
	n := len(vals)

	lastWindow := vals[n-window:]
	avg := mean(lastWindow)
	std := stdDev(lastWindow)

	z := 1.96
	if f.config.ConfidenceLevel == 0.99 {
		z = 2.576
	} else if f.config.ConfidenceLevel == 0.90 {
		z = 1.645
	}

	predictions := make([]float64, horizon)
	lowerBound := make([]float64, horizon)
	upperBound := make([]float64, horizon)

	for i := 0; i < horizon; i++ {
		predictions[i] = avg
		lowerBound[i] = avg - z*std*math.Sqrt(1+float64(i)/float64(window))
		upperBound[i] = avg + z*std*math.Sqrt(1+float64(i)/float64(window))
	}

	mae, rmse, mape := f.calculateErrors(vals, avg)

	return &ForecastResult{
		Predictions: predictions,
		LowerBound:  lowerBound,
		UpperBound:  upperBound,
		Method:      "SMA",
		MAE:         mae,
		RMSE:        rmse,
		MAPE:        mape,
		Horizon:     horizon,
	}
}

func (f *Forecaster) ForecastExponentialSmoothing(values []TimeSeriesPoint, horizon int) *ForecastResult {
	if len(values) < 3 {
		return nil
	}

	vals := extractValues(values)
	n := len(vals)
	alpha := f.config.Alpha

	ema := make([]float64, n)
	ema[0] = vals[0]
	for i := 1; i < n; i++ {
		ema[i] = alpha*vals[i] + (1-alpha)*ema[i-1]
	}

	predictions := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		predictions[i] = ema[n-1]
	}

	residuals := make([]float64, n-1)
	for i := 1; i < n; i++ {
		residuals[i-1] = vals[i] - ema[i-1]
	}
	std := stdDev(residuals)

	z := 1.96
	lowerBound := make([]float64, horizon)
	upperBound := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		lowerBound[i] = predictions[i] - z*std*math.Sqrt(1+float64(i)*alpha*alpha)
		upperBound[i] = predictions[i] + z*std*math.Sqrt(1+float64(i)*alpha*alpha)
	}

	mae, rmse, mape := f.calculateErrors(vals, ema[n-1])

	return &ForecastResult{
		Predictions: predictions,
		LowerBound:  lowerBound,
		UpperBound:  upperBound,
		Method:      "SES",
		MAE:         mae,
		RMSE:        rmse,
		MAPE:        mape,
		Horizon:     horizon,
	}
}

func (f *Forecaster) ForecastDoubleExponentialSmoothing(values []TimeSeriesPoint, horizon int) *ForecastResult {
	if len(values) < 5 {
		return nil
	}

	vals := extractValues(values)
	n := len(vals)
	alpha := f.config.Alpha
	beta := f.config.Beta

	level := make([]float64, n)
	trend := make([]float64, n)
	level[0] = vals[0]
	trend[0] = vals[1] - vals[0]

	for i := 1; i < n; i++ {
		level[i] = alpha*vals[i] + (1-alpha)*(level[i-1]+trend[i-1])
		trend[i] = beta*(level[i]-level[i-1]) + (1-beta)*trend[i-1]
	}

	predictions := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		predictions[i] = level[n-1] + float64(i+1)*trend[n-1]
	}

	residuals := make([]float64, n-2)
	for i := 2; i < n; i++ {
		pred := level[i-1] + trend[i-1]
		residuals[i-2] = vals[i] - pred
	}
	std := stdDev(residuals)

	z := 1.96
	lowerBound := make([]float64, horizon)
	upperBound := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		width := z * std * math.Sqrt(1+float64(i)*0.1)
		lowerBound[i] = predictions[i] - width
		upperBound[i] = predictions[i] + width
	}

	mae, rmse, mape := f.calculateErrors(vals, level[n-1])

	return &ForecastResult{
		Predictions: predictions,
		LowerBound:  lowerBound,
		UpperBound:  upperBound,
		Method:      "DES",
		MAE:         mae,
		RMSE:        rmse,
		MAPE:        mape,
		Horizon:     horizon,
	}
}

func (f *Forecaster) ForecastLinearRegression(values []TimeSeriesPoint, horizon int) *ForecastResult {
	if len(values) < 3 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)
	n := len(vals)

	var sumX, sumY, sumXY, sumX2 float64
	for i, y := range vals {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	m := float64(n)
	denominator := m*sumX2 - sumX*sumX
	if denominator == 0 {
		return nil
	}

	slope := (m*sumXY - sumX*sumY) / denominator
	intercept := (sumY - slope*sumX) / m

	predictions := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		predictions[i] = intercept + slope*float64(n+i)
	}

	var ssRes float64
	for i, y := range vals {
		pred := intercept + slope*float64(i)
		ssRes += (y - pred) * (y - pred)
	}
	se := math.Sqrt(ssRes / float64(n-2))

	meanX := sumX / m
	var sumXDiff float64
	for i := 0; i < n; i++ {
		sumXDiff += (float64(i) - meanX) * (float64(i) - meanX)
	}

	z := 1.96
	lowerBound := make([]float64, horizon)
	upperBound := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		x := float64(n + i)
		predSE := se * math.Sqrt(1+1/m+(x-meanX)*(x-meanX)/sumXDiff)
		lowerBound[i] = predictions[i] - z*predSE
		upperBound[i] = predictions[i] + z*predSE
	}

	mae, rmse, mape := f.calculateErrors(vals, intercept+slope*float64(n-1))

	return &ForecastResult{
		Predictions: predictions,
		LowerBound:  lowerBound,
		UpperBound:  upperBound,
		Method:      "LinearRegression",
		MAE:         mae,
		RMSE:        rmse,
		MAPE:        mape,
		Horizon:     horizon,
	}
}

func (f *Forecaster) ForecastSeasonalNaive(values []TimeSeriesPoint, period, horizon int) *ForecastResult {
	if len(values) < period*2 {
		return nil
	}

	vals := extractValues(values)
	n := len(vals)

	predictions := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		predictions[i] = vals[n-period+i%period]
	}

	recentVals := vals[maxInt2(0, n-period):]
	std := stdDev(recentVals)

	z := 1.96
	lowerBound := make([]float64, horizon)
	upperBound := make([]float64, horizon)
	for i := 0; i < horizon; i++ {
		lowerBound[i] = predictions[i] - z*std
		upperBound[i] = predictions[i] + z*std
	}

	mae, rmse, mape := f.calculateErrors(vals[n-period:], predictions[minInt2(horizon, period)])

	return &ForecastResult{
		Predictions: predictions,
		LowerBound:  lowerBound,
		UpperBound:  upperBound,
		Method:      "SeasonalNaive",
		MAE:         mae,
		RMSE:        rmse,
		MAPE:        mape,
		Horizon:     horizon,
	}
}

func (f *Forecaster) ForecastAuto(values []TimeSeriesPoint, horizon int) *ForecastResult {
	if len(values) < 5 {
		return nil
	}

	vals := extractValues(values)
	cv := coefficientOfVariation(vals)

	var result *ForecastResult

	if cv < 0.1 {
		result = f.ForecastSimpleMovingAverage(values, 5, horizon)
	} else if cv < 0.3 {
		result = f.ForecastExponentialSmoothing(values, horizon)
	} else {
		period := f.detectSeasonalPeriod(values)
		if period > 0 && len(vals) >= period*2 {
			result = f.ForecastSeasonalNaive(values, period, horizon)
		} else {
			trendStrength := f.detectTrendStrength(vals)
			if trendStrength > 0.5 {
				result = f.ForecastLinearRegression(values, horizon)
			} else {
				result = f.ForecastDoubleExponentialSmoothing(values, horizon)
			}
		}
	}

	return result
}

func (f *Forecaster) detectSeasonalPeriod(values []TimeSeriesPoint) int {
	n := len(values)
	if n < 48 {
		return 0
	}

	vals := extractValues(values)
	maxLag := minInt2(n/2, 168)

	bestPeriod := 0
	bestCorr := 0.3

	for lag := 6; lag < maxLag; lag++ {
		corr := f.calculateAutocorrelation(vals, lag)
		if corr > bestCorr {
			bestCorr = corr
			bestPeriod = lag
		}
	}

	return bestPeriod
}

func (f *Forecaster) calculateAutocorrelation(vals []float64, lag int) float64 {
	n := len(vals)
	if n <= lag {
		return 0
	}

	meanVal := mean(vals)
	var numerator, denominator float64

	for i := 0; i < n; i++ {
		denominator += (vals[i] - meanVal) * (vals[i] - meanVal)
	}

	if denominator == 0 {
		return 0
	}

	for i := lag; i < n; i++ {
		numerator += (vals[i] - meanVal) * (vals[i-lag] - meanVal)
	}

	return numerator / denominator
}

func (f *Forecaster) detectTrendStrength(vals []float64) float64 {
	n := len(vals)
	if n < 10 {
		return 0
	}

	first := mean(vals[:n/3])
	last := mean(vals[2*n/3:])

	if first == 0 {
		return 0
	}

	return math.Abs(last-first) / first
}

func (f *Forecaster) calculateErrors(actual []float64, predicted float64) (mae, rmse, mape float64) {
	n := len(actual)
	if n == 0 {
		return 0, 0, 0
	}

	var sumAE, sumSE, sumAPE float64
	validCount := 0

	for _, a := range actual {
		sumAE += math.Abs(a - predicted)
		sumSE += (a - predicted) * (a - predicted)
		if a != 0 {
			sumAPE += math.Abs(a-predicted) / math.Abs(a)
			validCount++
		}
	}

	mae = sumAE / float64(n)
	rmse = math.Sqrt(sumSE / float64(n))
	if validCount > 0 {
		mape = sumAPE / float64(validCount) * 100
	}

	return
}

func variance(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	return stdDev(vals) * stdDev(vals)
}
