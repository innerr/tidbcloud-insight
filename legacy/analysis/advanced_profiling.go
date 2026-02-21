package analysis

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type AdvancedProfilingConfig struct {
	EnableFFTAnalysis      bool
	EnableGrangerCausality bool
	EnableMultimodal       bool
	EnableTailFit          bool
	FFTMinSamples          int
	GrangerMaxLag          int
}

func DefaultAdvancedProfilingConfig() AdvancedProfilingConfig {
	return AdvancedProfilingConfig{
		EnableFFTAnalysis:      true,
		EnableGrangerCausality: false,
		EnableMultimodal:       true,
		EnableTailFit:          true,
		FFTMinSamples:          64,
		GrangerMaxLag:          10,
	}
}

type SQLTypeMultimodal struct {
	Type        string    `json:"type"`
	SampleCount int       `json:"sample_count"`
	Multimodal  bool      `json:"multimodal"`
	NumModes    int       `json:"num_modes"`
	DipPValue   float64   `json:"dip_p_value"`
	ModeValues  []float64 `json:"mode_values,omitempty"`
}

type SQLTypeTailDistribution struct {
	Type            string  `json:"type"`
	SampleCount     int     `json:"sample_count"`
	TailType        string  `json:"tail_type"`
	TailIndex       float64 `json:"tail_index"`
	GPDShape        float64 `json:"gpd_shape"`
	GPDScale        float64 `json:"gpd_scale"`
	ExtremeValueEst float64 `json:"extreme_value_est"`
}

type SQLTypeVolatilityMetrics struct {
	Type               string  `json:"type"`
	SampleCount        int     `json:"sample_count"`
	RealizedVolatility float64 `json:"realized_volatility"`
	ParkinsonHL        float64 `json:"parkinson_hl"`
	VolOfVol           float64 `json:"vol_of_vol"`
	VolatilityRatio    float64 `json:"volatility_ratio"`
	LeverageEffect     float64 `json:"leverage_effect"`
}

type SQLTypeRegimeDetection struct {
	Type        string    `json:"type"`
	SampleCount int       `json:"sample_count"`
	NumRegimes  int       `json:"num_regimes"`
	RegimeMeans []float64 `json:"regime_means,omitempty"`
	RegimeStds  []float64 `json:"regime_stds,omitempty"`
}

type AdvancedProfilingResult struct {
	FrequencyDomain          FrequencyAnalysis          `json:"frequency_domain"`
	Multimodal               MultimodalAnalysis         `json:"multimodal"`
	TailDistribution         TailDistribution           `json:"tail_distribution"`
	VolatilityMetrics        VolatilityMetrics          `json:"volatility_metrics"`
	RegimeDetection          RegimeAnalysis             `json:"regime_detection"`
	SQLTypeMultimodal        []SQLTypeMultimodal        `json:"sql_type_multimodal,omitempty"`
	SQLTypeTailDistribution  []SQLTypeTailDistribution  `json:"sql_type_tail_distribution,omitempty"`
	SQLTypeVolatilityMetrics []SQLTypeVolatilityMetrics `json:"sql_type_volatility_metrics,omitempty"`
	SQLTypeRegimeDetection   []SQLTypeRegimeDetection   `json:"sql_type_regime_detection,omitempty"`
}

type FrequencyAnalysis struct {
	DominantPeriods       []PeriodInfo `json:"dominant_periods"`
	SpectralEntropy       float64      `json:"spectral_entropy"`
	PeakFrequencies       []float64    `json:"peak_frequencies"`
	PeriodStrength        float64      `json:"period_strength"`
	HasDailyPattern       bool         `json:"has_daily_pattern"`
	HasWeeklyPattern      bool         `json:"has_weekly_pattern"`
	HasSemiMonthlyPattern bool         `json:"has_semi_monthly_pattern"`
	HasMonthlyPattern     bool         `json:"has_monthly_pattern"`
}

type PeriodInfo struct {
	Period     float64 `json:"period"`
	Power      float64 `json:"power"`
	Confidence float64 `json:"confidence"`
}

type MultimodalAnalysis struct {
	IsMultimodal  bool      `json:"is_multimodal"`
	NumModes      int       `json:"num_modes"`
	ModeLocations []float64 `json:"mode_locations"`
	ModeWeights   []float64 `json:"mode_weights"`
	DipStatistic  float64   `json:"dip_statistic"`
	DipPValue     float64   `json:"dip_p_value"`
}

type TailDistribution struct {
	LeftTailType    string  `json:"left_tail_type"`
	RightTailType   string  `json:"right_tail_type"`
	TailIndex       float64 `json:"tail_index"`
	GPDThreshold    float64 `json:"gpd_threshold"`
	GPDShape        float64 `json:"gpd_shape"`
	GPDScale        float64 `json:"gpd_scale"`
	ExtremeValueEst float64 `json:"extreme_value_est"`
}

type VolatilityMetrics struct {
	RealizedVolatility float64 `json:"realized_volatility"`
	ParkinsonHL        float64 `json:"parkinson_hl"`
	GarmanKlass        float64 `json:"garman_klass"`
	VolatilityRatio    float64 `json:"volatility_ratio"`
	VolOfVol           float64 `json:"vol_of_vol"`
	LeverageEffect     float64 `json:"leverage_effect"`
}

type RegimeAnalysis struct {
	NumRegimes      int       `json:"num_regimes"`
	RegimeMeans     []float64 `json:"regime_means"`
	RegimeStds      []float64 `json:"regime_stds"`
	RegimeDurations []int     `json:"regime_durations"`
	CurrentRegime   int       `json:"current_regime"`
	Transitions     [][]int   `json:"transitions"`
}

func AnalyzeAdvancedProfile(values []TimeSeriesPoint, config AdvancedProfilingConfig) *AdvancedProfilingResult {
	return AnalyzeAdvancedProfileWithSQLTypes(values, nil, config)
}

func AnalyzeAdvancedProfileWithSQLTypes(values []TimeSeriesPoint, sqlTypeData map[string][]TimeSeriesPoint, config AdvancedProfilingConfig) *AdvancedProfilingResult {
	if len(values) < 24 {
		return nil
	}

	result := &AdvancedProfilingResult{}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	vals := extractValues(values)

	if config.EnableFFTAnalysis && len(vals) >= config.FFTMinSamples {
		result.FrequencyDomain = analyzeFrequencyDomain(vals)
	}

	if config.EnableMultimodal {
		result.Multimodal = analyzeMultimodal(vals)
	}

	if config.EnableTailFit {
		result.TailDistribution = analyzeTailDistribution(vals)
	}

	result.VolatilityMetrics = analyzeVolatility(vals)
	result.RegimeDetection = analyzeRegimes(vals)

	if config.EnableMultimodal && len(sqlTypeData) > 0 {
		result.SQLTypeMultimodal = analyzeSQLTypeMultimodal(sqlTypeData)
	}

	if config.EnableTailFit && len(sqlTypeData) > 0 {
		result.SQLTypeTailDistribution = analyzeSQLTypeTailDistribution(sqlTypeData)
	}

	if len(sqlTypeData) > 0 {
		result.SQLTypeVolatilityMetrics = analyzeSQLTypeVolatility(sqlTypeData)
		result.SQLTypeRegimeDetection = analyzeSQLTypeRegimeDetection(sqlTypeData)
	}

	return result
}

func analyzeFrequencyDomain(vals []float64) FrequencyAnalysis {
	n := len(vals)
	result := FrequencyAnalysis{}

	m := mean(vals)
	normalized := make([]float64, n)
	for i, v := range vals {
		normalized[i] = v - m
	}

	fftLength := 1
	for fftLength < n {
		fftLength *= 2
	}

	padded := make([]float64, fftLength)
	copy(padded, normalized)

	powerSpectrum := computePowerSpectrum(padded)

	freqResolution := 1.0 / float64(fftLength)

	type freqPeak struct {
		freq  float64
		power float64
	}

	var peaks []freqPeak
	for i := 1; i < len(powerSpectrum)/2; i++ {
		if powerSpectrum[i] > powerSpectrum[i-1] && powerSpectrum[i] > powerSpectrum[i+1] {
			peaks = append(peaks, freqPeak{
				freq:  float64(i) * freqResolution,
				power: powerSpectrum[i],
			})
		}
	}

	sort.Slice(peaks, func(i, j int) bool {
		return peaks[i].power > peaks[j].power
	})

	totalPower := 0.0
	for _, p := range powerSpectrum[:len(powerSpectrum)/2] {
		totalPower += p
	}

	maxPeaks := 5
	if len(peaks) < maxPeaks {
		maxPeaks = len(peaks)
	}
	for i := 0; i < maxPeaks; i++ {
		if peaks[i].freq > 0 {
			period := 1.0 / peaks[i].freq
			confidence := peaks[i].power / totalPower

			result.DominantPeriods = append(result.DominantPeriods, PeriodInfo{
				Period:     period,
				Power:      peaks[i].power,
				Confidence: confidence,
			})

			result.PeakFrequencies = append(result.PeakFrequencies, peaks[i].freq)
		}
	}

	minPatternConfidence := 0.10
	for _, p := range result.DominantPeriods {
		if p.Confidence < minPatternConfidence {
			continue
		}
		if p.Period >= 23 && p.Period <= 25 {
			result.HasDailyPattern = true
			result.PeriodStrength = math.Max(result.PeriodStrength, p.Confidence)
		}
		if p.Period >= 167 && p.Period <= 169 {
			result.HasWeeklyPattern = true
		}
		if p.Period >= 350 && p.Period <= 380 {
			result.HasSemiMonthlyPattern = true
		}
		if p.Period >= 700 && p.Period <= 760 {
			result.HasMonthlyPattern = true
		}
	}

	spectralEntropy := 0.0
	for _, p := range powerSpectrum[:len(powerSpectrum)/2] {
		if p > 0 {
			prob := p / totalPower
			spectralEntropy -= prob * math.Log2(prob)
		}
	}
	maxEntropy := math.Log2(float64(len(powerSpectrum) / 2))
	result.SpectralEntropy = spectralEntropy / maxEntropy

	return result
}

func computePowerSpectrum(vals []float64) []float64 {
	n := len(vals)
	spectrum := make([]float64, n/2+1)

	for k := 0; k <= n/2; k++ {
		var realSum, imagSum float64
		for j := 0; j < n; j++ {
			angle := -2 * math.Pi * float64(j*k) / float64(n)
			realSum += vals[j] * math.Cos(angle)
			imagSum += vals[j] * math.Sin(angle)
		}
		spectrum[k] = (realSum*realSum + imagSum*imagSum) / float64(n*n)
	}

	return spectrum
}

func analyzeMultimodal(vals []float64) MultimodalAnalysis {
	result := MultimodalAnalysis{}

	dipStat, pValue := hartiganDipTest(vals)
	result.DipStatistic = dipStat
	result.DipPValue = pValue
	result.IsMultimodal = pValue < 0.05

	modes := findModes(vals)
	result.NumModes = len(modes)
	result.ModeLocations = modes

	result.ModeWeights = make([]float64, len(modes))
	if len(modes) > 0 {
		sorted := make([]float64, len(vals))
		copy(sorted, vals)
		sort.Float64s(sorted)

		for i, mode := range modes {
			count := 0
			bandwidth := (sorted[len(sorted)-1] - sorted[0]) / 20.0
			for _, v := range vals {
				if math.Abs(v-mode) < bandwidth {
					count++
				}
			}
			result.ModeWeights[i] = float64(count) / float64(len(vals))
		}
	}

	return result
}

func analyzeSQLTypeMultimodal(sqlTypeData map[string][]TimeSeriesPoint) []SQLTypeMultimodal {
	var results []SQLTypeMultimodal

	for sqlType, points := range sqlTypeData {
		if len(points) < 24 {
			continue
		}

		vals := extractValues(points)
		if len(vals) < 24 {
			continue
		}

		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		if sum == 0 {
			continue
		}

		analysis := SQLTypeMultimodal{
			Type:        sqlType,
			SampleCount: len(vals),
		}

		_, pValue := hartiganDipTest(vals)
		analysis.DipPValue = pValue
		analysis.Multimodal = pValue < 0.05

		modes := findModes(vals)
		analysis.NumModes = len(modes)
		if len(modes) > 0 && len(modes) <= 5 {
			analysis.ModeValues = modes
		}

		results = append(results, analysis)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].DipPValue < results[j].DipPValue
	})

	return results
}

func analyzeSQLTypeTailDistribution(sqlTypeData map[string][]TimeSeriesPoint) []SQLTypeTailDistribution {
	var results []SQLTypeTailDistribution

	for sqlType, points := range sqlTypeData {
		if len(points) < 30 {
			continue
		}

		vals := extractValues(points)
		if len(vals) < 30 {
			continue
		}

		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		if sum == 0 {
			continue
		}

		tail := analyzeTailDistribution(vals)
		analysis := SQLTypeTailDistribution{
			Type:            sqlType,
			SampleCount:     len(vals),
			TailType:        tail.RightTailType,
			TailIndex:       tail.TailIndex,
			GPDShape:        tail.GPDShape,
			GPDScale:        tail.GPDScale,
			ExtremeValueEst: tail.ExtremeValueEst,
		}

		results = append(results, analysis)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].TailIndex > results[j].TailIndex
	})

	return results
}

func analyzeSQLTypeVolatility(sqlTypeData map[string][]TimeSeriesPoint) []SQLTypeVolatilityMetrics {
	var results []SQLTypeVolatilityMetrics

	for sqlType, points := range sqlTypeData {
		if len(points) < 10 {
			continue
		}

		vals := extractValues(points)
		if len(vals) < 10 {
			continue
		}

		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		if sum == 0 {
			continue
		}

		vol := analyzeVolatility(vals)
		analysis := SQLTypeVolatilityMetrics{
			Type:               sqlType,
			SampleCount:        len(vals),
			RealizedVolatility: vol.RealizedVolatility,
			ParkinsonHL:        vol.ParkinsonHL,
			VolOfVol:           vol.VolOfVol,
			VolatilityRatio:    vol.VolatilityRatio,
			LeverageEffect:     vol.LeverageEffect,
		}

		results = append(results, analysis)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].RealizedVolatility > results[j].RealizedVolatility
	})

	return results
}

func analyzeSQLTypeRegimeDetection(sqlTypeData map[string][]TimeSeriesPoint) []SQLTypeRegimeDetection {
	var results []SQLTypeRegimeDetection

	for sqlType, points := range sqlTypeData {
		if len(points) < 20 {
			continue
		}

		vals := extractValues(points)
		if len(vals) < 20 {
			continue
		}

		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		if sum == 0 {
			continue
		}

		regime := analyzeRegimes(vals)
		analysis := SQLTypeRegimeDetection{
			Type:        sqlType,
			SampleCount: len(vals),
			NumRegimes:  regime.NumRegimes,
		}

		if regime.NumRegimes > 1 && regime.NumRegimes <= 5 {
			analysis.RegimeMeans = regime.RegimeMeans
			analysis.RegimeStds = regime.RegimeStds
		}

		results = append(results, analysis)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].NumRegimes > results[j].NumRegimes
	})

	return results
}

func hartiganDipTest(vals []float64) (float64, float64) {
	n := len(vals)
	if n < 10 {
		return 0, 1
	}

	sorted := make([]float64, n)
	copy(sorted, vals)
	sort.Float64s(sorted)

	Fn := make([]float64, n)
	for i := 0; i < n; i++ {
		Fn[i] = float64(i+1) / float64(n)
	}

	minVal, maxVal := sorted[0], sorted[n-1]
	range_ := maxVal - minVal
	if range_ == 0 {
		return 0, 1
	}

	normalized := make([]float64, n)
	for i, v := range sorted {
		normalized[i] = (v - minVal) / range_
	}

	dip := 0.0

	l, r := 0, n-1
	for l < r {
		fnL := normalized[l]
		fnR := normalized[r]
		targetSlope := (fnR - fnL) / float64(r-l)

		gcmL := fnL
		gcmR := fnL + targetSlope*float64(r-l)

		dL := gcmL - Fn[l]
		dR := gcmR - Fn[r]

		if dL < 0 {
			dL = -dL
		}
		if dR < 0 {
			dR = -dR
		}

		currentDip := math.Max(dL, dR)
		if currentDip > dip {
			dip = currentDip
		}

		if normalized[l+1] < gcmL+targetSlope {
			l++
		} else if normalized[r-1] > gcmR-targetSlope {
			r--
		} else {
			break
		}
	}

	expectedDip := 0.25 / math.Sqrt(float64(n))
	pValue := 1.0
	if dip > expectedDip*1.5 {
		pValue = 0.01
	} else if dip > expectedDip {
		pValue = 0.05
	} else {
		pValue = 0.1 + (expectedDip-dip)/expectedDip*0.4
	}

	return dip, math.Min(1.0, pValue)
}

func findModes(vals []float64) []float64 {
	n := len(vals)
	if n < 5 {
		return []float64{median(vals)}
	}

	sorted := make([]float64, n)
	copy(sorted, vals)
	sort.Float64s(sorted)

	bandwidth := 1.06 * stdDev(vals) * math.Pow(float64(n), -0.2)
	if bandwidth == 0 {
		bandwidth = 1
	}

	kde := func(x float64) float64 {
		sum := 0.0
		for _, v := range vals {
			u := (x - v) / bandwidth
			sum += math.Exp(-0.5 * u * u)
		}
		return sum / float64(n) / bandwidth
	}

	minVal, maxVal := sorted[0], sorted[n-1]
	step := (maxVal - minVal) / 1000

	var modes []float64
	prevKde := kde(minVal)
	currKde := kde(minVal + step)

	for x := minVal + step; x < maxVal; x += step {
		nextKde := kde(x + step)

		if currKde > prevKde && currKde > nextKde {
			modes = append(modes, x)
		}

		prevKde = currKde
		currKde = nextKde
	}

	if len(modes) == 0 {
		modes = append(modes, median(vals))
	}

	return modes
}

func analyzeTailDistribution(vals []float64) TailDistribution {
	result := TailDistribution{}

	n := len(vals)
	if n < 30 {
		return result
	}

	sorted := make([]float64, n)
	copy(sorted, vals)
	sort.Float64s(sorted)

	q95 := percentile(sorted, 0.95)
	q99 := percentile(sorted, 0.99)
	q99_9 := percentile(sorted, 0.999)

	var tailIndex float64
	if q99 > q95 && q99_9 > q99 {
		tailIndex = math.Log((q99_9-q99)/(q99-q95)) / math.Log(10)
	} else {
		tailIndex = 0
	}

	result.TailIndex = tailIndex

	if tailIndex > 1 {
		result.RightTailType = "heavy"
	} else if tailIndex > 0.5 {
		result.RightTailType = "moderate"
	} else {
		result.RightTailType = "light"
	}

	threshold := q95
	exceedances := make([]float64, 0)
	for _, v := range vals {
		if v > threshold {
			exceedances = append(exceedances, v-threshold)
		}
	}

	if len(exceedances) > 10 {
		result.GPDThreshold = threshold
		result.GPDShape, result.GPDScale = fitGPD(exceedances)

		extremeEst := threshold + result.GPDScale*math.Pow(float64(n)/float64(len(exceedances)), result.GPDShape)
		result.ExtremeValueEst = extremeEst
	}

	return result
}

func fitGPD(exceedances []float64) (shape, scale float64) {
	n := len(exceedances)
	if n < 2 {
		return 0, 1
	}

	meanEx := mean(exceedances)
	varEx := variance(exceedances)

	if varEx > 0 {
		shape = 0.5 * (meanEx*meanEx/varEx - 1)
		scale = meanEx * (1 - shape)
	} else {
		shape = 0
		scale = meanEx
	}

	return shape, scale
}

func analyzeVolatility(vals []float64) VolatilityMetrics {
	result := VolatilityMetrics{}

	n := len(vals)
	if n < 10 {
		return result
	}

	returns := make([]float64, n-1)
	for i := 1; i < n; i++ {
		if vals[i-1] != 0 {
			returns[i-1] = (vals[i] - vals[i-1]) / vals[i-1]
		}
	}

	result.RealizedVolatility = stdDev(returns) * math.Sqrt(float64(len(returns)))

	windowSize := 10
	parkinsons := make([]float64, 0)
	for i := windowSize; i < n; i++ {
		window := vals[i-windowSize : i]
		h := max(window)
		l := minFloatSlice(window)
		parkinsons = append(parkinsons, math.Pow(math.Log(h/l), 2))
	}
	result.ParkinsonHL = math.Sqrt(mean(parkinsons)) / math.Sqrt(4*math.Log(2))

	result.GarmanKlass = result.RealizedVolatility * 1.1

	vols := make([]float64, 0)
	for i := windowSize; i < len(returns); i++ {
		vols = append(vols, stdDev(returns[i-windowSize:i]))
	}
	result.VolOfVol = stdDev(vols) / mean(vols)

	if mean(vols) > 0 {
		result.VolatilityRatio = vols[len(vols)-1] / mean(vols)
	}

	avgReturns := mean(returns)
	avgVol := result.RealizedVolatility
	result.LeverageEffect = -avgReturns * avgVol / (math.Abs(avgReturns)*avgVol + 1e-10)

	return result
}

func analyzeRegimes(vals []float64) RegimeAnalysis {
	result := RegimeAnalysis{}

	n := len(vals)
	if n < 20 {
		result.NumRegimes = 1
		result.RegimeMeans = []float64{mean(vals)}
		result.RegimeStds = []float64{stdDev(vals)}
		return result
	}

	windowSize := 10
	means := make([]float64, n-windowSize+1)
	stds := make([]float64, n-windowSize+1)

	for i := 0; i <= n-windowSize; i++ {
		window := vals[i : i+windowSize]
		means[i] = mean(window)
		stds[i] = stdDev(window)
	}

	changePoints := detectRegimeChanges(means, stds)

	if len(changePoints) == 0 {
		result.NumRegimes = 1
		result.RegimeMeans = []float64{mean(vals)}
		result.RegimeStds = []float64{stdDev(vals)}
		result.RegimeDurations = []int{n}
		result.CurrentRegime = 0
		return result
	}

	changePoints = append([]int{0}, changePoints...)
	changePoints = append(changePoints, n)

	result.NumRegimes = len(changePoints) - 1
	result.RegimeMeans = make([]float64, result.NumRegimes)
	result.RegimeStds = make([]float64, result.NumRegimes)
	result.RegimeDurations = make([]int, result.NumRegimes)

	for i := 0; i < result.NumRegimes; i++ {
		start := changePoints[i]
		end := changePoints[i+1]
		segment := vals[start:end]

		result.RegimeMeans[i] = mean(segment)
		result.RegimeStds[i] = stdDev(segment)
		result.RegimeDurations[i] = end - start
	}

	result.CurrentRegime = result.NumRegimes - 1

	result.Transitions = make([][]int, result.NumRegimes)
	for i := range result.Transitions {
		result.Transitions[i] = make([]int, result.NumRegimes)
	}

	_ = windowSize
	return result
}

func detectRegimeChanges(means, stds []float64) []int {
	n := len(means)
	if n < 10 {
		return nil
	}

	var changePoints []int

	threshold := 2.5

	for i := 5; i < n-5; i++ {
		beforeMean := mean(means[i-5 : i])
		afterMean := mean(means[i : i+5])
		beforeStd := mean(stds[i-5 : i])
		afterStd := mean(stds[i : i+5])

		pooledStd := math.Sqrt((beforeStd*beforeStd + afterStd*afterStd) / 2)
		if pooledStd == 0 {
			continue
		}

		tStat := math.Abs(afterMean-beforeMean) / pooledStd / math.Sqrt(2.0/5.0)

		if tStat > threshold {
			changePoints = append(changePoints, i)
			i += 5
		}
	}

	return changePoints
}

func minFloatSlice(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals {
		if v < m {
			m = v
		}
	}
	return m
}

func (p *LoadProfile) EnhanceWithAdvancedAnalysis(qpsData []TimeSeriesPoint) {
	if len(qpsData) < 64 {
		return
	}

	advancedResult := AnalyzeAdvancedProfile(qpsData, DefaultAdvancedProfilingConfig())
	if advancedResult == nil {
		return
	}

	if advancedResult.FrequencyDomain.HasDailyPattern {
		p.Characteristics.Seasonality = math.Max(p.Characteristics.Seasonality,
			advancedResult.FrequencyDomain.PeriodStrength)
	}

	if advancedResult.Multimodal.IsMultimodal {
		p.QPSProfile.Skewness = 0
		if len(advancedResult.Multimodal.ModeLocations) > 1 {
			mode1 := advancedResult.Multimodal.ModeLocations[0]
			mode2 := advancedResult.Multimodal.ModeLocations[len(advancedResult.Multimodal.ModeLocations)-1]
			if p.QPSProfile.Mean > 0 {
				p.QPSProfile.Skewness = (mode2 - mode1) / p.QPSProfile.Mean
			}
		}
	}
}

func PrintAdvancedProfiling(result *AdvancedProfilingResult) {
	PrintAdvancedProfilingWithInterval(result, 1.0)
}

func PrintAdvancedProfilingWithInterval(result *AdvancedProfilingResult, sampleIntervalHours float64) {
	if result == nil {
		return
	}

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("ADVANCED PROFILING ANALYSIS")
	fmt.Println(stringsRepeat("-", 60))

	fmt.Println("\n--- Frequency Domain Analysis")
	fmt.Println("  (Detects periodic patterns using FFT spectral analysis)")
	if len(result.FrequencyDomain.DominantPeriods) > 0 {
		fmt.Println("  Dominant Periods (sorted by strength):")
		count := 0
		for _, p := range result.FrequencyDomain.DominantPeriods {
			if p.Confidence < 0.10 {
				continue
			}
			count++
			periodHours := p.Period * sampleIntervalHours
			periodDays := periodHours / 24
			var timeStr string
			if periodDays >= 1 {
				timeStr = fmt.Sprintf("%.1f days", periodDays)
			} else {
				timeStr = fmt.Sprintf("%.1f hours", periodHours)
			}
			fmt.Printf("    %d. %s | explains %.1f%% of variance\n", count, timeStr, p.Confidence*100)
		}
		if count == 0 {
			fmt.Println("    (no significant periods detected)")
		}
		entropyDesc := "moderate regularity"
		if result.FrequencyDomain.SpectralEntropy < 0.3 {
			entropyDesc = "highly regular/predictable"
		} else if result.FrequencyDomain.SpectralEntropy < 0.5 {
			entropyDesc = "mostly regular"
		} else if result.FrequencyDomain.SpectralEntropy < 0.7 {
			entropyDesc = "somewhat random"
		} else {
			entropyDesc = "highly random/unpredictable"
		}
		fmt.Printf("  Spectral Entropy: %.3f [0-1, lower=more regular] => %s\n", result.FrequencyDomain.SpectralEntropy, entropyDesc)

		fmt.Println("\n  Detected Patterns:")
		patterns := []struct {
			name     string
			detected bool
		}{
			{"Daily (24h)", result.FrequencyDomain.HasDailyPattern},
			{"Weekly (7d)", result.FrequencyDomain.HasWeeklyPattern},
			{"Semi-Monthly (~15d)", result.FrequencyDomain.HasSemiMonthlyPattern},
			{"Monthly (~30d)", result.FrequencyDomain.HasMonthlyPattern},
		}
		for _, p := range patterns {
			if p.detected {
				fmt.Printf("    + %s\n", p.name)
			}
		}
		hasAny := result.FrequencyDomain.HasDailyPattern || result.FrequencyDomain.HasWeeklyPattern ||
			result.FrequencyDomain.HasSemiMonthlyPattern || result.FrequencyDomain.HasMonthlyPattern
		if !hasAny {
			fmt.Println("    (no standard calendar patterns detected)")
		}
	}

	fmt.Println("\n--- Multimodal Analysis")
	fmt.Println("  (Checks if data has multiple distinct workload levels)")
	if result.Multimodal.IsMultimodal {
		fmt.Printf("  Multimodal: YES (%d distinct workload levels detected)\n", result.Multimodal.NumModes)
		fmt.Println("  => Traffic operates at distinct levels (e.g., day/night, peak/off-peak)")
	} else {
		fmt.Printf("  Multimodal: NO (single workload distribution)\n")
		fmt.Println("  => Traffic follows a single continuous pattern")
	}
	if len(result.Multimodal.ModeLocations) > 0 && len(result.Multimodal.ModeLocations) <= 5 {
		fmt.Printf("  Mode Values (QPS levels): %v\n", formatFloatSlice(result.Multimodal.ModeLocations))
		if len(result.Multimodal.ModeWeights) > 0 {
			fmt.Printf("  Mode Weights (time spent): %v\n", formatPercentSlice(result.Multimodal.ModeWeights))
		}
	}
	dipDesc := "likely unimodal"
	if result.Multimodal.DipPValue < 0.01 {
		dipDesc = "strongly multimodal"
	} else if result.Multimodal.DipPValue < 0.05 {
		dipDesc = "moderately multimodal"
	}
	fmt.Printf("  Dip Test p-value: %.4f [0-1, <0.05=multimodal] => %s\n", result.Multimodal.DipPValue, dipDesc)

	if len(result.SQLTypeMultimodal) > 0 {
		fmt.Println("\n--- SQL Type Multimodal Analysis")
		fmt.Println("  (Checks if each SQL type has distinct workload levels)")
		type multimodalGroupKey struct {
			isMultimodal bool
			numModes     int
		}
		groupedByMultimodal := make(map[multimodalGroupKey][]SQLTypeMultimodal)
		for _, sm := range result.SQLTypeMultimodal {
			key := multimodalGroupKey{isMultimodal: sm.Multimodal, numModes: sm.NumModes}
			groupedByMultimodal[key] = append(groupedByMultimodal[key], sm)
		}
		for _, isMultimodal := range []bool{true, false} {
			for numModes := 1; numModes <= 10; numModes++ {
				key := multimodalGroupKey{isMultimodal: isMultimodal, numModes: numModes}
				items := groupedByMultimodal[key]
				if len(items) == 0 {
					continue
				}
				var types []string
				for _, sm := range items {
					types = append(types, sm.Type)
				}
				if isMultimodal {
					if len(types) == len(result.SQLTypeMultimodal) {
						fmt.Printf("  ALL types: multimodal (%d modes)\n", numModes)
					} else {
						fmt.Printf("  %s: multimodal (%d modes)\n", strings.Join(types, ", "), numModes)
					}
				} else {
					if len(types) == len(result.SQLTypeMultimodal) {
						fmt.Printf("  ALL types: unimodal (single workload level)\n")
					} else {
						fmt.Printf("  %s: unimodal (single workload level)\n", strings.Join(types, ", "))
					}
				}
			}
		}
	}

	fmt.Println("\n--- Tail Distribution")
	fmt.Println("  (Analyzes extreme values and spike behavior)")
	switch result.TailDistribution.RightTailType {
	case "heavy":
		fmt.Println("  Tail Type: HEAVY")
		fmt.Println("  => Frequent extreme spikes, hard to predict max traffic")
	case "moderate":
		fmt.Println("  Tail Type: MODERATE")
		fmt.Println("  => Occasional spikes, some unpredictability")
	default:
		fmt.Println("  Tail Type: LIGHT")
		fmt.Println("  => Rare extreme values, traffic mostly predictable")
	}
	tailDesc := "light tail (bounded extremes)"
	if result.TailDistribution.TailIndex > 1.5 {
		tailDesc = "very heavy tail (frequent extremes)"
	} else if result.TailDistribution.TailIndex > 1.0 {
		tailDesc = "heavy tail (unbounded variance)"
	} else if result.TailDistribution.TailIndex > 0.5 {
		tailDesc = "moderate tail"
	}
	fmt.Printf("  Tail Index: %.3f [>1=heavy, <0.5=light] => %s\n", result.TailDistribution.TailIndex, tailDesc)
	if result.TailDistribution.GPDShape != 0 || result.TailDistribution.GPDScale != 0 {
		fmt.Printf("  GPD Parameters: shape=%.3f [affects tail weight], scale=%.3f\n",
			result.TailDistribution.GPDShape, result.TailDistribution.GPDScale)
		if result.TailDistribution.ExtremeValueEst > 0 {
			fmt.Printf("  Predicted Extreme (P99.9): %.0f\n", result.TailDistribution.ExtremeValueEst)
		}
	}

	if len(result.SQLTypeTailDistribution) > 0 {
		fmt.Println("\n--- SQL Type Tail Distribution")
		fmt.Println("  (Analyzes extreme values for each SQL type)")
		groupedByTail := make(map[string][]SQLTypeTailDistribution)
		for _, st := range result.SQLTypeTailDistribution {
			tailTypeStr := st.TailType
			if tailTypeStr == "" {
				tailTypeStr = "light"
			}
			groupedByTail[tailTypeStr] = append(groupedByTail[tailTypeStr], st)
		}
		for _, tailType := range []string{"heavy", "moderate", "light"} {
			items := groupedByTail[tailType]
			if len(items) == 0 {
				continue
			}
			var types []string
			var estSum float64
			var estCount int
			for _, st := range items {
				types = append(types, st.Type)
				if st.ExtremeValueEst > 0 {
					estSum += st.ExtremeValueEst
					estCount++
				}
			}
			tailTypeDesc := "light"
			if tailType == "heavy" {
				tailTypeDesc = "heavy (frequent extreme spikes)"
			} else if tailType == "moderate" {
				tailTypeDesc = "moderate (occasional spikes)"
			}
			if len(types) == len(result.SQLTypeTailDistribution) {
				fmt.Printf("  ALL types: %s tail => %s\n", strings.ToUpper(tailType), tailTypeDesc)
			} else {
				fmt.Printf("  %s: %s tail => %s\n", strings.Join(types, ", "), strings.ToUpper(tailType), tailTypeDesc)
			}
			if estCount > 0 {
				fmt.Printf("    Predicted Extreme (P99.9): avg=%.0f\n", estSum/float64(estCount))
			}
		}
	}

	fmt.Println("\n--- Volatility Metrics")
	fmt.Println("  (Measures traffic stability)")
	vol := result.VolatilityMetrics.RealizedVolatility
	volDesc := "low"
	if vol > 1.0 {
		volDesc = "very high"
	} else if vol > 0.5 {
		volDesc = "high"
	} else if vol > 0.2 {
		volDesc = "moderate"
	}
	fmt.Printf("  Realized Volatility: %.4f [higher=more variable] => %s variability\n", vol, volDesc)

	parkinsonDesc := "stable"
	if result.VolatilityMetrics.ParkinsonHL > 0.5 {
		parkinsonDesc = "unstable"
	} else if result.VolatilityMetrics.ParkinsonHL > 0.2 {
		parkinsonDesc = "moderately variable"
	}
	fmt.Printf("  Parkinson HL: %.4f [range-based] => %s\n", result.VolatilityMetrics.ParkinsonHL, parkinsonDesc)

	vovDesc := "consistent volatility"
	if result.VolatilityMetrics.VolOfVol > 1.0 {
		vovDesc = "highly variable volatility"
	} else if result.VolatilityMetrics.VolOfVol > 0.5 {
		vovDesc = "moderately variable volatility"
	}
	fmt.Printf("  Vol-of-Vol: %.4f [0-1+, lower=more stable] => %s\n", result.VolatilityMetrics.VolOfVol, vovDesc)

	if result.VolatilityMetrics.VolatilityRatio > 1.5 {
		fmt.Printf("  Volatility Ratio: %.2f [current vs avg] => ELEVATED (recent instability)\n", result.VolatilityMetrics.VolatilityRatio)
	} else if result.VolatilityMetrics.VolatilityRatio < 0.7 {
		fmt.Printf("  Volatility Ratio: %.2f [current vs avg] => LOW (recently calmer)\n", result.VolatilityMetrics.VolatilityRatio)
	} else {
		fmt.Printf("  Volatility Ratio: %.2f [current vs avg] => NORMAL\n", result.VolatilityMetrics.VolatilityRatio)
	}

	levDesc := "no significant effect"
	if result.VolatilityMetrics.LeverageEffect < -0.3 {
		levDesc = "volatility spikes when load drops (unusual)"
	} else if result.VolatilityMetrics.LeverageEffect > 0.3 {
		levDesc = "volatility spikes with load (typical)"
	}
	fmt.Printf("  Leverage Effect: %.4f [-1 to 1] => %s\n", result.VolatilityMetrics.LeverageEffect, levDesc)

	if len(result.SQLTypeVolatilityMetrics) > 0 {
		fmt.Println("\n--- SQL Type Volatility Metrics")
		fmt.Println("  (Measures traffic stability for each SQL type)")
		type volGroupKey struct {
			level string
			ratio string
		}
		groupedByVol := make(map[volGroupKey][]SQLTypeVolatilityMetrics)
		for _, sv := range result.SQLTypeVolatilityMetrics {
			level := "low"
			if sv.RealizedVolatility > 1.0 {
				level = "very_high"
			} else if sv.RealizedVolatility > 0.5 {
				level = "high"
			} else if sv.RealizedVolatility > 0.2 {
				level = "moderate"
			}
			ratio := "normal"
			if sv.VolatilityRatio > 1.5 {
				ratio = "elevated"
			} else if sv.VolatilityRatio < 0.7 {
				ratio = "low"
			}
			key := volGroupKey{level: level, ratio: ratio}
			groupedByVol[key] = append(groupedByVol[key], sv)
		}
		for _, level := range []string{"very_high", "high", "moderate", "low"} {
			for _, ratio := range []string{"elevated", "normal", "low"} {
				key := volGroupKey{level: level, ratio: ratio}
				items := groupedByVol[key]
				if len(items) == 0 {
					continue
				}
				var types []string
				for _, sv := range items {
					types = append(types, sv.Type)
				}
				levelDesc := level
				if level == "very_high" {
					levelDesc = "very high"
				}
				ratioDesc := ""
				if ratio == "elevated" {
					ratioDesc = " (ELEVATED - recent instability)"
				} else if ratio == "low" {
					ratioDesc = " (LOW - recently calmer)"
				}
				if len(types) == len(result.SQLTypeVolatilityMetrics) {
					fmt.Printf("  ALL types: %s variability%s\n", levelDesc, ratioDesc)
				} else {
					fmt.Printf("  %s: %s variability%s\n", strings.Join(types, ", "), levelDesc, ratioDesc)
				}
			}
		}
	}

	fmt.Println("\n--- Regime Detection")
	fmt.Println("  (Identifies distinct operational states)")
	if result.RegimeDetection.NumRegimes > 1 {
		fmt.Printf("  Regimes Detected: %d distinct operational modes\n", result.RegimeDetection.NumRegimes)
		fmt.Println("  => System operates at different levels (e.g., peak vs off-peak)")
		for i := 0; i < result.RegimeDetection.NumRegimes && i < 5; i++ {
			durationH := float64(result.RegimeDetection.RegimeDurations[i]) * sampleIntervalHours
			fmt.Printf("    Regime %d: mean=%.0f, std=%.0f, duration=%.1fh\n",
				i, result.RegimeDetection.RegimeMeans[i], result.RegimeDetection.RegimeStds[i], durationH)
		}
		fmt.Printf("  Current Regime: %d\n", result.RegimeDetection.CurrentRegime)
	} else {
		fmt.Println("  Regimes Detected: 1 (stable operational mode)")
		fmt.Println("  => Traffic follows a consistent pattern without distinct states")
	}

	if len(result.SQLTypeRegimeDetection) > 0 {
		fmt.Println("\n--- SQL Type Regime Detection")
		fmt.Println("  (Identifies distinct operational states for each SQL type)")
		groupedByRegime := make(map[int][]SQLTypeRegimeDetection)
		for _, sr := range result.SQLTypeRegimeDetection {
			groupedByRegime[sr.NumRegimes] = append(groupedByRegime[sr.NumRegimes], sr)
		}
		for numRegimes := 1; numRegimes <= 5; numRegimes++ {
			items := groupedByRegime[numRegimes]
			if len(items) == 0 {
				continue
			}
			var types []string
			for _, sr := range items {
				types = append(types, sr.Type)
			}
			regimeDesc := "stable"
			if numRegimes > 1 {
				regimeDesc = fmt.Sprintf("%d distinct operational modes", numRegimes)
			}
			if len(types) == len(result.SQLTypeRegimeDetection) {
				fmt.Printf("  ALL types: %s\n", regimeDesc)
			} else {
				fmt.Printf("  %s: %s\n", strings.Join(types, ", "), regimeDesc)
			}
		}
	}

	fmt.Println()
}

func formatFloatSlice(vals []float64) string {
	if len(vals) == 0 {
		return "[]"
	}
	if len(vals) <= 4 {
		formatted := make([]string, len(vals))
		for i, v := range vals {
			formatted[i] = fmt.Sprintf("%.0f", v)
		}
		return fmt.Sprintf("[%s]", strings.Join(formatted, ", "))
	}
	return fmt.Sprintf("[%.0f, %.0f, ... %d more]", vals[0], vals[1], len(vals)-2)
}

func formatPercentSlice(vals []float64) string {
	if len(vals) == 0 {
		return "[]"
	}
	if len(vals) <= 4 {
		formatted := make([]string, len(vals))
		for i, v := range vals {
			formatted[i] = fmt.Sprintf("%.0f%%", v*100)
		}
		return fmt.Sprintf("[%s]", strings.Join(formatted, ", "))
	}
	return fmt.Sprintf("[%.0f%%, %.0f%%, ...]", vals[0]*100, vals[1]*100)
}
