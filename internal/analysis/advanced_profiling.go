package analysis

import (
	"fmt"
	"math"
	"sort"
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

type AdvancedProfilingResult struct {
	FrequencyDomain   FrequencyAnalysis  `json:"frequency_domain"`
	Multimodal        MultimodalAnalysis `json:"multimodal"`
	TailDistribution  TailDistribution   `json:"tail_distribution"`
	VolatilityMetrics VolatilityMetrics  `json:"volatility_metrics"`
	RegimeDetection   RegimeAnalysis     `json:"regime_detection"`
}

type FrequencyAnalysis struct {
	DominantPeriods  []PeriodInfo `json:"dominant_periods"`
	SpectralEntropy  float64      `json:"spectral_entropy"`
	PeakFrequencies  []float64    `json:"peak_frequencies"`
	PeriodStrength   float64      `json:"period_strength"`
	HasDailyPattern  bool         `json:"has_daily_pattern"`
	HasWeeklyPattern bool         `json:"has_weekly_pattern"`
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

	for _, p := range result.DominantPeriods {
		if p.Period >= 23 && p.Period <= 25 {
			result.HasDailyPattern = true
			result.PeriodStrength = math.Max(result.PeriodStrength, p.Confidence)
		}
		if p.Period >= 167 && p.Period <= 169 {
			result.HasWeeklyPattern = true
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
	if result == nil {
		return
	}

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("ADVANCED PROFILING ANALYSIS")
	fmt.Println(stringsRepeat("-", 60))

	fmt.Println("\n--- Frequency Domain Analysis ---")
	if len(result.FrequencyDomain.DominantPeriods) > 0 {
		for _, p := range result.FrequencyDomain.DominantPeriods {
			fmt.Printf("  Period: %.1f samples (power=%.3f, confidence=%.2f%%)\n",
				p.Period, p.Power, p.Confidence*100)
		}
		fmt.Printf("  Spectral Entropy: %.3f\n", result.FrequencyDomain.SpectralEntropy)
		fmt.Printf("  Daily Pattern: %v\n", result.FrequencyDomain.HasDailyPattern)
		fmt.Printf("  Weekly Pattern: %v\n", result.FrequencyDomain.HasWeeklyPattern)
	}

	fmt.Println("\n--- Multimodal Analysis ---")
	fmt.Printf("  Is Multimodal: %v\n", result.Multimodal.IsMultimodal)
	fmt.Printf("  Number of Modes: %d\n", result.Multimodal.NumModes)
	if len(result.Multimodal.ModeLocations) > 0 {
		fmt.Printf("  Mode Locations: %v\n", result.Multimodal.ModeLocations)
	}
	fmt.Printf("  Dip Statistic: %.4f (p-value=%.4f)\n",
		result.Multimodal.DipStatistic, result.Multimodal.DipPValue)

	fmt.Println("\n--- Tail Distribution ---")
	fmt.Printf("  Right Tail Type: %s\n", result.TailDistribution.RightTailType)
	fmt.Printf("  Tail Index: %.3f\n", result.TailDistribution.TailIndex)
	if result.TailDistribution.GPDShape != 0 || result.TailDistribution.GPDScale != 0 {
		fmt.Printf("  GPD Parameters: shape=%.3f, scale=%.3f\n",
			result.TailDistribution.GPDShape, result.TailDistribution.GPDScale)
		fmt.Printf("  Extreme Value Estimate: %.0f\n", result.TailDistribution.ExtremeValueEst)
	}

	fmt.Println("\n--- Volatility Metrics ---")
	fmt.Printf("  Realized Volatility: %.4f\n", result.VolatilityMetrics.RealizedVolatility)
	fmt.Printf("  Parkinson HL: %.4f\n", result.VolatilityMetrics.ParkinsonHL)
	fmt.Printf("  Vol of Vol: %.4f\n", result.VolatilityMetrics.VolOfVol)
	fmt.Printf("  Volatility Ratio: %.4f\n", result.VolatilityMetrics.VolatilityRatio)
	fmt.Printf("  Leverage Effect: %.4f\n", result.VolatilityMetrics.LeverageEffect)

	fmt.Println("\n--- Regime Detection ---")
	fmt.Printf("  Number of Regimes: %d\n", result.RegimeDetection.NumRegimes)
	if result.RegimeDetection.NumRegimes > 1 {
		fmt.Printf("  Regime Means: %v\n", result.RegimeDetection.RegimeMeans)
		fmt.Printf("  Regime Stds: %v\n", result.RegimeDetection.RegimeStds)
		fmt.Printf("  Regime Durations: %v\n", result.RegimeDetection.RegimeDurations)
		fmt.Printf("  Current Regime: %d\n", result.RegimeDetection.CurrentRegime)
	}

	fmt.Println()
}
