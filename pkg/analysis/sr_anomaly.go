package analysis

import (
	"fmt"
	"math"
	"math/cmplx"
	"sort"
	"time"
)

type SpectralResidualDetector struct {
	config SRConfig
}

type SRConfig struct {
	WindowSize      int
	LocalWindowSize int
	Threshold       float64
	EstimatorWindow int
	AmpFactor       float64
}

func DefaultSRConfig() SRConfig {
	return SRConfig{
		WindowSize:      64,
		LocalWindowSize: 21,
		Threshold:       3.0,
		EstimatorWindow: 3,
		AmpFactor:       3.0,
	}
}

func NewSpectralResidualDetector(config SRConfig) *SpectralResidualDetector {
	return &SpectralResidualDetector{config: config}
}

func (d *SpectralResidualDetector) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.WindowSize*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	n := len(values)
	vals := extractValues(values)

	srScores := d.ComputeSRScores(vals)

	saliencyMap := d.computeSaliencyMap(srScores)

	thresholds := d.computeAdaptiveThreshold(saliencyMap)

	var anomalies []DetectedAnomaly
	m := median(vals)

	for i := d.config.LocalWindowSize; i < n-d.config.LocalWindowSize; i++ {
		if saliencyMap[i] > thresholds[i] {
			zScore := saliencyMap[i] / thresholds[i]

			severity := SeverityMedium
			if zScore > 2 {
				severity = SeverityHigh
			}
			if zScore > 3 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if vals[i] < m {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     vals[i],
				Baseline:  m,
				ZScore:    zScore,
				Detail:    fmt.Sprintf("Spectral Residual anomaly: %.0f (saliency=%.3f)", vals[i], saliencyMap[i]),
			})
		}
	}

	return MergeConsecutiveAnomalies(anomalies)
}

func (d *SpectralResidualDetector) ComputeSRScores(vals []float64) []float64 {
	n := len(vals)

	fftLength := 1
	for fftLength < n {
		fftLength *= 2
	}

	padded := make([]float64, fftLength)
	copy(padded, vals)

	fftResult := d.fft(padded)

	amplitude := make([]float64, fftLength/2+1)
	phase := make([]float64, fftLength/2+1)
	for i := 0; i <= fftLength/2; i++ {
		amplitude[i] = cmplx.Abs(fftResult[i])
		phase[i] = cmplx.Phase(fftResult[i])
		if amplitude[i] == 0 {
			amplitude[i] = 1e-10
		}
	}

	logAmplitude := make([]float64, len(amplitude))
	for i := range amplitude {
		logAmplitude[i] = math.Log(amplitude[i])
	}

	smoothedLogAmp := d.averageFilter(logAmplitude)

	spectralResidual := make([]float64, len(amplitude))
	for i := range amplitude {
		spectralResidual[i] = logAmplitude[i] - smoothedLogAmp[i]
	}

	reconstructedAmp := make([]float64, len(amplitude))
	for i := range amplitude {
		reconstructedAmp[i] = math.Exp(spectralResidual[i])
	}

	reconstructedFFT := make([]complex128, fftLength)
	for i := 0; i <= fftLength/2; i++ {
		reconstructedFFT[i] = cmplx.Rect(reconstructedAmp[i], phase[i])
	}
	for i := fftLength/2 + 1; i < fftLength; i++ {
		reconstructedFFT[i] = cmplx.Conj(reconstructedFFT[fftLength-i])
	}

	saliencyComplex := d.ifft(reconstructedFFT)

	saliency := make([]float64, n)
	for i := 0; i < n; i++ {
		saliency[i] = real(saliencyComplex[i])
	}

	return saliency
}

func (d *SpectralResidualDetector) computeSaliencyMap(srScores []float64) []float64 {
	n := len(srScores)
	saliencyMap := make([]float64, n)

	window := d.config.LocalWindowSize
	halfWindow := window / 2

	for i := halfWindow; i < n-halfWindow; i++ {
		localMean := 0.0
		localStd := 0.0
		count := 0

		for j := i - halfWindow; j <= i+halfWindow; j++ {
			localMean += math.Abs(srScores[j])
			count++
		}
		localMean /= float64(count)

		for j := i - halfWindow; j <= i+halfWindow; j++ {
			diff := math.Abs(srScores[j]) - localMean
			localStd += diff * diff
		}
		localStd = math.Sqrt(localStd / float64(count))

		if localStd > 0 {
			saliencyMap[i] = (math.Abs(srScores[i]) - localMean) / localStd
		} else {
			saliencyMap[i] = 0
		}
	}

	return saliencyMap
}

func (d *SpectralResidualDetector) computeAdaptiveThreshold(saliencyMap []float64) []float64 {
	n := len(saliencyMap)
	thresholds := make([]float64, n)

	estimatorWindow := d.config.EstimatorWindow

	for i := 0; i < n; i++ {
		start := i - estimatorWindow
		end := i + estimatorWindow + 1
		if start < 0 {
			start = 0
		}
		if end > n {
			end = n
		}

		windowValues := make([]float64, 0, end-start)
		for j := start; j < end; j++ {
			if saliencyMap[j] > 0 {
				windowValues = append(windowValues, saliencyMap[j])
			}
		}

		if len(windowValues) > 0 {
			sort.Float64s(windowValues)
			q := float64(len(windowValues)) * 0.75
			qIdx := int(q)
			if qIdx >= len(windowValues) {
				qIdx = len(windowValues) - 1
			}
			thresholds[i] = windowValues[qIdx] * d.config.Threshold
		} else {
			thresholds[i] = d.config.Threshold
		}
	}

	return thresholds
}

func (d *SpectralResidualDetector) averageFilter(vals []float64) []float64 {
	n := len(vals)
	result := make([]float64, n)

	window := 3
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

		sum := 0.0
		count := 0
		for j := start; j < end; j++ {
			sum += vals[j]
			count++
		}
		result[i] = sum / float64(count)
	}

	return result
}

func (d *SpectralResidualDetector) fft(vals []float64) []complex128 {
	n := len(vals)
	result := make([]complex128, n)

	for k := 0; k < n; k++ {
		var sum complex128
		for j := 0; j < n; j++ {
			angle := -2 * math.Pi * float64(j*k) / float64(n)
			sum += complex(vals[j], 0) * cmplx.Exp(complex(0, angle))
		}
		result[k] = sum
	}

	return result
}

func (d *SpectralResidualDetector) ifft(vals []complex128) []complex128 {
	n := len(vals)
	result := make([]complex128, n)

	for k := 0; k < n; k++ {
		var sum complex128
		for j := 0; j < n; j++ {
			angle := 2 * math.Pi * float64(j*k) / float64(n)
			sum += vals[j] * cmplx.Exp(complex(0, angle))
		}
		result[k] = sum / complex(float64(n), 0)
	}

	return result
}

type MultiScaleAnomalyDetector struct {
	config MultiScaleConfig
}

type MultiScaleConfig struct {
	Scales       []int
	Threshold    float64
	MinAgreement int
	UseWavelet   bool
}

func DefaultMultiScaleConfig() MultiScaleConfig {
	return MultiScaleConfig{
		Scales:       []int{6, 12, 24, 48},
		Threshold:    3.0,
		MinAgreement: 2,
		UseWavelet:   false,
	}
}

func NewMultiScaleAnomalyDetector(config MultiScaleConfig) *MultiScaleAnomalyDetector {
	return &MultiScaleAnomalyDetector{config: config}
}

func (d *MultiScaleAnomalyDetector) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.Scales[len(d.config.Scales)-1]*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	n := len(values)
	vals := extractValues(values)

	scaleScores := make([][]float64, len(d.config.Scales))
	for s, scale := range d.config.Scales {
		scaleScores[s] = d.computeScaleAnomalies(vals, scale)
	}

	combinedScores := make([]float64, n)
	agreementCount := make([]int, n)

	for i := 0; i < n; i++ {
		var sumScore float64
		var count int

		for s := range d.config.Scales {
			if i < len(scaleScores[s]) && scaleScores[s][i] > d.config.Threshold {
				sumScore += scaleScores[s][i]
				count++
			}
		}

		agreementCount[i] = count
		if count > 0 {
			combinedScores[i] = sumScore / float64(count)
		}
	}

	var anomalies []DetectedAnomaly
	m := median(vals)

	for i := 0; i < n; i++ {
		if agreementCount[i] >= d.config.MinAgreement {
			severity := SeverityMedium
			if agreementCount[i] >= 3 {
				severity = SeverityHigh
			}
			if agreementCount[i] >= 4 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if vals[i] < m {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     vals[i],
				Baseline:  m,
				ZScore:    combinedScores[i],
				Detail:    fmt.Sprintf("Multi-scale anomaly: %.0f (agreement=%d scales)", vals[i], agreementCount[i]),
			})
		}
	}

	return MergeConsecutiveAnomalies(anomalies)
}

func (d *MultiScaleAnomalyDetector) computeScaleAnomalies(vals []float64, scale int) []float64 {
	n := len(vals)
	scores := make([]float64, n)

	halfScale := scale / 2

	for i := halfScale; i < n-halfScale; i++ {
		leftWindow := vals[i-halfScale : i]
		rightWindow := vals[i+1 : i+halfScale+1]
		fullWindow := vals[i-halfScale : i+halfScale+1]

		leftMean := mean(leftWindow)
		rightMean := mean(rightWindow)
		fullMean := mean(fullWindow)
		fullStd := stdDev(fullWindow)

		if fullStd == 0 {
			continue
		}

		localDiff := (vals[i] - fullMean) / fullStd

		contextDiff := math.Abs(leftMean - rightMean)
		contextStd := math.Sqrt(variance(leftWindow) + variance(rightWindow))
		if contextStd > 0 {
			contextDiff /= contextStd
		}

		scores[i] = math.Abs(localDiff) + contextDiff*0.5
	}

	return scores
}

type DBSCANAnomalyDetector struct {
	config DBSCANConfig
}

type DBSCANConfig struct {
	Epsilon    float64
	MinSamples int
	WindowSize int
}

func DefaultDBSCANConfig() DBSCANConfig {
	return DBSCANConfig{
		Epsilon:    2.0,
		MinSamples: 5,
		WindowSize: 24,
	}
}

func NewDBSCANAnomalyDetector(config DBSCANConfig) *DBSCANAnomalyDetector {
	return &DBSCANAnomalyDetector{config: config}
}

func (d *DBSCANAnomalyDetector) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.WindowSize*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	n := len(values)
	vals := extractValues(values)

	m := median(vals)
	s := mad(vals) * 1.4826
	if s == 0 {
		s = stdDev(vals)
	}

	normalized := make([]float64, n)
	for i, v := range vals {
		normalized[i] = (v - m) / s
	}

	labels := d.dbscan(normalized)

	var anomalies []DetectedAnomaly

	for i, label := range labels {
		if label == -1 {
			severity := SeverityMedium
			zScore := math.Abs(normalized[i])
			if zScore > 3 {
				severity = SeverityHigh
			}
			if zScore > 4 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if vals[i] < m {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     vals[i],
				Baseline:  m,
				ZScore:    zScore,
				Detail:    fmt.Sprintf("DBSCAN outlier: %.0f (z=%.2f)", vals[i], normalized[i]),
			})
		}
	}

	return MergeConsecutiveAnomalies(anomalies)
}

func (d *DBSCANAnomalyDetector) dbscan(vals []float64) []int {
	n := len(vals)
	labels := make([]int, n)
	for i := range labels {
		labels[i] = -2
	}

	clusterID := 0

	for i := 0; i < n; i++ {
		if labels[i] != -2 {
			continue
		}

		neighbors := d.regionQuery(vals, i)

		if len(neighbors) < d.config.MinSamples {
			labels[i] = -1
			continue
		}

		clusterID++
		d.expandCluster(vals, labels, i, neighbors, clusterID)
	}

	return labels
}

func (d *DBSCANAnomalyDetector) regionQuery(vals []float64, idx int) []int {
	var neighbors []int
	n := len(vals)

	window := d.config.WindowSize
	start := idx - window
	end := idx + window + 1
	if start < 0 {
		start = 0
	}
	if end > n {
		end = n
	}

	for i := start; i < end; i++ {
		if i == idx {
			continue
		}
		dist := math.Abs(vals[i] - vals[idx])
		if dist <= d.config.Epsilon {
			neighbors = append(neighbors, i)
		}
	}

	return neighbors
}

func (d *DBSCANAnomalyDetector) expandCluster(vals []float64, labels []int, idx int, neighbors []int, clusterID int) {
	labels[idx] = clusterID

	queue := make([]int, len(neighbors))
	copy(queue, neighbors)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if labels[current] == -1 {
			labels[current] = clusterID
		}

		if labels[current] != -2 {
			continue
		}

		labels[current] = clusterID

		currentNeighbors := d.regionQuery(vals, current)
		if len(currentNeighbors) >= d.config.MinSamples {
			queue = append(queue, currentNeighbors...)
		}
	}
}

type HoltWintersDetector struct {
	config HoltWintersConfig
}

type HoltWintersConfig struct {
	Alpha         float64
	Beta          float64
	Gamma         float64
	SeasonLength  int
	Threshold     float64
	WarmupPeriods int
}

func DefaultHoltWintersConfig() HoltWintersConfig {
	return HoltWintersConfig{
		Alpha:         0.3,
		Beta:          0.1,
		Gamma:         0.1,
		SeasonLength:  24,
		Threshold:     3.0,
		WarmupPeriods: 2,
	}
}

func NewHoltWintersDetector(config HoltWintersConfig) *HoltWintersDetector {
	return &HoltWintersDetector{config: config}
}

func (d *HoltWintersDetector) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.SeasonLength*d.config.WarmupPeriods {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	n := len(values)
	vals := extractValues(values)

	level, trend, seasonal, forecasts := d.fitHoltWinters(vals)

	residuals := make([]float64, n)
	for i := 0; i < n; i++ {
		residuals[i] = vals[i] - forecasts[i]
	}

	residualMean := mean(residuals)
	residualStd := stdDev(residuals)
	if residualStd == 0 {
		return nil
	}

	var anomalies []DetectedAnomaly

	for i := d.config.SeasonLength * d.config.WarmupPeriods; i < n; i++ {
		zScore := math.Abs(residuals[i]-residualMean) / residualStd

		if zScore > d.config.Threshold {
			severity := SeverityMedium
			if zScore > d.config.Threshold*1.5 {
				severity = SeverityHigh
			}
			if zScore > d.config.Threshold*2 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if vals[i] < forecasts[i] {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     vals[i],
				Baseline:  forecasts[i],
				ZScore:    zScore,
				Detail:    fmt.Sprintf("Holt-Winters anomaly: %.0f (forecast=%.0f, z=%.2f)", vals[i], forecasts[i], zScore),
			})
		}
	}

	_ = level
	_ = trend
	_ = seasonal

	return MergeConsecutiveAnomalies(anomalies)
}

func (d *HoltWintersDetector) fitHoltWinters(vals []float64) ([]float64, []float64, []float64, []float64) {
	n := len(vals)
	seasonLength := d.config.SeasonLength

	level := make([]float64, n)
	trend := make([]float64, n)
	seasonal := make([]float64, n)
	forecasts := make([]float64, n)

	for i := 0; i < seasonLength; i++ {
		seasonal[i] = vals[i] - mean(vals[:seasonLength])
	}

	level[seasonLength-1] = vals[seasonLength-1] - seasonal[seasonLength-1]
	trend[seasonLength-1] = (vals[seasonLength-1] - vals[0]) / float64(seasonLength-1)

	for i := seasonLength; i < n; i++ {
		level[i] = d.config.Alpha*(vals[i]-seasonal[i-seasonLength]) + (1-d.config.Alpha)*(level[i-1]+trend[i-1])
		trend[i] = d.config.Beta*(level[i]-level[i-1]) + (1-d.config.Beta)*trend[i-1]
		seasonal[i] = d.config.Gamma*(vals[i]-level[i]) + (1-d.config.Gamma)*seasonal[i-seasonLength]

		forecasts[i] = level[i-1] + trend[i-1] + seasonal[i-seasonLength]
	}

	for i := 0; i < seasonLength; i++ {
		forecasts[i] = vals[i]
	}

	return level, trend, seasonal, forecasts
}
