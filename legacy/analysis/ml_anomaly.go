package analysis

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

type IsolationForest struct {
	numTrees      int
	samplingSize  int
	maxDepth      int
	trees         []*iTreeNode
	avgPathLength float64
	rng           *rand.Rand
}

type iTreeNode struct {
	splitFeature  int
	splitValue    float64
	left          *iTreeNode
	right         *iTreeNode
	size          int
	isExternal    bool
	pathLengthAdj float64
}

type IsolationForestConfig struct {
	NumTrees     int
	SamplingSize int
	MaxDepth     int
	RandomSeed   int64
}

func DefaultIsolationForestConfig() IsolationForestConfig {
	return IsolationForestConfig{
		NumTrees:     100,
		SamplingSize: 256,
		MaxDepth:     0,
		RandomSeed:   0,
	}
}

func NewIsolationForest(config IsolationForestConfig) *IsolationForest {
	if config.MaxDepth == 0 {
		config.MaxDepth = int(math.Ceil(math.Log2(float64(config.SamplingSize))))
	}

	seed := config.RandomSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	return &IsolationForest{
		numTrees:     config.NumTrees,
		samplingSize: config.SamplingSize,
		maxDepth:     config.MaxDepth,
		trees:        make([]*iTreeNode, 0, config.NumTrees),
		rng:          rand.New(rand.NewSource(seed)),
	}
}

func (f *IsolationForest) Fit(values []TimeSeriesPoint) {
	if len(values) < 2 {
		return
	}

	n := len(values)
	sampleSize := f.samplingSize
	if sampleSize > n {
		sampleSize = n
	}

	f.avgPathLength = averagePathLength(float64(sampleSize))

	data := make([][]float64, n)
	for i, p := range values {
		data[i] = []float64{float64(i % 24), p.Value, float64(p.Timestamp % 86400 / 3600)}
	}

	f.trees = make([]*iTreeNode, 0, f.numTrees)
	for i := 0; i < f.numTrees; i++ {
		sampleIndices := f.sampleIndices(n, sampleSize)
		sample := make([][]float64, sampleSize)
		for j, idx := range sampleIndices {
			sample[j] = data[idx]
		}

		tree := f.buildTree(sample, 0)
		f.trees = append(f.trees, tree)
	}
}

func (f *IsolationForest) sampleIndices(n, sampleSize int) []int {
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	f.rng.Shuffle(n, func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})

	return indices[:sampleSize]
}

func (f *IsolationForest) buildTree(data [][]float64, depth int) *iTreeNode {
	if len(data) <= 1 || depth >= f.maxDepth {
		return &iTreeNode{
			size:          len(data),
			isExternal:    true,
			pathLengthAdj: averagePathLength(float64(len(data))),
		}
	}

	numFeatures := len(data[0])
	splitFeature := f.rng.Intn(numFeatures)

	minVal, maxVal := data[0][splitFeature], data[0][splitFeature]
	for _, d := range data {
		if d[splitFeature] < minVal {
			minVal = d[splitFeature]
		}
		if d[splitFeature] > maxVal {
			maxVal = d[splitFeature]
		}
	}

	if minVal == maxVal {
		return &iTreeNode{
			size:          len(data),
			isExternal:    true,
			pathLengthAdj: averagePathLength(float64(len(data))),
		}
	}

	splitValue := minVal + f.rng.Float64()*(maxVal-minVal)

	var leftData, rightData [][]float64
	for _, d := range data {
		if d[splitFeature] < splitValue {
			leftData = append(leftData, d)
		} else {
			rightData = append(rightData, d)
		}
	}

	return &iTreeNode{
		splitFeature: splitFeature,
		splitValue:   splitValue,
		left:         f.buildTree(leftData, depth+1),
		right:        f.buildTree(rightData, depth+1),
		isExternal:   false,
	}
}

func (f *IsolationForest) pathLength(point []float64, node *iTreeNode, depth int) float64 {
	if node.isExternal {
		return float64(depth) + node.pathLengthAdj
	}

	if point[node.splitFeature] < node.splitValue {
		return f.pathLength(point, node.left, depth+1)
	}
	return f.pathLength(point, node.right, depth+1)
}

func (f *IsolationForest) AnomalyScore(values []TimeSeriesPoint) []float64 {
	if len(f.trees) == 0 || len(values) == 0 {
		return make([]float64, len(values))
	}

	scores := make([]float64, len(values))

	for i, p := range values {
		point := []float64{float64(i % 24), p.Value, float64(p.Timestamp % 86400 / 3600)}

		var totalPathLength float64
		for _, tree := range f.trees {
			totalPathLength += f.pathLength(point, tree, 0)
		}

		avgPath := totalPathLength / float64(len(f.trees))

		scores[i] = math.Pow(2, -avgPath/f.avgPathLength)
	}

	return scores
}

func (f *IsolationForest) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < f.samplingSize/2 {
		return nil
	}

	f.Fit(values)
	scores := f.AnomalyScore(values)

	vals := extractValues(values)
	m := median(vals)

	var anomalies []DetectedAnomaly

	threshold := 0.6

	for i, score := range scores {
		if score > threshold {
			severity := SeverityMedium
			if score > 0.75 {
				severity = SeverityHigh
			}
			if score > 0.85 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if values[i].Value < m {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     values[i].Value,
				Baseline:  m,
				ZScore:    score,
				Detail:    fmt.Sprintf("Isolation Forest anomaly: %.0f (score=%.3f)", values[i].Value, score),
			})
		}
	}

	return anomalies
}

func averagePathLength(n float64) float64 {
	if n <= 1 {
		return 0
	}
	if n == 2 {
		return 1
	}

	EULER_CONSTANT := 0.5772156649
	return 2*(math.Log(n-1)+EULER_CONSTANT) - 2*(n-1)/n
}

type SHESDDetector struct {
	config SHESDConfig
}

type SHESDConfig struct {
	MaxAnomalies   float64
	Alpha          float64
	Direction      string
	Period         int
	Longterm       bool
	PiecewiseCount int
}

func DefaultSHESDConfig() SHESDConfig {
	return SHESDConfig{
		MaxAnomalies:   0.02,
		Alpha:          0.05,
		Direction:      "both",
		Period:         24,
		Longterm:       true,
		PiecewiseCount: 10,
	}
}

func NewSHESDDetector(config SHESDConfig) *SHESDDetector {
	return &SHESDDetector{config: config}
}

func (d *SHESDDetector) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.Period*2 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	var anomalies []DetectedAnomaly

	if d.config.Longterm && len(values) > d.config.Period*d.config.PiecewiseCount {
		anomalies = d.detectPiecewise(values)
	} else {
		anomalies = d.detectSingle(values)
	}

	return anomalies
}

func (d *SHESDDetector) detectPiecewise(values []TimeSeriesPoint) []DetectedAnomaly {
	n := len(values)
	period := d.config.Period

	pieceSize := n / d.config.PiecewiseCount
	if pieceSize < period*2 {
		pieceSize = period * 2
	}

	var allAnomalies []DetectedAnomaly

	for i := 0; i < n; i += pieceSize {
		end := i + pieceSize
		if end > n {
			end = n
		}

		if end-i < period*2 {
			continue
		}

		piece := values[i:end]
		anomalies := d.detectSingle(piece)
		allAnomalies = append(allAnomalies, anomalies...)
	}

	return d.deduplicateAnomalies(allAnomalies)
}

func (d *SHESDDetector) detectSingle(values []TimeSeriesPoint) []DetectedAnomaly {
	decomp := d.decomposeSTL(values, d.config.Period)
	if decomp == nil {
		return nil
	}

	residuals := make([]TimeSeriesPoint, len(values))
	for i, v := range values {
		expected := decomp.Trend[i] + decomp.Seasonal[i]
		residuals[i] = TimeSeriesPoint{
			Timestamp: v.Timestamp,
			Value:     v.Value - expected,
		}
	}

	return d.detectESDAnomalies(residuals, values, decomp)
}

func (d *SHESDDetector) decomposeSTL(values []TimeSeriesPoint, period int) *DecompositionResult {
	if len(values) < period*2 {
		return nil
	}

	n := len(values)
	vals := extractValues(values)

	trend := make([]float64, n)
	windowSize := period
	if windowSize < 7 {
		windowSize = 7
	}

	for i := 0; i < n; i++ {
		start := i - windowSize/2
		end := i + windowSize/2 + 1
		if start < 0 {
			start = 0
		}
		if end > n {
			end = n
		}
		trend[i] = median(vals[start:end])
	}

	for iter := 0; iter < 2; iter++ {
		trend = d.loessSmooth(vals, trend, windowSize)
	}

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

func (d *SHESDDetector) loessSmooth(vals, trend []float64, window int) []float64 {
	n := len(vals)
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

		weights := make([]float64, 0)
		weightedVals := make([]float64, 0)

		for j := start; j < end; j++ {
			distance := math.Abs(float64(i - j))
			weight := math.Exp(-distance * distance / float64(halfWindow*halfWindow))
			weights = append(weights, weight)
			weightedVals = append(weightedVals, vals[j]*weight)
		}

		sumWeights := sum(weights)
		if sumWeights > 0 {
			smoothed[i] = sum(weightedVals) / sumWeights
		} else {
			smoothed[i] = trend[i]
		}
	}

	return smoothed
}

func (d *SHESDDetector) detectESDAnomalies(residuals []TimeSeriesPoint, original []TimeSeriesPoint, decomp *DecompositionResult) []DetectedAnomaly {
	n := len(residuals)
	anomalyCount := int(float64(n) * d.config.MaxAnomalies)
	if anomalyCount < 1 {
		anomalyCount = 1
	}

	var anomalies []DetectedAnomaly
	remaining := make([]int, n)
	for i := 0; i < n; i++ {
		remaining[i] = i
	}

	for k := 0; k < anomalyCount && len(remaining) >= 5; k++ {
		remainingResiduals := make([]float64, len(remaining))
		for i, idx := range remaining {
			remainingResiduals[i] = residuals[idx].Value
		}

		m := median(remainingResiduals)
		mad := mad(remainingResiduals)

		if mad == 0 {
			stdVal := stdDev(remainingResiduals)
			if stdVal > 0 {
				mad = stdVal / 1.4826
			} else {
				break
			}
		}

		maxZScore := 0.0
		maxIdx := 0
		direction := 1

		for i, idx := range remaining {
			z := (residuals[idx].Value - m) / (mad * 1.4826)
			absZ := math.Abs(z)

			if d.config.Direction == "pos" && z < 0 {
				continue
			}
			if d.config.Direction == "neg" && z > 0 {
				continue
			}

			if absZ > maxZScore {
				maxZScore = absZ
				maxIdx = i
				if z > 0 {
					direction = 1
				} else {
					direction = -1
				}
			}
		}

		criticalValue := d.grubbsCriticalValue(len(remaining))

		if maxZScore <= criticalValue {
			break
		}

		originalIdx := remaining[maxIdx]

		expected := decomp.Trend[originalIdx] + decomp.Seasonal[originalIdx]

		severity := SeverityMedium
		if maxZScore > criticalValue*1.5 {
			severity = SeverityHigh
		}
		if maxZScore > criticalValue*2 {
			severity = SeverityCritical
		}

		anomalyType := AnomalyQPSSpike
		if direction < 0 {
			anomalyType = AnomalyQPSDrop
		}

		anomalies = append(anomalies, DetectedAnomaly{
			Type:      anomalyType,
			Severity:  severity,
			Timestamp: original[originalIdx].Timestamp,
			TimeStr:   time.Unix(original[originalIdx].Timestamp, 0).Format("2006-01-02 15:04"),
			Value:     original[originalIdx].Value,
			Baseline:  expected,
			ZScore:    maxZScore,
			Detail:    fmt.Sprintf("S-H-ESD anomaly: %.0f (expected=%.0f, z=%.2f)", original[originalIdx].Value, expected, maxZScore),
		})

		remaining = append(remaining[:maxIdx], remaining[maxIdx+1:]...)
	}

	return anomalies
}

func (d *SHESDDetector) grubbsCriticalValue(n int) float64 {
	t := d.inverseTCDF(1-d.config.Alpha/(2*float64(n)), n-2)
	return (float64(n) - 1) / math.Sqrt(float64(n)) * math.Sqrt(t*t/(float64(n)-2+t*t))
}

func (d *SHESDDetector) inverseTCDF(p float64, df int) float64 {
	if p <= 0 {
		return -3.5
	}
	if p >= 1 {
		return 3.5
	}

	low, high := -5.0, 5.0
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

func (d *SHESDDetector) tDistributionCDF(x float64, df int) float64 {
	if df <= 0 {
		return 0.5 + 0.5*math.Erf(x/math.Sqrt2)
	}

	coeff := math.Gamma(float64(df+1)/2) / (math.Sqrt(float64(df)*math.Pi) * math.Gamma(float64(df)/2))
	integrand := math.Pow(1+x*x/float64(df), -float64(df+1)/2)

	return 0.5 + 0.5*math.Erf(x/math.Sqrt2)*coeff*integrand
}

func (d *SHESDDetector) deduplicateAnomalies(anomalies []DetectedAnomaly) []DetectedAnomaly {
	if len(anomalies) == 0 {
		return anomalies
	}

	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].Timestamp < anomalies[j].Timestamp
	})

	var result []DetectedAnomaly
	seen := make(map[int64]bool)

	for _, a := range anomalies {
		key := a.Timestamp / 1800 * 1800
		if !seen[key] {
			seen[key] = true
			result = append(result, a)
		}
	}

	return result
}

type RandomCutForest struct {
	numTrees    int
	treeSize    int
	trees       []*rcfTree
	timeDecay   float64
	rng         *rand.Rand
	shingleSize int
}

type rcfTree struct {
	root *rcfNode
	size int
}

type rcfNode struct {
	cutDimension int
	cutValue     float64
	left         *rcfNode
	right        *rcfNode
	point        []float64
	leaf         bool
	mass         int
}

type RandomCutForestConfig struct {
	NumTrees    int
	TreeSize    int
	TimeDecay   float64
	ShingleSize int
	RandomSeed  int64
}

func DefaultRandomCutForestConfig() RandomCutForestConfig {
	return RandomCutForestConfig{
		NumTrees:    50,
		TreeSize:    256,
		TimeDecay:   0.01,
		ShingleSize: 4,
		RandomSeed:  0,
	}
}

func NewRandomCutForest(config RandomCutForestConfig) *RandomCutForest {
	seed := config.RandomSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	return &RandomCutForest{
		numTrees:    config.NumTrees,
		treeSize:    config.TreeSize,
		timeDecay:   config.TimeDecay,
		shingleSize: config.ShingleSize,
		trees:       make([]*rcfTree, config.NumTrees),
		rng:         rand.New(rand.NewSource(seed)),
	}
}

func (f *RandomCutForest) Update(point []float64) float64 {
	if f.trees[0] == nil || f.trees[0].root == nil {
		for i := 0; i < f.numTrees; i++ {
			f.trees[i] = &rcfTree{root: nil, size: 0}
		}
	}

	var totalScore float64
	for _, tree := range f.trees {
		score := f.scorePoint(tree, point)
		f.addPoint(tree, point)
		totalScore += score
	}

	return totalScore / float64(f.numTrees)
}

func (f *RandomCutForest) scorePoint(tree *rcfTree, point []float64) float64 {
	if tree.root == nil {
		return 0
	}

	return f.computeScore(tree.root, point, 0)
}

func (f *RandomCutForest) computeScore(node *rcfNode, point []float64, depth int) float64 {
	if node.leaf {
		if node.point == nil {
			return 0
		}
		return 1.0 / (1.0 + float64(depth))
	}

	if point[node.cutDimension] < node.cutValue {
		if node.left == nil {
			return 0
		}
		return f.computeScore(node.left, point, depth+1)
	}
	if node.right == nil {
		return 0
	}
	return f.computeScore(node.right, point, depth+1)
}

func (f *RandomCutForest) addPoint(tree *rcfTree, point []float64) {
	if tree.size >= f.treeSize {
		tree.root = nil
		tree.size = 0
	}

	tree.root = f.insertPoint(tree.root, point, 0)
	tree.size++
}

func (f *RandomCutForest) insertPoint(node *rcfNode, point []float64, depth int) *rcfNode {
	if node == nil {
		return &rcfNode{
			point: point,
			leaf:  true,
			mass:  1,
		}
	}

	if node.leaf {
		boundingBox := make([][]float64, len(point))
		for i := range point {
			boundingBox[i] = []float64{math.Min(point[i], node.point[i]), math.Max(point[i], node.point[i])}
		}

		cutDim := f.selectDimension(boundingBox)
		cutValue := boundingBox[cutDim][0] + f.rng.Float64()*(boundingBox[cutDim][1]-boundingBox[cutDim][0])

		newNode := &rcfNode{
			cutDimension: cutDim,
			cutValue:     cutValue,
			leaf:         false,
			mass:         node.mass + 1,
		}

		if point[cutDim] < cutValue {
			newNode.left = &rcfNode{point: point, leaf: true, mass: 1}
			newNode.right = node
		} else {
			newNode.left = node
			newNode.right = &rcfNode{point: point, leaf: true, mass: 1}
		}

		return newNode
	}

	node.mass++
	if point[node.cutDimension] < node.cutValue {
		node.left = f.insertPoint(node.left, point, depth+1)
	} else {
		node.right = f.insertPoint(node.right, point, depth+1)
	}

	return node
}

func (f *RandomCutForest) selectDimension(boundingBox [][]float64) int {
	ranges := make([]float64, len(boundingBox))
	totalRange := 0.0

	for i, box := range boundingBox {
		ranges[i] = box[1] - box[0]
		totalRange += ranges[i]
	}

	if totalRange == 0 {
		return f.rng.Intn(len(boundingBox))
	}

	r := f.rng.Float64() * totalRange
	sum := 0.0
	for i, rg := range ranges {
		sum += rg
		if r <= sum {
			return i
		}
	}

	return len(boundingBox) - 1
}

func (f *RandomCutForest) Detect(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < f.shingleSize*2 {
		return nil
	}

	vals := extractValues(values)
	m := median(vals)

	var anomalies []DetectedAnomaly
	shingle := make([]float64, f.shingleSize)

	scores := make([]float64, len(values)-f.shingleSize+1)

	for i := f.shingleSize - 1; i < len(values); i++ {
		for j := 0; j < f.shingleSize; j++ {
			shingle[j] = vals[i-f.shingleSize+1+j]
		}

		score := f.Update(shingle)
		scores[i-f.shingleSize+1] = score

		if score > 1.0 {
			severity := SeverityMedium
			if score > 1.5 {
				severity = SeverityHigh
			}
			if score > 2.0 {
				severity = SeverityCritical
			}

			anomalyType := AnomalyQPSSpike
			if values[i].Value < m {
				anomalyType = AnomalyQPSDrop
			}

			anomalies = append(anomalies, DetectedAnomaly{
				Type:      anomalyType,
				Severity:  severity,
				Timestamp: values[i].Timestamp,
				TimeStr:   time.Unix(values[i].Timestamp, 0).Format("2006-01-02 15:04"),
				Value:     values[i].Value,
				Baseline:  m,
				ZScore:    score,
				Detail:    fmt.Sprintf("RCF anomaly: %.0f (score=%.3f)", values[i].Value, score),
			})
		}
	}

	return anomalies
}

type EnsembleAnomalyDetector struct {
	config        EnsembleAnomalyConfig
	madDetector   *AdvancedAnomalyDetector
	esdDetector   *AdvancedAnomalyDetector
	ifDetector    *IsolationForest
	shesdDetector *SHESDDetector
	rcfDetector   *RandomCutForest
}

type EnsembleAnomalyConfig struct {
	UseMAD             bool
	UseESD             bool
	UseIsolationForest bool
	UseSHESD           bool
	UseRCF             bool
	VotingThreshold    int
	MinSamples         int
}

func DefaultEnsembleAnomalyConfig() EnsembleAnomalyConfig {
	return EnsembleAnomalyConfig{
		UseMAD:             true,
		UseESD:             true,
		UseIsolationForest: true,
		UseSHESD:           true,
		UseRCF:             false,
		VotingThreshold:    2,
		MinSamples:         24,
	}
}

func NewEnsembleAnomalyDetector(config EnsembleAnomalyConfig) *EnsembleAnomalyDetector {
	return &EnsembleAnomalyDetector{
		config:        config,
		madDetector:   NewAdvancedAnomalyDetector(DefaultAdvancedAnomalyConfig()),
		esdDetector:   NewAdvancedAnomalyDetector(DefaultAdvancedAnomalyConfig()),
		ifDetector:    NewIsolationForest(DefaultIsolationForestConfig()),
		shesdDetector: NewSHESDDetector(DefaultSHESDConfig()),
		rcfDetector:   NewRandomCutForest(DefaultRandomCutForestConfig()),
	}
}

func (d *EnsembleAnomalyDetector) DetectAll(values []TimeSeriesPoint) []DetectedAnomaly {
	if len(values) < d.config.MinSamples {
		return nil
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp < values[j].Timestamp
	})

	votes := make(map[int64]*anomalyVote)

	if d.config.UseMAD {
		anomalies := d.madDetector.DetectMAD(values)
		d.recordVotes(votes, anomalies, "MAD")
	}

	if d.config.UseESD {
		anomalies := d.esdDetector.DetectESD(values, 0.02, 0.05)
		d.recordVotes(votes, anomalies, "ESD")
	}

	if d.config.UseIsolationForest {
		anomalies := d.ifDetector.Detect(values)
		d.recordVotes(votes, anomalies, "IsolationForest")
	}

	if d.config.UseSHESD {
		anomalies := d.shesdDetector.Detect(values)
		d.recordVotes(votes, anomalies, "SHESD")
	}

	if d.config.UseRCF {
		anomalies := d.rcfDetector.Detect(values)
		d.recordVotes(votes, anomalies, "RCF")
	}

	var result []DetectedAnomaly
	for ts, vote := range votes {
		if vote.count >= d.config.VotingThreshold {
			severity := vote.anomaly.Severity
			if vote.count >= 4 {
				severity = SeverityCritical
			} else if vote.count >= 3 {
				severity = SeverityHigh
			}

			anomaly := vote.anomaly
			anomaly.Severity = severity
			anomaly.Detail = fmt.Sprintf("%s (agreement: %d/%d methods)",
				anomaly.Detail, vote.count, d.countEnabledMethods())

			result = append(result, anomaly)
			_ = ts
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

type anomalyVote struct {
	anomaly DetectedAnomaly
	count   int
	methods map[string]bool
}

func (d *EnsembleAnomalyDetector) recordVotes(votes map[int64]*anomalyVote, anomalies []DetectedAnomaly, method string) {
	for _, a := range anomalies {
		ts := a.Timestamp
		if vote, exists := votes[ts]; exists {
			if !vote.methods[method] {
				vote.count++
				vote.methods[method] = true
				if a.ZScore > vote.anomaly.ZScore {
					vote.anomaly.ZScore = a.ZScore
				}
			}
		} else {
			votes[ts] = &anomalyVote{
				anomaly: a,
				count:   1,
				methods: map[string]bool{method: true},
			}
		}
	}
}

func (d *EnsembleAnomalyDetector) countEnabledMethods() int {
	count := 0
	if d.config.UseMAD {
		count++
	}
	if d.config.UseESD {
		count++
	}
	if d.config.UseIsolationForest {
		count++
	}
	if d.config.UseSHESD {
		count++
	}
	if d.config.UseRCF {
		count++
	}
	return count
}
