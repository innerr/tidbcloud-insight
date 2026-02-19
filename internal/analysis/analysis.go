package analysis

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func HistogramQuantile(buckets map[string]float64, quantile float64) float64 {
	if buckets == nil {
		return math.NaN()
	}

	total, exists := buckets["+Inf"]
	if !exists || total == 0 {
		return math.NaN()
	}

	target := total * quantile

	var sortedLE []struct {
		le    float64
		count float64
	}

	for le, count := range buckets {
		if le != "+Inf" {
			leVal, err := strconv.ParseFloat(le, 64)
			if err == nil {
				sortedLE = append(sortedLE, struct {
					le    float64
					count float64
				}{leVal, count})
			}
		}
	}

	sort.Slice(sortedLE, func(i, j int) bool {
		return sortedLE[i].le < sortedLE[j].le
	})

	prevLE := 0.0
	prevCount := 0.0

	for _, item := range sortedLE {
		if item.count >= target {
			if item.count == prevCount {
				return prevLE
			}
			return prevLE + (item.le-prevLE)/(item.count-prevCount)*(target-prevCount)
		}
		prevLE = item.le
		prevCount = item.count
	}

	return prevLE
}

func PearsonCorrelation(x, y []float64) float64 {
	if len(x) != len(y) || len(x) < 3 {
		return math.NaN()
	}

	n := len(x)
	sumX, sumY := 0.0, 0.0
	for i := 0; i < n; i++ {
		sumX += x[i]
		sumY += y[i]
	}
	meanX := sumX / float64(n)
	meanY := sumY / float64(n)

	cov := 0.0
	stdX, stdY := 0.0, 0.0
	for i := 0; i < n; i++ {
		cov += (x[i] - meanX) * (y[i] - meanY)
		stdX += (x[i] - meanX) * (x[i] - meanX)
		stdY += (y[i] - meanY) * (y[i] - meanY)
	}
	stdX = math.Sqrt(stdX)
	stdY = math.Sqrt(stdY)

	if stdX == 0 || stdY == 0 {
		return math.NaN()
	}

	return cov / (stdX * stdY)
}

func CalculateRate(values [][]interface{}) []struct {
	Timestamp int64
	Rate      float64
} {
	var rates []struct {
		Timestamp int64
		Rate      float64
	}

	for i := 1; i < len(values); i++ {
		ts1, ok1 := values[i-1][0].(float64)
		ts2, ok2 := values[i][0].(float64)
		v1, ok3 := values[i-1][1].(interface{})
		v2, ok4 := values[i][1].(interface{})

		if !ok1 || !ok2 || !ok3 || !ok4 {
			continue
		}

		dt := ts2 - ts1
		if dt <= 0 {
			continue
		}

		val1 := ToFloat64(v1)
		val2 := ToFloat64(v2)
		rate := (val2 - val1) / dt

		if rate >= 0 {
			rates = append(rates, struct {
				Timestamp int64
				Rate      float64
			}{int64(ts2), rate})
		}
	}

	return rates
}

func ToFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

type InstanceData struct {
	CPU                map[int64]float64
	QPS                map[int64]float64
	LatitudeHistograms map[int64]map[string]float64
}

type AnalysisResult struct {
	TiDBNodes         int
	TiKVNodes         int
	TotalAvgQPS       float64
	TotalMaxQPS       float64
	TotalAvgCPU       float64
	TotalMaxCPU       float64
	AvgUtilization    float64
	PeakUtilization   float64
	QPSP99Correlation float64
	Anomalies         []Anomaly
	TiDBSummary       []TiDBInstanceSummary
	TiKVSummary       []TiKVInstanceSummary
}

type TiDBInstanceSummary struct {
	Instance   string
	AvgCPU     float64
	MaxCPU     float64
	AvgQPS     float64
	MaxQPS     float64
	P50Ms      float64
	P99Ms      float64
	CPUPerKQPS float64
}

type TiKVInstanceSummary struct {
	Instance string
	AvgQPS   float64
	MaxQPS   float64
	P99Ms    float64
}

type Anomaly struct {
	Type     string
	Instance string
	Value    string
	ZScore   float64
	Detail   string
}

type CorrelationResult struct {
	Instance string
	CorrP50  float64
	CorrP99  float64
}

func LoadMetric(cacheDir, name string) (map[string]interface{}, error) {
	filePath := filepath.Join(cacheDir, name+".json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func GetResultData(metric map[string]interface{}) []map[string]interface{} {
	if metric == nil {
		return nil
	}
	data, ok := metric["data"].(map[string]interface{})
	if !ok {
		return nil
	}
	result, ok := data["result"].([]interface{})
	if !ok {
		return nil
	}

	var results []map[string]interface{}
	for _, r := range result {
		if m, ok := r.(map[string]interface{}); ok {
			results = append(results, m)
		}
	}
	return results
}

func GetMetricLabel(series map[string]interface{}, key string) string {
	metric, ok := series["metric"].(map[string]interface{})
	if !ok {
		return ""
	}
	val, ok := metric[key].(string)
	if !ok {
		return ""
	}
	return val
}

func GetValues(series map[string]interface{}) [][]interface{} {
	vals, ok := series["values"].([]interface{})
	if !ok {
		return nil
	}

	var values [][]interface{}
	for _, v := range vals {
		if arr, ok := v.([]interface{}); ok {
			values = append(values, arr)
		}
	}
	return values
}

func formatFloat(f float64) string {
	s := fmt.Sprintf("%.0f", f)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, c)
	}
	return string(result)
}

func RunComprehensiveAnalysis(cacheID, cacheBaseDir string) (*AnalysisResult, error) {
	cacheDir := filepath.Join(cacheBaseDir, cacheID)
	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("cache %s not found", cacheID)
	}

	tidbCPU, _ := LoadMetric(cacheDir, "process_cpu_seconds_total")
	tidbQPS, _ := LoadMetric(cacheDir, "tidb_server_query_total")
	tidbLatency, _ := LoadMetric(cacheDir, "tidb_server_handle_query_duration_seconds_bucket")
	tikvQPS, _ := LoadMetric(cacheDir, "tikv_grpc_msg_duration_seconds_count")
	tikvLatency, _ := LoadMetric(cacheDir, "tikv_grpc_msg_duration_seconds_bucket")

	var firstTS, lastTS float64
	for _, series := range GetResultData(tidbQPS) {
		vals := GetValues(series)
		if len(vals) > 0 {
			if ts, ok := vals[0][0].(float64); ok {
				if firstTS == 0 || ts < firstTS {
					firstTS = ts
				}
			}
			if ts, ok := vals[len(vals)-1][0].(float64); ok {
				if lastTS == 0 || ts > lastTS {
					lastTS = ts
				}
			}
		}
	}

	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("TiDB Cluster Comprehensive Analysis (48h)")
	fmt.Println(strings.Repeat("=", 70))

	if firstTS > 0 && lastTS > 0 {
		durationH := (lastTS - firstTS) / 3600
		startTime := time.Unix(int64(firstTS), 0)
		endTime := time.Unix(int64(lastTS), 0)
		fmt.Printf("Time Range: %s ~ %s (%.1fh)\n",
			startTime.Format("2006-01-02 15:04"),
			endTime.Format("2006-01-02 15:04"),
			durationH)
	}
	fmt.Println()

	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("PART 1: TiDB Node Analysis")
	fmt.Println(strings.Repeat("=", 70))

	tidbInstances := make(map[string]*InstanceData)

	for _, series := range GetResultData(tidbCPU) {
		inst := GetMetricLabel(series, "instance")
		if !strings.Contains(inst, "tidb") {
			continue
		}
		if _, exists := tidbInstances[inst]; !exists {
			tidbInstances[inst] = &InstanceData{
				CPU:                make(map[int64]float64),
				QPS:                make(map[int64]float64),
				LatitudeHistograms: make(map[int64]map[string]float64),
			}
		}

		for _, rate := range CalculateRate(GetValues(series)) {
			if rate.Rate > 0.1 {
				tidbInstances[inst].CPU[rate.Timestamp] = rate.Rate
			}
		}
	}

	for _, series := range GetResultData(tidbQPS) {
		inst := GetMetricLabel(series, "instance")
		if !strings.Contains(inst, "tidb") {
			continue
		}
		if _, exists := tidbInstances[inst]; !exists {
			tidbInstances[inst] = &InstanceData{
				CPU:                make(map[int64]float64),
				QPS:                make(map[int64]float64),
				LatitudeHistograms: make(map[int64]map[string]float64),
			}
		}

		for _, rate := range CalculateRate(GetValues(series)) {
			tidbInstances[inst].QPS[rate.Timestamp] += rate.Rate
		}
	}

	latencyByInstTs := make(map[string]map[int64]map[string]float64)
	for _, series := range GetResultData(tidbLatency) {
		inst := GetMetricLabel(series, "instance")
		if !strings.Contains(inst, "tidb") {
			continue
		}
		le := GetMetricLabel(series, "le")

		if _, exists := latencyByInstTs[inst]; !exists {
			latencyByInstTs[inst] = make(map[int64]map[string]float64)
		}

		for _, v := range GetValues(series) {
			if len(v) >= 2 {
				ts := int64(ToFloat64(v[0]))
				val := ToFloat64(v[1])

				if _, exists := latencyByInstTs[inst][ts]; !exists {
					latencyByInstTs[inst][ts] = make(map[string]float64)
				}
				latencyByInstTs[inst][ts][le] += val
			}
		}
	}

	latencyStats := make(map[string]map[int64]map[string]float64)
	for inst, tsData := range latencyByInstTs {
		if _, exists := tidbInstances[inst]; exists {
			latencyStats[inst] = calcHistogramRates(tsData)
		}
	}

	fmt.Println("\n--- TiDB Node Summary ---\n")
	fmt.Printf("%-15s %-10s %-10s %-10s %-10s %-10s %-10s\n",
		"Instance", "Avg CPU", "Max CPU", "Avg QPS", "Max QPS", "P50(ms)", "P99(ms)")
	fmt.Println(strings.Repeat("-", 85))

	result := &AnalysisResult{
		TiDBNodes: len(tidbInstances),
	}

	var summaries []TiDBInstanceSummary
	var sortedInstances []string
	for inst := range tidbInstances {
		sortedInstances = append(sortedInstances, inst)
	}
	sort.Strings(sortedInstances)

	for _, inst := range sortedInstances {
		data := tidbInstances[inst]

		var cpuVals, qpsVals []float64
		for _, v := range data.CPU {
			cpuVals = append(cpuVals, v)
		}
		for _, v := range data.QPS {
			qpsVals = append(qpsVals, v)
		}

		var p50Vals, p99Vals []struct {
			p     float64
			total float64
		}

		for _, buckets := range latencyStats[inst] {
			total := buckets["+Inf"]
			if total > 0 {
				p50 := HistogramQuantile(buckets, 0.5)
				p99 := HistogramQuantile(buckets, 0.99)
				if !math.IsNaN(p50) {
					p50Vals = append(p50Vals, struct {
						p     float64
						total float64
					}{p50, total})
				}
				if !math.IsNaN(p99) {
					p99Vals = append(p99Vals, struct {
						p     float64
						total float64
					}{p99, total})
				}
			}
		}

		avgCPU := mean(cpuVals)
		maxCPU := max(cpuVals)
		avgQPS := mean(qpsVals)
		maxQPS := max(qpsVals)

		avgP50 := weightedMean(p50Vals) * 1000
		avgP99 := weightedMean(p99Vals) * 1000

		cpuPerKQPS := 0.0
		if avgQPS > 0 {
			cpuPerKQPS = avgCPU / (avgQPS / 1000)
		}

		summary := TiDBInstanceSummary{
			Instance:   inst,
			AvgCPU:     avgCPU,
			MaxCPU:     maxCPU,
			AvgQPS:     avgQPS,
			MaxQPS:     maxQPS,
			P50Ms:      avgP50,
			P99Ms:      avgP99,
			CPUPerKQPS: cpuPerKQPS,
		}
		summaries = append(summaries, summary)

		fmt.Printf("%-15s %-10.2f %-10.2f %-10.0f %-10.0f %-10.2f %-10.2f\n",
			inst, avgCPU, maxCPU, avgQPS, maxQPS, avgP50, avgP99)
	}

	result.TiDBSummary = summaries

	for _, s := range summaries {
		result.TotalAvgQPS += s.AvgQPS
		result.TotalMaxQPS += s.MaxQPS
		result.TotalAvgCPU += s.AvgCPU
		result.TotalMaxCPU += s.MaxCPU
	}

	fmt.Println("\n--- QPS-Latency Correlation ---\n")

	var corrResults []CorrelationResult
	var sortedInstances2 []string
	for inst := range tidbInstances {
		sortedInstances2 = append(sortedInstances2, inst)
	}
	sort.Strings(sortedInstances2)

	for _, inst := range sortedInstances2 {
		qps := tidbInstances[inst].QPS
		rates := latencyStats[inst]

		commonTS := make(map[int64]bool)
		for ts := range qps {
			if _, exists := rates[ts]; exists {
				commonTS[ts] = true
			}
		}

		if len(commonTS) < 10 {
			continue
		}

		var sortedTS []int64
		for ts := range commonTS {
			sortedTS = append(sortedTS, ts)
		}
		sort.Slice(sortedTS, func(i, j int) bool { return sortedTS[i] < sortedTS[j] })

		var qpsList, p50List, p99List []float64
		for _, ts := range sortedTS {
			buckets := rates[ts]
			total := buckets["+Inf"]
			if total > 0 {
				qpsList = append(qpsList, qps[ts])
				p50 := HistogramQuantile(buckets, 0.5)
				p99 := HistogramQuantile(buckets, 0.99)
				if !math.IsNaN(p50) {
					p50List = append(p50List, p50)
				}
				if !math.IsNaN(p99) {
					p99List = append(p99List, p99)
				}
			}
		}

		minLen := minInt(len(qpsList), len(p50List), len(p99List))
		qpsList = qpsList[:minLen]
		p50List = p50List[:minLen]
		p99List = p99List[:minLen]

		corrP50 := math.NaN()
		corrP99 := math.NaN()
		if minLen >= 10 {
			corrP50 = PearsonCorrelation(qpsList, p50List)
			corrP99 = PearsonCorrelation(qpsList, p99List)
		}

		corrResults = append(corrResults, CorrelationResult{
			Instance: inst,
			CorrP50:  corrP50,
			CorrP99:  corrP99,
		})
	}

	fmt.Printf("%-15s %-12s %-12s %s\n", "Instance", "QPS-P50", "QPS-P99", "Assessment")
	fmt.Println(strings.Repeat("-", 60))
	for _, c := range corrResults {
		assessment := ""
		if !math.IsNaN(c.CorrP99) {
			if math.Abs(c.CorrP99) < 0.3 {
				assessment = "✓ Low correlation (stable latency)"
			} else if c.CorrP99 > 0.5 {
				assessment = "⚠️ High correlation (load sensitive)"
			} else {
				assessment = "Moderate correlation"
			}
		}
		fmt.Printf("%-15s %-12.3f %-12.3f %s\n", c.Instance, c.CorrP50, c.CorrP99, assessment)
	}

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("PART 2: Cost & Capacity Analysis")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("\n--- Cluster Metrics ---\n")
	fmt.Printf("TiDB Nodes: %d\n", len(summaries))
	fmt.Printf("Total Avg QPS: %s\n", formatFloat(result.TotalAvgQPS))
	fmt.Printf("Total Peak QPS: %s\n", formatFloat(result.TotalMaxQPS))
	if result.TotalAvgQPS > 0 {
		fmt.Printf("Peak/Avg Ratio: %.2fx\n", result.TotalMaxQPS/result.TotalAvgQPS)
	}
	fmt.Printf("Total Avg CPU: %.1f cores\n", result.TotalAvgCPU)
	fmt.Printf("Total Peak CPU: %.1f cores\n", result.TotalMaxCPU)

	coresPerNode := 8.0
	totalCapacity := float64(len(summaries)) * coresPerNode
	if totalCapacity > 0 {
		result.AvgUtilization = result.TotalAvgCPU / totalCapacity * 100
		result.PeakUtilization = result.TotalMaxCPU / totalCapacity * 100
	}

	fmt.Printf("\n--- Utilization (assuming %.0f cores/node) ---\n", coresPerNode)
	fmt.Printf("Total Capacity: %.0f cores\n", totalCapacity)
	fmt.Printf("Avg Utilization: %.1f%%\n", result.AvgUtilization)
	fmt.Printf("Peak Utilization: %.1f%%\n", result.PeakUtilization)

	fmt.Printf("\n--- Cost Assessment ---\n")
	if result.AvgUtilization < 30 {
		fmt.Printf("🔴 Severely under-utilized (%.1f%%) - Recommend reducing nodes\n", result.AvgUtilization)
	} else if result.AvgUtilization < 50 {
		fmt.Printf("🟡 Under-utilized (%.1f%%) - Room for consolidation\n", result.AvgUtilization)
	} else if result.AvgUtilization < 70 {
		fmt.Printf("🟢 Well-utilized (%.1f%%) - Good balance\n", result.AvgUtilization)
	} else {
		fmt.Printf("🟠 High utilization (%.1f%%) - May need scaling soon\n", result.AvgUtilization)
	}

	var validCorr []float64
	for _, c := range corrResults {
		if !math.IsNaN(c.CorrP99) {
			validCorr = append(validCorr, c.CorrP99)
		}
	}
	avgCorr := 0.0
	if len(validCorr) > 0 {
		avgCorr = mean(validCorr)
	}
	result.QPSP99Correlation = avgCorr

	fmt.Printf("\n--- Load Sensitivity ---\n")
	fmt.Printf("Avg QPS-P99 Correlation: %.3f\n", avgCorr)
	if math.Abs(avgCorr) < 0.3 {
		fmt.Println("✓ LOW LOAD SENSITIVITY - Latency stable regardless of traffic")
		fmt.Println("  → Good user experience, but may indicate over-provisioning")
		fmt.Println("  → Monitor traffic growth before considering downsizing")
	} else if avgCorr > 0.5 {
		fmt.Println("⚠️ HIGH LOAD SENSITIVITY - Latency affected by traffic")
		fmt.Println("  → Approaching capacity limits")
	} else {
		fmt.Println("→ MODERATE LOAD - Some latency variation with traffic")
	}

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("PART 3: TiKV Node Analysis")
	fmt.Println(strings.Repeat("=", 70))

	tikvInstances := make(map[string]*InstanceData)
	for _, series := range GetResultData(tikvQPS) {
		inst := GetMetricLabel(series, "instance")
		if !strings.Contains(inst, "tikv") {
			continue
		}
		if _, exists := tikvInstances[inst]; !exists {
			tikvInstances[inst] = &InstanceData{
				CPU:                make(map[int64]float64),
				QPS:                make(map[int64]float64),
				LatitudeHistograms: make(map[int64]map[string]float64),
			}
		}

		for _, rate := range CalculateRate(GetValues(series)) {
			tikvInstances[inst].QPS[rate.Timestamp] += rate.Rate
		}
	}

	tikvLatencyByInstTs := make(map[string]map[int64]map[string]float64)
	for _, series := range GetResultData(tikvLatency) {
		inst := GetMetricLabel(series, "instance")
		if !strings.Contains(inst, "tikv") {
			continue
		}
		le := GetMetricLabel(series, "le")

		if _, exists := tikvLatencyByInstTs[inst]; !exists {
			tikvLatencyByInstTs[inst] = make(map[int64]map[string]float64)
		}

		for _, v := range GetValues(series) {
			if len(v) >= 2 {
				ts := int64(ToFloat64(v[0]))
				val := ToFloat64(v[1])

				if _, exists := tikvLatencyByInstTs[inst][ts]; !exists {
					tikvLatencyByInstTs[inst][ts] = make(map[string]float64)
				}
				tikvLatencyByInstTs[inst][ts][le] += val
			}
		}
	}

	tikvLatencyStats := make(map[string]map[int64]map[string]float64)
	for inst, tsData := range tikvLatencyByInstTs {
		if _, exists := tikvInstances[inst]; exists {
			tikvLatencyStats[inst] = calcHistogramRates(tsData)
		}
	}

	var tikvSummaries []TiKVInstanceSummary
	for inst := range tikvInstances {
		data := tikvInstances[inst]

		var qpsVals []float64
		for _, v := range data.QPS {
			qpsVals = append(qpsVals, v)
		}

		avgQPS := mean(qpsVals)
		maxQPS := max(qpsVals)

		var p99Vals []struct {
			p     float64
			total float64
		}
		for _, buckets := range tikvLatencyStats[inst] {
			total := buckets["+Inf"]
			if total > 0 {
				p99 := HistogramQuantile(buckets, 0.99)
				if !math.IsNaN(p99) {
					p99Vals = append(p99Vals, struct {
						p     float64
						total float64
					}{p99, total})
				}
			}
		}

		avgP99 := weightedMean(p99Vals) * 1000

		tikvSummaries = append(tikvSummaries, TiKVInstanceSummary{
			Instance: inst,
			AvgQPS:   avgQPS,
			MaxQPS:   maxQPS,
			P99Ms:    avgP99,
		})
	}

	result.TiKVNodes = len(tikvInstances)
	result.TiKVSummary = tikvSummaries

	sort.Slice(tikvSummaries, func(i, j int) bool {
		return tikvSummaries[i].AvgQPS > tikvSummaries[j].AvgQPS
	})

	fmt.Printf("\n%-15s %-12s %-12s %-12s\n", "Instance", "Avg QPS", "Max QPS", "P99(ms)")
	fmt.Println(strings.Repeat("-", 55))
	for _, s := range tikvSummaries {
		fmt.Printf("%-15s %-12.0f %-12.0f %-12.2f\n", s.Instance, s.AvgQPS, s.MaxQPS, s.P99Ms)
	}

	if len(tikvSummaries) > 0 {
		var qpsVals []float64
		for _, s := range tikvSummaries {
			qpsVals = append(qpsVals, s.AvgQPS)
		}
		avgTiKVQPS := mean(qpsVals)
		stdTiKVQPS := stdDev(qpsVals)
		cv := 0.0
		if avgTiKVQPS > 0 {
			cv = stdTiKVQPS / avgTiKVQPS * 100
		}

		fmt.Printf("\nTiKV Load Balance: CV=%.1f%%\n", cv)
		if cv < 10 {
			fmt.Println("✓ Well balanced")
		} else if cv < 20 {
			fmt.Println("→ Moderately balanced")
		} else {
			fmt.Println("⚠️ Unbalanced - check for hot regions")
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("SUMMARY")
	fmt.Println(strings.Repeat("=", 70))

	corrLevel := "low"
	if math.Abs(avgCorr) >= 0.5 {
		corrLevel = "high"
	} else if math.Abs(avgCorr) >= 0.3 {
		corrLevel = "moderate"
	}

	latencyStmt := "System has stable latency regardless of load -> Low load"
	if math.Abs(avgCorr) >= 0.3 {
		latencyStmt = "Latency varies with load"
	}

	peakRatio := result.TotalMaxQPS / maxFloat64(1, result.TotalAvgQPS)

	fmt.Printf(`
📊 Cluster Status:
   - %d TiDB nodes, %d TiKV nodes
   - Avg QPS: %s, Peak QPS: %s (%.1fx)
   - CPU Utilization: %.1f%% avg, %.1f%% peak

🔍 QPS-Latency Analysis:
   - Correlation: %.3f (%s)
   - %s

💡 Cost Recommendations:
`, len(summaries), len(tikvSummaries), formatFloat(result.TotalAvgQPS), formatFloat(result.TotalMaxQPS),
		peakRatio,
		result.AvgUtilization, result.PeakUtilization,
		avgCorr, corrLevel, latencyStmt)

	if result.AvgUtilization < 50 && math.Abs(avgCorr) < 0.3 {
		fmt.Println("   ⚠️  Low utilization AND low load sensitivity")
		fmt.Println("   → Consider reducing TiDB nodes (potential cost savings)")
		fmt.Printf("   → Current: %d nodes at %.1f%% avg utilization\n", len(summaries), result.AvgUtilization)
		targetNodes := maxInt(2, int(float64(len(summaries))*result.AvgUtilization/60))
		fmt.Printf("   → Could consolidate to ~%d nodes\n", targetNodes)
	} else if result.AvgUtilization < 50 {
		fmt.Println("   → Low utilization but monitor load sensitivity before changes")
	} else {
		fmt.Println("   → Current utilization is reasonable")
	}

	for _, s := range summaries {
		var effVals []float64
		for _, x := range summaries {
			effVals = append(effVals, x.CPUPerKQPS)
		}
		avgEff := mean(effVals)
		if s.CPUPerKQPS > avgEff*1.15 {
			pct := (s.CPUPerKQPS/avgEff - 1) * 100
			fmt.Printf("\n⚠️  %s has %.0f%% higher CPU per QPS\n", s.Instance, pct)
			fmt.Println("   → Investigate: background tasks, hot data, or configuration")
		}
	}

	fmt.Println()

	return result, nil
}

func calcHistogramRates(tsData map[int64]map[string]float64) map[int64]map[string]float64 {
	var sortedTS []int64
	for ts := range tsData {
		sortedTS = append(sortedTS, ts)
	}
	sort.Slice(sortedTS, func(i, j int) bool {
		return sortedTS[i] < sortedTS[j]
	})

	result := make(map[int64]map[string]float64)

	for i := 1; i < len(sortedTS); i++ {
		prevTS := sortedTS[i-1]
		currTS := sortedTS[i]
		dt := currTS - prevTS

		if dt <= 0 {
			continue
		}

		prevBuckets := tsData[prevTS]
		currBuckets := tsData[currTS]

		delta := make(map[string]float64)
		for le := range prevBuckets {
			d := currBuckets[le] - prevBuckets[le]
			if d >= 0 {
				delta[le] = d
			}
		}
		for le := range currBuckets {
			if _, exists := prevBuckets[le]; !exists {
				delta[le] = currBuckets[le]
			}
		}

		if delta["+Inf"] > 0 {
			result[currTS] = delta
		}
	}

	return result
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func max(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals {
		if v > m {
			m = v
		}
	}
	return m
}

func stdDev(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := mean(vals)
	sum := 0.0
	for _, v := range vals {
		sum += (v - m) * (v - m)
	}
	return math.Sqrt(sum / float64(len(vals)))
}

func weightedMean(vals []struct {
	p     float64
	total float64
}) float64 {
	if len(vals) == 0 {
		return 0
	}
	sumP := 0.0
	sumW := 0.0
	for _, v := range vals {
		sumP += v.p * v.total
		sumW += v.total
	}
	if sumW == 0 {
		return 0
	}
	return sumP / sumW
}

func minInt(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
