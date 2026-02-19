package analysis

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type SQLTypeStats struct {
	Type        string  `json:"type"`
	Count       float64 `json:"count"`
	Rate        float64 `json:"rate"`
	Percent     float64 `json:"percent"`
	MeanLatency float64 `json:"mean_latency_ms"`
	P99Latency  float64 `json:"p99_latency_ms"`
}

type TiKVOpStats struct {
	Op          string  `json:"op"`
	Count       float64 `json:"count"`
	Rate        float64 `json:"rate"`
	Percent     float64 `json:"percent"`
	MeanLatency float64 `json:"mean_latency_ms"`
	P99Latency  float64 `json:"p99_latency_ms"`
}

type WorkloadProfile struct {
	SQLTypes           []SQLTypeStats `json:"sql_types"`
	TiKVOps            []TiKVOpStats  `json:"tikv_ops"`
	ReadRatio          float64        `json:"read_ratio"`
	WriteRatio         float64        `json:"write_ratio"`
	InternalRatio      float64        `json:"internal_ratio"`
	IsReadHeavy        bool           `json:"is_read_heavy"`
	IsWriteHeavy       bool           `json:"is_write_heavy"`
	DominantSQLType    string         `json:"dominant_sql_type"`
	DominantTiKVOp     string         `json:"dominant_tikv_op"`
	ComplexityScore    float64        `json:"complexity_score"`
	BalanceScore       float64        `json:"balance_score"`
	WriteAmplification float64        `json:"write_amplification"`
	TransactionPattern string         `json:"transaction_pattern"`
	HotspotRisk        string         `json:"hotspot_risk"`
}

type WorkloadAnomaly struct {
	Type      AnomalyType `json:"type"`
	Severity  Severity    `json:"severity"`
	Timestamp int64       `json:"timestamp"`
	TimeStr   string      `json:"time"`
	Category  string      `json:"category"`
	Value     float64     `json:"value"`
	Baseline  float64     `json:"baseline"`
	Detail    string      `json:"detail"`
}

func AnalyzeWorkloadProfile(
	sqlTypeData map[string][]TimeSeriesPoint,
	sqlLatencyData map[string][]TimeSeriesPoint,
	tikvOpData map[string][]TimeSeriesPoint,
	tikvLatencyData map[string][]TimeSeriesPoint,
) *WorkloadProfile {
	profile := &WorkloadProfile{
		SQLTypes: []SQLTypeStats{},
		TiKVOps:  []TiKVOpStats{},
	}

	if len(sqlTypeData) > 0 {
		profile.SQLTypes = analyzeSQLTypes(sqlTypeData, sqlLatencyData)
		profile.ReadRatio, profile.WriteRatio = calculateReadWriteRatio(sqlTypeData)
		profile.InternalRatio = 0
		profile.IsReadHeavy = profile.ReadRatio > 0.6
		profile.IsWriteHeavy = profile.WriteRatio > 0.3 || (profile.WriteRatio > profile.ReadRatio*2 && profile.ReadRatio < 0.2)

		if len(profile.SQLTypes) > 0 {
			profile.DominantSQLType = profile.SQLTypes[0].Type
		}

		profile.ComplexityScore = calculateComplexityScore(sqlTypeData)
		profile.BalanceScore = calculateBalanceScore(profile.SQLTypes)
	}

	if len(tikvOpData) > 0 {
		profile.TiKVOps = analyzeTiKVOps(tikvOpData, tikvLatencyData)

		if len(profile.TiKVOps) > 0 {
			profile.DominantTiKVOp = profile.TiKVOps[0].Op
		}

		profile.WriteAmplification = calculateWriteAmplification(tikvOpData, sqlTypeData)
	}

	profile.TransactionPattern = identifyTransactionPattern(profile)
	profile.HotspotRisk = assessHotspotRisk(profile)

	return profile
}

func calculateComplexityScore(sqlTypeData map[string][]TimeSeriesPoint) float64 {
	if len(sqlTypeData) == 0 {
		return 0
	}

	complexTypes := map[string]float64{
		"Join":     0.3,
		"Subquery": 0.25,
		"Union":    0.2,
		"Select":   0.1,
		"Insert":   0.05,
		"Update":   0.1,
		"Delete":   0.15,
	}

	var totalScore, totalCount float64
	for sqlType, points := range sqlTypeData {
		if len(points) == 0 {
			continue
		}
		vals := extractValues(points)
		sum := 0.0
		for _, v := range vals {
			sum += v
		}

		if weight, ok := complexTypes[sqlType]; ok {
			totalScore += sum * weight
		}
		totalCount += sum
	}

	if totalCount == 0 {
		return 0
	}

	return math.Min(1.0, totalScore/totalCount*2)
}

func calculateBalanceScore(sqlTypes []SQLTypeStats) float64 {
	if len(sqlTypes) < 2 {
		return 1.0
	}

	var percents []float64
	for _, s := range sqlTypes {
		percents = append(percents, s.Percent)
	}

	totalVariance := 0.0
	for _, p := range percents {
		totalVariance += p * p
	}

	idealVariance := 100.0 * 100.0 / float64(len(sqlTypes))
	if idealVariance == 0 {
		return 1.0
	}

	balanceRatio := totalVariance / idealVariance
	return math.Max(0, 1.0-balanceRatio)
}

func calculateWriteAmplification(tikvOpData map[string][]TimeSeriesPoint, sqlTypeData map[string][]TimeSeriesPoint) float64 {
	prewriteSum := 0.0
	if points, ok := tikvOpData["kv_prewrite"]; ok {
		for _, v := range extractValues(points) {
			prewriteSum += v
		}
	}

	writeSum := 0.0
	writeTypes := map[string]bool{"Insert": true, "Update": true, "Delete": true, "Replace": true}
	for sqlType, points := range sqlTypeData {
		if writeTypes[sqlType] {
			for _, v := range extractValues(points) {
				writeSum += v
			}
		}
	}

	if writeSum == 0 {
		return 1.0
	}

	return prewriteSum / writeSum
}

func identifyTransactionPattern(profile *WorkloadProfile) string {
	if profile.WriteRatio > 0.5 {
		return "write_intensive"
	} else if profile.ReadRatio > 0.7 {
		return "read_intensive"
	} else if profile.WriteAmplification > 3 {
		return "high_write_amplification"
	} else if profile.ComplexityScore > 0.5 {
		return "complex_queries"
	} else if profile.BalanceScore > 0.7 {
		return "balanced"
	}

	for _, op := range profile.TiKVOps {
		if strings.Contains(strings.ToLower(op.Op), "batch") && op.Percent > 30 {
			return "batch_oriented"
		}
	}

	return "mixed"
}

func assessHotspotRisk(profile *WorkloadProfile) string {
	if len(profile.TiKVOps) == 0 {
		return "unknown"
	}

	for _, op := range profile.TiKVOps {
		if op.Percent > 80 {
			if strings.Contains(strings.ToLower(op.Op), "prewrite") ||
				strings.Contains(strings.ToLower(op.Op), "commit") {
				return "high"
			}
		}
	}

	if profile.WriteAmplification > 5 {
		return "high"
	} else if profile.WriteAmplification > 3 || profile.WriteRatio > 0.4 {
		return "medium"
	}

	return "low"
}

func analyzeSQLTypes(typeData map[string][]TimeSeriesPoint, latencyData map[string][]TimeSeriesPoint) []SQLTypeStats {
	var stats []SQLTypeStats
	var total float64

	typeTotals := make(map[string]float64)
	for sqlType, points := range typeData {
		if len(points) == 0 {
			continue
		}
		vals := extractValues(points)
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		if sum == 0 {
			continue
		}
		typeTotals[sqlType] = sum
		total += sum
	}

	if total == 0 {
		return stats
	}

	for sqlType, sum := range typeTotals {
		latency := latencyData[sqlType]
		var meanLat, p99Lat float64
		if len(latency) > 0 {
			latVals := extractValues(latency)
			meanLat = mean(latVals) * 1000
			p99Lat = percentile(latVals, 0.99) * 1000
		}

		stats = append(stats, SQLTypeStats{
			Type:        sqlType,
			Count:       sum,
			Rate:        sum / float64(len(typeData[sqlType])),
			Percent:     sum / total * 100,
			MeanLatency: meanLat,
			P99Latency:  p99Lat,
		})
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Percent > stats[j].Percent
	})

	return stats
}

func analyzeTiKVOps(opData map[string][]TimeSeriesPoint, latencyData map[string][]TimeSeriesPoint) []TiKVOpStats {
	var stats []TiKVOpStats
	var total float64

	opTotals := make(map[string]float64)
	for op, points := range opData {
		if len(points) == 0 {
			continue
		}

		normalizedName := op
		if op == "kv_prewrite" || op == "kv_commit" {
			normalizedName = "kv_prewrite/commit"
		}

		vals := extractValues(points)
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		opTotals[normalizedName] += sum
	}

	if total == 0 {
		return stats
	}

	for _, sum := range opTotals {
		total += sum
	}

	for op, sum := range opTotals {
		var rate float64
		if prewritePoints := opData["kv_prewrite"]; len(prewritePoints) > 0 {
			rate = sum / float64(len(prewritePoints))
		} else if commitPoints := opData["kv_commit"]; len(commitPoints) > 0 {
			rate = sum / float64(len(commitPoints))
		}

		stats = append(stats, TiKVOpStats{
			Op:      op,
			Count:   sum,
			Rate:    rate,
			Percent: sum / total * 100,
		})
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Percent > stats[j].Percent
	})

	return stats
}

func calculateReadWriteRatio(typeData map[string][]TimeSeriesPoint) (read, write float64) {
	var readTotal, writeTotal, rwTotal float64

	readTypes := map[string]bool{"Select": true, "SelectStmt": true}
	writeTypes := map[string]bool{"Insert": true, "Update": true, "Delete": true, "Replace": true,
		"InsertStmt": true, "UpdateStmt": true, "DeleteStmt": true}

	for sqlType, points := range typeData {
		if len(points) == 0 {
			continue
		}
		vals := extractValues(points)
		sum := 0.0
		for _, v := range vals {
			sum += v
		}

		if readTypes[sqlType] {
			readTotal += sum
			rwTotal += sum
		} else if writeTypes[sqlType] {
			writeTotal += sum
			rwTotal += sum
		}
	}

	if rwTotal > 0 {
		read = readTotal / rwTotal
		write = writeTotal / rwTotal
	}

	return
}

func DetectWorkloadAnomalies(
	sqlTypeData map[string][]TimeSeriesPoint,
	tikvOpData map[string][]TimeSeriesPoint,
	config AnomalyConfig,
) []WorkloadAnomaly {
	var anomalies []WorkloadAnomaly

	for sqlType, points := range sqlTypeData {
		if len(points) < config.MinBaselineSamples {
			continue
		}

		vals := extractValues(points)
		m := median(vals)
		if m < 50 {
			continue
		}

		typeAnomalies := detectTypeAnomalies(points, sqlType, "sql", config)
		anomalies = append(anomalies, typeAnomalies...)
	}

	for op, points := range tikvOpData {
		if len(points) < config.MinBaselineSamples {
			continue
		}

		vals := extractValues(points)
		m := median(vals)
		if m < 100 {
			continue
		}

		opAnomalies := detectTypeAnomalies(points, op, "tikv", config)
		anomalies = append(anomalies, opAnomalies...)
	}

	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].Timestamp < anomalies[j].Timestamp
	})

	return anomalies
}

func detectTypeAnomalies(points []TimeSeriesPoint, category, categoryType string, config AnomalyConfig) []WorkloadAnomaly {
	var rawAnomalies []WorkloadAnomaly

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	vals := extractValues(points)
	m := median(vals)

	if m < 10 {
		return rawAnomalies
	}

	std := stdDev(vals)
	threshold := m - std*2

	if std < m*0.2 {
		return rawAnomalies
	}

	for _, v := range points {
		var anomalyType AnomalyType
		var severity Severity
		var detail string

		if v.Value < threshold {
			dropRatio := (m - v.Value) / m
			if dropRatio < 0.3 {
				continue
			}

			anomalyType = AnomalyQPSDrop
			if dropRatio > 0.7 {
				severity = SeverityCritical
			} else if dropRatio > 0.5 {
				severity = SeverityHigh
			} else {
				severity = SeverityMedium
			}
			detail = fmt.Sprintf("%s %s dropped to %.0f (baseline=%.0f, -%.0f%%)", categoryType, category, v.Value, m, dropRatio*100)
		} else if v.Value > m+std*2 {
			spikeRatio := v.Value / m
			if spikeRatio < 1.5 {
				continue
			}

			anomalyType = AnomalyQPSSpike
			if spikeRatio > 3 {
				severity = SeverityCritical
			} else if spikeRatio > 2 {
				severity = SeverityHigh
			} else {
				severity = SeverityMedium
			}
			detail = fmt.Sprintf("%s %s spiked to %.0f (baseline=%.0f, +%.0f%%)", categoryType, category, v.Value, m, (spikeRatio-1)*100)
		} else {
			continue
		}

		rawAnomalies = append(rawAnomalies, WorkloadAnomaly{
			Type:      anomalyType,
			Severity:  severity,
			Timestamp: v.Timestamp,
			TimeStr:   time.Unix(v.Timestamp, 0).Format("2006-01-02 15:04"),
			Category:  fmt.Sprintf("%s/%s", categoryType, category),
			Value:     v.Value,
			Baseline:  m,
			Detail:    detail,
		})
	}

	return mergeConsecutiveWorkloadAnomalies(rawAnomalies, categoryType, category, m)
}

func mergeConsecutiveWorkloadAnomalies(anomalies []WorkloadAnomaly, categoryType, category string, baseline float64) []WorkloadAnomaly {
	if len(anomalies) == 0 {
		return anomalies
	}

	sort.Slice(anomalies, func(i, j int) bool {
		if anomalies[i].Type != anomalies[j].Type {
			return anomalies[i].Type < anomalies[j].Type
		}
		return anomalies[i].Timestamp < anomalies[j].Timestamp
	})

	var result []WorkloadAnomaly
	var current *WorkloadAnomaly
	maxValue := 0.0
	startTS := int64(0)

	for _, a := range anomalies {
		if current == nil {
			current = &a
			maxValue = a.Value
			startTS = a.Timestamp
			continue
		}

		if a.Type == current.Type && a.Timestamp <= current.Timestamp+1800 {
			if a.Severity == SeverityCritical || a.Severity == SeverityHigh {
				current.Severity = a.Severity
			}
			if a.Value > maxValue {
				maxValue = a.Value
			}
			duration := int((a.Timestamp - startTS) / 60)
			spikeRatio := maxValue / baseline
			current.Value = maxValue
			current.Detail = fmt.Sprintf("%s %s sustained high (baseline=%.0f, peak=%.0f, +%.0f%%, duration: %dh)",
				categoryType, category, baseline, maxValue, (spikeRatio-1)*100, duration/60+1)
		} else {
			result = append(result, *current)
			current = &a
			maxValue = a.Value
			startTS = a.Timestamp
		}
	}

	if current != nil {
		result = append(result, *current)
	}

	return result
}

func PrintWorkloadProfile(profile *WorkloadProfile) {
	if len(profile.SQLTypes) == 0 && len(profile.TiKVOps) == 0 {
		return
	}

	fmt.Println(stringsRepeat("-", 60))
	fmt.Println("WORKLOAD COMPOSITION")
	fmt.Println(stringsRepeat("-", 60))

	if len(profile.SQLTypes) > 0 {
		var significantTypes []SQLTypeStats
		for _, s := range profile.SQLTypes {
			if s.Percent >= 5.0 || strings.EqualFold(s.Type, "delete") {
				significantTypes = append(significantTypes, s)
			}
		}

		if len(significantTypes) > 0 {
			fmt.Println("\n  SQL Statement Distribution:")
			fmt.Printf("  %-12s %8s %8s\n", "Type", "Percent", "Rate")
			fmt.Println("  " + stringsRepeat("-", 35))
			for _, s := range significantTypes {
				fmt.Printf("  %-12s %7.1f%% %8.1f\n", s.Type, s.Percent, s.Rate)
			}
			fmt.Println()
			fmt.Printf("  Read Ratio:   %.1f%%\n", profile.ReadRatio*100)
			fmt.Printf("  Write Ratio:  %.1f%%\n", profile.WriteRatio*100)
			if profile.InternalRatio > 0.01 {
				fmt.Printf("  Internal:     %.1f%%\n", profile.InternalRatio*100)
			}

			if profile.IsReadHeavy {
				fmt.Println("\n  Workload Type: READ-HEAVY")
			} else if profile.IsWriteHeavy {
				fmt.Println("\n  Workload Type: WRITE-HEAVY")
			} else {
				fmt.Println("\n  Workload Type: BALANCED")
			}

			if profile.DominantSQLType != "" {
				fmt.Printf("  Dominant SQL:  %s\n", profile.DominantSQLType)
			}

			if profile.ComplexityScore > 0.3 {
				fmt.Printf("  Complexity:    %.1f (high)\n", profile.ComplexityScore)
			}
		}
	}

	if len(profile.TiKVOps) > 0 {
		var significantOps []TiKVOpStats
		for _, op := range profile.TiKVOps {
			if op.Percent >= 5.0 || strings.Contains(strings.ToLower(op.Op), "delete") {
				significantOps = append(significantOps, op)
			}
		}

		if len(significantOps) > 0 {
			fmt.Println("\n  TiKV Operation Distribution:")
			fmt.Printf("  %-20s %8s %8s\n", "Operation", "Percent", "Rate")
			fmt.Println("  " + stringsRepeat("-", 45))
			for _, op := range significantOps {
				fmt.Printf("  %-20s %7.1f%% %8.1f\n", op.Op, op.Percent, op.Rate)
			}

			if profile.DominantTiKVOp != "" {
				fmt.Printf("\n  Dominant Op:   %s\n", profile.DominantTiKVOp)
			}

			if profile.WriteAmplification > 0 {
				fmt.Printf("  Write Amp:     %.1fx", profile.WriteAmplification)
				if profile.WriteAmplification > 3 {
					fmt.Print(" (high)")
				}
				fmt.Println()
			}
		}
	}

	if profile.TransactionPattern != "" || profile.HotspotRisk != "" {
		fmt.Println("\n  Workload Analysis:")
		if profile.TransactionPattern != "" {
			fmt.Printf("    Pattern:     %s\n", profile.TransactionPattern)
		}
		if profile.HotspotRisk != "" {
			fmt.Printf("    Hotspot Risk: %s\n", profile.HotspotRisk)
		}
	}

	fmt.Println()
}
