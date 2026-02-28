// Package analysis implements SQL fingerprinting and pattern analysis.
// This file provides SQL query classification, complexity scoring,
// risk assessment, and pattern-based optimization recommendations.
package analysis

import (
	"math"
	"sort"
	"strings"
)

type SQLFingerprint struct {
	Fingerprint      string     `json:"fingerprint"`
	OriginalExamples []string   `json:"original_examples"`
	Count            int        `json:"count"`
	Percent          float64    `json:"percent"`
	AvgLatencyMs     float64    `json:"avg_latency_ms"`
	MaxLatencyMs     float64    `json:"max_latency_ms"`
	P99LatencyMs     float64    `json:"p99_latency_ms"`
	ResourceScore    float64    `json:"resource_score"`
	ComplexityScore  float64    `json:"complexity_score"`
	Frequency        float64    `json:"frequency"`
	FirstSeen        int64      `json:"first_seen"`
	LastSeen         int64      `json:"last_seen"`
	Pattern          SQLPattern `json:"pattern"`
	Recommendations  []string   `json:"recommendations"`
}

type SQLPattern struct {
	Type            string `json:"type"`
	Category        string `json:"category"`
	IsTransactional bool   `json:"is_transactional"`
	IsAnalytical    bool   `json:"is_analytical"`
	TableCount      int    `json:"table_count"`
	HasJoin         bool   `json:"has_join"`
	HasSubquery     bool   `json:"has_subquery"`
	HasAggregation  bool   `json:"has_aggregation"`
	HasOrderBy      bool   `json:"has_order_by"`
	HasGroupBy      bool   `json:"has_group_by"`
	HasLimit        bool   `json:"has_limit"`
	ScanRisk        string `json:"scan_risk"`
	LockRisk        string `json:"lock_risk"`
}

type SQLFingerprintAnalysis struct {
	Fingerprints           []SQLFingerprint   `json:"fingerprints"`
	TopFingerprints        []SQLFingerprint   `json:"top_fingerprints"`
	ResourceHotspots       []SQLFingerprint   `json:"resource_hotspots"`
	LatencyHotspots        []SQLFingerprint   `json:"latency_hotspots"`
	PatternDistribution    map[string]int     `json:"pattern_distribution"`
	CategoryDistribution   map[string]float64 `json:"category_distribution"`
	ComplexityDistribution map[string]int     `json:"complexity_distribution"`
	TotalQueries           int                `json:"total_queries"`
	UniquePatterns         int                `json:"unique_patterns"`
	Recommendations        []string           `json:"recommendations"`
}

func AnalyzeSQLFingerprints(sqlTypeData map[string][]TimeSeriesPoint, sqlLatencyData map[string][]TimeSeriesPoint, durationHours float64) *SQLFingerprintAnalysis {
	analysis := &SQLFingerprintAnalysis{
		Fingerprints:           []SQLFingerprint{},
		PatternDistribution:    make(map[string]int),
		CategoryDistribution:   make(map[string]float64),
		ComplexityDistribution: make(map[string]int),
	}

	var totalCount float64
	for _, data := range sqlTypeData {
		if len(data) > 0 {
			last := data[len(data)-1].Value
			first := data[0].Value
			delta := last - first
			if delta > 0 {
				totalCount += delta
			}
		}
	}

	for sqlType, data := range sqlTypeData {
		if len(data) == 0 {
			continue
		}

		fp := SQLFingerprint{
			Fingerprint: sqlType,
			Pattern:     classifySQLPatternDetailed(sqlType),
		}

		last := data[len(data)-1].Value
		first := data[0].Value
		fp.Count = int(last - first)
		if fp.Count < 0 {
			fp.Count = 0
		}

		if totalCount > 0 {
			fp.Percent = float64(fp.Count) / totalCount * 100
		}

		if durationHours > 0 {
			fp.Frequency = float64(fp.Count) / durationHours
		}

		if latencyData, ok := sqlLatencyData[sqlType]; ok && len(latencyData) > 0 {
			vals := extractValuesFromTSP(latencyData)
			for i := range vals {
				vals[i] *= 1000
			}

			fp.AvgLatencyMs = mean(vals)

			sorted := make([]float64, len(vals))
			copy(sorted, vals)
			sort.Float64s(sorted)

			fp.MaxLatencyMs = sorted[len(sorted)-1]
			fp.P99LatencyMs = percentile(sorted, 0.99)
		}

		fp.ResourceScore = calculateSQLResourceScoreEnhanced(fp)
		fp.ComplexityScore = calculateSQLComplexityScore(fp.Pattern)
		fp.Recommendations = generateSQLRecommendations(fp)

		analysis.Fingerprints = append(analysis.Fingerprints, fp)

		analysis.PatternDistribution[fp.Pattern.Type]++
		analysis.CategoryDistribution[fp.Pattern.Category] += fp.Percent

		complexity := "low"
		if fp.ComplexityScore > 0.7 {
			complexity = "high"
		} else if fp.ComplexityScore > 0.4 {
			complexity = "medium"
		}
		analysis.ComplexityDistribution[complexity]++
	}

	sort.Slice(analysis.Fingerprints, func(i, j int) bool {
		return analysis.Fingerprints[i].Count > analysis.Fingerprints[j].Count
	})

	if len(analysis.Fingerprints) > 10 {
		analysis.TopFingerprints = analysis.Fingerprints[:10]
	} else {
		analysis.TopFingerprints = analysis.Fingerprints
	}

	sort.Slice(analysis.Fingerprints, func(i, j int) bool {
		return analysis.Fingerprints[i].ResourceScore > analysis.Fingerprints[j].ResourceScore
	})

	if len(analysis.Fingerprints) > 5 {
		analysis.ResourceHotspots = analysis.Fingerprints[:5]
	} else {
		analysis.ResourceHotspots = analysis.Fingerprints
	}

	sort.Slice(analysis.Fingerprints, func(i, j int) bool {
		return analysis.Fingerprints[i].P99LatencyMs > analysis.Fingerprints[j].P99LatencyMs
	})

	if len(analysis.Fingerprints) > 5 {
		analysis.LatencyHotspots = analysis.Fingerprints[:5]
	} else {
		analysis.LatencyHotspots = analysis.Fingerprints
	}

	analysis.TotalQueries = int(totalCount)
	analysis.UniquePatterns = len(analysis.Fingerprints)
	analysis.Recommendations = generateSQLAnalysisRecommendations(analysis)

	return analysis
}

func classifySQLPatternDetailed(sqlType string) SQLPattern {
	pattern := SQLPattern{}
	sqlType = strings.ToLower(sqlType)

	switch {
	case containsAny(sqlType, []string{"select"}):
		pattern.Type = "DQL"
		pattern.Category = "query"
	case containsAny(sqlType, []string{"insert", "update", "delete", "replace"}):
		pattern.Type = "DML"
		pattern.Category = "mutation"
	case containsAny(sqlType, []string{"create", "alter", "drop", "truncate"}):
		pattern.Type = "DDL"
		pattern.Category = "schema"
	case containsAny(sqlType, []string{"begin", "commit", "rollback"}):
		pattern.Type = "TCL"
		pattern.Category = "transaction"
	default:
		pattern.Type = "Other"
		pattern.Category = "other"
	}

	pattern.IsTransactional = containsAny(sqlType, []string{"insert", "update", "delete", "begin", "commit"})
	pattern.IsAnalytical = containsAny(sqlType, []string{"select", "group", "order", "having"})

	pattern.HasJoin = containsAny(sqlType, []string{"join"})
	pattern.HasSubquery = containsAny(sqlType, []string{"subquery", "select"})
	pattern.HasAggregation = containsAny(sqlType, []string{"count", "sum", "avg", "max", "min", "group"})
	pattern.HasOrderBy = containsAny(sqlType, []string{"order"})
	pattern.HasGroupBy = containsAny(sqlType, []string{"group"})
	pattern.HasLimit = containsAny(sqlType, []string{"limit"})

	tableCount := strings.Count(sqlType, "from") + strings.Count(sqlType, "join")
	pattern.TableCount = tableCount

	switch {
	case pattern.HasJoin && pattern.TableCount > 3:
		pattern.ScanRisk = "high"
	case pattern.HasJoin || pattern.TableCount > 2:
		pattern.ScanRisk = "medium"
	default:
		pattern.ScanRisk = "low"
	}

	if pattern.Type == "DML" && !containsAny(sqlType, []string{"limit", "where"}) {
		pattern.LockRisk = "high"
	} else if pattern.Type == "DML" {
		pattern.LockRisk = "medium"
	} else {
		pattern.LockRisk = "low"
	}

	return pattern
}

func calculateSQLResourceScoreEnhanced(fp SQLFingerprint) float64 {
	score := 0.0

	score += fp.Percent * 0.3

	if fp.AvgLatencyMs > 100 {
		score += 0.4
	} else if fp.AvgLatencyMs > 50 {
		score += 0.2
	}

	score += fp.ComplexityScore * 0.3

	return math.Min(1.0, score)
}

func calculateSQLComplexityScore(pattern SQLPattern) float64 {
	score := 0.0

	if pattern.HasJoin {
		score += 0.2
	}
	if pattern.HasSubquery {
		score += 0.2
	}
	if pattern.HasAggregation {
		score += 0.1
	}
	if pattern.HasOrderBy {
		score += 0.05
	}
	if pattern.HasGroupBy {
		score += 0.1
	}
	if pattern.TableCount > 2 {
		score += 0.15
	} else if pattern.TableCount > 1 {
		score += 0.1
	}

	if pattern.ScanRisk == "high" {
		score += 0.2
	} else if pattern.ScanRisk == "medium" {
		score += 0.1
	}

	return math.Min(1.0, score)
}

func generateSQLRecommendations(fp SQLFingerprint) []string {
	var recs []string

	if fp.AvgLatencyMs > 100 {
		recs = append(recs, "Consider optimizing this query for better latency")
	}

	if fp.Pattern.ScanRisk == "high" {
		recs = append(recs, "Review table scan patterns and add appropriate indexes")
	}

	if fp.Pattern.LockRisk == "high" {
		recs = append(recs, "Add WHERE clause or LIMIT to reduce lock scope")
	}

	if fp.Pattern.HasSubquery {
		recs = append(recs, "Consider rewriting subquery as JOIN for better performance")
	}

	if fp.Frequency > 1000 && fp.ComplexityScore > 0.5 {
		recs = append(recs, "High-frequency complex query - consider caching or materialized views")
	}

	return recs
}

func generateSQLAnalysisRecommendations(analysis *SQLFingerprintAnalysis) []string {
	var recs []string

	if len(analysis.ResourceHotspots) > 0 {
		recs = append(recs, "Optimize resource-intensive SQL patterns")
	}

	if len(analysis.LatencyHotspots) > 0 {
		recs = append(recs, "Investigate high-latency SQL queries")
	}

	if analysis.ComplexityDistribution["high"] > analysis.ComplexityDistribution["low"] {
		recs = append(recs, "Consider simplifying complex query patterns")
	}

	if analysis.CategoryDistribution["mutation"] > 50 {
		recs = append(recs, "High write ratio - consider write optimization strategies")
	}

	if analysis.UniquePatterns > 100 {
		recs = append(recs, "Many unique SQL patterns - consider query standardization")
	}

	if len(recs) == 0 {
		recs = append(recs, "SQL patterns are well optimized")
	}

	return recs
}
