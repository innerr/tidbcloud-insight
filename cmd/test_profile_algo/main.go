//go:build unittest
// +build unittest

package main

import (
	"fmt"
	"math/rand"
	"time"

	"tidbcloud-insight/pkg/analysis"
)

func generateTestQPSData(days int, instances int) (map[string][]analysis.TimeSeriesPoint, []analysis.TimeSeriesPoint) {
	instanceData := make(map[string][]analysis.TimeSeriesPoint)
	var aggregated []analysis.TimeSeriesPoint

	startTime := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	interval := 200 * time.Second
	pointsPerDay := int(24 * time.Hour / interval)

	for inst := 0; inst < instances; inst++ {
		instanceData[fmt.Sprintf("tidb-%d", inst)] = make([]analysis.TimeSeriesPoint, 0)
	}

	for i := 0; i < days*pointsPerDay; i++ {
		t := startTime.Add(time.Duration(i) * interval)
		ts := t.Unix()
		hour := t.Hour()
		weekday := t.Weekday()

		for inst := 0; inst < instances; inst++ {
			baseQPS := 500.0 + float64(inst)*100
			var qps float64 = baseQPS

			if hour >= 9 && hour <= 18 {
				qps = baseQPS * 2.5
			} else if hour >= 0 && hour <= 5 {
				qps = baseQPS * 0.3
			}

			if weekday == 0 || weekday == 6 {
				qps *= 0.6
			}

			noise := (rand.Float64() - 0.5) * baseQPS * 0.1
			qps += noise

			point := analysis.TimeSeriesPoint{
				Timestamp: ts * 1000,
				Value:     qps,
			}
			instanceData[fmt.Sprintf("tidb-%d", inst)] = append(instanceData[fmt.Sprintf("tidb-%d", inst)], point)
			aggregated = append(aggregated, point)
		}
	}

	return instanceData, aggregated
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("=== Algorithm Validation with Periodicity-Aware Scoring ===")

	for _, days := range []int{3, 7, 14} {
		for _, instances := range []int{1, 3} {
			instanceData, aggregated := generateTestQPSData(days, instances)

			profile := analysis.AnalyzeLoadProfileFull(
				fmt.Sprintf("test-cluster-%dd-%dinst", days, instances),
				aggregated, nil, nil, nil, nil, nil,
				instanceData, nil, nil, nil,
			)

			fmt.Printf("\n--- Test: %d days, %d instances ---\n", days, instances)
			fmt.Printf("DurationHours: %.1fh\n", profile.DurationHours)
			fmt.Printf("Samples: %d\n", profile.Samples)
			fmt.Printf("Stability: %s\n", profile.Characteristics.StabilityClass)
			fmt.Printf("Periodicity: %s (score=%.2f, daily=%.2f, weekly=%.2f)\n",
				profile.Periodicity.DominantPeriod,
				profile.Periodicity.PeriodicityScore,
				profile.Periodicity.DailyStrength,
				profile.Periodicity.WeeklyStrength)
			fmt.Printf("QPS: Mean=%.1f, PeakToAvg=%.2f, CV=%.2f\n",
				profile.QPSProfile.Mean,
				profile.QPSProfile.PeakToAvg,
				profile.QPSProfile.CV)
			fmt.Printf("Health: %s\n", profile.Insights.OverallHealth)
			fmt.Printf("Scores: Performance=%.2f, Stability=%.2f, Efficiency=%.2f\n",
				profile.Insights.PerformanceScore,
				profile.Insights.StabilityScore,
				profile.Insights.EfficiencyScore)
			fmt.Printf("StabilityClass: %s, NoiseLevel: %.2f, ChangePoints: %d\n",
				profile.Characteristics.StabilityClass,
				profile.Characteristics.NoiseLevel,
				profile.Characteristics.ChangePoints)
			fmt.Printf("ResourceEfficiency: %.2f\n", profile.ResourceEfficiency.EfficiencyScore)
			if profile.InstanceSkew != nil {
				fmt.Printf("Skew Risk: %s\n", profile.InstanceSkew.SkewRiskLevel)
			}
		}
	}

	fmt.Println("\n=== Validation Complete ===")
}
