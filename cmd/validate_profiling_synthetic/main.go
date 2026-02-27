package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"

	"tidbcloud-insight/pkg/analysis"
)

type ValidationStats struct {
	ClusterCount        int              `json:"cluster_count"`
	LoadClassDist       map[string]int   `json:"load_class_dist"`
	StabilityClassDist  map[string]int   `json:"stability_class_dist"`
	TrafficTypeDist     map[string]int   `json:"traffic_type_dist"`
	LoadSensitivityDist map[string]int   `json:"load_sensitivity_dist"`
	CVStats             StatsSummary     `json:"cv_stats"`
	BurstinessStats     StatsSummary     `json:"burstiness_stats"`
	SeasonalityStats    StatsSummary     `json:"seasonality_stats"`
	PeakToAvgStats      StatsSummary     `json:"peak_to_avg_stats"`
	DailyPatternRatio   float64          `json:"daily_pattern_ratio"`
	Issues              []string         `json:"issues,omitempty"`
	IssueCount          int              `json:"issue_count"`
	ProfileSamples      []*ProfileSample `json:"profile_samples,omitempty"`
}

type StatsSummary struct {
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Median float64 `json:"median"`
	StdDev float64 `json:"std_dev"`
}

type ProfileSample struct {
	ClusterID      string  `json:"cluster_id"`
	MeanQPS        float64 `json:"mean_qps"`
	CV             float64 `json:"cv"`
	Burstiness     float64 `json:"burstiness"`
	StabilityClass string  `json:"stability_class"`
	LoadClass      string  `json:"load_class"`
	TrafficType    string  `json:"traffic_type"`
}

func main() {
	numClusters := 80
	samplesPerCluster := 500
	durationHours := 72.0

	fmt.Printf("Generating synthetic validation data...\n")
	fmt.Printf("Clusters: %d, Samples per cluster: %d\n\n", numClusters, samplesPerCluster)

	var cvValues, burstinessValues, seasonalityValues, peakToAvgValues []float64
	var results []*ProfileSample
	loadClassDist := make(map[string]int)
	stabilityClassDist := make(map[string]int)
	trafficTypeDist := make(map[string]int)
	loadSensitivityDist := make(map[string]int)
	dailyPatternCount := 0
	var issues []string

	for i := 0; i < numClusters; i++ {
		clusterID := fmt.Sprintf("cluster-%d", i)
		data := generateTimeSeriesData(samplesPerCluster, durationHours, i)

		profile := analysis.AnalyzeLoadProfile(clusterID, data, nil)
		if profile == nil {
			issues = append(issues, fmt.Sprintf("cluster %s: nil profile", clusterID))
			continue
		}

		cvValues = append(cvValues, profile.QPSProfile.CV)
		burstinessValues = append(burstinessValues, profile.Characteristics.Burstiness)
		seasonalityValues = append(seasonalityValues, profile.Characteristics.Seasonality)
		peakToAvgValues = append(peakToAvgValues, profile.QPSProfile.PeakToAvg)

		loadClassDist[profile.Characteristics.LoadClass]++
		stabilityClassDist[profile.Characteristics.StabilityClass]++
		trafficTypeDist[profile.Characteristics.TrafficType]++
		loadSensitivityDist[profile.Correlation.LoadSensitivity]++

		if profile.DailyPattern.PeakToOffPeak > 1.5 {
			dailyPatternCount++
		}

		if profile.QPSProfile.CV < 0 {
			issues = append(issues, fmt.Sprintf("cluster %s: negative CV %.4f", clusterID, profile.QPSProfile.CV))
		}
		if profile.Characteristics.Burstiness < 0 || profile.Characteristics.Burstiness > 1 {
			issues = append(issues, fmt.Sprintf("cluster %s: invalid burstiness %.4f", clusterID, profile.Characteristics.Burstiness))
		}
		if profile.Characteristics.Seasonality < 0 || profile.Characteristics.Seasonality > 1 {
			issues = append(issues, fmt.Sprintf("cluster %s: invalid seasonality %.4f", clusterID, profile.Characteristics.Seasonality))
		}

		if i < 5 {
			results = append(results, &ProfileSample{
				ClusterID:      clusterID,
				MeanQPS:        profile.QPSProfile.Mean,
				CV:             profile.QPSProfile.CV,
				Burstiness:     profile.Characteristics.Burstiness,
				StabilityClass: profile.Characteristics.StabilityClass,
				LoadClass:      profile.Characteristics.LoadClass,
				TrafficType:    profile.Characteristics.TrafficType,
			})
		}
	}

	stats := &ValidationStats{
		ClusterCount:        numClusters,
		LoadClassDist:       loadClassDist,
		StabilityClassDist:  stabilityClassDist,
		TrafficTypeDist:     trafficTypeDist,
		LoadSensitivityDist: loadSensitivityDist,
		CVStats:             calcStats(cvValues),
		BurstinessStats:     calcStats(burstinessValues),
		SeasonalityStats:    calcStats(seasonalityValues),
		PeakToAvgStats:      calcStats(peakToAvgValues),
		DailyPatternRatio:   float64(dailyPatternCount) / float64(numClusters),
		Issues:              issues,
		IssueCount:          len(issues),
		ProfileSamples:      results,
	}

	printStats(stats)

	if len(os.Args) > 1 && os.Args[1] == "--json" {
		data, _ := json.MarshalIndent(stats, "", "  ")
		fmt.Println(string(data))
	}
}

func generateTimeSeriesData(numSamples int, durationHours float64, seed int) []analysis.TimeSeriesPoint {
	r := rand.New(rand.NewSource(int64(seed)))

	now := time.Now().Unix()
	startTime := now - int64(durationHours*3600)
	interval := int64(durationHours*3600) / int64(numSamples)

	data := make([]analysis.TimeSeriesPoint, numSamples)

	baseQPS := 100 + r.Float64()*5000
	cv := 0.1 + r.Float64()*0.6
	seasonality := r.Float64() * 0.5
	hasDailyPattern := r.Float64() > 0.5

	for i := 0; i < numSamples; i++ {
		t := startTime + int64(i)*interval
		hour := time.Unix(t, 0).Hour()

		qps := baseQPS

		if hasDailyPattern {
			if hour >= 9 && hour <= 17 {
				qps *= 1.5 + r.Float64()*0.5
			} else if hour >= 0 && hour <= 6 {
				qps *= 0.3 + r.Float64()*0.2
			}
		}

		if seasonality > 0.2 {
			qps *= 1 + 0.3*math.Sin(float64(i)/float64(numSamples)*2*math.Pi)
		}

		noise := r.NormFloat64() * baseQPS * cv
		qps += noise

		if qps < 0 {
			qps = r.Float64() * 10
		}

		data[i] = analysis.TimeSeriesPoint{
			Timestamp: t,
			Value:     qps,
		}
	}

	sort.Slice(data, func(i, j int) bool {
		return data[i].Timestamp < data[j].Timestamp
	})

	return data
}

func calcStats(vals []float64) StatsSummary {
	if len(vals) == 0 {
		return StatsSummary{}
	}

	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))

	variance := 0.0
	for _, v := range vals {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(len(vals))

	return StatsSummary{
		Min:    sorted[0],
		Max:    sorted[len(sorted)-1],
		Mean:   mean,
		Median: sorted[len(sorted)/2],
		StdDev: math.Sqrt(variance),
	}
}

func printStats(stats *ValidationStats) {
	fmt.Printf("=== Validation Summary ===\n")
	fmt.Printf("Clusters: %d\n", stats.ClusterCount)
	fmt.Printf("Daily Pattern Ratio: %.1f%%\n", stats.DailyPatternRatio*100)
	fmt.Printf("Issues Found: %d\n", stats.IssueCount)

	fmt.Printf("\n=== Distribution Analysis ===\n")
	printDist("Load Classes", stats.LoadClassDist, stats.ClusterCount)
	printDist("Stability Classes", stats.StabilityClassDist, stats.ClusterCount)
	printDist("Traffic Types", stats.TrafficTypeDist, stats.ClusterCount)
	printDist("Load Sensitivity", stats.LoadSensitivityDist, stats.ClusterCount)

	fmt.Printf("\n=== Statistical Summary ===\n")
	fmt.Printf("CV: min=%.3f, max=%.3f, mean=%.3f, median=%.3f, std=%.3f\n",
		stats.CVStats.Min, stats.CVStats.Max, stats.CVStats.Mean, stats.CVStats.Median, stats.CVStats.StdDev)
	fmt.Printf("Burstiness: min=%.3f, max=%.3f, mean=%.3f, median=%.3f, std=%.3f\n",
		stats.BurstinessStats.Min, stats.BurstinessStats.Max, stats.BurstinessStats.Mean, stats.BurstinessStats.Median, stats.BurstinessStats.StdDev)
	fmt.Printf("Seasonality: min=%.3f, max=%.3f, mean=%.3f, median=%.3f, std=%.3f\n",
		stats.SeasonalityStats.Min, stats.SeasonalityStats.Max, stats.SeasonalityStats.Mean, stats.SeasonalityStats.Median, stats.SeasonalityStats.StdDev)
	fmt.Printf("PeakToAvg: min=%.2f, max=%.2f, mean=%.2f, median=%.2f, std=%.2f\n",
		stats.PeakToAvgStats.Min, stats.PeakToAvgStats.Max, stats.PeakToAvgStats.Mean, stats.PeakToAvgStats.Median, stats.PeakToAvgStats.StdDev)

	fmt.Printf("\n=== Sample Profiles ===\n")
	for _, s := range stats.ProfileSamples {
		fmt.Printf("%s: QPS=%.0f, CV=%.3f, Burst=%.3f, Stability=%s, Load=%s, Type=%s\n",
			s.ClusterID, s.MeanQPS, s.CV, s.Burstiness, s.StabilityClass, s.LoadClass, s.TrafficType)
	}

	if len(stats.Issues) > 0 {
		fmt.Printf("\n=== Issues ===\n")
		for _, issue := range stats.Issues {
			fmt.Printf("  %s\n", issue)
		}
	} else {
		fmt.Printf("\n=== No Issues Found ===\n")
	}
}

func printDist(name string, dist map[string]int, total int) {
	fmt.Printf("\n%s:\n", name)
	for k, v := range dist {
		fmt.Printf("  %s: %d (%.1f%%)\n", k, v, float64(v)/float64(total)*100)
	}
}
