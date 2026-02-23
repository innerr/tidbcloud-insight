package impl

import (
	"math"
	"testing"

	"tidbcloud-insight/pkg/analysis"
)

func TestCompactContiguousAnomalies_MergeContinuousPoints(t *testing.T) {
	input := []analysis.DetectedAnomaly{
		{
			Type:       analysis.AnomalyQPSDrop,
			Severity:   analysis.SeverityMedium,
			Timestamp:  1000,
			EndTime:    1000,
			ZScore:     -3.1,
			DetectedBy: []string{"A"},
		},
		{
			Type:       analysis.AnomalyLatencySpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  1120,
			EndTime:    1120,
			ZScore:     4.0,
			DetectedBy: []string{"B"},
		},
	}

	merged := compactContiguousAnomalies(input, 240, 120)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged anomaly, got %d", len(merged))
	}
	if merged[0].Duration < 240 {
		t.Fatalf("expected merged duration >= 240, got %d", merged[0].Duration)
	}
	if merged[0].Type != analysis.AnomalyQPSSpike {
		t.Fatalf("expected merged type %s, got %s", analysis.AnomalyQPSSpike, merged[0].Type)
	}
	if len(merged[0].DetectedBy) != 2 {
		t.Fatalf("expected merged detected_by size 2, got %d", len(merged[0].DetectedBy))
	}
}

func TestCompactContiguousAnomalies_KeepSeparatedEvents(t *testing.T) {
	input := []analysis.DetectedAnomaly{
		{
			Type:       analysis.AnomalyQPSDrop,
			Severity:   analysis.SeverityMedium,
			Timestamp:  1000,
			EndTime:    1000,
			DetectedBy: []string{"A"},
		},
		{
			Type:       analysis.AnomalyQPSDrop,
			Severity:   analysis.SeverityMedium,
			Timestamp:  2000,
			EndTime:    2000,
			DetectedBy: []string{"A"},
		},
	}

	merged := compactContiguousAnomalies(input, 120, 120)
	if len(merged) != 2 {
		t.Fatalf("expected 2 anomalies, got %d", len(merged))
	}
	if merged[0].Duration != 120 || merged[1].Duration != 120 {
		t.Fatalf("expected single-point duration to be 120s, got %d and %d", merged[0].Duration, merged[1].Duration)
	}
}

func TestEstimateMedianSampleInterval(t *testing.T) {
	values := []analysis.TimeSeriesPoint{
		{Timestamp: 1000, Value: 1},
		{Timestamp: 1120, Value: 2},
		{Timestamp: 1240, Value: 3},
		{Timestamp: 1360, Value: 4},
	}

	interval := estimateMedianSampleInterval(values)
	if interval != 120 {
		t.Fatalf("expected median sample interval 120, got %d", interval)
	}
}

func TestCalculateDynamicEventGap(t *testing.T) {
	anomalies := []analysis.DetectedAnomaly{
		{Timestamp: 1000},
		{Timestamp: 1120},
		{Timestamp: 1240},
		{Timestamp: 2000},
	}
	gap := calculateDynamicEventGap(anomalies, 120)
	if gap < 240 || gap > 1200 {
		t.Fatalf("expected dynamic gap in [240,1200], got %d", gap)
	}
}

func TestCompactContiguousAnomalies_AdaptiveWindowMerge(t *testing.T) {
	input := []analysis.DetectedAnomaly{
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  1000,
			EndTime:    1000,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  1720, // 12 minutes later
			EndTime:    1720,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  2440, // another 12 minutes later
			EndTime:    2440,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
	}

	// Base gap is small (240s), but adaptive local window should expand and merge.
	merged := compactContiguousAnomalies(input, 240, 120)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged anomaly, got %d", len(merged))
	}
}

func TestCompactContiguousAnomalies_AdaptiveWindowDoesNotOverMerge(t *testing.T) {
	input := []analysis.DetectedAnomaly{
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  1000,
			EndTime:    1000,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  4600, // 60 minutes later
			EndTime:    4600,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
	}

	merged := compactContiguousAnomalies(input, 240, 120)
	if len(merged) != 2 {
		t.Fatalf("expected 2 anomalies, got %d", len(merged))
	}
}

func TestMergeNearbyEventsByCadence_MergeShortPeriodicEvents(t *testing.T) {
	input := []analysis.DetectedAnomaly{
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  1000,
			EndTime:    1000,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  1360, // +6m
			EndTime:    1360,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityCritical,
			Timestamp:  1720, // +6m
			EndTime:    1840, // 2m duration
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector", "HoltWintersDetector"},
		},
	}

	merged := mergeNearbyEventsByCadence(input, 120)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged event, got %d", len(merged))
	}
	if merged[0].Duration < 960 {
		t.Fatalf("expected merged duration >= 960s, got %d", merged[0].Duration)
	}
}

func TestMergeNearbyEventsByCadence_DoesNotChainMergeSparseLongRange(t *testing.T) {
	// 2m anomaly every 30m for 4h should stay fully split.
	var input []analysis.DetectedAnomaly
	for i := 0; i < 8; i++ {
		ts := int64(1000 + i*1800)
		input = append(input, analysis.DetectedAnomaly{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityMedium,
			Timestamp:  ts,
			EndTime:    ts,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		})
	}

	merged := mergeNearbyEventsByCadence(input, 120)
	if len(merged) != len(input) {
		t.Fatalf("expected sparse events to stay split, got %d (input=%d)", len(merged), len(input))
	}
}

func TestMergeNearbyEventsByCadence_MaxSpanBreaksLongChain(t *testing.T) {
	// 2m anomaly every 6m for 4h should not collapse to a single event.
	var input []analysis.DetectedAnomaly
	for i := 0; i < 40; i++ {
		ts := int64(1000 + i*360)
		input = append(input, analysis.DetectedAnomaly{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityMedium,
			Timestamp:  ts,
			EndTime:    ts,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		})
	}

	merged := mergeNearbyEventsByCadence(input, 120)
	if len(merged) <= 1 {
		t.Fatalf("expected long cadence chain to be split by max-span guard, got %d", len(merged))
	}
}

func TestMergeNearbyEventsByCadence_LatencyDominantChainSplitEarlier(t *testing.T) {
	// 2m latency anomalies every 8m for ~4h should be split for latency-dominant chains.
	var input []analysis.DetectedAnomaly
	for i := 0; i < 30; i++ {
		ts := int64(1000 + i*480)
		input = append(input, analysis.DetectedAnomaly{
			Type:       analysis.AnomalyLatencySpike,
			Severity:   analysis.SeverityHigh,
			Timestamp:  ts,
			EndTime:    ts,
			DetectedBy: []string{"LatencyDetector", "RawLatencyDetector"},
		})
	}

	merged := mergeNearbyEventsByCadence(input, 120)
	if len(merged) <= 1 {
		t.Fatalf("expected latency-dominant long chain to be split, got %d", len(merged))
	}
}

func TestCompactContiguousAnomalies_BreaksOverLongChain(t *testing.T) {
	// 2m anomaly every 6m for ~10h should not be compacted into one event.
	var input []analysis.DetectedAnomaly
	for i := 0; i < 100; i++ {
		ts := int64(1000 + i*360)
		input = append(input, analysis.DetectedAnomaly{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityMedium,
			Timestamp:  ts,
			EndTime:    ts,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		})
	}

	merged := compactContiguousAnomalies(input, 480, 120)
	if len(merged) <= 1 {
		t.Fatalf("expected compact phase to split overlong chain, got %d", len(merged))
	}
}

func TestCompactContiguousAnomalies_SoftCapMergesWhenTinyGapAndDense(t *testing.T) {
	// Two long dense segments separated by 6m should be merged into one incident.
	input := []analysis.DetectedAnomaly{
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityCritical,
			Timestamp:  1000,
			EndTime:    1000 + 4*3600,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
		{
			Type:       analysis.AnomalyQPSSpike,
			Severity:   analysis.SeverityCritical,
			Timestamp:  1000 + 4*3600 + 360,
			EndTime:    1000 + 8*3600 + 360,
			DetectedBy: []string{"AdvancedAnomalyDetector", "ContextualDetector"},
		},
	}

	merged := compactContiguousAnomalies(input, 480, 120)
	if len(merged) != 1 {
		t.Fatalf("expected dense long segments with tiny gap to merge, got %d", len(merged))
	}
}

func TestEvaluateAnomalyEvidence_LowBaselineBurst(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 0.1, p95: 0.2},
		windowStats{count: 20, median: 0.4, p90: 6.0, p95: 8.0},
		windowStats{},
		windowStats{},
		windowStats{},
		windowStats{},
		windowStats{},
		windowStats{},
		math.NaN(),
		math.NaN(),
		1200,
	)
	if !ev.keep || ev.reason != "LOW_BASELINE_BURST" {
		t.Fatalf("expected LOW_BASELINE_BURST keep=true, got reason=%s keep=%v", ev.reason, ev.keep)
	}
}

func TestEvaluateAnomalyEvidence_LatencyOnly(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 80, p95: 100},
		windowStats{count: 20, median: 82, p90: 100, p95: 105},
		windowStats{count: 10, median: 80},
		windowStats{count: 10, median: 81},
		windowStats{count: 40, p90: 0.03, p99: 0.1},
		windowStats{count: 20, p90: 0.5, p99: 2.0},
		windowStats{},
		windowStats{},
		0.2,
		0.1,
		1800,
	)
	if !ev.keep || ev.reason != "LATENCY_ONLY_DEGRADATION" {
		t.Fatalf("expected LATENCY_ONLY_DEGRADATION keep=true, got reason=%s keep=%v", ev.reason, ev.keep)
	}
}

func TestEvaluateAnomalyEvidence_WorkloadLinkedShift(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 100, p95: 120},
		windowStats{count: 20, median: 180, p90: 190, p95: 200},
		windowStats{count: 10, median: 110},
		windowStats{count: 10, median: 170},
		windowStats{count: 40, p90: 0.03, p99: 0.1},
		windowStats{count: 20, p90: 0.04, p99: 0.12},
		windowStats{},
		windowStats{},
		0.92,
		0.81,
		1800,
	)
	if !ev.keep || ev.reason != "WORKLOAD_LINKED_SHIFT" {
		t.Fatalf("expected WORKLOAD_LINKED_SHIFT keep=true, got reason=%s keep=%v", ev.reason, ev.keep)
	}
}

func TestExtractAnomalyReason(t *testing.T) {
	reason := extractAnomalyReason("reason=WORKLOAD_LINKED_SHIFT qps_p95=100")
	if reason != "WORKLOAD_LINKED_SHIFT" {
		t.Fatalf("unexpected reason: %s", reason)
	}
}

func TestDetectSustainedLatencyDegradation(t *testing.T) {
	var input []analysis.TimeSeriesPoint
	ts := int64(1000)
	for i := 0; i < 80; i++ {
		v := 0.02
		if i >= 20 && i < 35 {
			v = 2.5
		}
		input = append(input, analysis.TimeSeriesPoint{Timestamp: ts, Value: v})
		ts += 120
	}

	out := detectSustainedLatencyDegradation(input, "RawLatencySustainedDetector")
	if len(out) == 0 {
		t.Fatalf("expected sustained latency degradation anomaly")
	}
	if out[0].Type != analysis.AnomalyLatencyDegraded {
		t.Fatalf("unexpected anomaly type: %s", out[0].Type)
	}
}

func TestEvaluateAnomalyEvidence_BackendLatencyDegradation(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 100, p95: 120},
		windowStats{count: 20, median: 102, p90: 125, p95: 130},
		windowStats{count: 10, median: 100},
		windowStats{count: 10, median: 101},
		windowStats{count: 40, p90: 0.03, p95: 0.05, p99: 0.1},
		windowStats{count: 20, p90: 0.8, p95: 1.5, p99: 2.0},
		windowStats{count: 40, p99: 0.03},
		windowStats{count: 20, p99: 0.12},
		0.1,
		0.2,
		2400,
	)
	if !ev.keep || ev.reason != "BACKEND_LATENCY_DEGRADATION" {
		t.Fatalf("expected BACKEND_LATENCY_DEGRADATION keep=true, got reason=%s keep=%v", ev.reason, ev.keep)
	}
}

func TestEvaluateAnomalyEvidence_LongSmoothRampIsFiltered(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 100, p95: 120},
		windowStats{count: 20, median: 180, p90: 190, p95: 200},
		windowStats{count: 10, median: 165},
		windowStats{count: 10, median: 170},
		windowStats{count: 40, p90: 0.03, p99: 0.08},
		windowStats{count: 20, p90: 0.03, p99: 0.09},
		windowStats{},
		windowStats{},
		0.82,
		0.73,
		4*3600,
	)
	if ev.keep {
		t.Fatalf("expected long smooth ramp to be filtered, got keep=true reason=%s", ev.reason)
	}
}

func TestEvaluateAnomalyEvidence_LongShiftWithEdgeIsKept(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 100, p95: 120},
		windowStats{count: 20, median: 180, p90: 190, p95: 200},
		windowStats{count: 10, median: 110},
		windowStats{count: 10, median: 170},
		windowStats{count: 40, p90: 0.03, p99: 0.08},
		windowStats{count: 20, p90: 0.03, p99: 0.09},
		windowStats{},
		windowStats{},
		0.82,
		0.73,
		4*3600,
	)
	if !ev.keep || ev.reason != "WORKLOAD_LINKED_SHIFT" {
		t.Fatalf("expected long shift with edge to be kept, got keep=%v reason=%s", ev.keep, ev.reason)
	}
}

func TestEvaluateAnomalyEvidence_LongSparseWindowIsFiltered(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 100, p95: 120},
		windowStats{count: 20, median: 180, p90: 190, p95: 200, zeroRat: 0.75},
		windowStats{count: 10, median: 110},
		windowStats{count: 10, median: 170},
		windowStats{count: 40, p90: 0.03, p99: 0.08},
		windowStats{count: 20, p90: 0.03, p99: 0.09},
		windowStats{},
		windowStats{},
		0.82,
		0.73,
		3*3600,
	)
	if ev.keep {
		t.Fatalf("expected long sparse window to be filtered, got keep=true reason=%s", ev.reason)
	}
}

func TestEvaluateAnomalyEvidence_ShortIntenseBurst(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 50, p95: 60},
		windowStats{count: 20, median: 120, p90: 180, p95: 220},
		windowStats{count: 10, median: 55},
		windowStats{count: 10, median: 130},
		windowStats{count: 40, p90: 0.03, p99: 0.08},
		windowStats{count: 20, p90: 0.03, p99: 0.09},
		windowStats{},
		windowStats{},
		0.20,
		0.10,
		90*60,
	)
	if !ev.keep || ev.reason != "SHORT_INTENSE_BURST" {
		t.Fatalf("expected SHORT_INTENSE_BURST, got keep=%v reason=%s", ev.keep, ev.reason)
	}
}

func TestEvaluateAnomalyEvidence_LongWorkloadShiftNeedsStrongerEvidence(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 100, p95: 120},
		windowStats{count: 20, median: 145, p90: 150, p95: 170},
		windowStats{count: 10, median: 110},
		windowStats{count: 10, median: 132},
		windowStats{count: 40, p90: 0.03, p99: 0.08},
		windowStats{count: 20, p90: 0.03, p99: 0.09},
		windowStats{},
		windowStats{},
		0.70,
		0.55,
		3*3600,
	)
	if ev.keep {
		t.Fatalf("expected long workload shift with weak linkage to be filtered, got reason=%s", ev.reason)
	}
}

func TestEvaluateAnomalyEvidence_LongCoupledNeedsStrongerWorkloadEvidence(t *testing.T) {
	ev := evaluateAnomalyEvidence(
		windowStats{count: 40, median: 100, p95: 120},
		windowStats{count: 20, median: 170, p90: 185, p95: 210},
		windowStats{count: 10, median: 110},
		windowStats{count: 10, median: 132},
		windowStats{count: 40, p90: 0.03, p99: 0.08},
		windowStats{count: 20, p90: 0.8, p99: 2.0},
		windowStats{},
		windowStats{},
		0.70,
		0.55,
		3*3600,
	)
	if ev.keep {
		t.Fatalf("expected long coupled anomaly with weak linkage to be filtered, got reason=%s", ev.reason)
	}
}

func TestIsLikelyDiurnalRepeat_TrueWhenQPSAndLatencySimilar(t *testing.T) {
	ok := isLikelyDiurnalRepeat(
		windowStats{count: 12, p90: 180, p95: 200},
		windowStats{count: 12, p90: 170, p95: 195},
		windowStats{count: 12, p95: 0.30},
		windowStats{count: 12, p95: 0.28},
	)
	if !ok {
		t.Fatalf("expected diurnal repeat to be true")
	}
}

func TestIsLikelyDiurnalRepeat_FalseWhenLatencyDiverges(t *testing.T) {
	ok := isLikelyDiurnalRepeat(
		windowStats{count: 12, p90: 180, p95: 200},
		windowStats{count: 12, p90: 175, p95: 198},
		windowStats{count: 12, p95: 1.80},
		windowStats{count: 12, p95: 0.40},
	)
	if ok {
		t.Fatalf("expected diurnal repeat to be false when latency diverges")
	}
}

func TestApplyReasonToAnomalyType(t *testing.T) {
	a := analysis.DetectedAnomaly{Type: analysis.AnomalyQPSSpike}
	applyReasonToAnomalyType(&a, "LATENCY_ONLY_DEGRADATION")
	if a.Type != analysis.AnomalyLatencyDegraded {
		t.Fatalf("expected LATENCY_DEGRADED, got %s", a.Type)
	}
}

func TestApplyReasonToAnomalyMeasurement_UsesLatencyForLatencyReason(t *testing.T) {
	a := analysis.DetectedAnomaly{Value: 100, Baseline: 90}
	ev := anomalyEvidence{
		reason:      "LATENCY_ONLY_DEGRADATION",
		latEventP99: 1.8,
		latBaseP99:  0.3,
	}
	applyReasonToAnomalyMeasurement(&a, ev)
	if a.Value != 1.8 || a.Baseline != 0.3 {
		t.Fatalf("expected latency measurement applied, got value=%.3f baseline=%.3f", a.Value, a.Baseline)
	}
}

func TestApplyReasonToAnomalyMeasurement_UsesQPSForDefaultReason(t *testing.T) {
	a := analysis.DetectedAnomaly{Value: 10, Baseline: 8}
	ev := anomalyEvidence{
		reason:      "WORKLOAD_LINKED_SHIFT",
		qpsEventP95: 220,
		qpsBaseMed:  110,
	}
	applyReasonToAnomalyMeasurement(&a, ev)
	if a.Value != 220 || a.Baseline != 110 {
		t.Fatalf("expected qps measurement applied, got value=%.3f baseline=%.3f", a.Value, a.Baseline)
	}
}

func TestApplyReasonToAnomalyMeasurement_DoesNotOverrideLatencyWhenNotWorse(t *testing.T) {
	a := analysis.DetectedAnomaly{Value: 2.0, Baseline: 1.0}
	ev := anomalyEvidence{
		reason:      "LATENCY_ONLY_DEGRADATION",
		latEventP99: 0.9,
		latBaseP99:  1.0,
	}
	applyReasonToAnomalyMeasurement(&a, ev)
	if a.Value != 2.0 || a.Baseline != 1.0 {
		t.Fatalf("expected original measurement kept, got value=%.3f baseline=%.3f", a.Value, a.Baseline)
	}
}

func TestCalculateHighValueCoverage(t *testing.T) {
	values := []analysis.TimeSeriesPoint{
		{Timestamp: 1000, Value: 0.2},
		{Timestamp: 1120, Value: 0.9},
		{Timestamp: 1240, Value: 1.2},
		{Timestamp: 1360, Value: 0.1},
		{Timestamp: 1480, Value: 1.1},
	}
	cov, firstTS, lastTS := calculateHighValueCoverage(values, 1000, 1480, 0.8)
	if math.Abs(cov-0.6) > 1e-9 {
		t.Fatalf("expected coverage 0.6, got %.6f", cov)
	}
	if firstTS != 1120 || lastTS != 1480 {
		t.Fatalf("unexpected first/last high timestamp: %d %d", firstTS, lastTS)
	}
}
