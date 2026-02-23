package impl

import (
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
