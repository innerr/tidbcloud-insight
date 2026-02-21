package timerange

import (
	"testing"
)

func TestSubtractTimeRanges(t *testing.T) {
	tests := []struct {
		name      string
		requested TimeRange
		existing  TimeRangeList
		want      []TimeRange
	}{
		{
			name:      "no existing ranges",
			requested: TimeRange{Start: 100, End: 200},
			existing:  nil,
			want:      []TimeRange{{Start: 100, End: 200}},
		},
		{
			name:      "no overlap",
			requested: TimeRange{Start: 100, End: 200},
			existing:  TimeRangeList{{Start: 200, End: 300}},
			want:      []TimeRange{{Start: 100, End: 200}},
		},
		{
			name:      "full overlap",
			requested: TimeRange{Start: 100, End: 200},
			existing:  TimeRangeList{{Start: 50, End: 250}},
			want:      []TimeRange{},
		},
		{
			name:      "partial overlap start",
			requested: TimeRange{Start: 100, End: 200},
			existing:  TimeRangeList{{Start: 50, End: 150}},
			want:      []TimeRange{{Start: 150, End: 200}},
		},
		{
			name:      "partial overlap end",
			requested: TimeRange{Start: 100, End: 200},
			existing:  TimeRangeList{{Start: 150, End: 250}},
			want:      []TimeRange{{Start: 100, End: 150}},
		},
		{
			name:      "middle overlap",
			requested: TimeRange{Start: 100, End: 300},
			existing:  TimeRangeList{{Start: 150, End: 200}},
			want:      []TimeRange{{Start: 100, End: 150}, {Start: 200, End: 300}},
		},
		{
			name:      "multiple existing ranges",
			requested: TimeRange{Start: 100, End: 400},
			existing: TimeRangeList{
				{Start: 150, End: 200},
				{Start: 250, End: 300},
			},
			want: []TimeRange{
				{Start: 100, End: 150},
				{Start: 200, End: 250},
				{Start: 300, End: 400},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := subtractTimeRanges(tt.requested, tt.existing)
			if len(got) != len(tt.want) {
				t.Errorf("subtractTimeRanges() got %d ranges, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("subtractTimeRanges()[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestMergeTimeRanges(t *testing.T) {
	tests := []struct {
		name  string
		input TimeRangeList
		want  TimeRangeList
	}{
		{
			name:  "empty",
			input: nil,
			want:  nil,
		},
		{
			name:  "single range",
			input: TimeRangeList{{Start: 100, End: 200}},
			want:  TimeRangeList{{Start: 100, End: 200}},
		},
		{
			name:  "adjacent ranges",
			input: TimeRangeList{{Start: 100, End: 200}, {Start: 200, End: 300}},
			want:  TimeRangeList{{Start: 100, End: 300}},
		},
		{
			name:  "overlapping ranges",
			input: TimeRangeList{{Start: 100, End: 200}, {Start: 150, End: 250}},
			want:  TimeRangeList{{Start: 100, End: 250}},
		},
		{
			name:  "separate ranges",
			input: TimeRangeList{{Start: 100, End: 200}, {Start: 300, End: 400}},
			want:  TimeRangeList{{Start: 100, End: 200}, {Start: 300, End: 400}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeTimeRanges(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("mergeTimeRanges() got %d ranges, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("mergeTimeRanges()[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
