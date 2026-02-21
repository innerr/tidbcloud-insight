package integrate

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{"unix_timestamp", "1700000000", time.Unix(1700000000, 0).UTC(), false},
		{"rfc3339", "2023-11-15T10:30:00Z", time.Date(2023, 11, 15, 10, 30, 0, 0, time.UTC), false},
		{"rfc3339_nano", "2023-11-15T10:30:00.123456789Z", time.Date(2023, 11, 15, 10, 30, 0, 123456789, time.UTC), false},
		{"datetime_space", "2023-11-15 10:30:00", time.Date(2023, 11, 15, 10, 30, 0, 0, time.UTC), false},
		{"datetime_t", "2023-11-15T10:30:00", time.Date(2023, 11, 15, 10, 30, 0, 0, time.UTC), false},
		{"datetime_no_seconds", "2023-11-15 10:30", time.Date(2023, 11, 15, 10, 30, 0, 0, time.UTC), false},
		{"date_only", "2023-11-15", time.Date(2023, 11, 15, 0, 0, 0, 0, time.UTC), false},
		{"empty", "", time.Time{}, true},
		{"invalid", "not-a-time", time.Time{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTime(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("parseTime(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseDurationString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"go_standard", "1h30m", 1*time.Hour + 30*time.Minute, false},
		{"go_seconds", "90s", 90 * time.Second, false},
		{"go_milliseconds", "500ms", 500 * time.Millisecond, false},
		{"days", "7d", 7 * 24 * time.Hour, false},
		{"hours", "24h", 24 * time.Hour, false},
		{"minutes", "30m", 30 * time.Minute, false},
		{"combo_d_h", "1d12h", 36 * time.Hour, false},
		{"combo_d_h_m_s", "1d2h30m15s", 24*time.Hour + 2*time.Hour + 30*time.Minute + 15*time.Second, false},
		{"combo_h_m", "2h30m", 2*time.Hour + 30*time.Minute, false},
		{"combo_d_only", "3d", 3 * 24 * time.Hour, false},
		{"zero", "0s", 0, false},
		{"zero_days", "0d", 0, false},
		{"empty", "", 0, true},
		{"invalid", "abc", 0, true},
		{"invalid_unit", "7w", 0, true},
		{"no_number", "h", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDurationString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDurationString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseDurationString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseTimeRange(t *testing.T) {
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		start      string
		end        string
		agoAsEnd   string
		duration   string
		wantStart  time.Time
		wantEnd    time.Time
		wantErr    bool
		errContain string
	}{
		{
			name:      "duration_only",
			duration:  "7d",
			wantStart: now.Add(-7 * 24 * time.Hour),
			wantEnd:   now,
			wantErr:   false,
		},
		{
			name:      "start_end",
			start:     "2024-01-08T12:00:00Z",
			end:       "2024-01-15T12:00:00Z",
			wantStart: time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			wantErr:   false,
		},
		{
			name:       "ago_as_end_only_need_duration",
			agoAsEnd:   "7d",
			wantErr:    true,
			errContain: "missing start time",
		},
		{
			name:      "ago_as_end_duration",
			agoAsEnd:  "7d",
			duration:  "3d",
			wantStart: now.Add(-10 * 24 * time.Hour),
			wantEnd:   now.Add(-7 * 24 * time.Hour),
			wantErr:   false,
		},
		{
			name:       "start_duration_conflict",
			start:      "2024-01-10T12:00:00Z",
			duration:   "3d",
			wantErr:    true,
			errContain: "conflicting start time",
		},
		{
			name:      "start_end_duration_consistent",
			start:     "2024-01-08T12:00:00Z",
			end:       "2024-01-15T12:00:00Z",
			duration:  "7d",
			wantStart: time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			wantErr:   false,
		},
		{
			name:       "start_end_duration_conflict",
			start:      "2024-01-08T12:00:00Z",
			end:        "2024-01-15T12:00:00Z",
			duration:   "5d",
			wantErr:    true,
			errContain: "conflicting start time",
		},
		{
			name:       "start_equals_end",
			start:      "2024-01-15T12:00:00Z",
			end:        "2024-01-15T12:00:00Z",
			wantErr:    true,
			errContain: "must be before",
		},
		{
			name:       "start_after_end",
			start:      "2024-01-16T12:00:00Z",
			end:        "2024-01-15T12:00:00Z",
			wantErr:    true,
			errContain: "must be before",
		},
		{
			name:       "end_in_future",
			end:        "2024-01-16T12:00:00Z",
			duration:   "1d",
			wantErr:    true,
			errContain: "future",
		},
		{
			name:       "all_empty",
			wantErr:    true,
			errContain: "missing start time",
		},
		{
			name:      "unix_timestamp",
			start:     "1704753600",
			end:       "1704839999",
			wantStart: time.Unix(1704753600, 0).UTC(),
			wantEnd:   time.Unix(1704839999, 0).UTC(),
			wantErr:   false,
		},
		{
			name:      "date_only",
			start:     "2024-01-10",
			end:       "2024-01-15",
			wantStart: time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			wantErr:   false,
		},
		{
			name:      "combo_duration",
			duration:  "1d12h30m",
			wantStart: now.Add(-24*time.Hour - 12*time.Hour - 30*time.Minute),
			wantEnd:   now,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTimeRange(tt.start, tt.end, tt.agoAsEnd, tt.duration, now)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimeRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if tt.errContain != "" && err != nil && !containsString(err.Error(), tt.errContain) {
					t.Errorf("ParseTimeRange() error = %v, should contain %q", err, tt.errContain)
				}
				return
			}
			if !got.Start.Equal(tt.wantStart) {
				t.Errorf("ParseTimeRange() Start = %v, want %v", got.Start, tt.wantStart)
			}
			if !got.End.Equal(tt.wantEnd) {
				t.Errorf("ParseTimeRange() End = %v, want %v", got.End, tt.wantEnd)
			}
		})
	}
}

func TestTimeRangeMethods(t *testing.T) {
	tr := &TimeRange{
		Start: time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC),
	}

	expectedStartUnix := int64(1704844800)
	if tr.StartUnix() != expectedStartUnix {
		t.Errorf("StartUnix() = %d, want %d", tr.StartUnix(), expectedStartUnix)
	}

	expectedEndUnix := int64(1705321800)
	if tr.EndUnix() != expectedEndUnix {
		t.Errorf("EndUnix() = %d, want %d", tr.EndUnix(), expectedEndUnix)
	}

	expectedDur := 5*24*time.Hour + 12*time.Hour + 30*time.Minute
	if tr.Duration() != expectedDur {
		t.Errorf("Duration() = %v, want %v", tr.Duration(), expectedDur)
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
