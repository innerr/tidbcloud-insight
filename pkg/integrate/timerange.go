package integrate

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TimeRange struct {
	Start time.Time
	End   time.Time
}

func (tr *TimeRange) StartUnix() int64        { return tr.Start.Unix() }
func (tr *TimeRange) EndUnix() int64          { return tr.End.Unix() }
func (tr *TimeRange) Duration() time.Duration { return tr.End.Sub(tr.Start) }

func ParseTimeRange(start, end, agoAsEnd, duration string, now time.Time) (*TimeRange, error) {
	var startTime, endTime *time.Time

	// 1. Determine End
	if end != "" {
		t, err := parseTime(end)
		if err != nil {
			return nil, fmt.Errorf("invalid end time: %w", err)
		}
		endTime = &t
	}

	if agoAsEnd != "" {
		offsetDur, err := parseDurationString(agoAsEnd)
		if err != nil {
			return nil, fmt.Errorf("invalid ago-as-end: %w", err)
		}
		agoAsEndTime := now.Add(-offsetDur)
		if endTime != nil && !endTime.Equal(agoAsEndTime) {
			return nil, fmt.Errorf("conflicting end time: end=%s vs ago-as-end=%s",
				endTime.Format(time.RFC3339), agoAsEndTime.Format(time.RFC3339))
		}
		endTime = &agoAsEndTime
	}

	if endTime == nil {
		endTime = &now
	}

	// 2. Determine Start
	if start != "" {
		t, err := parseTime(start)
		if err != nil {
			return nil, fmt.Errorf("invalid start time: %w", err)
		}
		startTime = &t
	}

	if duration != "" {
		dur, err := parseDurationString(duration)
		if err != nil {
			return nil, fmt.Errorf("invalid duration: %w", err)
		}
		durStart := endTime.Add(-dur)
		if startTime != nil && !startTime.Equal(durStart) {
			return nil, fmt.Errorf("conflicting start time: start=%s vs duration=%s",
				startTime.Format(time.RFC3339), durStart.Format(time.RFC3339))
		}
		startTime = &durStart
	}

	if startTime == nil {
		return nil, fmt.Errorf("missing start time: need either start or duration")
	}

	// 3. Validate
	if startTime.After(*endTime) || startTime.Equal(*endTime) {
		return nil, fmt.Errorf("start time must be before end time")
	}
	if endTime.After(now) {
		return nil, fmt.Errorf("end time cannot be in the future")
	}

	return &TimeRange{Start: *startTime, End: *endTime}, nil
}

func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time string")
	}

	// Unix timestamp (seconds)
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(ts, 0).UTC(), nil
	}

	// Various time formats
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04",
		"2006-01-02",
	}

	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse time: %s", s)
}

func parseDurationString(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	// Try standard Go duration first
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Parse combined format like 1d2h3m4s
	re := regexp.MustCompile(`(\d+)([dhms])`)
	matches := re.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid duration format: %s", s)
	}

	var total time.Duration
	for _, m := range matches {
		val, _ := strconv.ParseInt(m[1], 10, 64)
		unit := m[2]
		switch unit {
		case "d":
			total += time.Duration(val) * 24 * time.Hour
		case "h":
			total += time.Duration(val) * time.Hour
		case "m":
			total += time.Duration(val) * time.Minute
		case "s":
			total += time.Duration(val) * time.Second
		}
	}

	// Check if we consumed the entire string
	reconstructed := ""
	for _, m := range matches {
		reconstructed += m[0]
	}
	if reconstructed != s {
		return 0, fmt.Errorf("invalid duration format: %s", s)
	}

	return total, nil
}
