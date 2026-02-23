package prometheus_storage

import "strings"

func inferMetricType(metricName string) string {
	switch {
	case strings.HasSuffix(metricName, "_bucket"):
		return "histogram"
	case strings.HasSuffix(metricName, "_total"),
		strings.HasSuffix(metricName, "_count"),
		strings.HasSuffix(metricName, "_sum"):
		return "counter"
	default:
		return "gauge"
	}
}
