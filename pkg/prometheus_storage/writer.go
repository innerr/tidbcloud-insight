package prometheus_storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type chunkData struct {
	startTS    int64
	endTS      int64
	seriesData map[string]map[int64]string
}

type PrometheusMetricWriter struct {
	mu         sync.Mutex
	storage    *PrometheusStorage
	clusterID  string
	metricName string
	gapStartTS int64
	gapEndTS   int64

	nextExpectedTS int64
	pendingChunks  map[int64]*chunkData

	tmpFile      *os.File
	bufWriter    *bufio.Writer
	tmpFilePath  string
	finalPath    string
	bytesWritten int64
	closed       bool
}

func (s *PrometheusStorage) NewMetricWriter(clusterID, metricName string, gapStartTS, gapEndTS int64) (*PrometheusMetricWriter, error) {
	metricDir := s.GetMetricDir(clusterID, metricName)
	if err := os.MkdirAll(metricDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metric directory: %w", err)
	}

	finalFilename := formatTimeRangeForFilename(gapStartTS, gapEndTS) + ".prom"
	finalPath := filepath.Join(metricDir, finalFilename)
	tmpPath := finalPath + ".tmp"

	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	bufWriter := bufio.NewWriterSize(tmpFile, 64*1024)

	if _, err := fmt.Fprintf(bufWriter, "# TYPE %s gauge\n\n", metricName); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return &PrometheusMetricWriter{
		storage:        s,
		clusterID:      clusterID,
		metricName:     metricName,
		gapStartTS:     gapStartTS,
		gapEndTS:       gapEndTS,
		nextExpectedTS: gapStartTS,
		pendingChunks:  make(map[int64]*chunkData),
		tmpFile:        tmpFile,
		bufWriter:      bufWriter,
		tmpFilePath:    tmpPath,
		finalPath:      finalPath,
	}, nil
}

func (w *PrometheusMetricWriter) WriteSeries(labels map[string]string, timestamps []int64, values []float64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("writer already closed")
	}

	if len(timestamps) == 0 || len(values) == 0 {
		return nil
	}

	if len(timestamps) != len(values) {
		return fmt.Errorf("timestamps and values length mismatch: %d vs %d", len(timestamps), len(values))
	}

	chunkStartTS := timestamps[0]
	chunkEndTS := timestamps[len(timestamps)-1]

	chunk := w.getOrCreateChunk(chunkStartTS, chunkEndTS)

	labelKey := w.buildLabelKey(labels)
	for i, ts := range timestamps {
		if chunk.seriesData[labelKey] == nil {
			chunk.seriesData[labelKey] = make(map[int64]string)
		}
		chunk.seriesData[labelKey][ts] = formatValue(values[i])
	}

	return w.trySlideWindow()
}

func (w *PrometheusMetricWriter) getOrCreateChunk(startTS, endTS int64) *chunkData {
	if chunk, exists := w.pendingChunks[startTS]; exists {
		if endTS > chunk.endTS {
			chunk.endTS = endTS
		}
		return chunk
	}

	chunk := &chunkData{
		startTS:    startTS,
		endTS:      endTS,
		seriesData: make(map[string]map[int64]string),
	}
	w.pendingChunks[startTS] = chunk
	return chunk
}

func (w *PrometheusMetricWriter) buildLabelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := ""
	for i, k := range keys {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf(`%s="%s"`, k, labels[k])
	}
	return result
}

func formatValue(v float64) string {
	return fmt.Sprintf("%.6g", v)
}

func (w *PrometheusMetricWriter) trySlideWindow() error {
	for {
		chunk, exists := w.pendingChunks[w.nextExpectedTS]
		if !exists {
			break
		}

		if err := w.writeChunkToFile(chunk); err != nil {
			return err
		}

		delete(w.pendingChunks, w.nextExpectedTS)
		w.nextExpectedTS = chunk.endTS

		if err := w.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (w *PrometheusMetricWriter) writeChunkToFile(chunk *chunkData) error {
	var labelKeys []string
	for k := range chunk.seriesData {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)

	for _, labelKey := range labelKeys {
		tsMap := chunk.seriesData[labelKey]

		var timestamps []int64
		for ts := range tsMap {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

		for _, ts := range timestamps {
			value := tsMap[ts]
			var line string
			if labelKey != "" {
				line = fmt.Sprintf("%s{%s} %s %d\n", w.metricName, labelKey, value, ts*1000)
			} else {
				line = fmt.Sprintf("%s %s %d\n", w.metricName, value, ts*1000)
			}

			n, err := w.bufWriter.WriteString(line)
			if err != nil {
				return fmt.Errorf("failed to write to file: %w", err)
			}
			w.bytesWritten += int64(n)
		}
		w.bufWriter.WriteString("\n")
	}

	return nil
}

func (w *PrometheusMetricWriter) flush() error {
	if w.bufWriter == nil {
		return nil
	}
	return w.bufWriter.Flush()
}

func (w *PrometheusMetricWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if len(w.pendingChunks) > 0 {
		var pendingStarts []int64
		for start := range w.pendingChunks {
			pendingStarts = append(pendingStarts, start)
		}
		sort.Slice(pendingStarts, func(i, j int) bool { return pendingStarts[i] < pendingStarts[j] })

		for _, start := range pendingStarts {
			chunk := w.pendingChunks[start]
			if err := w.writeChunkToFile(chunk); err != nil {
				w.tmpFile.Close()
				os.Remove(w.tmpFilePath)
				return err
			}
			delete(w.pendingChunks, start)
		}
	}

	if err := w.flush(); err != nil {
		w.tmpFile.Close()
		os.Remove(w.tmpFilePath)
		return fmt.Errorf("failed to flush: %w", err)
	}

	if err := w.tmpFile.Close(); err != nil {
		os.Remove(w.tmpFilePath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(w.tmpFilePath, w.finalPath); err != nil {
		os.Remove(w.tmpFilePath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

func (w *PrometheusMetricWriter) BytesWritten() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytesWritten
}

func (w *PrometheusMetricWriter) GapTimeRange() (int64, int64) {
	return w.gapStartTS, w.gapEndTS
}
