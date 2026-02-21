package analysis

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

type CSVMetricWriter struct {
	file       *os.File
	writer     *csv.Writer
	headerFile *os.File
	mu         sync.Mutex
	labels     map[string]map[string]string
	dataPoints int
}

func NewCSVMetricWriter(filePath string) (*CSVMetricWriter, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	headerFile, err := os.Create(filePath + ".labels")
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &CSVMetricWriter{
		file:       file,
		writer:     csv.NewWriter(file),
		headerFile: headerFile,
		labels:     make(map[string]map[string]string),
	}, nil
}

func (w *CSVMetricWriter) WriteSeries(labels map[string]string, timestamps []int64, values []float64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	seriesID := labelsToID(labels)
	if _, exists := w.labels[seriesID]; !exists {
		w.labels[seriesID] = labels
	}

	for i, ts := range timestamps {
		if i >= len(values) {
			break
		}
		record := []string{
			seriesID,
			strconv.FormatInt(ts, 10),
			strconv.FormatFloat(values[i], 'f', -1, 64),
		}
		if err := w.writer.Write(record); err != nil {
			return err
		}
		w.dataPoints++
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *CSVMetricWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return err
	}

	var labelKeys map[string]bool
	for _, lbls := range w.labels {
		if labelKeys == nil {
			labelKeys = make(map[string]bool)
		}
		for k := range lbls {
			labelKeys[k] = true
		}
	}

	if len(labelKeys) > 0 {
		var sortedKeys []string
		for k := range labelKeys {
			sortedKeys = append(sortedKeys, k)
		}
		sort.Strings(sortedKeys)

		headerWriter := csv.NewWriter(w.headerFile)
		header := append([]string{"series_id"}, sortedKeys...)
		if err := headerWriter.Write(header); err != nil {
			return err
		}

		for id, lbls := range w.labels {
			record := []string{id}
			for _, k := range sortedKeys {
				record = append(record, lbls[k])
			}
			if err := headerWriter.Write(record); err != nil {
				return err
			}
		}
		headerWriter.Flush()
		if err := headerWriter.Error(); err != nil {
			return err
		}
	}

	err1 := w.file.Close()
	err2 := w.headerFile.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (w *CSVMetricWriter) DataPoints() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dataPoints
}

func labelsToID(labels map[string]string) string {
	if len(labels) == 0 {
		return "default"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	id := ""
	for _, k := range keys {
		if id != "" {
			id += "_"
		}
		id += k + "=" + labels[k]
	}
	return id
}

func ReadCSVMetricFile(dataPath, labelsPath string) (map[string]interface{}, error) {
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dataFile.Close() }()

	labelsMap := make(map[string]map[string]string)
	if labelsData, err := os.ReadFile(labelsPath); err == nil {
		lines := parseCSVLines(string(labelsData))
		if len(lines) > 1 {
			header := lines[0]
			for _, line := range lines[1:] {
				if len(line) < len(header) {
					continue
				}
				seriesID := line[0]
				lbls := make(map[string]string)
				for i, k := range header[1:] {
					if i+1 < len(line) {
						lbls[k] = line[i+1]
					}
				}
				labelsMap[seriesID] = lbls
			}
		}
	}

	data, err := os.ReadFile(dataPath)
	if err != nil {
		return nil, err
	}
	lines := parseCSVLines(string(data))

	seriesData := make(map[string][]interface{})
	for _, line := range lines {
		if len(line) < 3 {
			continue
		}
		seriesID := line[0]
		ts, err1 := strconv.ParseInt(line[1], 10, 64)
		val, err2 := strconv.ParseFloat(line[2], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		seriesData[seriesID] = append(seriesData[seriesID], []interface{}{float64(ts), fmt.Sprintf("%g", val)})
	}

	var results []interface{}
	for seriesID, values := range seriesData {
		series := map[string]interface{}{
			"metric": labelsMap[seriesID],
			"values": values,
		}
		results = append(results, series)
	}

	return map[string]interface{}{
		"data": map[string]interface{}{
			"result": results,
		},
	}, nil
}

func parseCSVLines(data string) [][]string {
	var lines [][]string
	var current []string
	inQuotes := false
	currentField := ""

	for _, r := range data {
		switch r {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				current = append(current, currentField)
				currentField = ""
			} else {
				currentField += string(r)
			}
		case '\n':
			if !inQuotes {
				current = append(current, currentField)
				lines = append(lines, current)
				current = nil
				currentField = ""
			} else {
				currentField += string(r)
			}
		case '\r':
		default:
			currentField += string(r)
		}
	}
	if currentField != "" || len(current) > 0 {
		current = append(current, currentField)
		lines = append(lines, current)
	}
	return lines
}
