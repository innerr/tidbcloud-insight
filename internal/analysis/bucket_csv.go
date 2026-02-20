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

type BucketCSVWriter struct {
	dataFile   *os.File
	dataWriter *csv.Writer
	labelsFile *os.File
	mu         sync.Mutex
	labels     map[string]int
	labelOrder []string
	nextID     int
	dataPoints int
}

func NewBucketCSVWriter(basePath string) (*BucketCSVWriter, error) {
	if err := os.MkdirAll(filepath.Dir(basePath), 0755); err != nil {
		return nil, err
	}

	dataFile, err := os.Create(basePath + ".data")
	if err != nil {
		return nil, err
	}

	labelsFile, err := os.Create(basePath + ".labels")
	if err != nil {
		_ = dataFile.Close()
		return nil, err
	}

	return &BucketCSVWriter{
		dataFile:   dataFile,
		dataWriter: csv.NewWriter(dataFile),
		labelsFile: labelsFile,
		labels:     make(map[string]int),
		nextID:     1,
	}, nil
}

func (w *BucketCSVWriter) WriteBucketPoint(labels map[string]string, le string, ts int64, value float64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	labelsWithoutLE := make(map[string]string)
	for k, v := range labels {
		if k != "le" {
			labelsWithoutLE[k] = v
		}
	}
	seriesKey := labelsToID(labelsWithoutLE)

	seriesID, exists := w.labels[seriesKey]
	if !exists {
		seriesID = w.nextID
		w.labels[seriesKey] = seriesID
		w.labelOrder = append(w.labelOrder, seriesKey)
		w.nextID++
	}

	record := []string{
		strconv.Itoa(seriesID),
		le,
		strconv.FormatInt(ts, 10),
		strconv.FormatFloat(value, 'f', -1, 64),
	}
	if err := w.dataWriter.Write(record); err != nil {
		return err
	}
	w.dataPoints++
	w.dataWriter.Flush()
	return w.dataWriter.Error()
}

func (w *BucketCSVWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.dataWriter.Flush()
	if err := w.dataWriter.Error(); err != nil {
		return err
	}

	var labelKeys map[string]bool
	labelData := make(map[int]map[string]string)
	for key, id := range w.labels {
		lbls := parseIDToLabels(key)
		labelData[id] = lbls
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

		headerWriter := csv.NewWriter(w.labelsFile)
		header := append([]string{"series_id"}, sortedKeys...)
		if err := headerWriter.Write(header); err != nil {
			return err
		}

		ids := make([]int, 0, len(labelData))
		for id := range labelData {
			ids = append(ids, id)
		}
		sort.Ints(ids)

		for _, id := range ids {
			lbls := labelData[id]
			record := []string{strconv.Itoa(id)}
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

	err1 := w.dataFile.Close()
	err2 := w.labelsFile.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (w *BucketCSVWriter) DataPoints() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dataPoints
}

func parseIDToLabels(id string) map[string]string {
	result := make(map[string]string)
	if id == "default" || id == "" {
		return result
	}

	start := 0
	for i := 0; i <= len(id); i++ {
		if i == len(id) || id[i] == '_' {
			if i > start {
				part := id[start:i]
				for j := 0; j < len(part); j++ {
					if part[j] == '=' {
						result[part[:j]] = part[j+1:]
						break
					}
				}
			}
			start = i + 1
		}
	}
	return result
}

func ReadBucketCSVFile(dataPath, labelsPath string) (map[string]interface{}, error) {
	labelsData, err := os.ReadFile(labelsPath)
	if err != nil {
		return nil, err
	}
	labelLines := parseCSVLines(string(labelsData))
	if len(labelLines) < 1 {
		return nil, fmt.Errorf("empty labels file")
	}

	labelHeader := labelLines[0]
	labelsMap := make(map[int]map[string]string)
	for _, line := range labelLines[1:] {
		if len(line) < len(labelHeader) {
			continue
		}
		seriesID, err := strconv.Atoi(line[0])
		if err != nil {
			continue
		}
		lbls := make(map[string]string)
		for i, k := range labelHeader[1:] {
			if i+1 < len(line) {
				lbls[k] = line[i+1]
			}
		}
		labelsMap[seriesID] = lbls
	}

	dataBytes, err := os.ReadFile(dataPath)
	if err != nil {
		return nil, err
	}
	dataLines := parseCSVLines(string(dataBytes))

	type bucketSeriesKey struct {
		seriesID int
		le       string
	}
	seriesData := make(map[bucketSeriesKey][]interface{})

	for _, line := range dataLines {
		if len(line) < 4 {
			continue
		}
		seriesID, err1 := strconv.Atoi(line[0])
		le := line[1]
		ts, err2 := strconv.ParseInt(line[2], 10, 64)
		val, err3 := strconv.ParseFloat(line[3], 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		key := bucketSeriesKey{seriesID: seriesID, le: le}
		seriesData[key] = append(seriesData[key], []interface{}{float64(ts), fmt.Sprintf("%g", val)})
	}

	type fullSeriesKey struct {
		seriesID int
		le       string
		labels   map[string]string
	}
	seriesByBase := make(map[int][]fullSeriesKey)
	for key := range seriesData {
		labels := labelsMap[key.seriesID]
		if labels == nil {
			labels = make(map[string]string)
		}
		seriesByBase[key.seriesID] = append(seriesByBase[key.seriesID], fullSeriesKey{
			seriesID: key.seriesID,
			le:       key.le,
			labels:   labels,
		})
	}

	var results []interface{}
	processedSeries := make(map[string]bool)

	for baseID, variants := range seriesByBase {
		for _, variant := range variants {
			fullLabels := make(map[string]string)
			for k, v := range variant.labels {
				fullLabels[k] = v
			}
			fullLabels["le"] = variant.le

			fullKeyStr := fmt.Sprintf("%d_%s", baseID, variant.le)
			if processedSeries[fullKeyStr] {
				continue
			}
			processedSeries[fullKeyStr] = true

			key := bucketSeriesKey{seriesID: variant.seriesID, le: variant.le}
			series := map[string]interface{}{
				"metric": fullLabels,
				"values": seriesData[key],
			}
			results = append(results, series)
		}
	}

	return map[string]interface{}{
		"data": map[string]interface{}{
			"result": results,
		},
	}, nil
}

func IsBucketCSVFormat(dataPath string) bool {
	_, err := os.Stat(dataPath + ".data")
	return err == nil
}
