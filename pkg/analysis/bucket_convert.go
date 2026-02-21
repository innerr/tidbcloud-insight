package analysis

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func ConvertOldBucketCSVToNew(csvPath string) error {
	labelsPath := csvPath + ".labels"
	if _, err := os.Stat(labelsPath); os.IsNotExist(err) {
		return fmt.Errorf("labels file not found: %s", labelsPath)
	}

	labelsData, err := os.ReadFile(labelsPath)
	if err != nil {
		return fmt.Errorf("failed to read labels file: %w", err)
	}
	labelLines := parseCSVLines(string(labelsData))
	if len(labelLines) < 1 {
		return fmt.Errorf("empty labels file")
	}

	labelHeader := labelLines[0]
	labelsMap := make(map[string]map[string]string)
	for _, line := range labelLines[1:] {
		if len(line) < len(labelHeader) {
			continue
		}
		seriesID := line[0]
		lbls := make(map[string]string)
		for i, k := range labelHeader[1:] {
			if i+1 < len(line) {
				lbls[k] = line[i+1]
			}
		}
		labelsMap[seriesID] = lbls
	}

	dataBytes, err := os.ReadFile(csvPath)
	if err != nil {
		return fmt.Errorf("failed to read data file: %w", err)
	}
	dataLines := parseCSVLines(string(dataBytes))

	basePath := strings.TrimSuffix(csvPath, ".csv")
	writer, err := NewBucketCSVWriter(basePath)
	if err != nil {
		return fmt.Errorf("failed to create bucket writer: %w", err)
	}

	converted := 0
	for _, line := range dataLines {
		if len(line) < 3 {
			continue
		}
		seriesID := line[0]
		ts := int64(0)
		val := 0.0
		if _, err := fmt.Sscanf(line[1], "%d", &ts); err != nil {
			continue
		}
		if _, err := fmt.Sscanf(line[2], "%f", &val); err != nil {
			continue
		}

		lbls := labelsMap[seriesID]
		if lbls == nil {
			lbls = make(map[string]string)
		}

		le := lbls["le"]
		if le == "" {
			le = "+Inf"
		}

		if err := writer.WriteBucketPoint(lbls, le, ts, val); err != nil {
			fmt.Printf("Warning: failed to write point: %v\n", err)
			continue
		}
		converted++
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	if err := os.Remove(csvPath); err != nil {
		fmt.Printf("Warning: failed to remove old csv file: %v\n", err)
	}
	if err := os.Remove(labelsPath); err != nil {
		fmt.Printf("Warning: failed to remove old labels file: %v\n", err)
	}

	fmt.Printf("Converted %d data points from %s to new format\n", converted, csvPath)
	return nil
}

func IsBucketMetric(name string) bool {
	return strings.HasSuffix(name, "_bucket") ||
		strings.Contains(name, "latency") ||
		strings.Contains(name, "duration")
}

func ConvertBucketDataInDir(dir string, dryRun bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(dir, entry.Name())
			if err := ConvertBucketDataInDir(subDir, dryRun); err != nil {
				fmt.Printf("Error processing %s: %v\n", subDir, err)
			}
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".csv") {
			continue
		}

		baseName := strings.TrimSuffix(name, ".csv")
		if !IsBucketMetric(baseName) {
			continue
		}

		csvPath := filepath.Join(dir, name)
		newDataPath := csvPath + ".data"

		if _, err := os.Stat(newDataPath); err == nil {
			continue
		}

		if dryRun {
			fmt.Printf("Would convert: %s\n", csvPath)
			continue
		}

		fmt.Printf("Converting: %s\n", csvPath)
		if err := ConvertOldBucketCSVToNew(csvPath); err != nil {
			fmt.Printf("Error converting %s: %v\n", csvPath, err)
		}
	}

	return nil
}
