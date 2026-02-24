package lock

import (
	"os"
	"path/filepath"
)

func CleanupTempFiles(cacheDir string) (int, error) {
	metricsDir := filepath.Join(cacheDir, "metrics")

	if _, err := os.Stat(metricsDir); os.IsNotExist(err) {
		return 0, nil
	}

	var cleaned int
	var firstErr error

	err := filepath.Walk(metricsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".tmp" {
			if removeErr := os.Remove(path); removeErr == nil {
				cleaned++
			} else if firstErr == nil {
				firstErr = removeErr
			}
		}
		return nil
	})

	if err != nil {
		return cleaned, err
	}
	return cleaned, firstErr
}
