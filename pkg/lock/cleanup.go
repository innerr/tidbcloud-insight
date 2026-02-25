package lock

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"
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

func CleanupStaleLocks(cacheDir string) (int, error) {
	var cleaned int
	var firstErr error

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".lock" {
			continue
		}

		lockPath := filepath.Join(cacheDir, name)
		pid := readPidFromPath(lockPath)
		if pid <= 0 {
			continue
		}

		if !processExists(pid) {
			if removeErr := os.Remove(lockPath); removeErr == nil {
				cleaned++
			} else if firstErr == nil {
				firstErr = removeErr
			}
		}
	}

	return cleaned, firstErr
}

func readPidFromPath(path string) int {
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return 0
	}
	content := string(data)
	if data[len(data)-1] == '\n' {
		content = content[:len(data)-1]
	}
	pid, _ := strconv.Atoi(content)
	return pid
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil
}
