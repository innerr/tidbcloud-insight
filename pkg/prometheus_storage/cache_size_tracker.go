package prometheus_storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type CacheSizeTracker struct {
	mu             sync.RWMutex
	baseDir        string
	cachedSize     int64
	lastCalculated time.Time
	dirty          bool
}

var (
	globalTracker     *CacheSizeTracker
	globalTrackerOnce sync.Once
)

func GetCacheSizeTracker(baseDir string) *CacheSizeTracker {
	globalTrackerOnce.Do(func() {
		globalTracker = &CacheSizeTracker{
			baseDir:    baseDir,
			cachedSize: -1,
			dirty:      true,
		}
	})
	if globalTracker.baseDir != baseDir {
		globalTracker.mu.Lock()
		globalTracker.baseDir = baseDir
		globalTracker.cachedSize = -1
		globalTracker.dirty = true
		globalTracker.mu.Unlock()
	}
	return globalTracker
}

func (t *CacheSizeTracker) GetSize() (int64, error) {
	t.mu.RLock()
	if !t.dirty && t.cachedSize >= 0 && time.Since(t.lastCalculated) < 5*time.Minute {
		size := t.cachedSize
		t.mu.RUnlock()
		return size, nil
	}
	t.mu.RUnlock()

	return t.RecalculateSize()
}

func (t *CacheSizeTracker) RecalculateSize() (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var totalSize int64
	metricsDir := t.baseDir

	err := filepath.Walk(metricsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to calculate cache size: %w", err)
	}

	t.cachedSize = totalSize
	t.lastCalculated = time.Now()
	t.dirty = false

	return totalSize, nil
}

func (t *CacheSizeTracker) AddBytes(bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cachedSize >= 0 {
		t.cachedSize += bytes
	}
}

func (t *CacheSizeTracker) CheckLimit(maxSizeMB int) (int64, error) {
	if maxSizeMB <= 0 {
		return 0, nil
	}

	currentSize, err := t.GetSize()
	if err != nil {
		return 0, err
	}

	maxSizeBytes := int64(maxSizeMB) * 1024 * 1024
	if currentSize >= maxSizeBytes {
		return currentSize, fmt.Errorf("cache size (%d MB) exceeds limit (%d MB)", currentSize/(1024*1024), maxSizeMB)
	}

	return currentSize, nil
}

func (t *CacheSizeTracker) MarkDirty() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dirty = true
}

func (t *CacheSizeTracker) FormatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
