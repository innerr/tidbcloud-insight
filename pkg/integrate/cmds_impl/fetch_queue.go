package impl

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"tidbcloud-insight/pkg/logger"
)

const minChunkSize = 300        // 5 minutes
const initialChunkSize = 1800   // 30 minutes, for first fetch
const maxMergeChunkSize = 86400 // 1 day
const maxMergeMultiplier = 8    // max 8x growth per merge
const maxMergeMultiplierAfterSplit = 2

type AdjustRecord struct {
	LastAdjust string // "merge" or "split"
}

type TaskResult struct {
	Task         *FetchTask
	Success      bool
	Error        error
	NeedSplit    bool
	FailedSize   int
	AbortCluster bool
	AbortAll     bool
	NeedMerge    bool
	ActualBytes  int64
	TargetBytes  int64
	ChunkSize    int
	EmptyData    bool
	IsFirstFetch bool
}

type TaskHandler func(ctx context.Context, task *FetchTask) *TaskResult

type CacheLimitChecker interface {
	CheckLimit(maxSizeMB int) (int64, error)
}

type FetchQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	ctx    context.Context
	cancel context.CancelFunc

	pending    map[string][]*FetchTask
	allPending []*FetchTask
	inProgress int

	results []*TaskResult

	chunkSizeCache  map[string]int
	adjustHistory   map[string]*AdjustRecord
	firstFetchDone  map[string]bool
	abortedClusters map[string]bool

	stopped bool
	workers int

	handler TaskHandler

	cacheLimitChecker CacheLimitChecker
	cacheMaxSizeMB    int

	wg sync.WaitGroup
}

func NewFetchQueue(parentCtx context.Context, workers int, handler TaskHandler,
	chunkSizeCache map[string]int, adjustHistory map[string]*AdjustRecord, firstFetchDone map[string]bool,
	cacheLimitChecker CacheLimitChecker, cacheMaxSizeMB int) *FetchQueue {
	ctx, cancel := context.WithCancel(parentCtx)

	if chunkSizeCache == nil {
		chunkSizeCache = make(map[string]int)
	}
	if adjustHistory == nil {
		adjustHistory = make(map[string]*AdjustRecord)
	}
	if firstFetchDone == nil {
		firstFetchDone = make(map[string]bool)
	}

	q := &FetchQueue{
		ctx:               ctx,
		cancel:            cancel,
		pending:           make(map[string][]*FetchTask),
		chunkSizeCache:    chunkSizeCache,
		adjustHistory:     adjustHistory,
		firstFetchDone:    firstFetchDone,
		abortedClusters:   make(map[string]bool),
		workers:           workers,
		handler:           handler,
		cacheLimitChecker: cacheLimitChecker,
		cacheMaxSizeMB:    cacheMaxSizeMB,
	}
	q.cond = sync.NewCond(&q.mu)

	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}

	return q
}

func (q *FetchQueue) Submit(task *FetchTask) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.stopped {
		return false
	}

	key := q.taskKey(task)
	if !q.firstFetchDone[key] {
		task.ChunkSize = initialChunkSize
	} else if cachedSize, exists := q.chunkSizeCache[key]; exists {
		task.ChunkSize = cachedSize
	}
	if _, exists := q.chunkSizeCache[key]; !exists {
		q.chunkSizeCache[key] = task.ChunkSize
	}

	q.pending[key] = append(q.pending[key], task)
	q.allPending = append(q.allPending, task)
	q.cond.Broadcast()

	return true
}

func (q *FetchQueue) SubmitBatch(tasks []*FetchTask) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.stopped {
		return
	}

	for _, task := range tasks {
		key := q.taskKey(task)
		if !q.firstFetchDone[key] {
			task.ChunkSize = initialChunkSize
		} else if cachedSize, exists := q.chunkSizeCache[key]; exists {
			task.ChunkSize = cachedSize
		}
		if _, exists := q.chunkSizeCache[key]; !exists {
			q.chunkSizeCache[key] = task.ChunkSize
		}
		q.pending[key] = append(q.pending[key], task)
		q.allPending = append(q.allPending, task)
	}
	q.cond.Broadcast()
}

func (q *FetchQueue) taskKey(task *FetchTask) string {
	return task.ClusterID + ":" + task.Metric
}

func (q *FetchQueue) worker(id int) {
	defer q.wg.Done()

	for {
		task := q.takeTask()
		if task == nil {
			return
		}

		if q.cacheLimitChecker != nil && q.cacheMaxSizeMB > 0 {
			_, err := q.cacheLimitChecker.CheckLimit(q.cacheMaxSizeMB)
			if err != nil {
				result := &TaskResult{
					Task:     task,
					Success:  false,
					Error:    err,
					AbortAll: true,
				}
				q.completeTask(result)
				continue
			}
		}

		result := q.handler(q.ctx, task)
		if result == nil {
			result = &TaskResult{
				Task:    task,
				Success: true,
			}
		}

		q.completeTask(result)
	}
}

func (q *FetchQueue) takeTask() *FetchTask {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.stopped {
			return nil
		}

		if len(q.allPending) > 0 {
			for len(q.allPending) > 0 {
				task := q.allPending[0]
				q.allPending = q.allPending[1:]

				if q.abortedClusters[task.ClusterID] {
					continue
				}

				key := q.taskKey(task)
				if chunkSize, ok := q.chunkSizeCache[key]; ok {
					task.ChunkSize = chunkSize
				}

				q.inProgress++
				return task
			}
			continue
		}

		if q.inProgress == 0 {
			return nil
		}

		q.cond.Wait()
	}
}

func (q *FetchQueue) completeTask(result *TaskResult) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.inProgress--

	task := result.Task

	key := q.taskKey(task)
	q.removeTaskFromPendingLocked(key, task)

	if q.abortedClusters[task.ClusterID] {
		q.cond.Broadcast()
		return
	}

	if result.AbortAll {
		logger.Warnf("Aborting all tasks due to cache limit exceeded: %v", result.Error)
		q.abortedClusters[task.ClusterID] = true
		q.pending = make(map[string][]*FetchTask)
		q.allPending = nil
		q.results = append(q.results, result)
		q.stopped = true
		if q.cancel != nil {
			q.cancel()
		}
		q.cond.Broadcast()
		return
	}

	if result.AbortCluster {
		logger.Warnf("Aborting cluster %s due to fatal error: %v", task.ClusterID, result.Error)
		q.abortedClusters[task.ClusterID] = true
		q.removeClusterPendingTasksLocked(task.ClusterID)
		q.cond.Broadcast()
		return
	}

	if result.EmptyData && result.IsFirstFetch {
		logger.Warnf("First fetch for %s returned empty, skipping remaining tasks", key)
		q.firstFetchDone[key] = true
		delete(q.pending, key)
		q.rebuildAllPendingLocked()
		q.results = append(q.results, result)
		q.cond.Broadcast()
		return
	}

	if result.Success && result.IsFirstFetch {
		q.firstFetchDone[key] = true
	}

	if result.NeedSplit {
		q.handleSplitLocked(result)
		q.cond.Broadcast()
		return
	}

	if result.NeedMerge {
		q.handleMergeLocked(result)
	}

	if !result.Success {
		q.requeueTaskLocked(task)
		q.cond.Broadcast()
		return
	}

	q.results = append(q.results, result)

	if q.cacheLimitChecker != nil && q.cacheMaxSizeMB > 0 {
		_, err := q.cacheLimitChecker.CheckLimit(q.cacheMaxSizeMB)
		if err != nil {
			logger.Warnf("Aborting all tasks due to cache limit exceeded: %v", err)
			q.abortedClusters[task.ClusterID] = true
			q.pending = make(map[string][]*FetchTask)
			q.allPending = nil
			q.stopped = true
			if q.cancel != nil {
				q.cancel()
			}
		}
	}

	q.cond.Broadcast()
}

func (q *FetchQueue) removeTaskFromPendingLocked(key string, task *FetchTask) {
	tasks, exists := q.pending[key]
	if !exists {
		return
	}
	var newTasks []*FetchTask
	for _, t := range tasks {
		if t.GapStart != task.GapStart || t.GapEnd != task.GapEnd {
			newTasks = append(newTasks, t)
		}
	}
	q.pending[key] = newTasks
}

func (q *FetchQueue) handleSplitLocked(result *TaskResult) {
	task := result.Task
	key := q.taskKey(task)

	newChunkSize := result.FailedSize / 2
	if newChunkSize < minChunkSize {
		logger.Warnf("Cluster %s metric %s: chunk size %d too small after split (< %d), aborting cluster",
			task.ClusterID, task.Metric, newChunkSize, minChunkSize)
		q.abortedClusters[task.ClusterID] = true
		q.removeClusterPendingTasksLocked(task.ClusterID)
		return
	}

	logger.Warnf("Splitting tasks for %s: chunk size %d -> %d", key, result.FailedSize, newChunkSize)

	q.adjustHistory[key] = &AdjustRecord{LastAdjust: "split"}
	q.chunkSizeCache[key] = newChunkSize

	q.splitPendingTasksLocked(key, newChunkSize)

	q.requeueTaskLocked(task)
}

func (q *FetchQueue) handleMergeLocked(result *TaskResult) {
	task := result.Task
	key := q.taskKey(task)

	if result.ActualBytes <= 0 || result.ChunkSize <= 0 || result.TargetBytes <= 0 {
		return
	}

	ratio := float64(result.ActualBytes) / float64(result.TargetBytes)

	record := q.adjustHistory[key]
	var targetRatio float64
	var maxGrowth int

	if record != nil && record.LastAdjust == "split" {
		if ratio >= 0.25 {
			return
		}
		targetRatio = 0.5
		maxGrowth = result.ChunkSize * maxMergeMultiplierAfterSplit
	} else {
		if ratio >= 0.4 {
			return
		}
		targetRatio = 0.7
		maxGrowth = result.ChunkSize * maxMergeMultiplier
	}

	estimatedChunkSize := int(float64(result.ChunkSize) * float64(result.TargetBytes) * targetRatio / float64(result.ActualBytes))

	if estimatedChunkSize > maxGrowth {
		estimatedChunkSize = maxGrowth
	}

	if estimatedChunkSize > maxMergeChunkSize {
		estimatedChunkSize = maxMergeChunkSize
	}

	if estimatedChunkSize <= result.ChunkSize {
		return
	}

	logger.Warnf("Merging tasks for %s: chunk size %d -> %d (actual %d bytes, target %d bytes, ratio %.2f)",
		key, result.ChunkSize, estimatedChunkSize, result.ActualBytes, result.TargetBytes, ratio)

	q.adjustHistory[key] = &AdjustRecord{LastAdjust: "merge"}
	q.chunkSizeCache[key] = estimatedChunkSize
	q.mergePendingTasksLocked(key, estimatedChunkSize)
}

func (q *FetchQueue) mergePendingTasksLocked(key string, newChunkSize int) {
	tasks, exists := q.pending[key]
	if !exists || len(tasks) == 0 {
		return
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].GapStart < tasks[j].GapStart
	})

	var mergedTasks []*FetchTask
	for _, task := range tasks {
		if len(mergedTasks) == 0 {
			mergedTasks = append(mergedTasks, &FetchTask{
				ID:        task.ID,
				ClusterID: task.ClusterID,
				Metric:    task.Metric,
				GapStart:  task.GapStart,
				GapEnd:    task.GapEnd,
				DSURL:     task.DSURL,
				Step:      task.Step,
				ChunkSize: newChunkSize,
			})
			continue
		}

		last := mergedTasks[len(mergedTasks)-1]
		mergedDuration := task.GapEnd - last.GapStart

		if mergedDuration <= int64(newChunkSize) {
			last.GapEnd = task.GapEnd
		} else {
			mergedTasks = append(mergedTasks, &FetchTask{
				ID:        task.ID,
				ClusterID: task.ClusterID,
				Metric:    task.Metric,
				GapStart:  task.GapStart,
				GapEnd:    task.GapEnd,
				DSURL:     task.DSURL,
				Step:      task.Step,
				ChunkSize: newChunkSize,
			})
		}
	}

	q.pending[key] = mergedTasks
	q.rebuildAllPendingLocked()
}

func (q *FetchQueue) splitPendingTasksLocked(key string, newChunkSize int) {
	tasks, exists := q.pending[key]
	if !exists {
		return
	}

	var newTasks []*FetchTask
	for _, task := range tasks {
		split := q.splitTaskLocked(task, newChunkSize)
		newTasks = append(newTasks, split...)
	}

	q.pending[key] = newTasks
	q.rebuildAllPendingLocked()
}

func (q *FetchQueue) splitTaskLocked(task *FetchTask, maxChunkSize int) []*FetchTask {
	gap := task.GapEnd - task.GapStart
	if gap <= int64(maxChunkSize) {
		task.ChunkSize = maxChunkSize
		return []*FetchTask{task}
	}

	var result []*FetchTask
	taskID := task.ID
	if taskID == "" {
		taskID = fmt.Sprintf("%s_%d_%d", task.Metric, task.GapStart, task.GapEnd)
	}

	splitIdx := 0
	for start := task.GapStart; start < task.GapEnd; {
		end := start + int64(maxChunkSize)
		if end > task.GapEnd {
			end = task.GapEnd
		}

		newTask := &FetchTask{
			ID:        fmt.Sprintf("%s_%d", taskID, splitIdx),
			ClusterID: task.ClusterID,
			Metric:    task.Metric,
			GapStart:  start,
			GapEnd:    end,
			DSURL:     task.DSURL,
			Step:      task.Step,
			ChunkSize: maxChunkSize,
		}
		result = append(result, newTask)
		splitIdx++
		start = end
	}

	return result
}

func (q *FetchQueue) rebuildAllPendingLocked() {
	q.allPending = nil
	for _, tasks := range q.pending {
		q.allPending = append(q.allPending, tasks...)
	}
}

func (q *FetchQueue) requeueTaskLocked(task *FetchTask) {
	key := q.taskKey(task)
	chunkSize := q.chunkSizeCache[key]
	splitTasks := q.splitTaskLocked(task, chunkSize)
	q.pending[key] = append(q.pending[key], splitTasks...)
	q.rebuildAllPendingLocked()
}

func (q *FetchQueue) removeClusterPendingTasksLocked(clusterID string) {
	for key := range q.pending {
		if len(key) > len(clusterID) && key[:len(clusterID)] == clusterID && key[len(clusterID)] == ':' {
			delete(q.pending, key)
		}
	}
	q.rebuildAllPendingLocked()
}

func (q *FetchQueue) Wait() []*TaskResult {
	q.mu.Lock()
	defer q.mu.Unlock()

	for !q.stopped && (len(q.allPending) > 0 || q.inProgress > 0) {
		q.cond.Wait()
	}

	q.stopped = true
	q.cond.Broadcast()

	return q.results
}

func (q *FetchQueue) Stop() {
	q.mu.Lock()
	q.stopped = true
	q.cond.Broadcast()
	q.mu.Unlock()

	if q.cancel != nil {
		q.cancel()
	}

	q.wg.Wait()
}

func (q *FetchQueue) GetStats() (pending, inProgress, completed int, abortedClusters []string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending = len(q.allPending)
	inProgress = q.inProgress
	completed = len(q.results)

	for clusterID := range q.abortedClusters {
		abortedClusters = append(abortedClusters, clusterID)
	}
	return
}

func (q *FetchQueue) IsClusterAborted(clusterID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.abortedClusters[clusterID]
}

func formatDuration(seconds int) string {
	d := time.Duration(seconds) * time.Second
	return d.String()
}
