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
const maxMergeChunkSize = 86400 // 1 day
const maxMergeMultiplier = 8    // max 8x growth per merge

type SplitRecord struct {
	ClusterID  string
	Metric     string
	BeforeSize int
}

type MergeRecord struct {
	ClusterID string
	Metric    string
	AfterSize int
}

type TaskResult struct {
	Task         *FetchTask
	Success      bool
	Error        error
	NeedSplit    bool
	FailedSize   int
	AbortCluster bool
	NeedMerge    bool
	ActualBytes  int64
	TargetBytes  int64
	ChunkSize    int
}

type TaskHandler func(ctx context.Context, task *FetchTask) *TaskResult

type FetchQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	ctx    context.Context
	cancel context.CancelFunc

	pending    map[string][]*FetchTask
	allPending []*FetchTask
	inProgress int

	results []*TaskResult

	splitHistory     []SplitRecord
	mergeHistory     []MergeRecord
	currentChunkSize map[string]int
	abortedClusters  map[string]bool

	stopped bool
	workers int

	handler TaskHandler

	wg sync.WaitGroup
}

func NewFetchQueue(parentCtx context.Context, workers int, handler TaskHandler) *FetchQueue {
	ctx, cancel := context.WithCancel(parentCtx)

	q := &FetchQueue{
		ctx:              ctx,
		cancel:           cancel,
		pending:          make(map[string][]*FetchTask),
		splitHistory:     make([]SplitRecord, 0),
		mergeHistory:     make([]MergeRecord, 0),
		currentChunkSize: make(map[string]int),
		abortedClusters:  make(map[string]bool),
		workers:          workers,
		handler:          handler,
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
	if _, exists := q.currentChunkSize[key]; !exists {
		q.currentChunkSize[key] = task.ChunkSize
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
		if _, exists := q.currentChunkSize[key]; !exists {
			q.currentChunkSize[key] = task.ChunkSize
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
				if chunkSize, ok := q.currentChunkSize[key]; ok {
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

	if result.AbortCluster {
		logger.Warnf("Aborting cluster %s due to fatal error: %v", task.ClusterID, result.Error)
		q.abortedClusters[task.ClusterID] = true
		q.removeClusterPendingTasksLocked(task.ClusterID)
		q.cond.Broadcast()
		return
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

	for _, r := range q.splitHistory {
		if r.ClusterID == task.ClusterID && r.Metric == task.Metric && r.BeforeSize == result.FailedSize {
			logger.Debugf("Split already done for %s with size %d, requeueing task", key, result.FailedSize)
			q.requeueTaskLocked(task)
			return
		}
	}

	newChunkSize := result.FailedSize / 2
	if newChunkSize < minChunkSize {
		logger.Warnf("Cluster %s metric %s: chunk size %d too small after split (< %d), aborting cluster",
			task.ClusterID, task.Metric, newChunkSize, minChunkSize)
		q.abortedClusters[task.ClusterID] = true
		q.removeClusterPendingTasksLocked(task.ClusterID)
		return
	}

	logger.Warnf("Splitting tasks for %s: chunk size %d -> %d", key, result.FailedSize, newChunkSize)

	q.splitHistory = append(q.splitHistory, SplitRecord{
		ClusterID:  task.ClusterID,
		Metric:     task.Metric,
		BeforeSize: result.FailedSize,
	})
	q.currentChunkSize[key] = newChunkSize

	q.splitPendingTasksLocked(key, newChunkSize)

	q.requeueTaskLocked(task)
}

func (q *FetchQueue) handleMergeLocked(result *TaskResult) {
	task := result.Task
	key := q.taskKey(task)

	for _, r := range q.mergeHistory {
		if r.ClusterID == task.ClusterID && r.Metric == task.Metric {
			return
		}
	}

	if result.ActualBytes <= 0 || result.ChunkSize <= 0 {
		return
	}

	targetHalf := result.TargetBytes / 2
	estimatedChunkSize := int(float64(result.ChunkSize) * float64(targetHalf) / float64(result.ActualBytes))

	maxGrowth := result.ChunkSize * maxMergeMultiplier
	if estimatedChunkSize > maxGrowth {
		estimatedChunkSize = maxGrowth
	}

	if estimatedChunkSize > maxMergeChunkSize {
		estimatedChunkSize = maxMergeChunkSize
	}

	if estimatedChunkSize <= result.ChunkSize {
		return
	}

	logger.Warnf("Merging tasks for %s: chunk size %d -> %d (actual %d bytes, target half %d bytes)",
		key, result.ChunkSize, estimatedChunkSize, result.ActualBytes, targetHalf)

	q.mergeHistory = append(q.mergeHistory, MergeRecord{
		ClusterID: task.ClusterID,
		Metric:    task.Metric,
		AfterSize: estimatedChunkSize,
	})
	q.currentChunkSize[key] = estimatedChunkSize
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
	chunkSize := q.currentChunkSize[key]
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
