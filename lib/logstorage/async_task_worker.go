package logstorage

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// startAsyncTaskWorker launches a background goroutine, which periodically
// scans partitions for parts lagging behind async tasks and applies these
// tasks by re-executing their underlying queries via MarkRows (with
// createTask=false).
func (s *Storage) startAsyncTaskWorker() {
	s.wg.Add(1)
	const duration = 5 * time.Second
	go func() {
		defer s.wg.Done()
		ctx := context.Background()

		logger.Infof("DEBUG (task): start async task worker")

		timer := time.NewTimer(duration)
		defer timer.Stop()

		const maxFailedTime = 3
		var failedTime int

		for {
			select {
			case <-s.stopCh:
				// Drain the timer channel if needed before returning
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
				// Honour pause requests, if any. If cannot process, just reset timer and continue.
				if !s.asyncTaskState.canProcess() {
					timer.Reset(5 * time.Second)
					continue
				}

				seq, err := s.runAsyncTasksOnce(ctx)
				if err != nil {
					logger.Errorf("async task worker: %s", err)
					failedTime++
				} else {
					failedTime = 0
				}

				if failedTime > maxFailedTime {
					s.failAsyncTask(seq, err)
					failedTime = 0
				}

				// Start the 5-second wait *after* the task completes
				timer.Reset(duration)
			}
		}
	}()
}

// runAsyncTasksOnce performs a single pass over all partitions (latest → oldest)
// and applies pending async tasks to every part that hasn't caught up yet.
func (s *Storage) runAsyncTasksOnce(ctx context.Context) (uint64, error) {
	var seq uint64

	// Snapshot partitions (most recent first).
	s.partitionsLock.Lock()
	ptws := append([]*partitionWrapper{}, s.partitions...)
	for _, ptw := range ptws {
		ptw.incRef()
	}
	s.partitionsLock.Unlock()

	defer func() {
		for _, ptw := range ptws {
			ptw.decRef()
		}
	}()

	oudatedPtws, task := s.advanceNextAsyncTask(ptws)
	if task.Type == asyncTaskNone {
		return 0, nil
	}

	logger.Infof("DEBUG (task): found task seq=%d", task.Seq)
	seq = task.Seq

	// Gather all lagging parts in the target partition for this sequence.
	var lagging []*partWrapper
	var pendingPws []*partWrapper
	pending := 0
	for _, ptw := range oudatedPtws {
		pt := ptw.pt
		pt.ddb.partsLock.Lock()
		allPws := [][]*partWrapper{pt.ddb.inmemoryParts, pt.ddb.smallParts, pt.ddb.bigParts}
		for _, arr := range allPws {
			for _, pw := range arr {
				if pw.taskSeq.Load() >= task.Seq {
					continue
				}

				if pw.isInMerge {
					pending++
					pendingPws = append(pendingPws, pw)
					continue
				}

				if pw.mustDrop.Load() {
					continue
				}

				pw.incRef()
				lagging = append(lagging, pw)
			}
		}
		pt.ddb.partsLock.Unlock()
	}

	// If there are no lagging parts,
	// mark the task as success and return.
	if len(lagging) == 0 {
		if pending > 0 {
			logger.Infof("DEBUG (task): no lagging parts, but there are pending parts (pending=%d), waiting for them to finish", pending)
			for _, pw := range pendingPws {
				fmt.Println("DEBUG (task): pending part", pw.p.path, pw.taskSeq.Load())
			}
			return seq, nil
		}

		s.setTaskAsDone(oudatedPtws, task.Seq, taskSuccess, false)
		return seq, nil
	}

	defer func() {
		for _, pw := range lagging {
			pw.decRef()
		}
	}()

	if task.Type == asyncTaskDelete {
		logger.Infof("DEBUG (task): start deleting (seq=%d) on %d lagging parts (%d pending)", task.Seq, len(lagging), pending)
		err := s.runDeleteTask(ctx, task, lagging)
		if err != nil {
			return 0, fmt.Errorf("run delete task: %w", err)
		}
	}

	for _, pw := range lagging {
		pw.taskSeq.Store(task.Seq)
	}

	if pending == 0 {
		s.setTaskAsDone(oudatedPtws, task.Seq, taskSuccess, false)
	}

	logger.Infof("DEBUG (task): task (seq=%d, query=%q) applied to %d parts", task.Seq, task.Payload.Query, len(lagging))
	return seq, nil
}

func (s *Storage) failAsyncTask(sequence uint64, err error) {
	if sequence == 0 || err == nil {
		return
	}

	// Take a snapshot of partitions
	s.partitionsLock.Lock()
	ptws := append([]*partitionWrapper{}, s.partitions...)
	for _, ptw := range ptws {
		ptw.incRef()
	}
	s.partitionsLock.Unlock()

	// Mark the tasks as error for partitions and parts
	s.setTaskAsDone(ptws, sequence, taskError, true)

	for _, ptw := range ptws {
		ptw.decRef()
	}
}

func (s *Storage) setTaskAsDone(ptws []*partitionWrapper, taskSeq uint64, ats asyncTaskStatus, includeParts bool) {
	for _, ptw := range ptws {
		pt := ptw.pt

		if includeParts {
			// Ensure every part in this partition has taskSeq at least `sequence`.
			pt.ddb.partsLock.Lock()
			all := [][]*partWrapper{pt.ddb.inmemoryParts, pt.ddb.smallParts, pt.ddb.bigParts}
			for _, arr := range all {
				for _, pw := range arr {
					if pw.taskSeq.Load() < taskSeq {
						pw.taskSeq.Store(taskSeq)
					}
				}
			}
			pt.ddb.partsLock.Unlock()
		}

		pt.ats.markResolvedSync(taskSeq, ats)
	}

	logger.Infof("DEBUG (task): setTaskAsDone: taskSeq=%d, ats=%s, includeParts=%t", taskSeq, ats, includeParts)
}

func (s *Storage) runDeleteTask(ctx context.Context, task asyncTask, lagging []*partWrapper) error {
	// Build allowed set
	allowed := make(map[*partition][]*partWrapper, len(lagging))
	for _, pw := range lagging {
		allowed[pw.p.pt] = append(allowed[pw.p.pt], pw)
	}

	queryStr := task.Payload.Query
	err := s.markDeleteRowsOnParts(ctx, task.TenantIDs, queryStr, task.Seq, allowed)
	if err != nil {
		return fmt.Errorf("failed to mark delete rows on parts: %w", err)
	}

	return err
}

func (s *Storage) advanceNextAsyncTask(ptws []*partitionWrapper) ([]*partitionWrapper, asyncTask) {
	var minSeq uint64 = math.MaxUint64
	var result asyncTask
	var resultPtws []*partitionWrapper

	for _, ptw := range ptws {
		pt := ptw.pt

		task := pt.ats.updatePending()
		if task.Type == asyncTaskNone || task.Seq > minSeq {
			continue
		}

		// If we find a smaller sequence,
		// reset the slice to start a new collection.
		if task.Seq < minSeq {
			minSeq = task.Seq
			resultPtws = append(resultPtws[:0], ptw)
			result = task
			continue
		}

		resultPtws = append(resultPtws, ptw)
	}

	s.asyncTaskSeq.Store(result.Seq)
	return resultPtws, result
}
