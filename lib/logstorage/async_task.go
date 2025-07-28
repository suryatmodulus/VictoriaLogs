package logstorage

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// asyncTaskType identifies the type of background (asynchronous) task attached to a partition.
// More types can be added in the future (e.g. compaction, ttl, schema-changes).
type asyncTaskType int

const (
	asyncTaskNone   asyncTaskType = iota // no task
	asyncTaskDelete                      // delete rows matching a query
)

// Status field tracks the outcome of the task.
type asyncTaskStatus int

const (
	taskPending asyncTaskStatus = iota
	taskSuccess
	taskError
)

type asyncTask struct {
	Type        asyncTaskType      `json:"type"`                  // task type (delete, etc.)
	TenantIDs   []TenantID         `json:"tenantIDs,omitempty"`   // affected tenants
	Payload     asyncDeletePayload `json:"payload"`               // task parameters
	Seq         uint64             `json:"seq,omitempty"`         // global sequence number
	Status      asyncTaskStatus    `json:"status,omitempty"`      // pending/success/error
	CreatedTime int64              `json:"createdTime,omitempty"` // creation time (UnixNano)
	DoneTime    int64              `json:"doneTime,omitempty"`    // completion time (UnixNano)
	ErrorMsg    string             `json:"error,omitempty"`       // last error message
}

// asyncDeletePayload contains parameters for async tasks.
// For now it only includes LogSQL query for delete tasks.
type asyncDeletePayload struct {
	Query string `json:"query,omitempty"`
}

type asyncTasks struct {
	pt *partition

	mu  sync.Mutex
	ts  []asyncTask
	seq atomic.Uint64
}

func newAsyncTasks(pt *partition, tasks []asyncTask) *asyncTasks {
	ast := &asyncTasks{
		pt: pt,
		ts: tasks,
	}
	return ast
}

func (at *asyncTasks) updatePending() asyncTask {
	var result asyncTask

	at.mu.Lock()
	for i := range at.ts {
		task := at.ts[i]
		if task.Status == taskPending {
			result = task
			break
		}
	}
	at.mu.Unlock()

	at.seq.Store(result.Seq)
	return result
}

// unmarshalAsyncTasks converts JSON data back to async tasks
func unmarshalAsyncTasks(data []byte) ([]asyncTask, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var tasks []asyncTask
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, fmt.Errorf("unmarshal async tasks: %w", err)
	}
	return tasks, nil
}

// markResolvedSync updates task status and persists the change to disk.
// It holds the internal mutex only for in‐memory modification; the slow fs write
// is executed after the lock is released to avoid blocking other readers.
func (at *asyncTasks) markResolvedSync(seq uint64, status asyncTaskStatus, err error) {
	var updated bool

	at.mu.Lock()
	for i := len(at.ts) - 1; i >= 0; i-- {
		if at.ts[i].Seq == seq && at.ts[i].Status == taskPending {
			at.ts[i].Status = status
			at.ts[i].DoneTime = time.Now().UnixNano()
			if status == taskError && err != nil {
				at.ts[i].ErrorMsg = err.Error()
			}
			updated = true
			break
		}
	}
	at.mu.Unlock()

	if updated {
		at.pt.mustSaveAsyncTasks()
	}
}

// addDeleteTask appends a delete task to the partition's task list
func (at *asyncTasks) addDeleteTaskSync(tenantIDs []TenantID, q *Query, seq uint64) uint64 {
	task := asyncTask{
		Seq:         seq,
		Type:        asyncTaskDelete,
		TenantIDs:   append([]TenantID(nil), tenantIDs...),
		Payload:     asyncDeletePayload{Query: q.String()},
		Status:      taskPending,
		CreatedTime: time.Now().UnixNano(),
	}

	at.mu.Lock()
	at.ts = append(at.ts, task)
	at.mu.Unlock()

	at.pt.mustSaveAsyncTasks()
	return seq
}

// taskSeq provides unique, monotonically increasing sequence numbers for async tasks.
var taskSeq atomic.Uint64

func init() {
	// Initialise with current unix-nano in order to minimise collision with seqs that may be present on disk.
	taskSeq.Store(uint64(time.Now().UnixNano()))
}
