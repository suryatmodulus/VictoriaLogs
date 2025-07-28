package logstorage

import (
	"strings"
	"sync"
	"time"
)

// AsyncTaskInfo represents brief information about a background async task.
// It is intended to be exposed via monitoring endpoints / UI pages.
type AsyncTaskInfo struct {
	Seq     uint64             `json:"seq"`
	Type    string             `json:"type"`
	Status  string             `json:"status"`
	Tenant  string             `json:"tenant"`
	Payload asyncDeletePayload `json:"payload"`

	CreatedTime int64  `json:"createdTime,omitempty"`
	DoneTime    int64  `json:"doneTime,omitempty"`
	Error       string `json:"error,omitempty"`
}

type AsyncTaskInfoWithSource struct {
	AsyncTaskInfo `json:",inline"`
	Storage       string `json:"storage"`
}

// simple cache valid for 5 seconds for the sole Storage instance
var asyncTasksCache struct {
	mu   sync.Mutex
	ts   time.Time
	data []AsyncTaskInfo
}

// ListAsyncTasks gathers information about all async tasks known to this
// Storage instance. The returned slice isn't sorted.
func (s *Storage) ListAsyncTasks() []AsyncTaskInfo {
	// Try cached value first (valid for 5 seconds)
	asyncTasksCache.mu.Lock()
	const duration = 5 * time.Second
	if time.Since(asyncTasksCache.ts) < duration && asyncTasksCache.data != nil {
		data := asyncTasksCache.data
		asyncTasksCache.mu.Unlock()
		return data
	}
	asyncTasksCache.mu.Unlock()

	var out []AsyncTaskInfo

	s.partitionsLock.Lock()
	ptws := append([]*partitionWrapper(nil), s.partitions...)
	for _, ptw := range ptws {
		ptw.incRef()
	}
	s.partitionsLock.Unlock()

	defer func() {
		for _, ptw := range ptws {
			ptw.decRef()
		}
	}()

	for _, ptw := range ptws {
		ats := ptw.pt.ats
		if ats == nil {
			continue
		}

		ats.mu.Lock()
		tasks := append([]asyncTask(nil), ats.ts...)
		ats.mu.Unlock()

		for _, t := range tasks {
			tenantStr := "*"
			if len(t.TenantIDs) > 0 {
				var sb strings.Builder
				for i, tid := range t.TenantIDs {
					if i > 0 {
						sb.WriteString(",")
					}
					sb.WriteString(tid.String())
				}
				tenantStr = sb.String()
			}

			info := AsyncTaskInfo{
				Seq:         t.Seq,
				Type:        asyncTaskTypeString(t.Type),
				Status:      asyncTaskStatusString(t.Status),
				Tenant:      tenantStr,
				Payload:     t.Payload,
				CreatedTime: t.CreatedTime,
				DoneTime:    t.DoneTime,
				Error:       t.ErrorMsg,
			}
			out = append(out, info)
		}
	}

	// Store in cache
	asyncTasksCache.mu.Lock()
	asyncTasksCache.ts = time.Now()
	asyncTasksCache.data = out
	asyncTasksCache.mu.Unlock()

	return out
}

func asyncTaskTypeString(tt asyncTaskType) string {
	switch tt {
	case asyncTaskDelete:
		return "delete"
	case asyncTaskNone:
		fallthrough
	default:
		return "none"
	}
}

func asyncTaskStatusString(st asyncTaskStatus) string {
	switch st {
	case taskSuccess:
		return "success"
	case taskError:
		return "error"
	case taskPending:
		fallthrough
	default:
		return "pending"
	}
}
