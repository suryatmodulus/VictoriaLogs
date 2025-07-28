package logstorage

import "strings"

// AsyncTaskInfo represents brief information about a background async task.
// It is intended to be exposed via monitoring endpoints / UI pages.
type AsyncTaskInfo struct {
	Seq     uint64             `json:"seq"`
	Type    string             `json:"type"`
	Status  string             `json:"status"`
	Tenant  string             `json:"tenant"`
	Payload asyncDeletePayload `json:"payload"`
}

// ListAsyncTasks gathers information about all async tasks known to this
// Storage instance. The returned slice isn't sorted.
func (s *Storage) ListAsyncTasks() []AsyncTaskInfo {
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
				Seq:     t.Seq,
				Type:    asyncTaskTypeString(t.Type),
				Status:  asyncTaskStatusString(t.Status),
				Tenant:  tenantStr,
				Payload: t.Payload,
			}
			out = append(out, info)
		}
	}

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
