package netselect

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// ListAsyncTasks gathers all async tasks from every storage node and returns them along with the originating storage address.
func (s *Storage) ListAsyncTasks(ctx context.Context) ([]logstorage.AsyncTaskInfoWithSource, error) {
	// Fast-path for mis-configured storage.
	if len(s.sns) == 0 {
		return nil, nil
	}

	// Derive a cancelable context so that we can abort all in-flight
	// requests as soon as the first error is encountered. This prevents
	// returning partial results and avoids wasting resources.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Aggregate tasks from all storage nodes.
	outCh := make(chan []logstorage.AsyncTaskInfoWithSource, len(s.sns))
	errCh := make(chan error, len(s.sns))

	for _, sn := range s.sns {
		go func(sn *storageNode) {
			tasks, err := sn.getAsyncTasks(ctx)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case outCh <- tasks:
			default:
			}
		}(sn)
	}

	var result []logstorage.AsyncTaskInfoWithSource
	for i := 0; i < len(s.sns); i++ {
		select {
		case tasks := <-outCh:
			result = append(result, tasks...)
		case err := <-errCh:
			// Cancel the context to abort outstanding requests.
			cancel()
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return result, nil
}

func (sn *storageNode) getAsyncTasks(ctx context.Context) ([]logstorage.AsyncTaskInfoWithSource, error) {
	args := url.Values{}
	args.Set("version", AsyncTasksProtocolVersion)

	reqURL := sn.getRequestURL("/internal/async_tasks", args)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	if err := sn.ac.SetHeaders(req, true); err != nil {
		return nil, fmt.Errorf("cannot set auth headers for %q: %w", reqURL, err)
	}

	resp, err := sn.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read the entire response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body from %q: %w", reqURL, err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code for %q: %d; response: %q", reqURL, resp.StatusCode, body)
	}

	var tasks []logstorage.AsyncTaskInfoWithSource
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, fmt.Errorf("cannot decode async tasks response from %q: %w; response body: %q", reqURL, err, body)
	}

	// Attach origin address.
	for i := range tasks {
		if tasks[i].Storage == "" {
			tasks[i].Storage = sn.addr
		}
	}
	return tasks, nil
}
