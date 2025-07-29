package vlselect

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
)

var asyncTasksTmpl = template.Must(template.New("asyncTasks").Parse(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta http-equiv="refresh" content="5">
    <meta charset="utf-8">
    <title>VictoriaLogs — Async tasks (aggregated)</title>
    <style>
        :root {
            --bg: #f9f9f9;
            --border: #d0d0d0;
            --header-bg: #fafafa;
            --row-alt-bg: #ffffff;
            --row-hover: #eef2ff;
            --font: system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",sans-serif;
            --font-size: 14px;
        }
        html,body { margin: 0; padding: 0; font-family: var(--font); background: var(--bg); font-size: var(--font-size); }
        main { padding: 16px; max-width: 1400px; }
        h2 { margin: 0 0 12px; font-weight: 600; }
        .table-container { overflow-x: auto; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.05); border-radius: 8px; }
        table { width: 100%; border-collapse: collapse; }
        th,td { padding: 8px 12px; border: 1px solid var(--border); text-align: left; vertical-align: top; word-wrap: break-word; }
        thead th { background: var(--header-bg); position: sticky; top: 0; z-index: 1; font-weight: 600; }
        tbody tr:nth-child(odd) { background: var(--row-alt-bg); }
        tbody tr:hover { background: var(--row-hover); }
        .col-storage { width: 200px; max-width: 200px; }
        .col-created { width: 180px; max-width: 180px; }
        .col-type { width: 120px; max-width: 120px; }
        .col-status { width: 100px; max-width: 100px; }
        .col-tenant { width: 180px; max-width: 180px; }
        .col-done { width: 180px; max-width: 180px; }
        .col-result { width: 150px; max-width: 150px; }
        .col-payload { width: 300px; max-width: 300px; }
        .status { font-weight: 600; padding: 3px 8px; border-radius: 4px; display:inline-block; text-transform: capitalize; font-size: 12px; }
        .status.pending { background:#fff4cc; color:#856404; }
        .status.success { background:#d3f9d8; color:#14532d; }
        .status.error { background:#f8d7da; color:#842029; }
        .payload-cell { max-height: 100px; overflow-y: auto; }
        .payload-cell pre { margin: 0; white-space: pre-wrap; word-break: break-word; font-size: 12px; }
        .truncated { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    </style>
</head>
<body>
    <main>
    <h2>Async tasks</h2>

    {{- if .Tasks }}
    <div class="table-container">
    <table>
        <thead>
            <tr>
                <th class="col-storage">Storage</th>
                <th class="col-created">Created</th>
                <th class="col-type">Type</th>
                <th class="col-status">Status</th>
                {{- if .ShowTenant }}
                <th class="col-tenant">Tenant</th>
                {{- end }}
                <th class="col-done">Done</th>
                <th class="col-result">Result</th>
                <th class="col-payload">Payload</th>
            </tr>
        </thead>
        <tbody>
            {{- range .Tasks }}
            <tr>
                <td class="col-storage truncated" title="{{ .Storage }}">{{ .Storage }}</td>
                <td>{{ .Created }}</td>
                <td>{{ .Type }}</td>
                <td><span class="status {{ .Status }}">{{ .Status }}</span></td>
                {{- if $.ShowTenant }}
                <td class="col-tenant truncated" title="{{ .Tenant }}">{{ html .Tenant }}</td>
                {{- end }}
                <td>{{ .Done }}</td>
                <td class="col-result truncated" title="{{ .Result }}">{{ html .Result }}</td>
                <td class="col-payload payload-cell"><pre>{{ html .PayloadJSON }}</pre></td>
            </tr>
            {{- end }}
        </tbody>
    </table>
    </div>
    {{- else }}
    <p>No async tasks found.</p>
    {{- end }}
    </main>
</body>
</html>
`))

func processAsyncTasksRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	tasks, err := vlstorage.ListAsyncTasks(ctx)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// JSON output support
	if format := r.FormValue("format"); format == "json" || r.Header.Get("Accept") == "application/json" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(tasks); err != nil {
			httpserver.Errorf(w, r, "cannot encode response: %s", err)
		}
		return
	}

	// Sort tasks by Created time descending for better UX in HTML view
	sort.Slice(tasks, func(i, j int) bool {
		// Sort by CreatedTime desc, then by Storage desc
		if tasks[i].CreatedTime != tasks[j].CreatedTime {
			return tasks[i].CreatedTime > tasks[j].CreatedTime
		}
		return tasks[i].Storage > tasks[j].Storage
	})

	// Build view model for template.
	type row struct {
		Storage     string
		Type        string
		Status      string
		Tenant      string
		PayloadJSON string
		Created     string
		Done        string
		Result      string
	}

	// Check if we should show tenant column (if any tenant is not the default)
	showTenant := false
	for _, t := range tasks {
		if t.Tenant != "{accountID=0,projectID=0}" && t.Tenant != "*" {
			showTenant = true
			break
		}
	}

	vm := struct {
		Tasks      []row
		ShowTenant bool
	}{
		ShowTenant: showTenant,
	}

	for _, t := range tasks {
		payloadJSON, _ := json.Marshal(t.Payload)

		createdStr := time.Unix(0, t.CreatedTime).Format(time.RFC3339)
		done := "-"
		if t.DoneTime > 0 {
			done = time.Unix(0, t.DoneTime).Format(time.RFC3339)
		}

		result := t.Error
		if t.Status == "success" {
			result = "OK"
		}

		vm.Tasks = append(vm.Tasks, row{
			Storage:     t.Storage,
			Type:        string(t.Type),
			Status:      string(t.Status),
			Tenant:      t.Tenant,
			PayloadJSON: string(payloadJSON),
			Created:     createdStr,
			Done:        done,
			Result:      result,
		})
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := asyncTasksTmpl.Execute(w, vm); err != nil {
		logger.Errorf("cannot execute async tasks template: %s", err)
		httpserver.Errorf(w, r, "internal error: %s", err)
	}
}
