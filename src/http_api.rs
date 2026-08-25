use super::*;

pub(super) fn response_json<T: Serialize>(status: StatusCode, value: T) -> Response {
    (status, Json(value)).into_response()
}

pub(super) fn is_problem_model_conflict(error: &str) -> bool {
    error.contains("already exists with a different model")
}

pub(super) fn solve_error_status(error: &str) -> StatusCode {
    if error.contains("already has running solve")
        || error.contains("coordination lock busy")
        || is_problem_model_conflict(error)
    {
        StatusCode::CONFLICT
    } else if error.contains("coordination")
        || error.contains("live-mutex")
        || error.contains("Redis")
    {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::BAD_REQUEST
    }
}

pub(super) fn build_info_document() -> Value {
    json!({
        "packageVersion": env!("CARGO_PKG_VERSION"),
        "gitCommit": option_env!("DD_GIT_COMMIT").unwrap_or("unknown"),
        "gitCommitShort": option_env!("DD_GIT_COMMIT_SHORT").unwrap_or("unknown"),
        "gitRef": option_env!("DD_GIT_REF").unwrap_or("unknown"),
        "gitDirty": option_env!("DD_GIT_DIRTY").unwrap_or("unknown"),
        "builtAt": option_env!("DD_BUILD_TIME_UTC").unwrap_or("unknown"),
    })
}

pub(super) fn runtime_source_document() -> Value {
    json!({
        "clusterGitUrl": optional_env_value("MIP_SOLVER_CLUSTER_GIT_URL"),
        "clusterGitRef": optional_env_value("MIP_SOLVER_CLUSTER_GIT_REF"),
        "natsUrlConfigured": optional_env_value("NATS_URL").is_some(),
        "postgresEnv": first_configured_env(&[
            "MIP_SOLVER_DATABASE_URL",
            "AGENT_TASKS_RDS_DATABASE_URL",
            "RDS_DATABASE_URL",
            "DATABASE_URL",
            "PG_DATABASE_URL",
        ]),
        "redisEnv": first_configured_env(&["MIP_SOLVER_REDIS_URL", "REDIS_URL"]),
    })
}

pub(super) fn subjects_document(state: &AppState) -> Value {
    json!({
        "jobs": &state.jobs_subject,
        "results": &state.results_subject,
        "control": &state.control_subject,
        "events": &state.events_subject,
    })
}

pub(super) fn links_document() -> Value {
    json!({
        "home": "/home",
        "healthz": "/healthz",
        "readyz": "/readyz",
        "version": "/version",
        "versionJson": "/version.json",
        "apiDocs": "/docs/api",
        "apiDocsJson": "/api/docs.json",
        "natsStatus": "/mip-solver-cluster/nats",
        "workers": "/mip-solver-cluster/workers",
        "solves": "/mip-solver-cluster/solves",
        "metrics": "/metrics",
        "exampleModel": "/model/example",
        "soccerFormationMipModel": "/model/soccer-formation",
        "soccerFormationLpModel": "/model/soccer-formation-lp",
        "uploadProblem": "/problems/{problemId}",
        "solveProblem": "/problems/{problemId}/solve",
        "solve": "/solve",
    })
}

pub(super) fn version_document(state: &AppState) -> Value {
    json!({
        "ok": true,
        "service": SERVICE_NAME,
        "description": SERVICE_DESCRIPTION,
        "version": env!("CARGO_PKG_VERSION"),
        "role": state.role.as_str(),
        "nodeId": &state.node_id,
        "build": build_info_document(),
        "runtime": runtime_source_document(),
    })
}

pub(super) fn nats_status_document(state: &AppState) -> Value {
    let connected = state.nats.is_some();
    let workers = current_worker_snapshot(state);
    json!({
        "ok": connected,
        "service": SERVICE_NAME,
        "role": state.role.as_str(),
        "nodeId": &state.node_id,
        "connected": connected,
        "readyForDistributedWork": connected,
        "stream": {
            "name": DD_REMOTE_MIP_SOLVER_STREAM_NAME,
            "subjects": DD_REMOTE_MIP_SOLVER_STREAM_SUBJECTS,
        },
        "subjects": subjects_document(state),
        "workerConsumer": env_value("MIP_SOLVER_NATS_CONSUMER", MIP_SOLVER_WORKERS_QUEUE_GROUP),
        "workerQueueGroup": MIP_SOLVER_WORKERS_QUEUE_GROUP,
        "workersKnown": workers.len(),
        "workers": workers,
        "notes": [
            "masters publish subproblem jobs to the jobs subject using JetStream publish acks",
            "slaves pull from the shared durable worker consumer and publish results to the results subject",
            "slaves also publish worker-ready/request-work/worker-completed control frames that masters observe"
        ],
    })
}

pub(super) fn api_docs_document(state: &AppState) -> Value {
    json!({
        "schema": API_DOCS_SCHEMA,
        "service": {
            "name": SERVICE_NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "description": SERVICE_DESCRIPTION,
        },
        "build": build_info_document(),
        "nats": nats_status_document(state),
        "links": links_document(),
        "endpoints": [
            {"method": "GET", "path": "/", "kind": "json", "description": "Service descriptor with runtime subjects, persistence, GPU status, and links."},
            {"method": "GET", "path": "/home", "kind": "html", "description": "Human-readable service home page."},
            {"method": "GET", "path": "/healthz", "kind": "json", "description": "Process liveness. Does not require NATS."},
            {"method": "GET", "path": "/readyz", "kind": "json", "description": "Readiness. Requires an active NATS connection for distributed solver work."},
            {"method": "GET", "path": "/version", "kind": "html", "description": "Build and runtime version page with git commit metadata."},
            {"method": "GET", "path": "/version.json", "kind": "json", "description": "Build and runtime version metadata."},
            {"method": "GET", "path": "/docs/api", "kind": "html", "description": "Human-readable API documentation."},
            {"method": "GET", "path": "/api/docs", "kind": "html", "description": "Compatibility alias for the API documentation page."},
            {"method": "GET", "path": "/api/docs.json", "kind": "json", "description": "Machine-readable API documentation."},
            {"method": "GET", "path": "/mip-solver-cluster/nats", "kind": "json", "description": "NATS stream, subject, durable worker consumer, and observed worker status."},
            {"method": "GET", "path": "/mip-solver-cluster/workers", "kind": "json", "description": "Slave workers the master has observed through NATS control frames."},
            {"method": "GET", "path": "/mip-solver-cluster/solves", "kind": "json", "description": "Master solve registry with job counts and per-attempt status."},
            {"method": "DELETE", "path": "/mip-solver-cluster/solves/:solve_id", "kind": "json", "description": "Cancel a running solve by solve id."},
            {"method": "POST", "path": "/mip-solver-cluster/solves/:solve_id/cancel", "kind": "json", "description": "Cancel a running solve by solve id."},
            {"method": "POST", "path": "/mip-solver-cluster/requests/:request_id/cancel", "kind": "json", "description": "Cancel a running solve by request id."},
            {"method": "GET", "path": "/workers", "kind": "json", "description": "Compatibility alias for /mip-solver-cluster/workers."},
            {"method": "GET", "path": "/model/example", "kind": "json", "description": "Example knapsack MIP request payload."},
            {"method": "GET", "path": "/model/soccer-formation", "kind": "json", "description": "Akrion-derived binary F433 roster-to-grid assignment MIP request payload."},
            {"method": "GET", "path": "/model/soccer-formation-lp", "kind": "json", "description": "Continuous relaxation of the Akrion-derived F433 assignment model, configured for the internal IPM solver."},
            {"method": "POST", "path": "/problems/:problem_id", "kind": "json", "description": "Stream and store a problem model by UUID for later solve-by-reference requests."},
            {"method": "POST", "path": "/problems/:problem_id/solve", "kind": "json", "description": "Solve a previously stored problem model by UUID."},
            {"method": "POST", "path": "/solve", "kind": "json", "description": "Submit a MIP/IP/LP solve request to a master node."},
            {"method": "GET", "path": "/sessions/:session_id", "kind": "json", "description": "Read a live session model snapshot."},
            {"method": "POST", "path": "/sessions/:session_id/events", "kind": "json", "description": "Apply live model editing events to a session."},
            {"method": "POST", "path": "/sessions/:session_id/solve", "kind": "json", "description": "Solve the current live session model snapshot."},
            {"method": "GET", "path": "/metrics", "kind": "text", "description": "Prometheus metrics for HTTP, NATS jobs/results, worker control, solve registry, and errors."}
        ],
    })
}

pub(super) fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

pub(super) fn service_page(title: &str, subtitle: &str, body: String) -> Html<String> {
    Html(format!(
        concat!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">",
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
            "<title>{title}</title>",
            "<style>",
            "body{{margin:0;background:#f7f8fb;color:#18202f;font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;}}",
            "main{{max-width:980px;margin:0 auto;padding:32px 20px 48px;}}",
            "h1{{font-size:2rem;line-height:1.15;margin:0 0 8px;letter-spacing:0;}}",
            "h2{{font-size:1rem;margin:28px 0 10px;color:#30405c;}}",
            "p{{line-height:1.55;color:#4d5b73;}}",
            "a{{color:#0b5cab;text-decoration:none;}}a:hover{{text-decoration:underline;}}",
            ".nav{{display:flex;flex-wrap:wrap;gap:10px;margin:20px 0 28px;}}",
            ".nav a,.pill{{border:1px solid #cbd5e1;border-radius:6px;padding:6px 10px;background:#fff;color:#23324a;}}",
            ".grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;}}",
            ".card{{border:1px solid #d8dee8;border-radius:8px;background:#fff;padding:14px;}}",
            "code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;}}",
            "pre{{overflow:auto;background:#101827;color:#e5edf7;border-radius:8px;padding:14px;}}",
            "table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #d8dee8;border-radius:8px;overflow:hidden;}}",
            "th,td{{text-align:left;border-bottom:1px solid #e7ebf2;padding:9px 10px;vertical-align:top;}}",
            "th{{font-size:.8rem;text-transform:uppercase;color:#5a6780;background:#f0f3f8;}}",
            ".ok{{color:#067647;}}.warn{{color:#b54708;}}",
            "</style></head><body><main>",
            "<h1>{title}</h1><p>{subtitle}</p>",
            "<nav class=\"nav\"><a href=\"/home\">Home</a><a href=\"/docs/api\">API Docs</a>",
            "<a href=\"/version\">Version</a><a href=\"/mip-solver-cluster/nats\">NATS</a>",
            "<a href=\"/healthz\">Health</a><a href=\"/readyz\">Readiness</a><a href=\"/metrics\">Metrics</a></nav>",
            "{body}</main></body></html>"
        ),
        title = html_escape(title),
        subtitle = html_escape(subtitle),
        body = body
    ))
}

pub(super) async fn home(State(state): State<AppState>) -> Html<String> {
    let connected = state.nats.is_some();
    let nats_class = if connected { "ok" } else { "warn" };
    let nats_text = if connected {
        "connected"
    } else {
        "not connected"
    };
    service_page(
        SERVICE_NAME,
        SERVICE_DESCRIPTION,
        format!(
            concat!(
                "<section class=\"grid\">",
                "<div class=\"card\"><h2>Node</h2><p><strong>Role:</strong> {role}<br><strong>Node:</strong> <code>{node}</code><br><strong>Version:</strong> {version}</p></div>",
                "<div class=\"card\"><h2>NATS</h2><p><strong>Status:</strong> <span class=\"{nats_class}\">{nats_text}</span><br><strong>Stream:</strong> <code>{stream}</code><br><strong>Worker consumer:</strong> <code>{consumer}</code></p></div>",
                "<div class=\"card\"><h2>Cluster</h2><p><strong>Workers observed:</strong> {workers}<br><strong>Solves tracked:</strong> {solves}</p></div>",
                "</section>",
                "<h2>Common Routes</h2>",
                "<p><a href=\"/model/example\"><code>/model/example</code></a> gives a sample solve request. ",
                "<a href=\"/mip-solver-cluster/workers\"><code>/mip-solver-cluster/workers</code></a> shows slave heartbeats observed by the master. ",
                "<a href=\"/api/docs.json\"><code>/api/docs.json</code></a> is the machine-readable API contract.</p>"
            ),
            role = html_escape(state.role.as_str()),
            node = html_escape(&state.node_id),
            version = html_escape(env!("CARGO_PKG_VERSION")),
            nats_class = nats_class,
            nats_text = nats_text,
            stream = html_escape(DD_REMOTE_MIP_SOLVER_STREAM_NAME),
            consumer = html_escape(&env_value(
                "MIP_SOLVER_NATS_CONSUMER",
                MIP_SOLVER_WORKERS_QUEUE_GROUP,
            )),
            workers = current_worker_snapshot(&state).len(),
            solves = state.solves.lock().expect("solves mutex poisoned").len(),
        ),
    )
}

pub(super) async fn version_json(State(state): State<AppState>) -> impl IntoResponse {
    Json(version_document(&state))
}

pub(super) async fn version_page(State(state): State<AppState>) -> Html<String> {
    let doc = serde_json::to_string_pretty(&version_document(&state)).unwrap_or_default();
    service_page(
        "Version",
        "Build and runtime metadata for the solver node.",
        format!("<pre>{}</pre>", html_escape(&doc)),
    )
}

pub(super) async fn api_docs_json(State(state): State<AppState>) -> impl IntoResponse {
    (
        [("content-type", "application/json; charset=utf-8")],
        Json(api_docs_document(&state)),
    )
}

pub(super) async fn api_docs_html(State(state): State<AppState>) -> Html<String> {
    let docs = api_docs_document(&state);
    let rows = docs
        .get("endpoints")
        .and_then(Value::as_array)
        .map(|endpoints| {
            endpoints
                .iter()
                .map(|endpoint| {
                    let method = endpoint
                        .get("method")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let path = endpoint.get("path").and_then(Value::as_str).unwrap_or("");
                    let kind = endpoint.get("kind").and_then(Value::as_str).unwrap_or("");
                    let description = endpoint
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    format!(
                        "<tr><td><code>{}</code></td><td><a href=\"{}\"><code>{}</code></a></td><td>{}</td><td>{}</td></tr>",
                        html_escape(method),
                        html_escape(path),
                        html_escape(path),
                        html_escape(kind),
                        html_escape(description),
                    )
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();
    service_page(
        "API Docs",
        "Canonical HTTP surface for the distributed MIP solver node.",
        format!(
            concat!(
                "<p><span class=\"pill\">schema {schema}</span><span class=\"pill\">version {version}</span>",
                "<span class=\"pill\">stream {stream}</span></p>",
                "<table><thead><tr><th>Method</th><th>Path</th><th>Kind</th><th>Description</th></tr></thead><tbody>{rows}</tbody></table>"
            ),
            schema = html_escape(API_DOCS_SCHEMA),
            version = html_escape(env!("CARGO_PKG_VERSION")),
            stream = html_escape(DD_REMOTE_MIP_SOLVER_STREAM_NAME),
            rows = rows,
        ),
    )
}

pub(super) async fn nats_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(nats_status_document(&state))
}

pub(super) async fn root(State(state): State<AppState>) -> impl IntoResponse {
    let tasks = runtime_task_entries(&state);
    let workers_known = current_worker_snapshot(&state).len();
    let active_tasks = tasks
        .iter()
        .filter(|task| task.finished_at_ms.is_none())
        .count();
    Json(json!({
        "ok": true,
        "service": SERVICE_NAME,
        "description": SERVICE_DESCRIPTION,
        "version": env!("CARGO_PKG_VERSION"),
        "role": state.role.as_str(),
        "nodeId": &state.node_id,
        "subjects": subjects_document(&state),
        "stream": DD_REMOTE_MIP_SOLVER_STREAM_NAME,
        "queueGroup": MIP_SOLVER_WORKERS_QUEUE_GROUP,
        "workersKnown": workers_known,
        "tasksTracked": tasks.len(),
        "activeTasks": active_tasks,
        "solvesTracked": state.solves.lock().expect("solves mutex poisoned").len(),
        "nats": nats_status_document(&state),
        "build": build_info_document(),
        "links": links_document(),
        "persistence": persistence_contract(),
        "gpu": gpu_status(),
    }))
}

pub(super) async fn healthz() -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "service": SERVICE_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "gitCommit": option_env!("DD_GIT_COMMIT_SHORT").unwrap_or("unknown"),
    }))
}

pub(super) async fn readyz(State(state): State<AppState>) -> Response {
    let nats_ready = state.nats.is_some();
    let status = if nats_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    response_json(
        status,
        json!({
            "ok": nats_ready,
            "service": SERVICE_NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "role": state.role.as_str(),
            "nodeId": &state.node_id,
            "nats": nats_ready,
            "subjects": subjects_document(&state),
            "stream": DD_REMOTE_MIP_SOLVER_STREAM_NAME,
            "reason": if nats_ready { Value::Null } else { json!("NATS connection is required for distributed solver readiness") },
        }),
    )
}

pub(super) fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub(super) async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let m = &state.metrics;
    let published = m.subproblem_jobs_published_total.load(Ordering::Relaxed);
    let completed = m.subproblem_jobs_completed_total.load(Ordering::Relaxed);
    let in_flight = published.saturating_sub(completed);
    let solves = state.solves.lock().expect("solves mutex poisoned");
    let solves_tracked = solves.len();
    let active_solves = solves
        .values()
        .filter(|solve| solve.finished_at_ms.is_none())
        .count();
    drop(solves);
    let node_id = prometheus_label_value(&state.node_id);
    let role = prometheus_label_value(state.role.as_str());
    let body = format!(
        concat!(
            "# HELP dd_mip_solver_node_info Static solver node metadata.\n",
            "# TYPE dd_mip_solver_node_info gauge\n",
            "dd_mip_solver_node_info{{role=\"{}\",node_id=\"{}\"}} 1\n",
            "# HELP dd_mip_solver_http_requests_total Total HTTP requests handled by this node.\n",
            "# TYPE dd_mip_solver_http_requests_total counter\n",
            "dd_mip_solver_http_requests_total {}\n",
            "# HELP dd_mip_solver_stream_events_total Total live model stream events applied by this node.\n",
            "# TYPE dd_mip_solver_stream_events_total counter\n",
            "dd_mip_solver_stream_events_total {}\n",
            "# HELP dd_mip_solver_solve_requests_total Total solve requests handled by this node.\n",
            "# TYPE dd_mip_solver_solve_requests_total counter\n",
            "dd_mip_solver_solve_requests_total {}\n",
            "# HELP dd_mip_solver_solve_cancel_requests_total Total top-level solve cancellation requests accepted by this node.\n",
            "# TYPE dd_mip_solver_solve_cancel_requests_total counter\n",
            "dd_mip_solver_solve_cancel_requests_total {}\n",
            "# HELP dd_mip_solver_subproblem_jobs_published_total Total NATS subproblem jobs published by masters.\n",
            "# TYPE dd_mip_solver_subproblem_jobs_published_total counter\n",
            "dd_mip_solver_subproblem_jobs_published_total {}\n",
            "# HELP dd_mip_solver_subproblem_jobs_completed_total Total expected NATS subproblem results accepted by masters.\n",
            "# TYPE dd_mip_solver_subproblem_jobs_completed_total counter\n",
            "dd_mip_solver_subproblem_jobs_completed_total {}\n",
            "# HELP dd_mip_solver_subproblem_jobs_in_flight Current master-observed subproblem jobs awaiting accepted results.\n",
            "# TYPE dd_mip_solver_subproblem_jobs_in_flight gauge\n",
            "dd_mip_solver_subproblem_jobs_in_flight {}\n",
            "# HELP dd_mip_solver_subproblem_jobs_redelegated_total Total errored subproblem jobs re-published by masters.\n",
            "# TYPE dd_mip_solver_subproblem_jobs_redelegated_total counter\n",
            "dd_mip_solver_subproblem_jobs_redelegated_total {}\n",
            "# HELP dd_mip_solver_subproblem_jobs_split_total Total accepted subproblem attempts split into child jobs by masters.\n",
            "# TYPE dd_mip_solver_subproblem_jobs_split_total counter\n",
            "dd_mip_solver_subproblem_jobs_split_total {}\n",
            "# HELP dd_mip_solver_workers_known Current worker nodes observed through NATS control messages.\n",
            "# TYPE dd_mip_solver_workers_known gauge\n",
            "dd_mip_solver_workers_known {}\n",
            "# HELP dd_mip_solver_worker_control_messages_total Total worker control messages received by master nodes.\n",
            "# TYPE dd_mip_solver_worker_control_messages_total counter\n",
            "dd_mip_solver_worker_control_messages_total {}\n",
            "# HELP dd_mip_solver_solves_tracked Current solve records retained in master memory.\n",
            "# TYPE dd_mip_solver_solves_tracked gauge\n",
            "dd_mip_solver_solves_tracked {}\n",
            "# HELP dd_mip_solver_active_solves Current solves without a terminal aggregate response.\n",
            "# TYPE dd_mip_solver_active_solves gauge\n",
            "dd_mip_solver_active_solves {}\n",
            "# HELP dd_mip_solver_slave_jobs_processed_total Total subproblem jobs processed by slave nodes.\n",
            "# TYPE dd_mip_solver_slave_jobs_processed_total counter\n",
            "dd_mip_solver_slave_jobs_processed_total {}\n",
            "# HELP dd_mip_solver_errors_total Total errors observed by this node.\n",
            "# TYPE dd_mip_solver_errors_total counter\n",
            "dd_mip_solver_errors_total {}\n"
        ),
        role,
        node_id,
        m.http_requests_total.load(Ordering::Relaxed),
        m.stream_events_total.load(Ordering::Relaxed),
        m.solve_requests_total.load(Ordering::Relaxed),
        m.solve_cancel_requests_total.load(Ordering::Relaxed),
        published,
        completed,
        in_flight,
        m.subproblem_jobs_redelegated_total.load(Ordering::Relaxed),
        m.subproblem_jobs_split_total.load(Ordering::Relaxed),
        current_worker_snapshot(&state).len(),
        m.worker_control_messages_total.load(Ordering::Relaxed),
        solves_tracked,
        active_solves,
        m.slave_jobs_processed_total.load(Ordering::Relaxed),
        m.errors_total.load(Ordering::Relaxed),
    );
    ([("Content-Type", "text/plain; version=0.0.4")], body)
}

pub(super) async fn example() -> impl IntoResponse {
    Json(json!({
        "requestId": "knapsack-demo",
        "problem": {
            "sense": "max",
            "c": [10.0, 40.0, 30.0, 50.0],
            "a": [[5.0, 4.0, 6.0, 3.0]],
            "b": [10.0],
            "integerVars": [true, true, true, true],
            "ub": [1.0, 1.0, 1.0, 1.0],
            "varNames": ["item0", "item1", "item2", "item3"]
        },
        "options": {
            "splitDepth": 2,
            "maxNodes": 10000,
            "timeoutMs": 120000
        }
    }))
}

pub(super) async fn soccer_formation_model() -> impl IntoResponse {
    Json(soccer_formation::model_document(false))
}

pub(super) async fn soccer_formation_lp_model() -> impl IntoResponse {
    Json(soccer_formation::model_document(true))
}

pub(super) async fn read_limited_body(body: Body) -> Result<Vec<u8>, String> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    let max_http_body_bytes = max_http_body_bytes();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| format!("read request body: {error}"))?;
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| "request body is too large".to_string())?;
        if next_len > max_http_body_bytes {
            return Err(format!(
                "request body exceeds {} bytes",
                max_http_body_bytes
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(super) fn problem_from_upload_bytes(bytes: &[u8]) -> Result<MipProblemSpec, String> {
    if let Ok(problem) = serde_json::from_slice::<MipProblemSpec>(bytes) {
        return normalized_problem(problem);
    }
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| format!("parse problem JSON: {error}"))?;
    let problem = value
        .get("problem")
        .cloned()
        .ok_or_else(|| {
            "problem upload must be a problem object or contain a problem field".to_string()
        })
        .and_then(|problem| {
            serde_json::from_value::<MipProblemSpec>(problem)
                .map_err(|error| format!("parse problem field: {error}"))
        })?;
    normalized_problem(problem)
}

pub(super) async fn upload_problem(
    State(state): State<AppState>,
    AxumPath(problem_id): AxumPath<String>,
    body: Body,
) -> Response {
    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);
    if state.role != NodeRole::Master {
        return response_json(
            StatusCode::CONFLICT,
            json!({"ok":false,"error":"this pod booted as slave and will not store problems"}),
        );
    }
    let problem_id = match Uuid::parse_str(problem_id.trim()) {
        Ok(uuid) => uuid.to_string(),
        Err(_) => {
            return response_json(
                StatusCode::BAD_REQUEST,
                json!({"ok":false,"error":"problem id must be a UUID"}),
            )
        }
    };
    let body = match read_limited_body(body).await {
        Ok(body) => body,
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"ok":false,"error":error}),
            );
        }
    };
    let problem = match problem_from_upload_bytes(&body) {
        Ok(problem) => problem,
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(StatusCode::BAD_REQUEST, json!({"ok":false,"error":error}));
        }
    };
    let revision = 0;
    let store_status = match store_problem_model(&state, &problem_id, revision, &problem).await {
        Ok(status) => status,
        Err(error) if state.redis.is_none() => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(
                solve_error_status(&error),
                json!({"ok":false,"error":error}),
            );
        }
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(
                solve_error_status(&error),
                json!({"ok":false,"error":error}),
            );
        }
    };
    let redis_stored = state.redis.is_some();
    response_json(
        StatusCode::OK,
        json!({
            "ok": true,
            "problemId": problem_id,
            "revision": revision,
            "stored": true,
            "storeStatus": store_status.as_str(),
            "redisStored": redis_stored,
            "distributedReady": redis_stored,
            "bytes": body.len(),
            "variables": problem.c.len(),
            "constraints": problem.a.len(),
        }),
    )
}

pub(super) async fn solve_http(
    State(state): State<AppState>,
    Json(input): Json<SolveHttpRequest>,
) -> Response {
    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .solve_requests_total
        .fetch_add(1, Ordering::Relaxed);
    if state.role != NodeRole::Master {
        return response_json(
            StatusCode::CONFLICT,
            json!({"ok":false,"error":"this pod booted as slave and will not act as master"}),
        );
    }
    let request_id = request_id(input.request_id);
    let problem_id = match problem_id(input.problem_id, &request_id) {
        Ok(problem_id) => problem_id,
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(StatusCode::BAD_REQUEST, json!({"ok":false,"error":error}));
        }
    };
    let options = SolveOptions::merged(input.options);
    let (problem, revision) = if let Some(problem) = input.problem {
        (problem, 0)
    } else if let Some(commands) = input.commands {
        match parse_problem_from_commands(&commands) {
            Ok((problem, revision, _frames)) => (problem, revision),
            Err(error) => {
                state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                return response_json(StatusCode::BAD_REQUEST, json!({"ok":false,"error":error}));
            }
        }
    } else {
        return response_json(
            StatusCode::BAD_REQUEST,
            json!({"ok":false,"error":"request needs either problem or commands"}),
        );
    };

    match run_problem_task_with_coordination(
        state.clone(),
        request_id,
        problem_id,
        revision,
        problem,
        options,
    )
    .await
    {
        Ok(response) => response_json(StatusCode::OK, response),
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            response_json(
                solve_error_status(&error),
                json!({"ok":false,"error":error}),
            )
        }
    }
}

pub(super) async fn solve_stored_problem(
    State(state): State<AppState>,
    AxumPath(problem_id): AxumPath<String>,
    Json(input): Json<StoredProblemSolveRequest>,
) -> Response {
    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .solve_requests_total
        .fetch_add(1, Ordering::Relaxed);
    if state.role != NodeRole::Master {
        return response_json(
            StatusCode::CONFLICT,
            json!({"ok":false,"error":"this pod booted as slave and will not act as master"}),
        );
    }
    let problem_id = match Uuid::parse_str(problem_id.trim()) {
        Ok(uuid) => uuid.to_string(),
        Err(_) => {
            return response_json(
                StatusCode::BAD_REQUEST,
                json!({"ok":false,"error":"problem id must be a UUID"}),
            )
        }
    };
    let revision = 0;
    let Some(problem) = load_problem_model(&state, &problem_id, revision).await else {
        return response_json(
            StatusCode::NOT_FOUND,
            json!({"ok":false,"error":"stored problem not found"}),
        );
    };
    let request_id = request_id(input.request_id);
    let options = SolveOptions::merged(input.options);
    match run_problem_task_with_coordination(
        state.clone(),
        request_id,
        problem_id,
        revision,
        problem,
        options,
    )
    .await
    {
        Ok(response) => response_json(StatusCode::OK, response),
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            response_json(
                solve_error_status(&error),
                json!({"ok":false,"error":error}),
            )
        }
    }
}

pub(super) async fn stream_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(input): Json<Value>,
) -> Response {
    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);
    let commands = match input {
        Value::Array(items) => items,
        value => vec![value],
    };
    if commands.len() > max_stream_commands() {
        return response_json(
            StatusCode::BAD_REQUEST,
            json!({"ok":false,"error":"too many stream commands"}),
        );
    }

    if !state.coordination.enabled() && state.pg.is_none() {
        let mut frames = Vec::new();
        let (revision, session_snapshot) = {
            let mut sessions = state.sessions.lock().expect("sessions mutex poisoned");
            let session = sessions.entry(session_id.clone()).or_insert(LiveSession {
                problem: None,
                revision: 0,
            });
            for command in &commands {
                if let Err(error) = apply_stream_command(
                    &mut session.problem,
                    &mut session.revision,
                    command,
                    &mut frames,
                ) {
                    state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                    frames
                        .push(json!({"event":"error","message":error,"revision":session.revision}));
                } else {
                    state
                        .metrics
                        .stream_events_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            (session.revision, session.clone())
        };
        snapshot_session_model(&state, &session_id, session_snapshot).await;
        return response_json(
            StatusCode::OK,
            json!({
                "ok": true,
                "sessionId": session_id,
                "revision": revision,
                "frames": frames,
            }),
        );
    }

    let lock_key =
        mip_solver_session_revision_lock_key(&state.coordination.redis_lock_prefix, &session_id);
    let guard = match acquire_coordination_lock(&state, lock_key, state.coordination.ttl_ms).await {
        Ok(guard) => guard,
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(
                solve_error_status(&error),
                json!({"ok":false,"error":error}),
            );
        }
    };

    let mut frames = Vec::new();
    let mut session = load_session_model(&state, &session_id)
        .await
        .unwrap_or(LiveSession {
            problem: None,
            revision: 0,
        });
    let expected_revision = session.revision;
    for command in &commands {
        if let Err(error) = apply_stream_command(
            &mut session.problem,
            &mut session.revision,
            command,
            &mut frames,
        ) {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            frames.push(json!({"event":"error","message":error,"revision":session.revision}));
        } else {
            state
                .metrics
                .stream_events_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    let response = match persist_session_model_checked(
        &state,
        &session_id,
        &session,
        expected_revision,
    )
    .await
    {
        Ok(true) => {
            state
                .sessions
                .lock()
                .expect("sessions mutex poisoned")
                .insert(session_id.clone(), session.clone());
            cache_session_model(&state, &session_id, session.clone()).await;
            response_json(
                StatusCode::OK,
                json!({
                    "ok": true,
                    "sessionId": session_id,
                    "revision": session.revision,
                    "frames": frames,
                }),
            )
        }
        Ok(false) => response_json(
            StatusCode::CONFLICT,
            json!({"ok":false,"error":"session revision changed before this update could be persisted"}),
        ),
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            response_json(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"ok":false,"error":error}),
            )
        }
    };
    release_coordination_guard(&state, guard).await;
    response
}

pub(super) async fn get_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Response {
    match load_session_model(&state, &session_id).await {
        Some(session) => response_json(
            StatusCode::OK,
            json!({
                "ok": true,
                "sessionId": session_id,
                "revision": session.revision,
                "problem": session.problem,
            }),
        ),
        None => response_json(
            StatusCode::NOT_FOUND,
            json!({"ok":false,"error":"session not found"}),
        ),
    }
}

pub(super) async fn workers(State(state): State<AppState>) -> Response {
    let workers = current_worker_snapshot(&state);
    response_json(
        StatusCode::OK,
        json!({
            "ok": true,
            "role": state.role.as_str(),
            "count": workers.len(),
            "workers": workers,
        }),
    )
}

pub(super) async fn runtime_tasks(State(state): State<AppState>) -> Response {
    let tasks = runtime_task_entries(&state);
    let active = tasks
        .iter()
        .filter(|task| task.finished_at_ms.is_none())
        .count();
    response_json(
        StatusCode::OK,
        json!({
            "ok": true,
            "role": state.role.as_str(),
            "nodeId": state.node_id,
            "count": tasks.len(),
            "active": active,
            "tasks": tasks,
        }),
    )
}

pub(super) async fn runtime_task(
    State(state): State<AppState>,
    AxumPath(task_id): AxumPath<String>,
) -> Response {
    match runtime_task_lookup(&state, task_id.trim()) {
        Some(task) => response_json(
            StatusCode::OK,
            json!({
                "ok": true,
                "role": state.role.as_str(),
                "nodeId": state.node_id,
                "task": task,
            }),
        ),
        None => response_json(
            StatusCode::NOT_FOUND,
            json!({"ok":false,"error":format!("runtime task not found: {task_id}")}),
        ),
    }
}

pub(super) async fn cluster_solves(
    State(state): State<AppState>,
    Query(query): Query<ClusterSolvesQuery>,
) -> Response {
    let problem_filter = match query.problem.as_deref() {
        Some(problem) if problem.trim().is_empty() => {
            return response_json(
                StatusCode::BAD_REQUEST,
                json!({"ok":false,"error":"problem query parameter must be a UUID"}),
            );
        }
        Some(problem) => match Uuid::parse_str(problem.trim()) {
            Ok(uuid) => Some(uuid),
            Err(_) => {
                return response_json(
                    StatusCode::BAD_REQUEST,
                    json!({"ok":false,"error":"problem query parameter must be a UUID"}),
                );
            }
        },
        None => None,
    };
    let mut solves: Vec<SolveRegistryEntry> = state
        .solves
        .lock()
        .expect("solves mutex poisoned")
        .values()
        .cloned()
        .collect();
    if let Some(problem_uuid) = problem_filter.as_ref() {
        solves.retain(|solve| {
            solve
                .problem_id
                .as_deref()
                .or(Some(solve.request_id.as_str()))
                .and_then(|id| Uuid::parse_str(id).ok())
                .map(|request_uuid| request_uuid == *problem_uuid)
                .unwrap_or(false)
        });
    }
    solves.sort_by(|left, right| {
        right
            .started_at_ms
            .cmp(&left.started_at_ms)
            .then_with(|| left.solve_id.cmp(&right.solve_id))
    });
    let active = solves
        .iter()
        .filter(|solve| solve.finished_at_ms.is_none())
        .count();
    response_json(
        StatusCode::OK,
        json!({
            "ok": true,
            "role": state.role.as_str(),
            "problem": problem_filter.map(|uuid| uuid.to_string()),
            "count": solves.len(),
            "active": active,
            "solves": solves,
        }),
    )
}

pub(super) async fn publish_solve_cancel(state: &AppState, info: &CancelInfo) {
    publish_control(
        state,
        "cancel-solve",
        json!({
            "solveId": &info.solve_id,
            "requestId": &info.request_id,
            "problemId": &info.problem_id,
            "reason": &info.reason,
            "requestedBy": &info.requested_by,
            "requestedAtMs": info.requested_at_ms,
        }),
    )
    .await;
    publish_event(
        state,
        "solve-cancel-requested",
        json!({
            "solveId": &info.solve_id,
            "requestId": &info.request_id,
            "problemId": &info.problem_id,
            "reason": &info.reason,
            "requestedBy": &info.requested_by,
        }),
    )
    .await;
}

pub(super) async fn cancel_solve_key(
    state: AppState,
    key: String,
    input: CancelSolveRequest,
) -> Response {
    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);
    if state.role != NodeRole::Master {
        return response_json(
            StatusCode::CONFLICT,
            json!({"ok":false,"error":"this pod booted as slave and will not cancel master solves"}),
        );
    }
    let reason = input
        .reason
        .unwrap_or_else(|| "cancel requested by client".to_string());
    let requested_by = input.requested_by.unwrap_or_else(|| state.node_id.clone());
    match request_solve_cancel(&state, &key, reason, requested_by) {
        Ok(info) => {
            snapshot_solve_state(&state, &info.solve_id).await;
            publish_solve_cancel(&state, &info).await;
            response_json(
                StatusCode::ACCEPTED,
                json!({
                    "ok": true,
                    "status": "cancelling",
                    "solveId": info.solve_id,
                    "requestId": info.request_id,
                    "problemId": info.problem_id,
                    "reason": info.reason,
                    "requestedBy": info.requested_by,
                    "requestedAtMs": info.requested_at_ms,
                }),
            )
        }
        Err(error) => response_json(StatusCode::NOT_FOUND, json!({"ok":false,"error":error})),
    }
}

pub(super) async fn cancel_solve(
    State(state): State<AppState>,
    AxumPath(solve_id): AxumPath<String>,
    Json(input): Json<CancelSolveRequest>,
) -> Response {
    cancel_solve_key(state, solve_id, input).await
}

pub(super) async fn cancel_solve_default(
    State(state): State<AppState>,
    AxumPath(solve_id): AxumPath<String>,
) -> Response {
    match cleanup_finished_solve(&state, &solve_id) {
        Ok(Some(entry)) => {
            state
                .metrics
                .http_requests_total
                .fetch_add(1, Ordering::Relaxed);
            return response_json(
                StatusCode::OK,
                json!({
                    "ok": true,
                    "status": "cleaned",
                    "solveId": entry.solve_id,
                    "requestId": entry.request_id,
                    "previousStatus": entry.status,
                    "jobsRemoved": entry.jobs.len(),
                }),
            );
        }
        Ok(None) => {}
        Err(_) => {}
    }
    cancel_solve_key(state, solve_id, CancelSolveRequest::default()).await
}

pub(super) async fn cancel_request(
    State(state): State<AppState>,
    AxumPath(request_id): AxumPath<String>,
    Json(input): Json<CancelSolveRequest>,
) -> Response {
    cancel_solve_key(state, request_id, input).await
}

pub(super) async fn cancel_problem(
    State(state): State<AppState>,
    AxumPath(problem_id): AxumPath<String>,
    Json(input): Json<CancelSolveRequest>,
) -> Response {
    match Uuid::parse_str(problem_id.trim()) {
        Ok(uuid) => cancel_solve_key(state, uuid.to_string(), input).await,
        Err(_) => response_json(
            StatusCode::BAD_REQUEST,
            json!({"ok":false,"error":"problem id must be a UUID"}),
        ),
    }
}

pub(super) async fn solve_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(input): Json<SolveHttpRequest>,
) -> Response {
    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .solve_requests_total
        .fetch_add(1, Ordering::Relaxed);
    if state.role != NodeRole::Master {
        return response_json(
            StatusCode::CONFLICT,
            json!({"ok":false,"error":"this pod booted as slave and will not act as master"}),
        );
    }

    let lock_key =
        mip_solver_session_revision_lock_key(&state.coordination.redis_lock_prefix, &session_id);
    let guard = match acquire_coordination_lock(&state, lock_key, state.coordination.ttl_ms).await {
        Ok(guard) => guard,
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(
                solve_error_status(&error),
                json!({"ok":false,"error":error}),
            );
        }
    };
    let snapshot = match load_session_model(&state, &session_id).await {
        Some(session) => match session.problem.clone() {
            Some(problem) => Ok((problem, session.revision)),
            None => Err(response_json(
                StatusCode::BAD_REQUEST,
                json!({"ok":false,"error":"session has no initialized problem"}),
            )),
        },
        None => Err(response_json(
            StatusCode::NOT_FOUND,
            json!({"ok":false,"error":"session not found"}),
        )),
    };
    release_coordination_guard(&state, guard).await;
    let (problem, revision) = match snapshot {
        Ok(snapshot) => snapshot,
        Err(response) => return response,
    };

    let request_id = request_id(input.request_id.or(Some(session_id)));
    let problem_id = match problem_id(input.problem_id, &request_id) {
        Ok(problem_id) => problem_id,
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return response_json(StatusCode::BAD_REQUEST, json!({"ok":false,"error":error}));
        }
    };
    let options = SolveOptions::merged(input.options);
    match run_problem_task_with_coordination(
        state.clone(),
        request_id,
        problem_id,
        revision,
        problem,
        options,
    )
    .await
    {
        Ok(response) => response_json(StatusCode::OK, response),
        Err(error) => {
            state.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            response_json(
                solve_error_status(&error),
                json!({"ok":false,"error":error}),
            )
        }
    }
}
