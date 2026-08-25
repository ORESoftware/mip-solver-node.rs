use super::*;
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request},
};
use serde_json::json;
use tower::ServiceExt;

fn test_state(role: NodeRole) -> AppState {
    AppState {
        role,
        node_id: "test-node".to_string(),
        nats: None,
        redis: None,
        pg: None,
        coordination: CoordinationConfig {
            backends: Vec::new(),
            redis_lock_prefix: redis_key_prefix(),
            ttl_ms: 30_000,
            wait_ms: 0,
            live_mutex: None,
        },
        jobs_subject: MIP_SOLVER_JOBS_SUBJECT.to_string(),
        results_subject: MIP_SOLVER_RESULTS_SUBJECT.to_string(),
        control_subject: MIP_SOLVER_CONTROL_SUBJECT.to_string(),
        events_subject: MIP_SOLVER_EVENTS_SUBJECT.to_string(),
        sessions: Arc::new(Mutex::new(HashMap::new())),
        problems: Arc::new(Mutex::new(HashMap::new())),
        workers: Arc::new(Mutex::new(HashMap::new())),
        solves: Arc::new(Mutex::new(HashMap::new())),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        cancelled_solves: Arc::new(Mutex::new(HashMap::new())),
        metrics: Arc::new(Metrics::default()),
    }
}

fn binary_knapsack_problem() -> MipProblemSpec {
    MipProblemSpec {
        sense: "max".to_string(),
        c: vec![10.0, 40.0, 30.0, 50.0],
        a: vec![vec![5.0, 4.0, 6.0, 3.0]],
        b: vec![10.0],
        integer_vars: vec![true, true, true, true],
        ub: Some(vec![1.0, 1.0, 1.0, 1.0]),
        var_names: None,
        con_names: None,
    }
}

fn pure_lp_problem() -> MipProblemSpec {
    MipProblemSpec {
        sense: "max".to_string(),
        c: vec![3.0, 2.0],
        a: vec![vec![1.0, 1.0], vec![1.0, 0.0], vec![0.0, 1.0]],
        b: vec![4.0, 2.0, 3.0],
        integer_vars: vec![false, false],
        ub: None,
        var_names: Some(vec!["x0".to_string(), "x1".to_string()]),
        con_names: Some(vec![
            "shared".to_string(),
            "x0_cap".to_string(),
            "x1_cap".to_string(),
        ]),
    }
}

fn general_integer_problem() -> MipProblemSpec {
    MipProblemSpec {
        sense: "max".to_string(),
        c: vec![1.0, 1.0],
        a: vec![vec![1.0, 1.0]],
        b: vec![3.5],
        integer_vars: vec![true, true],
        ub: Some(vec![10.0, 10.0]),
        var_names: None,
        con_names: None,
    }
}

fn hundred_variable_two_hundred_constraint_mip() -> MipProblemSpec {
    let mut c = vec![0.0; 100];
    c[0] = 1.0;
    c[1] = 1.0;
    c[2] = 1.0;

    let mut a = Vec::with_capacity(200);
    let mut b = Vec::with_capacity(200);
    let mut con_names = Vec::with_capacity(200);

    let mut knapsack = vec![0.0; 100];
    knapsack[0] = 2.0;
    knapsack[1] = 2.0;
    knapsack[2] = 2.0;
    a.push(knapsack);
    b.push(5.0);
    con_names.push("three_item_capacity".to_string());

    for var in 0..99 {
        let mut row = vec![0.0; 100];
        row[var] = 1.0;
        a.push(row);
        b.push(1.0);
        con_names.push(format!("x{var}_upper"));
    }

    for var in 0..100 {
        let mut row = vec![0.0; 100];
        row[var] = -1.0;
        a.push(row);
        b.push(0.0);
        con_names.push(format!("x{var}_lower"));
    }

    assert_eq!(a.len(), 200);
    assert_eq!(b.len(), 200);
    assert_eq!(con_names.len(), 200);

    MipProblemSpec {
        sense: "max".to_string(),
        c,
        a,
        b,
        integer_vars: vec![true; 100],
        ub: Some(vec![1.0; 100]),
        var_names: Some((0..100).map(|index| format!("x{index}")).collect()),
        con_names: Some(con_names),
    }
}

fn hundred_variable_one_hundred_fifty_constraint_dispatch_mip() -> MipProblemSpec {
    let n = 100;
    let mut c = vec![0.0; n];
    for pair in 0..50 {
        c[pair * 2] = 1000.0 - pair as f64;
        c[pair * 2 + 1] = 25.0 + pair as f64;
    }

    let mut a = Vec::with_capacity(150);
    let mut b = Vec::with_capacity(150);
    let mut con_names = Vec::with_capacity(150);

    let fleet_budget = vec![2.0; n];
    a.push(fleet_budget);
    b.push(99.0);
    con_names.push("fleet_budget_allows_49_full_dispatches".to_string());

    for pair in 0..50 {
        let mut row = vec![0.0; n];
        row[pair * 2] = 1.0;
        row[pair * 2 + 1] = 1.0;
        a.push(row);
        b.push(1.0);
        con_names.push(format!("route_pair_{pair}_choose_at_most_one"));
    }

    for var in 0..99 {
        let mut row = vec![0.0; n];
        row[var] = 1.0;
        a.push(row);
        b.push(1.0);
        con_names.push(format!("dispatch_{var}_capacity"));
    }

    assert_eq!(a.len(), 150);
    assert_eq!(b.len(), 150);
    assert_eq!(con_names.len(), 150);

    MipProblemSpec {
        sense: "max".to_string(),
        c,
        a,
        b,
        integer_vars: vec![true; n],
        ub: Some(vec![1.0; n]),
        var_names: Some((0..n).map(|index| format!("dispatch_{index}")).collect()),
        con_names: Some(con_names),
    }
}

fn test_job(problem: MipProblemSpec) -> SubproblemJob {
    SubproblemJob {
        solve_id: "solve-test".to_string(),
        request_id: "request-test".to_string(),
        job_id: "job-test".to_string(),
        job_uuid: new_uuid_string(),
        problem_id: Some("33333333-3333-4333-8333-333333333333".to_string()),
        problem_stored: false,
        revision: 0,
        depth: 0,
        master_node: "master-test".to_string(),
        problem: Some(problem),
        extra_constraints: Vec::new(),
        avoid_worker_nodes: Vec::new(),
        options: SolveOptions {
            split_depth: Some(0),
            ..SolveOptions::default()
        },
        submitted_at_ms: now_ms(),
    }
}

fn assert_solution_certificate(
    problem: &MipProblemSpec,
    result: &SubproblemResult,
    tolerance: f64,
) {
    assert!(result.ok, "solver error: {:?}", result.error);
    assert_eq!(result.status, "optimal");
    assert_eq!(result.x.len(), problem.c.len());

    for (index, value) in result.x.iter().copied().enumerate() {
        assert!(value.is_finite(), "x[{index}] is not finite: {value}");
        assert!(value >= -tolerance, "x[{index}] is negative: {value}");
        if let Some(upper_bounds) = problem.ub.as_ref() {
            assert!(
                value <= upper_bounds[index] + tolerance,
                "x[{index}]={value} exceeds upper bound {}",
                upper_bounds[index]
            );
        }
        if problem.integer_vars[index] {
            assert!(
                (value - value.round()).abs() <= tolerance,
                "integer x[{index}] is fractional: {value}"
            );
        }
    }

    for (row_index, (row, rhs)) in problem.a.iter().zip(&problem.b).enumerate() {
        let activity = row
            .iter()
            .zip(&result.x)
            .map(|(coefficient, value)| coefficient * value)
            .sum::<f64>();
        assert!(
            activity <= rhs + tolerance,
            "row {row_index} is violated: activity {activity} > rhs {rhs}"
        );
    }

    let objective = problem
        .c
        .iter()
        .zip(&result.x)
        .map(|(coefficient, value)| coefficient * value)
        .sum::<f64>();
    assert!(
        result
            .z
            .is_some_and(|reported| (reported - objective).abs() <= tolerance),
        "reported objective {:?} does not certify computed objective {objective}",
        result.z
    );
}

async fn post_json(app: Router, path: &str, payload: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
        .await
        .unwrap();
    let value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

async fn delete_json(app: Router, path: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(Method::DELETE)
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
        .await
        .unwrap();
    let value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

async fn get_text(app: Router, path: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    (status, text)
}

async fn get_json(app: Router, path: &str) -> (StatusCode, Value) {
    let (status, text) = get_text(app, path).await;
    let value = serde_json::from_str(&text).unwrap();
    (status, value)
}

#[tokio::test]
async fn metadata_pages_and_api_docs_are_served() {
    let app = app_router(test_state(NodeRole::Master));

    let (status, home) = get_text(app.clone(), "/home").await;
    assert_eq!(status, StatusCode::OK);
    assert!(home.contains(SERVICE_NAME));
    assert!(home.contains(DD_REMOTE_MIP_SOLVER_STREAM_NAME));

    let (status, version_page) = get_text(app.clone(), "/version").await;
    assert_eq!(status, StatusCode::OK);
    assert!(version_page.contains("gitCommit"));
    assert!(version_page.contains(env!("CARGO_PKG_VERSION")));

    let (status, version_json) = get_json(app.clone(), "/version.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(version_json.get("service"), Some(&json!(SERVICE_NAME)));
    assert_eq!(
        version_json.pointer("/build/packageVersion"),
        Some(&json!(env!("CARGO_PKG_VERSION")))
    );

    let (status, docs_html) = get_text(app.clone(), "/docs/api").await;
    assert_eq!(status, StatusCode::OK);
    assert!(docs_html.contains("/mip-solver-cluster/nats"));
    assert!(docs_html.contains("/api/docs.json"));

    let (status, docs_json) = get_json(app, "/api/docs.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(docs_json.get("schema"), Some(&json!(API_DOCS_SCHEMA)));
    assert!(docs_json
        .get("endpoints")
        .and_then(Value::as_array)
        .is_some_and(|endpoints| endpoints.iter().any(|endpoint| {
            endpoint.get("path").and_then(Value::as_str) == Some("/mip-solver-cluster/nats")
        })));
}

#[test]
fn streaming_edits_update_live_problem_revision() {
    let commands = vec![
        json!({"op":"init","sense":"max","c":[3.0,2.0],"a":[[1.0,1.0]],"b":[4.0],"integerVars":[true,false]}),
        json!({"op":"set_rhs","index":0,"rhs":5.0}),
        json!({"op":"add_constraint","coefs":[2.0,1.0],"rhs":8.0}),
        json!({"op":"add_variable","c":4.0,"column":[0.0,1.0],"integer":true,"ub":3.0}),
        json!({"op":"set_variable","index":2,"c":5.0,"integer":true}),
        json!({"op":"remove_constraint","index":0}),
        json!({"op":"snapshot"}),
    ];
    let (problem, revision, frames) = parse_problem_from_commands(&commands).unwrap();
    assert_eq!(revision, 6);
    assert_eq!(problem.c, vec![3.0, 2.0, 5.0]);
    assert_eq!(problem.a, vec![vec![2.0, 1.0, 1.0]]);
    assert_eq!(problem.b, vec![8.0]);
    assert_eq!(problem.integer_vars, vec![true, false, true]);
    assert_eq!(problem.ub.as_ref().unwrap()[2], 3.0);
    assert!(frames
        .iter()
        .any(|frame| frame.get("event") == Some(&json!("model"))));
}

#[test]
fn streaming_edits_reject_non_numeric_scalars_instead_of_defaulting() {
    let commands = vec![
        json!({"op":"init","sense":"max","c":[1.0],"a":[[1.0]],"b":[1.0],"integerVars":[true]}),
        json!({"op":"set_rhs","index":0,"rhs":"not-a-number"}),
    ];

    let error = parse_problem_from_commands(&commands).unwrap_err();

    assert!(error.contains("rhs must be a finite number"));
}

#[test]
fn streaming_init_rejects_non_numeric_matrix_cells() {
    let commands = vec![json!({
        "op": "init",
        "sense": "max",
        "c": [1.0],
        "a": [["bad-cell"]],
        "b": [1.0],
        "integerVars": [true]
    })];

    let error = parse_problem_from_commands(&commands).unwrap_err();

    assert!(error.contains("a[0][0] must be a finite number"));
}

#[test]
fn frontier_builder_splits_fractional_lp_relaxation() {
    let problem = normalized_problem(MipProblemSpec {
        sense: "max".to_string(),
        c: vec![1.0],
        a: vec![vec![1.0]],
        b: vec![1.5],
        integer_vars: vec![true],
        ub: None,
        var_names: None,
        con_names: None,
    })
    .unwrap();
    let options = SolveOptions {
        split_depth: Some(1),
        ..SolveOptions::default()
    };
    let (jobs, warnings) = build_frontier_jobs(
        &problem,
        "solve-test",
        "request-test",
        "33333333-3333-4333-8333-333333333333",
        7,
        "master-a",
        &options,
        false,
    )
    .unwrap();
    assert!(warnings.is_empty());
    assert_eq!(jobs.len(), 1);
    assert!(jobs.iter().all(|job| job.revision == 7));
    assert!(jobs
        .iter()
        .all(|job| job.problem_id.as_deref() == Some("33333333-3333-4333-8333-333333333333")));
    assert!(jobs
        .iter()
        .all(|job| Uuid::parse_str(&job.job_uuid).is_ok()));
    assert!(jobs.iter().all(|job| job.extra_constraints.len() == 1));
    assert_eq!(jobs[0].extra_constraints[0].coefs, vec![1.0]);
    assert_eq!(jobs[0].extra_constraints[0].rhs, 1.0);
    assert_eq!(jobs[0].depth, 1);
}

#[test]
fn frontier_builder_can_emit_ref_only_jobs() {
    let problem = normalized_problem(binary_knapsack_problem()).unwrap();
    let options = SolveOptions {
        split_depth: Some(1),
        ..SolveOptions::default()
    };

    let (jobs, warnings) = build_frontier_jobs(
        &problem,
        "solve-ref-test",
        "request-ref-test",
        "77777777-7777-4777-8777-777777777777",
        3,
        "master-a",
        &options,
        true,
    )
    .unwrap();

    assert!(warnings.is_empty());
    assert!(!jobs.is_empty());
    assert!(jobs.iter().all(|job| job.problem_stored));
    assert!(jobs.iter().all(|job| job.problem.is_none()));
    let payload = json_value(&jobs[0]);
    assert!(payload.get("problem").is_none());
    assert_eq!(payload.get("problemStored"), Some(&json!(true)));
}

#[tokio::test]
async fn hydrate_subproblem_job_loads_problem_from_cache() {
    let state = test_state(NodeRole::Slave);
    let problem = normalized_problem(binary_knapsack_problem()).unwrap();
    let problem_id = "88888888-8888-4888-8888-888888888888";
    remember_problem_model(&state, problem_id, 4, problem.clone());
    let mut job = test_job(problem.clone()).without_problem_payload();
    job.problem_id = Some(problem_id.to_string());
    job.revision = 4;

    hydrate_subproblem_job(&state, &mut job).await.unwrap();

    assert_eq!(
        job.problem.as_ref().map(|problem| &problem.c),
        Some(&problem.c)
    );
}

#[tokio::test]
async fn hydrate_subproblem_job_uses_stored_model_over_embedded_payload() {
    let state = test_state(NodeRole::Slave);
    let stored = normalized_problem(binary_knapsack_problem()).unwrap();
    let problem_id = "8aaaaaaa-8888-4888-8888-888888888888";
    remember_problem_model(&state, problem_id, 0, stored.clone());
    let mut embedded = stored.clone();
    embedded.c[0] = 999.0;
    let mut job = test_job(embedded);
    job.problem_id = Some(problem_id.to_string());
    job.problem_stored = true;

    hydrate_subproblem_job(&state, &mut job).await.unwrap();

    assert_eq!(
        job.problem.as_ref().map(|problem| &problem.c),
        Some(&stored.c)
    );
}

#[tokio::test]
async fn store_problem_model_rejects_same_id_with_different_model() {
    let state = test_state(NodeRole::Master);
    let problem_id = "8bbbbbbb-8888-4888-8888-888888888888";
    let first = normalized_problem(binary_knapsack_problem()).unwrap();
    let mut second = first.clone();
    second.c[0] = 11.0;

    let created = store_problem_model(&state, problem_id, 0, &first)
        .await
        .unwrap();
    let existing = store_problem_model(&state, problem_id, 0, &first)
        .await
        .unwrap();
    let conflict = store_problem_model(&state, problem_id, 0, &second)
        .await
        .unwrap_err();

    assert_eq!(created, ProblemStoreStatus::Created);
    assert_eq!(existing, ProblemStoreStatus::Existing);
    assert!(is_problem_model_conflict(&conflict));
}

#[test]
fn validate_subproblem_job_payload_enforces_reference_contract() {
    let problem = normalized_problem(binary_knapsack_problem()).unwrap();
    let mut ref_job = test_job(problem.clone()).without_problem_payload();
    ref_job.problem_id = Some("8ccccccc-8888-4888-8888-888888888888".to_string());
    assert!(validate_subproblem_job_payload(&ref_job).is_ok());

    let mut leaked_ref_job = ref_job.clone();
    leaked_ref_job.problem = Some(problem);
    assert!(validate_subproblem_job_payload(&leaked_ref_job)
        .unwrap_err()
        .contains("embedded problem"));

    let mut missing_embedded = test_job(binary_knapsack_problem());
    missing_embedded.problem = None;
    assert!(validate_subproblem_job_payload(&missing_embedded)
        .unwrap_err()
        .contains("no stored problem reference"));
}

#[test]
fn solve_options_merge_request_values_over_runtime_defaults() {
    let defaults = SolveOptions {
        max_nodes: Some(111),
        max_ticks: Some(222),
        lp_max_iters: Some(333),
        lp_algorithm: Some("internal-simplex".to_string()),
        int_tol: Some(1e-4),
        split_depth: Some(2),
        max_subproblems: Some(12),
        max_job_retries: Some(4),
        timeout_ms: Some(444),
        emit_trace: Some(false),
        verify_external: Some(false),
        external_verification_method: Some("highs".to_string()),
        external_verification_tolerance: Some(1e-5),
    };
    let input = SolveOptions {
        max_nodes: Some(999),
        max_ticks: None,
        lp_max_iters: Some(777),
        lp_algorithm: Some("internal-ipm".to_string()),
        int_tol: None,
        split_depth: Some(5),
        max_subproblems: Some(3),
        max_job_retries: Some(9),
        timeout_ms: None,
        emit_trace: Some(true),
        verify_external: Some(true),
        external_verification_method: Some("highs-ds".to_string()),
        external_verification_tolerance: None,
    };

    let merged = SolveOptions::merged_with_defaults(Some(input), defaults);

    assert_eq!(merged.max_nodes, Some(999));
    assert_eq!(merged.max_ticks, Some(222));
    assert_eq!(merged.lp_max_iters, Some(777));
    assert_eq!(merged.lp_algorithm.as_deref(), Some("internal-ipm"));
    assert_eq!(merged.int_tol, Some(1e-4));
    assert_eq!(merged.split_depth, Some(5));
    assert_eq!(merged.max_subproblems, Some(3));
    assert_eq!(merged.max_job_retries, Some(9));
    assert_eq!(merged.timeout_ms, Some(444));
    assert_eq!(merged.emit_trace, Some(true));
    assert_eq!(merged.verify_external, Some(true));
    assert_eq!(
        merged.external_verification_method.as_deref(),
        Some("highs-ds")
    );
    assert_eq!(merged.external_verification_tolerance, Some(1e-5));
}

#[cfg(not(feature = "external-solver-verification"))]
#[test]
fn external_verification_request_reports_unavailable_without_feature() {
    let problem = binary_knapsack_problem();
    let state = test_state(NodeRole::Master);
    let optimal = SubproblemResult {
        solve_id: "solve-verify-test".to_string(),
        request_id: "request-verify-test".to_string(),
        job_id: "job-verify".to_string(),
        job_uuid: new_uuid_string(),
        problem_id: Some("77777777-7777-4777-8777-777777777777".to_string()),
        revision: 0,
        worker_node: "worker-verify".to_string(),
        ok: true,
        status: "optimal".to_string(),
        z: Some(90.0),
        x: vec![0.0, 1.0, 0.0, 1.0],
        best_bound: Some(90.0),
        gap: Some(0.0),
        lp: None,
        child_jobs: Vec::new(),
        nodes_explored: 1,
        lp_solves: 1,
        elapsed_ms: 1.0,
        accelerator: AcceleratorReport::default(),
        error: None,
        finished_at_ms: now_ms(),
    };
    let options = SolveOptions {
        verify_external: Some(true),
        external_verification_method: Some("highs".to_string()),
        ..SolveOptions::default()
    };

    let response = aggregate_results(
        "solve-verify-test".to_string(),
        "request-verify-test".to_string(),
        None,
        0,
        &problem,
        &options,
        1,
        1,
        0,
        0,
        vec![optimal],
        false,
        true,
        &state,
        Vec::new(),
    );

    let verification = response.external_verification.expect("verification report");
    assert_eq!(verification.status, "unavailable");
    assert!(!verification.enabled);
    assert!(response
        .warnings
        .iter()
        .any(|warning| warning.contains("feature is not enabled")));
}

#[test]
fn frontier_builder_caps_presplit_subproblem_count() {
    let problem = normalized_problem(MipProblemSpec {
        sense: "max".to_string(),
        c: vec![1.0],
        a: vec![vec![1.0]],
        b: vec![1.5],
        integer_vars: vec![true],
        ub: None,
        var_names: None,
        con_names: None,
    })
    .unwrap();
    let options = SolveOptions {
        split_depth: Some(4),
        max_subproblems: Some(1),
        ..SolveOptions::default()
    };

    let (jobs, warnings) = build_frontier_jobs(
        &problem,
        "solve-test",
        "request-test",
        "44444444-4444-4444-8444-444444444444",
        7,
        "master-a",
        &options,
        false,
    )
    .unwrap();

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].depth, 0);
    assert!(jobs[0].extra_constraints.is_empty());
    assert!(warnings
        .iter()
        .any(|warning| warning.contains("frontier split capped at 1 subproblems")));
}

#[test]
fn branch_constraints_extend_named_constraint_metadata() {
    let problem = normalized_problem(MipProblemSpec {
        sense: "max".to_string(),
        c: vec![1.0],
        a: vec![vec![1.0]],
        b: vec![1.5],
        integer_vars: vec![true],
        ub: Some(vec![3.0]),
        var_names: Some(vec!["x0".to_string()]),
        con_names: Some(vec!["capacity".to_string()]),
    })
    .unwrap();
    let extra = vec![BranchConstraint {
        coefs: vec![1.0],
        rhs: 1.0,
        name: "branch_d0_x0_le_1".to_string(),
    }];

    let ipmip = to_ipmip_problem(&problem, &extra).unwrap();
    let lp = to_lp_problem(&problem, &extra).unwrap();

    assert_eq!(
        ipmip.con_names,
        Some(vec![
            "capacity".to_string(),
            "branch_d0_x0_le_1".to_string()
        ])
    );
    assert_eq!(
        lp.con_names,
        Some(vec![
            "capacity".to_string(),
            "branch_d0_x0_le_1".to_string()
        ])
    );
}

#[test]
fn branch_constraints_are_validated_before_lp_and_mip_conversion() {
    let lp_problem = normalized_problem(pure_lp_problem()).unwrap();
    let bad_width = BranchConstraint {
        coefs: vec![1.0],
        rhs: 1.0,
        name: "bad_width".to_string(),
    };
    let err = to_lp_problem(&lp_problem, &[bad_width]).unwrap_err();
    assert!(err.contains("bad_width has length 1, expected 2"));

    let mip_problem = normalized_problem(binary_knapsack_problem()).unwrap();
    let bad_coef = BranchConstraint {
        coefs: vec![1.0, f64::NAN, 0.0, 0.0],
        rhs: 1.0,
        name: "bad_coef".to_string(),
    };
    let err = to_ipmip_problem(&mip_problem, &[bad_coef]).unwrap_err();
    assert!(err.contains("bad_coef contains a non-finite coefficient"));

    let bad_rhs = BranchConstraint {
        coefs: vec![1.0, 0.0, 0.0, 0.0],
        rhs: f64::INFINITY,
        name: "bad_rhs".to_string(),
    };
    let err = preprocess_bounds_with_mode(&mip_problem, &[bad_rhs], "off").unwrap_err();
    assert!(err.contains("bad_rhs rhs must be finite"));
}

#[test]
fn bound_preprocess_prunes_rows_impossible_under_lower_bounds() {
    let problem = MipProblemSpec {
        sense: "max".to_string(),
        c: vec![1.0],
        a: vec![vec![1.0]],
        b: vec![-1.0],
        integer_vars: vec![false],
        ub: Some(vec![10.0]),
        var_names: None,
        con_names: None,
    };

    let report = preprocess_bounds_with_mode(&problem, &[], "off").unwrap();

    assert!(report
        .infeasible_reason
        .as_deref()
        .unwrap_or_default()
        .contains("bound preprocessing proved row 0 infeasible"));
    assert_eq!(report.accelerator.backend, "in-house-cpu");
    assert!(!report.accelerator.used_gpu);
}

#[test]
fn bound_preprocess_reports_rows_always_satisfied_by_bounds() {
    let problem = MipProblemSpec {
        sense: "max".to_string(),
        c: vec![1.0],
        a: vec![vec![1.0]],
        b: vec![5.0],
        integer_vars: vec![false],
        ub: Some(vec![3.0]),
        var_names: None,
        con_names: None,
    };

    let report = preprocess_bounds_with_mode(&problem, &[], "off").unwrap();

    assert!(report.infeasible_reason.is_none());
    assert!(report
        .accelerator
        .notes
        .iter()
        .any(|note| note.contains("rows always satisfied by variable bounds")));
}

#[test]
fn solve_options_force_in_house_lp_and_mip_engines() {
    let options = SolveOptions::default().to_ipmip_options().unwrap();

    assert_eq!(options.allow_external_solvers, Some(false));
    assert!(matches!(
        options.lp_algorithm,
        Some(LpRelaxationAlgorithm::Concrete(
            ConcreteLpRelaxationAlgorithm::InternalSimplex
        ))
    ));
    assert!(matches!(
        options.branch_rule,
        Some(BranchRule::MostFractional)
    ));
}

#[test]
fn solve_options_select_internal_ipm_and_reject_external_or_unknown_algorithms() {
    for alias in ["internal-ipm", "internal_interior_point", "ipm"] {
        let options = SolveOptions {
            lp_algorithm: Some(alias.to_string()),
            ..SolveOptions::default()
        }
        .to_ipmip_options()
        .unwrap();
        assert!(matches!(
            options.lp_algorithm,
            Some(LpRelaxationAlgorithm::Concrete(
                ConcreteLpRelaxationAlgorithm::InternalInteriorPoint
            ))
        ));
        assert_eq!(options.allow_external_solvers, Some(false));
    }

    for rejected in ["external-highs", "auto", "mystery"] {
        let error = SolveOptions {
            lp_algorithm: Some(rejected.to_string()),
            ..SolveOptions::default()
        }
        .to_ipmip_options()
        .unwrap_err();
        assert!(error.contains("unsupported lpAlgorithm"), "{error}");
    }
}

#[tokio::test]
async fn solve_http_rejects_unsupported_lp_algorithm_before_tracking_work() {
    let state = test_state(NodeRole::Master);
    let app = app_router(state.clone());
    let mut payload = soccer_formation::model_document(false);
    payload["options"]["lpAlgorithm"] = json!("external-highs");

    let (status, response) = post_json(app, "/solve", payload).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response.get("ok"), Some(&json!(false)));
    assert!(response
        .get("error")
        .and_then(Value::as_str)
        .is_some_and(|error| error.contains("unsupported lpAlgorithm")));
    assert!(state.solves.lock().unwrap().is_empty());
}

#[test]
fn nats_subjects_are_generated_mip_solver_namespace() {
    assert!(dd_nats_subject_defs::NATS_CONTRACT_FINGERPRINT.starts_with("sha256:"));
    assert_eq!(dd_nats_subject_defs::NATS_CONTRACT_FINGERPRINT.len(), 71);
    let subjects = [
        MIP_SOLVER_JOBS_SUBJECT,
        MIP_SOLVER_RESULTS_SUBJECT,
        MIP_SOLVER_CONTROL_SUBJECT,
        MIP_SOLVER_EVENTS_SUBJECT,
    ];
    for subject in subjects {
        assert!(subject.starts_with("dd.remote.mip_solver."));
        assert!(DD_REMOTE_MIP_SOLVER_STREAM_SUBJECTS.contains(&subject));
    }

    let mut unique = subjects.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), subjects.len());
    assert_eq!(DD_REMOTE_MIP_SOLVER_STREAM_NAME, "DD_REMOTE_MIP_SOLVER");
    assert_eq!(
        MIP_SOLVER_WORKERS_QUEUE_GROUP,
        "dd-in-house-mip-solver-node-workers"
    );
}

#[test]
fn deployment_bootstrap_is_self_contained_except_for_des() {
    for manifest in [
        include_str!("../k8s/master-deployment.yaml"),
        include_str!("../k8s/slave-deployment.yaml"),
    ] {
        assert!(manifest.contains(
            "submodule update --init --depth 1 remote/submodules/discrete-event-system.rs"
        ));
        assert!(!manifest.contains("remote/libs"));
        assert!(manifest.contains("memory: 64Mi"));
        assert!(manifest.contains("failureThreshold: 360"));
    }

    let dockerfile = include_str!("../Dockerfile");
    assert!(!dockerfile.contains("remote/libs"));
    assert!(dockerfile.contains("remote/deployments/mip-solver-node.rs"));
    assert!(dockerfile.contains("remote/submodules/discrete-event-system.rs"));

    for manifest in [
        include_str!("../Cargo.toml"),
        include_str!("../local/Cargo.toml"),
    ] {
        for dependency in [
            "vendor/nats-subject-defs",
            "vendor/pg-defs",
            "vendor/redis-interfaces",
        ] {
            assert!(manifest.contains(dependency), "missing {dependency}");
        }
    }
}

#[test]
fn persistence_contract_uses_generated_mip_pg_defs_and_redis_namespace() {
    let contract = persistence_contract();

    assert_eq!(contract.postgres.session_table, MIP_SOLVER_SESSIONS_TABLE);
    assert_eq!(contract.postgres.solve_table, MIP_SOLVER_SOLVES_TABLE);
    assert_eq!(contract.postgres.job_table, MIP_SOLVER_JOBS_TABLE);
    assert_eq!(contract.postgres.event_table, MIP_SOLVER_EVENTS_TABLE);
    assert!(contract
        .postgres
        .journal_kinds
        .contains(&"mip-solver.subproblem-split"));
    assert_eq!(
        mip_solver_solve_snapshot_key("dd:mip-solver", "solve-a"),
        "dd:mip-solver:solve:solve-a:snapshot"
    );
    assert_eq!(
        mip_solver_session_model_key("dd:mip-solver", "session-a"),
        "dd:mip-solver:session:session-a:model"
    );
    assert!(contract
        .redis
        .generated_mutex_key
        .contains("dd:container-pool:affinity:mip-solver"));
}

#[test]
fn coordination_backend_parser_supports_auto_and_explicit_both() {
    assert_eq!(
        parse_coordination_backends("auto", true, true),
        vec![CoordinationBackend::Redis, CoordinationBackend::LiveMutex]
    );
    assert_eq!(
        parse_coordination_backends("redis,live-mutex", true, true),
        vec![CoordinationBackend::Redis, CoordinationBackend::LiveMutex]
    );
    assert_eq!(
        parse_coordination_backends("both", true, false),
        vec![CoordinationBackend::Redis]
    );
    assert!(parse_coordination_backends("none", true, true).is_empty());
}

#[test]
fn live_mutex_http_endpoint_parser_preserves_base_path() {
    let endpoint =
        parse_http_endpoint("http://dd-rust-network-mutex.default.svc.cluster.local:6971/api")
            .unwrap();

    assert_eq!(
        endpoint.addr,
        "dd-rust-network-mutex.default.svc.cluster.local:6971"
    );
    assert_eq!(
        endpoint.host_header,
        "dd-rust-network-mutex.default.svc.cluster.local:6971"
    );
    assert_eq!(http_path(&endpoint.path_prefix, "/v1/lock"), "/api/v1/lock");
    assert!(parse_http_endpoint("https://example.com").is_err());
    assert!(parse_http_endpoint("http://example.com\r\nX-Bad: yes").is_err());
}

#[test]
fn chunked_decoder_accepts_chunk_extensions() {
    let decoded = decode_chunked_body(b"4;foo=bar\r\nrust\r\n0\r\n\r\n").unwrap();

    assert_eq!(decoded, b"rust");
}

#[tokio::test]
async fn live_mutex_rejects_crlf_auth_token_before_network() {
    let config = LiveMutexConfig {
        base_url: "http://127.0.0.1:1".to_string(),
        auth_token: Some("bad\r\nAuthorization: nope".to_string()),
        request_timeout_ms: 1,
        max_response_bytes: 1024,
    };

    let error = live_mutex_post_json(&config, "/v1/lock", json!({}))
        .await
        .unwrap_err();

    assert!(error.contains("CRLF"));
}

#[tokio::test]
async fn live_mutex_http_client_stops_after_content_length_without_eof() {
    let (mut client, mut server_stream) = tokio::io::duplex(2048);
    let server = tokio::spawn(async move {
        let body = br#"{"acquired":true,"lockUuid":"lock-a"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        server_stream.write_all(response.as_bytes()).await.unwrap();
        server_stream.write_all(body).await.unwrap();
        tokio::time::sleep(Duration::from_millis(750)).await;
    });

    let started = Instant::now();
    let response = read_http_response(&mut client, 1024).await.unwrap();
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let value: Value = serde_json::from_slice(&response[header_end + 4..]).unwrap();

    assert_eq!(value["lockUuid"], "lock-a");
    assert!(started.elapsed() < Duration::from_millis(500));
    server.await.unwrap();
}

#[test]
fn jetstream_stream_config_contains_generated_subjects() {
    let config = mip_stream_config();

    assert_eq!(config.name, DD_REMOTE_MIP_SOLVER_STREAM_NAME);
    for subject in DD_REMOTE_MIP_SOLVER_STREAM_SUBJECTS {
        assert!(config
            .subjects
            .iter()
            .any(|configured| configured == subject));
    }
    assert_eq!(
        config.subjects.len(),
        DD_REMOTE_MIP_SOLVER_STREAM_SUBJECTS.len()
    );
}

#[test]
fn result_consumer_config_reads_persisted_results_from_job_sequence() {
    let name = result_consumer_name("solve-test");
    let config = result_consumer_config(&name, MIP_SOLVER_RESULTS_SUBJECT, 42);

    assert_eq!(config.name.as_deref(), Some(name.as_str()));
    assert_eq!(config.filter_subject, MIP_SOLVER_RESULTS_SUBJECT);
    assert_eq!(config.durable_name, None);
    assert_eq!(config.max_deliver, 1);
    assert_eq!(config.inactive_threshold, Duration::from_secs(120));
    assert!(matches!(
        config.deliver_policy,
        async_nats::jetstream::consumer::DeliverPolicy::ByStartSequence { start_sequence: 42 }
    ));
}

#[test]
fn worker_consumer_config_uses_runtime_jobs_subject_and_delivery_limits() {
    let config = worker_consumer_config(
        MIP_SOLVER_WORKERS_QUEUE_GROUP,
        "dd.remote.mip_solver.jobs.custom",
        Duration::from_secs(900),
        64,
        7,
    );

    assert_eq!(
        config.durable_name.as_deref(),
        Some(MIP_SOLVER_WORKERS_QUEUE_GROUP)
    );
    assert_eq!(config.filter_subject, "dd.remote.mip_solver.jobs.custom");
    assert_eq!(config.ack_wait, Duration::from_secs(900));
    assert_eq!(config.max_ack_pending, 64);
    assert_eq!(config.max_deliver, 7);
}

#[tokio::test]
async fn nats_status_route_reports_master_slave_wiring() {
    let state = test_state(NodeRole::Master);
    let seen_at = now_ms();
    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-route-test",
            "role":"slave",
            "commandName":"worker-ready",
            "payload":{
                "consumer": MIP_SOLVER_WORKERS_QUEUE_GROUP,
                "jobsSubject": MIP_SOLVER_JOBS_SUBJECT,
                "resultsSubject": MIP_SOLVER_RESULTS_SUBJECT
            },
            "timeMs": seen_at
        }),
    )
    .unwrap();
    let app = app_router(state);

    let (status, body) = get_json(app, "/mip-solver-cluster/nats").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("ok"), Some(&json!(false)));
    assert_eq!(body.get("connected"), Some(&json!(false)));
    assert_eq!(
        body.pointer("/stream/name"),
        Some(&json!(DD_REMOTE_MIP_SOLVER_STREAM_NAME))
    );
    assert_eq!(
        body.pointer("/subjects/jobs"),
        Some(&json!(MIP_SOLVER_JOBS_SUBJECT))
    );
    assert_eq!(
        body.get("workerConsumer"),
        Some(&json!(MIP_SOLVER_WORKERS_QUEUE_GROUP))
    );
    assert_eq!(body.get("workersKnown"), Some(&json!(1)));
    assert_eq!(
        body.pointer("/workers/0/nodeId"),
        Some(&json!("worker-route-test"))
    );
}

#[test]
fn worker_control_frames_update_master_registry() {
    let state = test_state(NodeRole::Master);

    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-a",
            "role":"slave",
            "commandName":"worker-ready",
            "payload":{
                "consumer":"dd-in-house-mip-solver-node-workers",
                "jobsSubject": MIP_SOLVER_JOBS_SUBJECT,
                "resultsSubject": MIP_SOLVER_RESULTS_SUBJECT
            },
            "timeMs": 1000
        }),
    )
    .unwrap();
    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-a",
            "role":"slave",
            "commandName":"request-work",
            "payload":{"consumer":"dd-in-house-mip-solver-node-workers"},
            "timeMs": 1100
        }),
    )
    .unwrap();
    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-a",
            "role":"slave",
            "commandName":"worker-completed",
            "payload":{
                "consumer":"dd-in-house-mip-solver-node-workers",
                "jobId":"solve-test-0",
                "solveId":"solve-test",
                "status":"optimal",
                "resultsSubject": MIP_SOLVER_RESULTS_SUBJECT
            },
            "timeMs": 1200
        }),
    )
    .unwrap();

    let workers = state.workers.lock().expect("workers mutex poisoned");
    let worker = workers.get("worker-a").expect("worker-a");
    assert_eq!(worker.ready_at_ms, Some(1000));
    assert_eq!(worker.last_seen_ms, 1200);
    assert_eq!(worker.request_count, 1);
    assert_eq!(worker.completed_count, 1);
    assert_eq!(worker.last_command, "worker-completed");
    assert_eq!(worker.last_job_id.as_deref(), Some("solve-test-0"));
    assert_eq!(worker.last_solve_id.as_deref(), Some("solve-test"));
    assert_eq!(worker.last_status.as_deref(), Some("optimal"));
}

#[test]
fn worker_progress_updates_active_job_and_stale_detection() {
    let state = test_state(NodeRole::Master);
    let job = test_job(binary_knapsack_problem());
    let problem_id = job.problem_id.clone().unwrap();
    track_solve_started(
        &state,
        &job.solve_id,
        &job.request_id,
        &problem_id,
        job.revision,
        1,
        true,
    )
    .unwrap();
    track_job_submitted(&state, &job);

    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-progress-a",
            "role":"slave",
            "commandName":"request-work",
            "payload":{
                "consumer":"dd-in-house-mip-solver-node-workers",
                "jobId": &job.job_id,
                "jobUuid": &job.job_uuid,
                "solveId": &job.solve_id,
                "problemId": &problem_id,
                "startedAtMs": 1000
            },
            "timeMs": 1000
        }),
    )
    .unwrap();
    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-progress-a",
            "role":"slave",
            "commandName":"worker-progress",
            "payload":{
                "consumer":"dd-in-house-mip-solver-node-workers",
                "jobId": &job.job_id,
                "jobUuid": &job.job_uuid,
                "solveId": &job.solve_id,
                "problemId": &problem_id,
                "startedAtMs": 1000
            },
            "timeMs": 26_000
        }),
    )
    .unwrap();

    let workers = state.workers.lock().expect("workers mutex poisoned");
    let worker = workers.get("worker-progress-a").expect("worker");
    assert_eq!(worker.request_count, 1);
    assert_eq!(worker.last_job_id.as_deref(), Some(job.job_id.as_str()));
    assert_eq!(worker.last_solve_id.as_deref(), Some(job.solve_id.as_str()));
    assert_eq!(worker.last_status.as_deref(), Some("running"));
    assert!(worker.active_jobs.contains_key(&job.job_uuid));
    drop(workers);

    let solves = state.solves.lock().expect("solves mutex poisoned");
    let tracked = solves
        .get(&job.solve_id)
        .and_then(|solve| solve.jobs.get(&job.job_id))
        .expect("tracked job");
    assert_eq!(tracked.status, "running");
    assert_eq!(tracked.worker_node.as_deref(), Some("worker-progress-a"));
    assert_eq!(tracked.last_heartbeat_ms, Some(26_000));
    drop(solves);

    let active = HashSet::from([job.job_id.clone()]);
    let completed = HashSet::new();
    assert!(stale_worker_jobs(
        &state,
        &job.solve_id,
        &active,
        &completed,
        Duration::from_secs(100),
        126_000,
    )
    .is_empty());
    let stale = stale_worker_jobs(
        &state,
        &job.solve_id,
        &active,
        &completed,
        Duration::from_secs(100),
        126_002,
    );
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].job_id, job.job_id);
    assert_eq!(stale[0].job_uuid.as_deref(), Some(job.job_uuid.as_str()));
    assert_eq!(stale[0].worker_node, "worker-progress-a");
}

#[test]
fn solve_subproblem_solves_binary_mip_with_in_house_solver() {
    let result = solve_subproblem(
        test_job(binary_knapsack_problem()),
        "worker-test".to_string(),
    );

    assert!(result.ok, "subproblem error: {:?}", result.error);
    assert_eq!(result.status, "optimal");
    assert_eq!(result.z, Some(90.0));
    assert_eq!(result.x.len(), 4);
    assert!((result.x[1] - 1.0).abs() < 1e-6);
    assert!((result.x[3] - 1.0).abs() < 1e-6);
}

#[test]
fn solve_subproblem_can_split_fractional_delegated_subtree() {
    let problem = normalized_problem(MipProblemSpec {
        sense: "max".to_string(),
        c: vec![1.0],
        a: vec![vec![1.0]],
        b: vec![1.5],
        integer_vars: vec![true],
        ub: None,
        var_names: None,
        con_names: None,
    })
    .unwrap();
    let mut job = test_job(problem);
    job.options.split_depth = Some(1);

    let result = solve_subproblem(job, "worker-test".to_string());

    assert!(!result.ok);
    assert_eq!(result.status, "split");
    assert_eq!(result.child_jobs.len(), 2);
    assert_eq!(result.lp_solves, 1);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("split"));
    assert!(result
        .child_jobs
        .iter()
        .all(|child| child.depth == 1 && child.extra_constraints.len() == 1));
}

#[test]
fn solve_subproblem_solves_lp_with_in_house_solver() {
    let result = solve_subproblem(test_job(pure_lp_problem()), "worker-test".to_string());

    assert!(result.ok, "subproblem error: {:?}", result.error);
    assert_eq!(result.status, "optimal");
    assert_eq!(result.x.len(), 2);
    assert!(result.z.is_some_and(|z| (z - 10.0).abs() < 1e-6));
    assert!((result.x[0] - 2.0).abs() < 1e-6);
    assert!((result.x[1] - 2.0).abs() < 1e-6);
    let lp = result.lp.as_ref().expect("LP solve report");
    assert_eq!(
        lp.dual.row_names.as_ref().unwrap(),
        &vec![
            "shared".to_string(),
            "x0_cap".to_string(),
            "x1_cap".to_string()
        ]
    );
    let dual = lp.dual.inequality.as_ref().expect("row duals");
    assert_eq!(dual.len(), 3);
    assert!((dual[0] - 2.0).abs() < 1e-6, "dual = {dual:?}");
    assert!((dual[1] - 1.0).abs() < 1e-6, "dual = {dual:?}");
    assert!(dual[2].abs() < 1e-6, "dual = {dual:?}");
    assert_eq!(
        lp.basis.variables.as_ref().unwrap(),
        &vec!["basic".to_string(), "basic".to_string()]
    );
}

#[test]
fn solve_subproblem_solves_lp_with_internal_ipm() {
    let mut job = test_job(pure_lp_problem());
    job.options.lp_algorithm = Some("internal-ipm".to_string());

    let result = solve_subproblem(job, "worker-test".to_string());

    assert!(result.ok, "subproblem error: {:?}", result.error);
    assert_eq!(result.status, "optimal");
    assert!(result.z.is_some_and(|z| (z - 10.0).abs() < 1e-5));
    assert_eq!(
        result.lp.as_ref().map(|report| report.solver.as_str()),
        Some("internal-ipm")
    );
}

#[test]
fn soccer_formation_mip_and_lp_ipm_match_the_expected_optimum() {
    let mip_problem = soccer_formation::problem(false);
    let lp_problem = soccer_formation::problem(true);
    let mip = solve_subproblem(
        test_job(mip_problem.clone()),
        "worker-soccer-mip".to_string(),
    );
    let mut mip_ipm_job = test_job(mip_problem.clone());
    mip_ipm_job.options.lp_algorithm = Some("internal-ipm".to_string());
    let mip_ipm = solve_subproblem(mip_ipm_job, "worker-soccer-mip-ipm".to_string());
    let mut lp_job = test_job(lp_problem.clone());
    lp_job.options.lp_algorithm = Some("internal-ipm".to_string());
    let lp = solve_subproblem(lp_job, "worker-soccer-lp".to_string());

    assert_solution_certificate(&mip_problem, &mip, 1e-6);
    assert_solution_certificate(&mip_problem, &mip_ipm, 1e-4);
    assert_solution_certificate(&lp_problem, &lp, 1e-4);
    let expected = soccer_formation::expected_objective();
    assert!(mip.z.is_some_and(|z| (z - expected).abs() < 1e-6));
    assert!(mip_ipm.z.is_some_and(|z| (z - expected).abs() < 1e-4));
    assert!(
        lp.z.is_some_and(|z| (z - expected).abs() < 1e-4),
        "LP/IPM objective {:?}, expected {expected}; report {:?}",
        lp.z,
        lp.lp
    );
    assert_eq!(
        soccer_formation::decode_assignment(&mip.x).unwrap(),
        soccer_formation::decode_assignment(&mip_ipm.x).unwrap()
    );
    assert_eq!(
        soccer_formation::decode_assignment(&mip.x).unwrap(),
        soccer_formation::decode_assignment(&lp.x).unwrap()
    );
    assert!(lp
        .x
        .iter()
        .all(|value| { value.abs() < 1e-5 || (*value - 1.0).abs() < 1e-5 }));
}

#[test]
fn soccer_formation_rejects_forcing_one_player_into_two_slots() {
    let problem = soccer_formation::problem(false);
    let names = problem.var_names.as_ref().unwrap();
    let forced_variables = [
        "assign_iker_lane_to_left_back",
        "assign_iker_lane_to_right_back",
    ]
    .map(|variable_name| {
        let index = names
            .iter()
            .position(|name| name == variable_name)
            .unwrap_or_else(|| panic!("missing soccer variable {variable_name}"));
        (variable_name, index)
    });
    let variable_count = names.len();
    let mut job = test_job(problem);

    for (variable_name, variable_index) in forced_variables {
        let mut coefs = vec![0.0; variable_count];
        coefs[variable_index] = -1.0;
        job.extra_constraints.push(BranchConstraint {
            coefs,
            rhs: -1.0,
            name: format!("force_{variable_name}"),
        });
    }

    let result = solve_subproblem(job, "worker-soccer-infeasible".to_string());

    assert!(!result.ok);
    assert_eq!(result.status, "infeasible");
    assert!(result.z.is_none());
    assert!(result.x.is_empty());
}

#[tokio::test]
async fn soccer_formation_models_are_served_with_matching_matrices() {
    let app = app_router(test_state(NodeRole::Master));
    let (mip_status, mip) = get_json(app.clone(), "/model/soccer-formation").await;
    let (lp_status, lp) = get_json(app, "/model/soccer-formation-lp").await;

    assert_eq!(mip_status, StatusCode::OK);
    assert_eq!(lp_status, StatusCode::OK);
    assert_eq!(mip.pointer("/scenario/pitchGrid/lanes"), Some(&json!(12)));
    assert_eq!(mip.pointer("/scenario/pitchGrid/rows"), Some(&json!(24)));
    assert_eq!(mip.pointer("/scenario/decisionVariables"), Some(&json!(46)));
    assert_eq!(mip.pointer("/scenario/constraints"), Some(&json!(37)));
    assert_eq!(mip.pointer("/problem/c"), lp.pointer("/problem/c"));
    assert_eq!(mip.pointer("/problem/a"), lp.pointer("/problem/a"));
    assert_eq!(mip.pointer("/problem/b"), lp.pointer("/problem/b"));
    assert_eq!(
        lp.pointer("/options/lpAlgorithm"),
        Some(&json!("internal-ipm"))
    );
}

#[tokio::test]
async fn published_soccer_models_solve_end_to_end_over_http() {
    let app = app_router(test_state(NodeRole::Master));
    let expected = soccer_formation::expected_objective();
    let mut decoded_assignments = Vec::new();

    for relaxed in [false, true] {
        let payload = soccer_formation::model_document(relaxed);
        let (status, body) = post_json(app.clone(), "/solve", payload).await;

        assert_eq!(status, StatusCode::OK, "response: {body}");
        assert_eq!(body.get("ok"), Some(&json!(true)));
        assert_eq!(body.get("status"), Some(&json!("optimal")));
        assert!(
            body.get("z")
                .and_then(Value::as_f64)
                .is_some_and(|objective| (objective - expected).abs() <= 1e-4),
            "response: {body}"
        );
        assert_eq!(body.get("distributed"), Some(&json!(false)));
        let x = body
            .get("x")
            .and_then(Value::as_array)
            .expect("solution vector")
            .iter()
            .map(|value| value.as_f64().expect("numeric solution value"))
            .collect::<Vec<_>>();
        decoded_assignments.push(soccer_formation::decode_assignment(&x).unwrap());
    }

    assert_eq!(decoded_assignments[0], decoded_assignments[1]);
}

#[test]
fn solve_subproblem_solves_general_integer_program_with_in_house_solver() {
    let result = solve_subproblem(
        test_job(general_integer_problem()),
        "worker-test".to_string(),
    );

    assert!(result.ok, "subproblem error: {:?}", result.error);
    assert_eq!(result.status, "optimal");
    assert_eq!(result.x.len(), 2);
    assert!(result.z.is_some_and(|z| (z - 3.0).abs() < 1e-6));
    assert!(result
        .x
        .iter()
        .all(|value| { *value >= -1e-6 && (*value - value.round()).abs() < 1e-6 }));
    assert!(result.x.iter().sum::<f64>() <= 3.0 + 1e-6);
}

#[test]
fn solve_subproblem_accepts_named_constraints_with_branch_rows() {
    let mut problem = binary_knapsack_problem();
    problem.var_names = Some(vec![
        "item0".to_string(),
        "item1".to_string(),
        "item2".to_string(),
        "item3".to_string(),
    ]);
    problem.con_names = Some(vec!["capacity".to_string()]);
    let mut job = test_job(problem);
    job.extra_constraints.push(BranchConstraint {
        coefs: vec![1.0, 0.0, 0.0, 0.0],
        rhs: 0.0,
        name: "branch_d0_x0_le_0".to_string(),
    });

    let result = solve_subproblem(job, "worker-test".to_string());

    assert!(result.ok, "subproblem error: {:?}", result.error);
    assert_eq!(result.status, "optimal");
}

#[tokio::test]
async fn master_local_fallback_solves_binary_mip() {
    let state = test_state(NodeRole::Master);
    let options = SolveOptions {
        split_depth: Some(2),
        max_nodes: Some(10_000),
        ..SolveOptions::default()
    };

    let response = solve_problem_distributed(
        state,
        "request-test".to_string(),
        "33333333-3333-4333-8333-333333333333".to_string(),
        3,
        binary_knapsack_problem(),
        options,
    )
    .await
    .unwrap();

    assert!(response.ok, "warnings: {:?}", response.warnings);
    assert_eq!(response.status, "optimal");
    assert_eq!(response.revision, 3);
    assert_eq!(response.z, Some(90.0));
    assert!(!response.distributed);
    assert_eq!(response.jobs_expected, response.jobs_completed);
    assert_eq!(response.jobs_redelegated, 0);
    assert!(response.jobs_published >= response.jobs_completed);
    assert!(response.jobs_published > 0);
}

#[cfg(feature = "external-solver-verification")]
#[tokio::test]
async fn master_local_fallback_verifies_binary_mip_with_external_highs() {
    if std::process::Command::new("highs")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("SKIP external HiGHS verification test: highs command not installed");
        return;
    }
    let state = test_state(NodeRole::Master);
    let options = SolveOptions {
        split_depth: Some(2),
        max_nodes: Some(10_000),
        verify_external: Some(true),
        external_verification_method: Some("highs".to_string()),
        external_verification_tolerance: Some(1e-6),
        ..SolveOptions::default()
    };

    let response = solve_problem_distributed(
        state,
        "request-external-verify-test".to_string(),
        "88888888-8888-4888-8888-888888888888".to_string(),
        5,
        binary_knapsack_problem(),
        options,
    )
    .await
    .unwrap();

    assert!(response.ok, "warnings: {:?}", response.warnings);
    assert_eq!(response.status, "optimal");
    assert_eq!(response.z, Some(90.0));
    let verification = response
        .external_verification
        .expect("external verification report");
    assert_eq!(verification.status, "verified", "{verification:?}");
    assert_eq!(verification.solution_status.as_deref(), Some("optimal"));
    assert!(verification
        .objective_delta
        .is_some_and(|delta| delta <= verification.tolerance));
    assert!(verification
        .message
        .as_deref()
        .is_some_and(|message| message.contains("usesExternalSolvers=true")));
}

#[tokio::test]
async fn pure_lp_request_uses_single_local_lp_job() {
    let state = test_state(NodeRole::Master);
    let response = solve_problem_distributed(
        state.clone(),
        "request-lp-local".to_string(),
        "44444444-4444-4444-8444-444444444444".to_string(),
        4,
        pure_lp_problem(),
        SolveOptions {
            split_depth: Some(8),
            max_subproblems: Some(1_000),
            ..SolveOptions::default()
        },
    )
    .await
    .unwrap();

    assert!(response.ok, "warnings: {:?}", response.warnings);
    assert_eq!(response.status, "optimal");
    assert_eq!(response.z, Some(10.0));
    assert_eq!(response.jobs_expected, 1);
    assert_eq!(response.jobs_published, 1);
    assert_eq!(response.jobs_completed, 1);
    assert_eq!(response.jobs_redelegated, 0);
    assert_eq!(response.jobs_split, 0);
    assert!(!response.distributed);
    assert!(response.lp.is_some());

    let solves = state.solves.lock().expect("solves mutex poisoned");
    let solve = solves.get(&response.solve_id).expect("tracked LP solve");
    assert!(!solve.distributed);
    assert_eq!(solve.jobs.len(), 1);
    let (job_id, job) = solve.jobs.iter().next().expect("single LP job");
    assert!(job_id.ends_with("-lp"));
    assert_eq!(job.status, "optimal");
    assert_eq!(job.depth, 0);
    drop(solves);

    let tasks = runtime_task_entries(&state);
    assert!(tasks.iter().any(|task| {
        task.kind == "local-lp" && task.solve_id.as_deref() == Some(&response.solve_id)
    }));
}

#[test]
fn simulated_three_slave_delegation_solves_100_by_200_mip() {
    let state = test_state(NodeRole::Master);
    let problem = normalized_problem(hundred_variable_two_hundred_constraint_mip()).unwrap();
    let options = SolveOptions {
        split_depth: Some(2),
        max_subproblems: Some(8),
        max_nodes: Some(10_000),
        max_ticks: Some(10_000),
        ..SolveOptions::default()
    };
    assert_eq!(problem.c.len(), 100);
    assert_eq!(problem.a.len(), 200);

    let (jobs, warnings) = build_frontier_jobs(
        &problem,
        "solve-large-test",
        "request-large-test",
        "55555555-5555-4555-8555-555555555555",
        11,
        "master-test",
        &options,
        false,
    )
    .unwrap();
    assert!(
        jobs.len() >= 3,
        "expected at least three delegated subproblems, got {} with warnings {:?}",
        jobs.len(),
        warnings
    );

    let workers = ["slave-a", "slave-b", "slave-c"];
    let mut pending: VecDeque<SubproblemJob> = jobs.into_iter().collect();
    let mut results = Vec::new();
    let mut used_workers = HashSet::new();
    let mut jobs_expected = pending.len();
    let mut jobs_published = pending.len();
    let mut jobs_split = 0usize;
    let mut next_worker = 0usize;

    while let Some(job) = pending.pop_front() {
        let worker = workers[next_worker % workers.len()].to_string();
        next_worker += 1;
        used_workers.insert(worker.clone());
        let result = solve_subproblem(job, worker);
        if result.status == "split" && !result.child_jobs.is_empty() {
            jobs_split += 1;
            jobs_expected = jobs_expected.saturating_add(result.child_jobs.len().saturating_sub(1));
            jobs_published = jobs_published.saturating_add(result.child_jobs.len());
            for child in result.child_jobs {
                pending.push_back(child);
            }
        } else {
            results.push(result);
        }
    }

    let response = aggregate_results(
        "solve-large-test".to_string(),
        "request-large-test".to_string(),
        None,
        11,
        &problem,
        &options,
        jobs_expected,
        jobs_published,
        0,
        jobs_split,
        results,
        false,
        true,
        &state,
        warnings,
    );

    assert_eq!(used_workers.len(), 3, "used workers: {used_workers:?}");
    assert!(response.ok, "response warnings: {:?}", response.warnings);
    assert_eq!(response.status, "optimal");
    assert_eq!(response.revision, 11);
    assert_eq!(response.jobs_expected, response.jobs_completed);
    assert!(response.jobs_published >= 3);
    assert!(response.distributed);
    assert_eq!(response.z, Some(2.0));
    assert_eq!(response.x.len(), 100);
    assert_eq!(
        response
            .x
            .iter()
            .take(3)
            .filter(|value| **value > 0.5)
            .count(),
        2
    );
}

#[test]
fn simulated_three_slave_delegation_solves_real_100_by_150_dispatch_mip() {
    let state = test_state(NodeRole::Master);
    let problem =
        normalized_problem(hundred_variable_one_hundred_fifty_constraint_dispatch_mip()).unwrap();
    let options = SolveOptions {
        split_depth: Some(2),
        max_subproblems: Some(8),
        max_nodes: Some(20_000),
        max_ticks: Some(20_000),
        lp_max_iters: Some(10_000),
        ..SolveOptions::default()
    };
    #[cfg(feature = "external-solver-verification")]
    let (options, expect_external_verification) = {
        let mut options = options;
        let expect_external_verification = if std::process::Command::new("highs")
            .arg("--version")
            .output()
            .is_ok()
        {
            options.verify_external = Some(true);
            options.external_verification_method = Some("highs".to_string());
            options.external_verification_tolerance = Some(1e-6);
            true
        } else {
            eprintln!("SKIP 100x150 external HiGHS verification: highs command not installed");
            false
        };
        (options, expect_external_verification)
    };
    #[cfg(not(feature = "external-solver-verification"))]
    let expect_external_verification = false;
    assert_eq!(problem.c.len(), 100);
    assert_eq!(problem.a.len(), 150);

    let (jobs, warnings) = build_frontier_jobs(
        &problem,
        "solve-dispatch-test",
        "request-dispatch-test",
        "66666666-6666-4666-8666-666666666666",
        12,
        "master-test",
        &options,
        false,
    )
    .unwrap();
    assert!(
        jobs.len() >= 3,
        "expected at least three delegated subproblems, got {} with warnings {:?}",
        jobs.len(),
        warnings
    );

    let workers = ["slave-a", "slave-b", "slave-c"];
    let mut pending: VecDeque<SubproblemJob> = jobs.into_iter().collect();
    let mut results = Vec::new();
    let mut used_workers = HashSet::new();
    let mut jobs_expected = pending.len();
    let mut jobs_published = pending.len();
    let mut jobs_split = 0usize;
    let mut next_worker = 0usize;

    while let Some(job) = pending.pop_front() {
        let worker = workers[next_worker % workers.len()].to_string();
        next_worker += 1;
        used_workers.insert(worker.clone());
        let result = solve_subproblem(job, worker);
        if result.status == "split" && !result.child_jobs.is_empty() {
            jobs_split += 1;
            jobs_expected = jobs_expected.saturating_add(result.child_jobs.len().saturating_sub(1));
            jobs_published = jobs_published.saturating_add(result.child_jobs.len());
            for child in result.child_jobs {
                pending.push_back(child);
            }
        } else {
            results.push(result);
        }
    }

    let response = aggregate_results(
        "solve-dispatch-test".to_string(),
        "request-dispatch-test".to_string(),
        None,
        12,
        &problem,
        &options,
        jobs_expected,
        jobs_published,
        0,
        jobs_split,
        results,
        false,
        true,
        &state,
        warnings,
    );

    let expected_z = (0..49).map(|pair| 1000.0 - pair as f64).sum::<f64>();
    assert_eq!(used_workers.len(), 3, "used workers: {used_workers:?}");
    assert!(response.ok, "response warnings: {:?}", response.warnings);
    assert_eq!(response.status, "optimal");
    assert_eq!(response.revision, 12);
    assert_eq!(response.jobs_expected, response.jobs_completed);
    assert!(response.jobs_published >= 3);
    assert!(response.distributed);
    assert_eq!(response.z, Some(expected_z));
    assert_eq!(response.x.len(), 100);
    assert_eq!(response.x.iter().filter(|value| **value > 0.5).count(), 49);
    for pair in 0..49 {
        assert!(
            response.x[pair * 2] > 0.5,
            "expected primary dispatch option for pair {pair}"
        );
        assert!(
            response.x[pair * 2 + 1] < 1e-6,
            "did not expect alternate dispatch option for pair {pair}"
        );
    }
    assert!(response.x[98] < 1e-6);
    assert!(response.x[99] < 1e-6);
    if expect_external_verification {
        let verification = response
            .external_verification
            .as_ref()
            .expect("external verification report");
        assert_eq!(verification.status, "verified", "{verification:?}");
        assert_eq!(verification.method.as_deref(), Some("highs"));
        assert_eq!(
            verification.solver.as_deref(),
            Some("des-ipmip-external-lp")
        );
        assert_eq!(verification.solution_status.as_deref(), Some("optimal"));
        assert!(
            verification
                .objective_delta
                .is_some_and(|delta| delta <= verification.tolerance),
            "{verification:?}"
        );
        assert!(
            verification
                .objective
                .is_some_and(|objective| (objective - expected_z).abs() <= verification.tolerance),
            "{verification:?}"
        );
        assert!(
            verification
                .message
                .as_deref()
                .is_some_and(|message| message.contains("usesExternalSolvers=true")),
            "{verification:?}"
        );
    } else {
        assert!(response.external_verification.is_none());
    }
}

#[cfg(feature = "external-solver-verification")]
#[test]
fn external_highs_methods_verify_real_100_by_150_dispatch_mip_optimum() {
    if std::process::Command::new("highs")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("SKIP external HiGHS method verification: highs command not installed");
        return;
    }
    let problem =
        normalized_problem(hundred_variable_one_hundred_fifty_constraint_dispatch_mip()).unwrap();
    let options = SolveOptions {
        max_nodes: Some(20_000),
        max_ticks: Some(20_000),
        lp_max_iters: Some(10_000),
        ..SolveOptions::default()
    };
    let expected_z = (0..49).map(|pair| 1000.0 - pair as f64).sum::<f64>();

    for method in ["highs", "highs-ds", "highs-ipm"] {
        let verification = run_external_verification(&problem, &options, expected_z, method, 1e-6)
            .unwrap_or_else(|error| panic!("{method} verification failed: {error}"));
        assert_eq!(
            verification.status, "verified",
            "{method}: {verification:?}"
        );
        assert_eq!(verification.method.as_deref(), Some(method));
        assert_eq!(
            verification.solver.as_deref(),
            Some("des-ipmip-external-lp")
        );
        assert_eq!(verification.solution_status.as_deref(), Some("optimal"));
        assert!(
            verification
                .objective
                .is_some_and(|objective| (objective - expected_z).abs() <= 1e-6),
            "{method}: {verification:?}"
        );
        assert!(
            verification
                .objective_delta
                .is_some_and(|delta| delta <= verification.tolerance),
            "{method}: {verification:?}"
        );
        assert!(
            verification
                .message
                .as_deref()
                .is_some_and(|message| message.contains("usesExternalSolvers=true")),
            "{method}: {verification:?}"
        );
    }
}

#[cfg(feature = "external-solver-verification")]
#[test]
fn external_highs_methods_verify_soccer_formation_optimum() {
    if std::process::Command::new("highs")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("SKIP external soccer verification: highs command not installed");
        return;
    }
    let problem = normalized_problem(soccer_formation::problem(false)).unwrap();
    let options = SolveOptions::default();
    let expected_z = soccer_formation::expected_objective();

    for method in ["highs", "highs-ds", "highs-ipm"] {
        let verification = run_external_verification(&problem, &options, expected_z, method, 1e-5)
            .unwrap_or_else(|error| panic!("soccer {method} verification failed: {error}"));
        assert_eq!(
            verification.status, "verified",
            "{method}: {verification:?}"
        );
        assert_eq!(verification.method.as_deref(), Some(method));
        assert_eq!(verification.solution_status.as_deref(), Some("optimal"));
        assert!(
            verification
                .objective
                .is_some_and(|objective| (objective - expected_z).abs() <= 1e-5),
            "{method}: {verification:?}"
        );
        assert!(
            verification
                .objective_delta
                .is_some_and(|delta| delta <= verification.tolerance),
            "{method}: {verification:?}"
        );
    }
}

#[cfg(feature = "external-solver-verification")]
#[test]
fn external_highs_detects_an_incorrect_soccer_objective_claim() {
    if std::process::Command::new("highs")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("SKIP external soccer mismatch test: highs command not installed");
        return;
    }
    let problem = normalized_problem(soccer_formation::problem(false)).unwrap();
    let incorrect_z = soccer_formation::expected_objective() + 1.0;

    let verification = run_external_verification(
        &problem,
        &SolveOptions::default(),
        incorrect_z,
        "highs",
        1e-5,
    )
    .unwrap();

    assert_eq!(verification.status, "mismatch", "{verification:?}");
    assert_eq!(verification.solution_status.as_deref(), Some("optimal"));
    assert!(verification
        .objective
        .is_some_and(|value| { (value - soccer_formation::expected_objective()).abs() <= 1e-5 }));
    assert!(verification
        .objective_delta
        .is_some_and(|delta| (delta - 1.0).abs() <= 1e-5));
}

#[tokio::test]
async fn http_solve_endpoint_solves_binary_mip() {
    let app = app_router(test_state(NodeRole::Master));
    let problem_id = "33333333-3333-4333-8333-333333333333";
    let payload = json!({
        "requestId": "http-test",
        "problemId": problem_id,
        "problem": binary_knapsack_problem(),
        "options": {
            "splitDepth": 2,
            "maxNodes": 10000
        }
    });

    let (status, body) = post_json(app, "/solve", payload).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert_eq!(body.get("status"), Some(&json!("optimal")));
    assert_eq!(body.get("z"), Some(&json!(90.0)));
    assert_eq!(body.get("distributed"), Some(&json!(false)));
    assert_eq!(body.get("problemId"), Some(&json!(problem_id)));
    assert_eq!(body.pointer("/role"), Some(&json!("master")));
}

#[tokio::test]
async fn http_problem_upload_then_solve_uses_stored_model() {
    let app = app_router(test_state(NodeRole::Master));
    let problem_id = "99999999-9999-4999-8999-999999999999";

    let (upload_status, upload_body) = post_json(
        app.clone(),
        &format!("/problems/{problem_id}"),
        json!({
            "problem": binary_knapsack_problem()
        }),
    )
    .await;
    assert_eq!(upload_status, StatusCode::OK);
    assert_eq!(upload_body.get("ok"), Some(&json!(true)));
    assert_eq!(upload_body.get("problemId"), Some(&json!(problem_id)));
    assert_eq!(upload_body.get("storeStatus"), Some(&json!("created")));
    assert_eq!(upload_body.get("redisStored"), Some(&json!(false)));

    let (solve_status, solve_body) = post_json(
        app,
        &format!("/problems/{problem_id}/solve"),
        json!({
            "requestId": "stored-problem-http-test",
            "options": {
                "splitDepth": 2,
                "maxNodes": 10000
            }
        }),
    )
    .await;

    assert_eq!(solve_status, StatusCode::OK);
    assert_eq!(solve_body.get("ok"), Some(&json!(true)));
    assert_eq!(solve_body.get("status"), Some(&json!("optimal")));
    assert_eq!(solve_body.get("z"), Some(&json!(90.0)));
    assert_eq!(solve_body.get("problemId"), Some(&json!(problem_id)));
}

#[tokio::test]
async fn http_problem_upload_rejects_different_model_for_existing_problem_id() {
    let app = app_router(test_state(NodeRole::Master));
    let problem_id = "9aaaaaaa-9999-4999-8999-999999999999";
    let mut changed = binary_knapsack_problem();
    changed.c[0] = 11.0;

    let (first_status, first_body) = post_json(
        app.clone(),
        &format!("/problems/{problem_id}"),
        json!({"problem": binary_knapsack_problem()}),
    )
    .await;
    let (second_status, second_body) = post_json(
        app,
        &format!("/problems/{problem_id}"),
        json!({"problem": changed}),
    )
    .await;

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(first_body.get("storeStatus"), Some(&json!("created")));
    assert_eq!(second_status, StatusCode::CONFLICT);
    assert_eq!(second_body.get("ok"), Some(&json!(false)));
    assert!(second_body
        .get("error")
        .and_then(Value::as_str)
        .is_some_and(is_problem_model_conflict));
}

#[tokio::test]
async fn http_solve_rejects_duplicate_running_problem_uuid() {
    let state = test_state(NodeRole::Master);
    let problem_id = "55555555-5555-4555-8555-555555555555";
    state.solves.lock().expect("solves mutex poisoned").insert(
        "solve-active-problem".to_string(),
        SolveRegistryEntry {
            solve_id: "solve-active-problem".to_string(),
            request_id: "request-active-problem".to_string(),
            problem_id: Some(problem_id.to_string()),
            status: "running".to_string(),
            jobs_expected: 1,
            started_at_ms: 1000,
            updated_at_ms: 1000,
            ..SolveRegistryEntry::default()
        },
    );
    let app = app_router(state);
    let payload = json!({
        "requestId": "duplicate-problem",
        "problemId": problem_id,
        "problem": binary_knapsack_problem()
    });

    let (status, body) = post_json(app, "/solve", payload).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body.get("ok"), Some(&json!(false)));
    assert!(body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .contains("already has running solve"));
}

#[tokio::test]
async fn metrics_endpoint_exposes_prometheus_node_and_inflight_metrics() {
    let state = test_state(NodeRole::Master);
    state
        .metrics
        .subproblem_jobs_published_total
        .store(7, Ordering::Relaxed);
    state
        .metrics
        .subproblem_jobs_completed_total
        .store(3, Ordering::Relaxed);
    state
        .metrics
        .subproblem_jobs_redelegated_total
        .store(2, Ordering::Relaxed);
    state
        .metrics
        .subproblem_jobs_split_total
        .store(4, Ordering::Relaxed);
    state
        .metrics
        .worker_control_messages_total
        .store(5, Ordering::Relaxed);
    state
        .workers
        .lock()
        .expect("workers mutex poisoned")
        .insert(
            "worker-a".to_string(),
            WorkerNodeStatus {
                node_id: "worker-a".to_string(),
                last_command: "worker-ready".to_string(),
                last_seen_ms: now_ms(),
                ..WorkerNodeStatus::default()
            },
        );
    state.solves.lock().expect("solves mutex poisoned").insert(
        "solve-a".to_string(),
        SolveRegistryEntry {
            solve_id: "solve-a".to_string(),
            request_id: "request-a".to_string(),
            status: "running".to_string(),
            jobs_expected: 2,
            started_at_ms: 1000,
            updated_at_ms: 1000,
            ..SolveRegistryEntry::default()
        },
    );
    let app = app_router(state);

    let (status, body) = get_text(app, "/metrics").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("# TYPE dd_mip_solver_node_info gauge"));
    assert!(body.contains("dd_mip_solver_node_info{role=\"master\",node_id=\"test-node\"} 1"));
    assert!(body.contains("# TYPE dd_mip_solver_subproblem_jobs_in_flight gauge"));
    assert!(body.contains("dd_mip_solver_subproblem_jobs_in_flight 4"));
    assert!(body.contains("# TYPE dd_mip_solver_subproblem_jobs_redelegated_total counter"));
    assert!(body.contains("dd_mip_solver_subproblem_jobs_redelegated_total 2"));
    assert!(body.contains("# TYPE dd_mip_solver_subproblem_jobs_split_total counter"));
    assert!(body.contains("dd_mip_solver_subproblem_jobs_split_total 4"));
    assert!(body.contains("# TYPE dd_mip_solver_workers_known gauge"));
    assert!(body.contains("dd_mip_solver_workers_known 1"));
    assert!(body.contains("# TYPE dd_mip_solver_worker_control_messages_total counter"));
    assert!(body.contains("dd_mip_solver_worker_control_messages_total 5"));
    assert!(body.contains("# TYPE dd_mip_solver_solves_tracked gauge"));
    assert!(body.contains("dd_mip_solver_solves_tracked 1"));
    assert!(body.contains("# TYPE dd_mip_solver_active_solves gauge"));
    assert!(body.contains("dd_mip_solver_active_solves 1"));
}

#[tokio::test]
async fn mip_solver_cluster_workers_endpoint_reports_master_observed_slaves() {
    let state = test_state(NodeRole::Master);
    let seen_at = now_ms();
    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-b",
            "role":"slave",
            "commandName":"worker-ready",
            "payload":{
                "consumer":"dd-in-house-mip-solver-node-workers",
                "jobsSubject": MIP_SOLVER_JOBS_SUBJECT,
                "resultsSubject": MIP_SOLVER_RESULTS_SUBJECT
            },
            "timeMs": seen_at
        }),
    )
    .unwrap();
    let app = app_router(state);

    let (status, body) = get_json(app.clone(), "/workers").await;
    let (compat_status, compat_body) = get_json(app, "/mip-solver-cluster/workers").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(compat_status, StatusCode::OK);
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert_eq!(body.get("count"), Some(&json!(1)));
    assert_eq!(compat_body.get("count"), Some(&json!(1)));
    assert_eq!(body.pointer("/workers/0/nodeId"), Some(&json!("worker-b")));
    assert_eq!(
        body.pointer("/workers/0/consumer"),
        Some(&json!("dd-in-house-mip-solver-node-workers"))
    );
}

#[tokio::test]
async fn worker_rotation_prunes_stale_registry_entries() {
    let state = test_state(NodeRole::Master);
    let seen_at = now_ms();
    let stale_at = seen_at.saturating_sub(worker_stale_after().as_millis() + 1);
    state
        .workers
        .lock()
        .expect("workers mutex poisoned")
        .insert(
            "worker-old".to_string(),
            WorkerNodeStatus {
                node_id: "worker-old".to_string(),
                last_command: "worker-ready".to_string(),
                last_seen_ms: stale_at,
                ..WorkerNodeStatus::default()
            },
        );

    record_worker_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"worker-new",
            "role":"slave",
            "commandName":"worker-ready",
            "payload":{
                "consumer":"dd-in-house-mip-solver-node-workers",
                "jobsSubject": MIP_SOLVER_JOBS_SUBJECT,
                "resultsSubject": MIP_SOLVER_RESULTS_SUBJECT
            },
            "timeMs": seen_at
        }),
    )
    .unwrap();

    let app = app_router(state.clone());
    let (status, body) = get_json(app, "/workers").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("count"), Some(&json!(1)));
    assert_eq!(
        body.pointer("/workers/0/nodeId"),
        Some(&json!("worker-new"))
    );
    let workers = state.workers.lock().expect("workers mutex poisoned");
    assert!(!workers.contains_key("worker-old"));
    assert!(workers.contains_key("worker-new"));
}

#[tokio::test]
async fn runtime_tasks_endpoint_reports_live_problem_threads() {
    let state = test_state(NodeRole::Master);
    let problem_id = "12121212-1212-4212-8212-121212121212";
    track_runtime_task_started(
        &state,
        "problem-task-a".to_string(),
        "problem",
        Some(problem_id.to_string()),
        Some("solve-task-a".to_string()),
        Some("request-task-a".to_string()),
        None,
        None,
        None,
    );
    let app = app_router(state);

    let (status, body) = get_json(app.clone(), "/tasks").await;
    let (compat_status, compat_body) = get_json(app.clone(), "/mip-solver-cluster/tasks").await;
    let (lookup_status, lookup_body) = get_json(app, &format!("/tasks/{problem_id}")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(compat_status, StatusCode::OK);
    assert_eq!(lookup_status, StatusCode::OK);
    assert_eq!(body.get("count"), Some(&json!(1)));
    assert_eq!(body.get("active"), Some(&json!(1)));
    assert_eq!(compat_body.get("count"), Some(&json!(1)));
    assert_eq!(lookup_body.pointer("/task/kind"), Some(&json!("problem")));
    assert_eq!(
        lookup_body.pointer("/task/problemId"),
        Some(&json!(problem_id))
    );
    assert_eq!(
        lookup_body.pointer("/task/solveId"),
        Some(&json!("solve-task-a"))
    );
}

#[tokio::test]
async fn mip_solver_cluster_solves_endpoint_reports_tracked_jobs() {
    let state = test_state(NodeRole::Master);
    let app = app_router(state.clone());
    let response = solve_problem_distributed(
        state.clone(),
        "tracked-solve".to_string(),
        "44444444-4444-4444-8444-444444444444".to_string(),
        9,
        pure_lp_problem(),
        SolveOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(response.status, "optimal");
    let (status, body) = get_json(app.clone(), "/solves").await;
    let (compat_status, compat_body) = get_json(app, "/mip-solver-cluster/solves").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(compat_status, StatusCode::OK);
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert_eq!(body.get("count"), Some(&json!(1)));
    assert_eq!(body.get("active"), Some(&json!(0)));
    assert_eq!(compat_body.get("count"), Some(&json!(1)));
    assert_eq!(
        body.pointer("/solves/0/requestId"),
        Some(&json!("tracked-solve"))
    );
    assert_eq!(body.pointer("/solves/0/status"), Some(&json!("optimal")));
    assert_eq!(body.pointer("/solves/0/jobsExpected"), Some(&json!(1)));
    assert_eq!(body.pointer("/solves/0/jobsCompleted"), Some(&json!(1)));
    let jobs = body
        .pointer("/solves/0/jobs")
        .and_then(Value::as_object)
        .expect("jobs object");
    assert_eq!(jobs.len(), 1);
    let job = jobs.values().next().expect("one job");
    assert_eq!(job.get("status"), Some(&json!("optimal")));
}

#[tokio::test]
async fn solves_endpoint_filters_by_problem_uuid_query() {
    let state = test_state(NodeRole::Master);
    let problem_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let other_problem_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    state.solves.lock().expect("solves mutex poisoned").extend([
        (
            "solve-problem-a".to_string(),
            SolveRegistryEntry {
                solve_id: "solve-problem-a".to_string(),
                request_id: "request-problem-a".to_string(),
                problem_id: Some(problem_id.to_string()),
                status: "running".to_string(),
                jobs_expected: 2,
                jobs_published: 2,
                jobs_completed: 1,
                started_at_ms: 2000,
                updated_at_ms: 2100,
                ..SolveRegistryEntry::default()
            },
        ),
        (
            "solve-problem-b".to_string(),
            SolveRegistryEntry {
                solve_id: "solve-problem-b".to_string(),
                request_id: "request-problem-b".to_string(),
                problem_id: Some(other_problem_id.to_string()),
                status: "running".to_string(),
                jobs_expected: 3,
                jobs_published: 3,
                jobs_completed: 0,
                started_at_ms: 3000,
                updated_at_ms: 3100,
                ..SolveRegistryEntry::default()
            },
        ),
    ]);
    let app = app_router(state);

    let (status, body) = get_json(app.clone(), &format!("/solves?problem={problem_id}")).await;
    let (compat_status, compat_body) = get_json(
        app.clone(),
        &format!("/mip-solver-cluster/solves?problem={problem_id}"),
    )
    .await;
    let (invalid_status, invalid_body) = get_json(app, "/solves?problem=not-a-uuid").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(compat_status, StatusCode::OK);
    assert_eq!(body.get("problem"), Some(&json!(problem_id.to_string())));
    assert_eq!(body.get("count"), Some(&json!(1)));
    assert_eq!(body.get("active"), Some(&json!(1)));
    assert_eq!(
        body.pointer("/solves/0/solveId"),
        Some(&json!("solve-problem-a"))
    );
    assert_eq!(compat_body.get("count"), Some(&json!(1)));
    assert_eq!(invalid_status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_body.get("ok"), Some(&json!(false)));
}

#[tokio::test]
async fn tasks_endpoint_reports_problem_and_job_uuid_lookups() {
    let state = test_state(NodeRole::Master);
    let problem_id = "99999999-9999-4999-8999-999999999999".to_string();
    let job_uuid = new_uuid_string();
    track_runtime_task_started(
        &state,
        problem_id.clone(),
        "problem",
        Some(problem_id.clone()),
        Some("solve-task-test".to_string()),
        Some("request-task-test".to_string()),
        None,
        None,
        None,
    );
    track_runtime_task_started(
        &state,
        job_uuid.clone(),
        "local-subproblem",
        Some(problem_id.clone()),
        Some("solve-task-test".to_string()),
        Some("request-task-test".to_string()),
        Some("job-task-test".to_string()),
        Some(job_uuid.clone()),
        None,
    );
    let app = app_router(state);

    let (list_status, list_body) = get_json(app.clone(), "/tasks").await;
    let (problem_status, problem_body) =
        get_json(app.clone(), &format!("/tasks/{problem_id}")).await;
    let (job_status, job_body) = get_json(app, &format!("/tasks/{job_uuid}")).await;

    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list_body.get("count"), Some(&json!(2)));
    assert_eq!(list_body.get("active"), Some(&json!(2)));
    assert_eq!(problem_status, StatusCode::OK);
    assert_eq!(problem_body.pointer("/task/kind"), Some(&json!("problem")));
    assert_eq!(job_status, StatusCode::OK);
    assert_eq!(
        job_body.pointer("/task/jobId"),
        Some(&json!("job-task-test"))
    );
    assert_eq!(job_body.pointer("/task/jobUuid"), Some(&json!(job_uuid)));
}

#[tokio::test]
async fn cancel_endpoint_marks_running_solve_by_request_id() {
    let state = test_state(NodeRole::Master);
    let problem_id = "77777777-7777-4777-8777-777777777777";
    state.solves.lock().expect("solves mutex poisoned").insert(
        "solve-cancel-test".to_string(),
        SolveRegistryEntry {
            solve_id: "solve-cancel-test".to_string(),
            request_id: "request-cancel-test".to_string(),
            problem_id: Some(problem_id.to_string()),
            status: "running".to_string(),
            jobs_expected: 4,
            jobs_published: 2,
            started_at_ms: 1000,
            updated_at_ms: 1000,
            ..SolveRegistryEntry::default()
        },
    );
    let app = app_router(state.clone());

    let (status, body) = post_json(
        app,
        "/mip-solver-cluster/requests/request-cancel-test/cancel",
        json!({"reason":"client changed the model","requestedBy":"unit-test"}),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert_eq!(body.get("status"), Some(&json!("cancelling")));
    assert_eq!(body.get("solveId"), Some(&json!("solve-cancel-test")));
    assert_eq!(body.get("problemId"), Some(&json!(problem_id)));
    assert!(solve_cancel_requested(&state, "solve-cancel-test"));
    let solves = state.solves.lock().expect("solves mutex poisoned");
    let solve = solves.get("solve-cancel-test").unwrap();
    assert_eq!(solve.status, "cancelling");
    assert!(solve.cancel_requested);
    assert_eq!(
        solve.cancel_reason.as_deref(),
        Some("client changed the model")
    );
}

#[tokio::test]
async fn cancel_endpoint_marks_running_solve_by_problem_id() {
    let state = test_state(NodeRole::Master);
    let problem_id = "88888888-8888-4888-8888-888888888888";
    state.solves.lock().expect("solves mutex poisoned").insert(
        "solve-cancel-problem-test".to_string(),
        SolveRegistryEntry {
            solve_id: "solve-cancel-problem-test".to_string(),
            request_id: "request-cancel-problem-test".to_string(),
            problem_id: Some(problem_id.to_string()),
            status: "running".to_string(),
            jobs_expected: 2,
            started_at_ms: 1000,
            updated_at_ms: 1000,
            ..SolveRegistryEntry::default()
        },
    );
    let app = app_router(state.clone());

    let (status, body) = post_json(
        app,
        &format!("/problems/{problem_id}/cancel"),
        json!({"reason":"problem cancelled","requestedBy":"unit-test"}),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        body.get("solveId"),
        Some(&json!("solve-cancel-problem-test"))
    );
    assert_eq!(body.get("problemId"), Some(&json!(problem_id)));
    assert!(solve_cancel_requested(&state, "solve-cancel-problem-test"));
}

#[tokio::test]
async fn delete_endpoint_cleans_finished_solve_from_memory() {
    let state = test_state(NodeRole::Master);
    state.solves.lock().expect("solves mutex poisoned").insert(
        "solve-clean-test".to_string(),
        SolveRegistryEntry {
            solve_id: "solve-clean-test".to_string(),
            request_id: "request-clean-test".to_string(),
            status: "cancelled".to_string(),
            jobs_expected: 1,
            jobs_published: 1,
            started_at_ms: 1000,
            updated_at_ms: 1200,
            finished_at_ms: Some(1200),
            cancel_requested: true,
            cancel_requested_at_ms: Some(1100),
            cancel_reason: Some("test cleanup".to_string()),
            jobs: HashMap::from([(
                "job-clean-test".to_string(),
                JobRegistryEntry {
                    job_id: "job-clean-test".to_string(),
                    status: "cancelled".to_string(),
                    submitted_at_ms: 1000,
                    finished_at_ms: Some(1200),
                    ..JobRegistryEntry::default()
                },
            )]),
            ..SolveRegistryEntry::default()
        },
    );
    state
        .cancelled_solves
        .lock()
        .expect("cancelled solves mutex poisoned")
        .insert(
            "solve-clean-test".to_string(),
            CancelInfo {
                solve_id: "solve-clean-test".to_string(),
                request_id: Some("request-clean-test".to_string()),
                problem_id: None,
                reason: "test cleanup".to_string(),
                requested_by: "unit-test".to_string(),
                requested_at_ms: 1100,
            },
        );
    let app = app_router(state.clone());

    let (status, body) = delete_json(app, "/mip-solver-cluster/solves/solve-clean-test").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("status"), Some(&json!("cleaned")));
    assert_eq!(body.get("jobsRemoved"), Some(&json!(1)));
    assert!(!state
        .solves
        .lock()
        .expect("solves mutex poisoned")
        .contains_key("solve-clean-test"));
    assert!(!state
        .cancelled_solves
        .lock()
        .expect("cancelled solves mutex poisoned")
        .contains_key("solve-clean-test"));
}

#[test]
fn cancel_control_frames_update_local_cancel_map_without_solve_registry() {
    let state = test_state(NodeRole::Slave);

    let handled = record_cancel_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"master-a",
            "role":"master",
            "commandName":"cancel-solve",
            "payload":{
                "solveId":"solve-cancel-broadcast",
                "requestId":"request-cancel-broadcast",
                "reason":"client cancelled",
                "requestedBy":"master-a",
                "requestedAtMs": 3000
            },
            "timeMs": 3001
        }),
    )
    .unwrap();

    assert!(handled);
    let info = solve_cancel_info(&state, "solve-cancel-broadcast").unwrap();
    assert_eq!(info.request_id.as_deref(), Some("request-cancel-broadcast"));
    assert_eq!(info.reason, "client cancelled");
    assert_eq!(info.requested_at_ms, 3000);
}

#[test]
fn cancel_control_frame_resolves_problem_id_to_running_solve() {
    let state = test_state(NodeRole::Master);
    let problem_id = "99999999-9999-4999-8999-999999999999";
    state.solves.lock().expect("solves mutex poisoned").insert(
        "solve-problem-cancel".to_string(),
        SolveRegistryEntry {
            solve_id: "solve-problem-cancel".to_string(),
            request_id: "request-problem-cancel".to_string(),
            problem_id: Some(problem_id.to_string()),
            status: "running".to_string(),
            jobs_expected: 1,
            started_at_ms: 1000,
            updated_at_ms: 1000,
            ..SolveRegistryEntry::default()
        },
    );

    let handled = record_cancel_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"master-a",
            "role":"master",
            "commandName":"cancel-solve",
            "payload":{
                "problemId": problem_id,
                "reason":"nats problem cancel",
                "requestedBy":"master-a",
                "requestedAtMs": 4000
            },
            "timeMs": 4001
        }),
    )
    .unwrap();

    assert!(handled);
    let info = solve_cancel_info(&state, "solve-problem-cancel").unwrap();
    assert_eq!(info.request_id.as_deref(), Some("request-problem-cancel"));
    assert_eq!(info.problem_id.as_deref(), Some(problem_id));
    assert_eq!(info.reason, "nats problem cancel");
    let solves = state.solves.lock().expect("solves mutex poisoned");
    let solve = solves.get("solve-problem-cancel").unwrap();
    assert_eq!(solve.status, "cancelling");
    assert!(solve.cancel_requested);
}

#[test]
fn cancel_control_frame_records_problem_id_for_workers_without_registry() {
    let state = test_state(NodeRole::Slave);
    let problem_id = "abababab-abab-4bab-8bab-abababababab";

    let handled = record_cancel_control_frame(
        &state,
        &json!({
            "schema":"dd.mip-solver.control.v1",
            "service": SERVICE_NAME,
            "nodeId":"master-a",
            "role":"master",
            "commandName":"cancel-solve",
            "payload":{
                "problemId": problem_id,
                "reason":"nats problem cancel",
                "requestedBy":"master-a",
                "requestedAtMs": 5000
            },
            "timeMs": 5001
        }),
    )
    .unwrap();

    assert!(handled);
    let info = solve_cancel_info(&state, problem_id).unwrap();
    assert_eq!(info.problem_id.as_deref(), Some(problem_id));
    assert_eq!(info.reason, "nats problem cancel");
    assert!(solve_cancel_requested_for(
        &state,
        "solve-worker-local",
        Some(problem_id)
    ));
}

#[tokio::test]
async fn readyz_requires_nats_connection_for_cluster_readiness() {
    let app = app_router(test_state(NodeRole::Slave));

    let (status, body) = get_json(app, "/readyz").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.get("ok"), Some(&json!(false)));
    assert_eq!(body.get("nats"), Some(&json!(false)));
    assert!(body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .contains("NATS connection is required"));
}

#[tokio::test]
async fn http_slave_rejects_master_solve_endpoint() {
    let app = app_router(test_state(NodeRole::Slave));
    let payload = json!({
        "requestId": "slave-test",
        "problem": binary_knapsack_problem()
    });

    let (status, body) = post_json(app, "/solve", payload).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body.get("ok"), Some(&json!(false)));
    assert!(body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .contains("booted as slave"));
}

#[tokio::test]
async fn http_session_streams_dynamic_edits_then_solves() {
    let app = app_router(test_state(NodeRole::Master));
    let commands = json!([
        {
            "op": "init",
            "sense": "max",
            "c": [10.0, 40.0, 30.0, 50.0],
            "a": [[5.0, 4.0, 6.0, 3.0]],
            "b": [7.0],
            "integerVars": [true, true, true, true],
            "ub": [1.0, 1.0, 1.0, 1.0]
        },
        {
            "op": "set_rhs",
            "index": 0,
            "rhs": 10.0
        },
        {
            "op": "snapshot"
        }
    ]);

    let (events_status, events_body) =
        post_json(app.clone(), "/sessions/live-mip/events", commands).await;
    assert_eq!(events_status, StatusCode::OK);
    assert_eq!(events_body.get("ok"), Some(&json!(true)));
    assert_eq!(events_body.get("revision"), Some(&json!(2)));
    assert!(events_body
        .get("frames")
        .and_then(Value::as_array)
        .is_some_and(|frames| frames
            .iter()
            .any(|frame| frame.get("event") == Some(&json!("model")))));

    let solve_payload = json!({
        "requestId": "live-mip",
        "options": {
            "splitDepth": 2,
            "maxNodes": 10000
        }
    });
    let (solve_status, solve_body) =
        post_json(app, "/sessions/live-mip/solve", solve_payload).await;

    assert_eq!(solve_status, StatusCode::OK);
    assert_eq!(solve_body.get("ok"), Some(&json!(true)));
    assert_eq!(solve_body.get("status"), Some(&json!("optimal")));
    assert_eq!(solve_body.get("revision"), Some(&json!(2)));
    assert_eq!(solve_body.get("z"), Some(&json!(90.0)));
}

#[tokio::test]
async fn http_session_streams_lp_edits_and_returns_primal_dual_certificate() {
    let app = app_router(test_state(NodeRole::Master));
    let commands = json!([
        {
            "op": "init",
            "sense": "max",
            "c": [3.0, 2.0],
            "a": [[1.0, 1.0], [1.0, 0.0]],
            "b": [4.0, 2.0],
            "integerVars": [false, false],
            "varNames": ["x0", "x1"],
            "conNames": ["shared", "x0_cap"]
        },
        {
            "op": "add_constraint",
            "coefs": [0.0, 1.0],
            "rhs": 3.0,
            "name": "x1_cap"
        },
        {
            "op": "change_constraint_weight",
            "row": 2,
            "col": 1,
            "value": 1.0
        },
        {
            "op": "snapshot"
        }
    ]);

    let (events_status, events_body) =
        post_json(app.clone(), "/sessions/live-lp/events", commands).await;
    assert_eq!(events_status, StatusCode::OK);
    assert_eq!(events_body.get("ok"), Some(&json!(true)));
    assert_eq!(events_body.get("revision"), Some(&json!(3)));

    let (solve_status, solve_body) = post_json(
        app,
        "/sessions/live-lp/solve",
        json!({"requestId":"live-lp"}),
    )
    .await;

    assert_eq!(solve_status, StatusCode::OK);
    assert_eq!(solve_body.get("ok"), Some(&json!(true)));
    assert_eq!(solve_body.get("status"), Some(&json!("optimal")));
    assert_eq!(solve_body.get("distributed"), Some(&json!(false)));
    assert_eq!(
        solve_body.pointer("/lp/primal/objective"),
        Some(&json!(10.0))
    );
    assert_eq!(solve_body.pointer("/lp/primal/x"), Some(&json!([2.0, 2.0])));
    let dual = solve_body
        .pointer("/lp/dual/inequality")
        .and_then(Value::as_array)
        .expect("LP inequality duals");
    assert_eq!(dual.len(), 3);
    assert!((dual[0].as_f64().unwrap() - 2.0).abs() < 1e-6);
    assert!((dual[1].as_f64().unwrap() - 1.0).abs() < 1e-6);
    assert!(dual[2].as_f64().unwrap().abs() < 1e-6);
    assert_eq!(
        solve_body.pointer("/lp/dual/rowNames"),
        Some(&json!(["shared", "x0_cap", "x1_cap"]))
    );
}

#[test]
fn aggregate_results_counts_infeasible_subtrees_as_complete() {
    let problem = binary_knapsack_problem();
    let state = test_state(NodeRole::Master);
    let optimal = SubproblemResult {
        solve_id: "solve-test".to_string(),
        request_id: "request-test".to_string(),
        job_id: "job-0".to_string(),
        job_uuid: new_uuid_string(),
        problem_id: Some("11111111-1111-4111-8111-111111111111".to_string()),
        revision: 0,
        worker_node: "worker-a".to_string(),
        ok: true,
        status: "optimal".to_string(),
        z: Some(90.0),
        x: vec![0.0, 1.0, 0.0, 1.0],
        best_bound: Some(90.0),
        gap: Some(0.0),
        lp: None,
        child_jobs: Vec::new(),
        nodes_explored: 1,
        lp_solves: 1,
        elapsed_ms: 1.0,
        accelerator: AcceleratorReport::default(),
        error: None,
        finished_at_ms: now_ms(),
    };
    let infeasible = SubproblemResult {
        solve_id: "solve-test".to_string(),
        request_id: "request-test".to_string(),
        job_id: "job-1".to_string(),
        job_uuid: new_uuid_string(),
        problem_id: Some("11111111-1111-4111-8111-111111111111".to_string()),
        revision: 0,
        worker_node: "worker-b".to_string(),
        ok: false,
        status: "infeasible".to_string(),
        z: None,
        x: Vec::new(),
        best_bound: None,
        gap: None,
        lp: None,
        child_jobs: Vec::new(),
        nodes_explored: 0,
        lp_solves: 0,
        elapsed_ms: 1.0,
        accelerator: AcceleratorReport::default(),
        error: Some("pruned".to_string()),
        finished_at_ms: now_ms(),
    };

    let response = aggregate_results(
        "solve-test".to_string(),
        "request-test".to_string(),
        None,
        0,
        &problem,
        &SolveOptions::default(),
        2,
        2,
        0,
        0,
        vec![optimal, infeasible],
        false,
        true,
        &state,
        Vec::new(),
    );

    assert!(response.ok);
    assert_eq!(response.status, "optimal");
    assert_eq!(response.jobs_completed, 2);
    assert_eq!(response.jobs_expected, 2);
    assert_eq!(response.jobs_redelegated, 0);
    assert_eq!(response.jobs_split, 0);
    assert_eq!(response.z, Some(90.0));
}

#[test]
fn redelegated_job_preserves_payload_and_advances_retry_id() {
    let mut job = test_job(binary_knapsack_problem());
    job.job_id = "solve-test-0".to_string();
    job.extra_constraints.push(BranchConstraint {
        coefs: vec![1.0, 0.0, 0.0, 0.0],
        rhs: 0.0,
        name: "branch_x0_le_0".to_string(),
    });

    let retry = redelegated_job(&job, 2);

    assert_eq!(retry.job_id, "solve-test-0-retry-2");
    assert_eq!(retry.solve_id, job.solve_id);
    assert_eq!(retry.request_id, job.request_id);
    assert_eq!(retry.revision, job.revision);
    assert_eq!(
        retry.problem.as_ref().map(|problem| &problem.c),
        job.problem.as_ref().map(|problem| &problem.c)
    );
    assert_eq!(retry.extra_constraints, job.extra_constraints);
    assert_ne!(retry.job_uuid, job.job_uuid);
    assert_eq!(retry.problem_id, job.problem_id);
    assert!(retry.submitted_at_ms >= job.submitted_at_ms);
}

#[test]
fn aggregate_results_treats_redelegated_attempt_as_complete() {
    let problem = binary_knapsack_problem();
    let state = test_state(NodeRole::Master);
    let optimal_retry = SubproblemResult {
        solve_id: "solve-test".to_string(),
        request_id: "request-test".to_string(),
        job_id: "job-0-retry-1".to_string(),
        job_uuid: new_uuid_string(),
        problem_id: Some("11111111-1111-4111-8111-111111111111".to_string()),
        revision: 0,
        worker_node: "worker-b".to_string(),
        ok: true,
        status: "optimal".to_string(),
        z: Some(90.0),
        x: vec![0.0, 1.0, 0.0, 1.0],
        best_bound: Some(90.0),
        gap: Some(0.0),
        lp: None,
        child_jobs: Vec::new(),
        nodes_explored: 1,
        lp_solves: 1,
        elapsed_ms: 1.0,
        accelerator: AcceleratorReport::default(),
        error: None,
        finished_at_ms: now_ms(),
    };

    let response = aggregate_results(
        "solve-test".to_string(),
        "request-test".to_string(),
        None,
        0,
        &problem,
        &SolveOptions::default(),
        1,
        2,
        1,
        0,
        vec![optimal_retry],
        false,
        true,
        &state,
        Vec::new(),
    );

    assert!(response.ok);
    assert_eq!(response.status, "optimal");
    assert_eq!(response.jobs_expected, 1);
    assert_eq!(response.jobs_published, 2);
    assert_eq!(response.jobs_completed, 1);
    assert_eq!(response.jobs_redelegated, 1);
    assert_eq!(response.jobs_split, 0);
    assert_eq!(response.z, Some(90.0));
}

#[test]
fn result_acceptance_ignores_duplicate_and_unknown_jobs() {
    let mut expected = HashSet::new();
    expected.insert("job-0".to_string());
    expected.insert("job-1".to_string());
    let mut completed = HashSet::new();
    let result = SubproblemResult {
        solve_id: "solve-test".to_string(),
        request_id: "request-test".to_string(),
        job_id: "job-0".to_string(),
        job_uuid: new_uuid_string(),
        problem_id: Some("11111111-1111-4111-8111-111111111111".to_string()),
        revision: 0,
        worker_node: "worker-a".to_string(),
        ok: true,
        status: "optimal".to_string(),
        z: Some(90.0),
        x: vec![0.0, 1.0],
        best_bound: Some(90.0),
        gap: Some(0.0),
        lp: None,
        child_jobs: Vec::new(),
        nodes_explored: 1,
        lp_solves: 1,
        elapsed_ms: 1.0,
        accelerator: AcceleratorReport::default(),
        error: None,
        finished_at_ms: now_ms(),
    };

    assert!(
        accept_subproblem_result(result.clone(), "solve-test", &expected, &mut completed)
            .unwrap()
            .is_some()
    );
    assert_eq!(completed.len(), 1);

    let duplicate =
        accept_subproblem_result(result.clone(), "solve-test", &expected, &mut completed)
            .unwrap_err();
    assert!(duplicate.contains("duplicate"));
    assert_eq!(completed.len(), 1);

    let mut unknown = result.clone();
    unknown.job_id = "job-missing".to_string();
    let warning =
        accept_subproblem_result(unknown, "solve-test", &expected, &mut completed).unwrap_err();
    assert!(warning.contains("unknown job"));
    assert_eq!(completed.len(), 1);

    let mut other_solve = result;
    other_solve.solve_id = "solve-other".to_string();
    assert!(
        accept_subproblem_result(other_solve, "solve-test", &expected, &mut completed)
            .unwrap()
            .is_none()
    );
    assert_eq!(completed.len(), 1);
}

#[test]
fn explicit_problem_id_is_trimmed_and_canonicalized() {
    let uppercase = "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE";

    let parsed = problem_id(Some(format!("  {uppercase}  ")), "request-not-a-uuid").unwrap();

    assert_eq!(parsed, "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
    assert_eq!(
        problem_id(Some("not-a-uuid".to_string()), "request-not-a-uuid"),
        Err("problemId must be a UUID".to_string())
    );
}

#[test]
fn missing_problem_id_reuses_uuid_request_id_or_generates_a_uuid() {
    let request_uuid = "11111111-2222-4333-8444-555555555555";

    assert_eq!(problem_id(None, request_uuid).unwrap(), request_uuid);
    let generated = problem_id(None, "human-readable-request").unwrap();
    assert_eq!(Uuid::parse_str(&generated).unwrap().get_version_num(), 4);
}

#[test]
fn retry_identifier_helpers_handle_plain_valid_and_malformed_suffixes() {
    assert_eq!(job_retry_root("solve-7"), "solve-7");
    assert_eq!(job_retry_index("solve-7"), 0);
    assert_eq!(job_retry_root("solve-7-retry-3"), "solve-7");
    assert_eq!(job_retry_index("solve-7-retry-3"), 3);
    assert_eq!(job_retry_root("solve-7-retry-invalid"), "solve-7");
    assert_eq!(job_retry_index("solve-7-retry-invalid"), 0);
}

#[test]
fn solve_errors_map_conflicts_dependencies_and_input_failures() {
    assert_eq!(
        solve_error_status("problem already has running solve"),
        StatusCode::CONFLICT
    );
    assert_eq!(
        solve_error_status("coordination lock busy"),
        StatusCode::CONFLICT
    );
    assert_eq!(
        solve_error_status("live-mutex request failed"),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        solve_error_status("Redis connection unavailable"),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        solve_error_status("objective contains a non-finite coefficient"),
        StatusCode::BAD_REQUEST
    );
}
