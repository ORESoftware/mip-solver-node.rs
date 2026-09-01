//! Cross-layer regression contract for Linear DEN-679.
//!
//! This test intentionally stays credential-free and does not require a live
//! cluster. It proves that the deployed master/slave manifests and the Rust
//! HTTP surface continue to agree on the Prometheus scrape contract.

const MAIN_RS: &str = include_str!("../src/main.rs");
const HTTP_API_RS: &str = include_str!("../src/http_api.rs");
const MASTER_DEPLOYMENT: &str = include_str!("../k8s/master-deployment.yaml");
const SLAVE_DEPLOYMENT: &str = include_str!("../k8s/slave-deployment.yaml");

#[test]
fn metrics_route_and_prometheus_content_type_remain_stable() {
    assert!(
        MAIN_RS.contains(".route(\"/metrics\", get(metrics))"),
        "the application router must expose GET /metrics"
    );
    assert!(
        HTTP_API_RS.contains("text/plain; version=0.0.4"),
        "the endpoint must retain the Prometheus text exposition content type"
    );

    for required_metric in [
        "dd_mip_solver_http_requests_total",
        "dd_mip_solver_solve_requests_total",
        "dd_mip_solver_solve_cancel_requests_total",
        "dd_mip_solver_subproblem_jobs_in_flight",
        "dd_mip_solver_workers_known",
        "dd_mip_solver_active_solves",
        "dd_mip_solver_errors_total",
    ] {
        assert!(
            HTTP_API_RS.contains(required_metric),
            "missing required bounded metric: {required_metric}"
        );
    }
}

#[test]
fn master_and_slave_scrape_annotations_match_the_http_contract() {
    for (role, manifest) in [
        ("master", MASTER_DEPLOYMENT),
        ("slave", SLAVE_DEPLOYMENT),
    ] {
        assert!(
            manifest.contains("prometheus.io/scrape: 'true'"),
            "{role} deployment must opt into Prometheus scraping"
        );
        assert!(
            manifest.contains("prometheus.io/port: '8117'"),
            "{role} deployment must expose the solver HTTP/metrics port"
        );
        assert!(
            manifest.contains("prometheus.io/path: /metrics"),
            "{role} deployment must scrape the stable /metrics path"
        );
    }
}

#[test]
fn metrics_exposition_does_not_add_sensitive_or_unbounded_domain_labels() {
    let start = HTTP_API_RS
        .find("pub(super) async fn metrics")
        .expect("metrics handler must exist");
    let remainder = &HTTP_API_RS[start..];
    let end = remainder
        .find("pub(super) async fn example")
        .expect("metrics handler must end before the example handler");
    let metrics_handler = &remainder[..end];

    for forbidden_label in [
        "problem_id=",
        "request_id=",
        "job_id=",
        "user_id=",
        "tenant_id=",
        "file_path=",
        "variable_name=",
        "constraint_name=",
        "variables=",
        "constraints=",
    ] {
        assert!(
            !metrics_handler.contains(forbidden_label),
            "sensitive or high-cardinality label key entered /metrics: {forbidden_label}"
        );
    }
}
