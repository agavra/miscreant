mod common;

use std::collections::HashSet;
use std::sync::OnceLock;

use common::{TestServer, commit_file, git_ok, init_repo, test_config};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Installs the process's global metrics recorder exactly once. Recorders
/// from `metrics-exporter-prometheus` are process-global state — installing
/// one is a one-shot `metrics::set_global_recorder` call that errors on a
/// second attempt — and `#[tokio::test]` functions in one binary can run
/// concurrently, so a `OnceLock` (rather than installing per test) is the
/// only safe way to share one recorder across every test in this file. That
/// sharing is also why the assertions below use `>=` against a fixture-sized
/// floor rather than exact counts: a future second test in this file would
/// contribute to the same series.
fn metrics_handle() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let builder = miscreant::telemetry::configure_byte_buckets(PrometheusBuilder::new())
                .expect("configure metrics buckets");
            let handle = builder
                .install_recorder()
                .expect("install metrics recorder");
            miscreant::telemetry::describe();
            handle
        })
        .clone()
}

/// One parsed sample line from a Prometheus text-exposition scrape: the
/// metric name with its label set verbatim (e.g. `push_total{outcome="ok"}`)
/// and its numeric value. `# HELP`/`# TYPE` comment lines are not samples.
struct Sample {
    name: String,
    value: f64,
}

fn parse_samples(body: &str) -> Vec<Sample> {
    body.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (name, value) = line.rsplit_once(' ').expect("metric line has a value");
            Sample {
                name: name.to_owned(),
                value: value.parse().expect("numeric metric value"),
            }
        })
        .collect()
}

/// The value of the sample named exactly `name` (including its label set),
/// or 0 if no such series has been recorded.
fn value_of(samples: &[Sample], name: &str) -> f64 {
    samples
        .iter()
        .find(|sample| sample.name == name)
        .map_or(0.0, |sample| sample.value)
}

/// The value of `label` inside a sample name's label set (e.g.
/// `label_value("http_requests_total{endpoint=\"x\",status=\"200\"}", "endpoint")`
/// is `Some("x")`), or `None` if the label is absent.
fn label_value<'a>(sample_name: &'a str, label: &str) -> Option<&'a str> {
    let needle = format!("{label}=\"");
    let start = sample_name.find(&needle)? + needle.len();
    let rest = &sample_name[start..];
    rest.find('"').map(|end| &rest[..end])
}

#[tokio::test(flavor = "multi_thread")]
async fn should_expose_push_and_fetch_metrics_on_the_scrape_endpoint() {
    // given: a server wired to the shared recorder, then a real push (two
    // commits, so two blobs/trees/commits are newly promoted) and a real
    // clone against it
    let server = TestServer::spawn_with_metrics(test_config(), metrics_handle()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    let url = format!("{}/metrics-repo.git", server.base_url());
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    let clone_dir = server.tempdir().join("clone");
    git_ok(
        server.tempdir(),
        &["clone", &url, clone_dir.to_str().expect("utf-8 clone path")],
    );

    let client = reqwest::Client::new();
    // A request to the excluded operational endpoint, to prove it never
    // shows up as an `http_requests_total` series below.
    client
        .get(format!("{}/healthz", server.base_url()))
        .send()
        .await
        .expect("GET /healthz");

    // when
    let scrape = client
        .get(format!("{}/metrics", server.base_url()))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("read scrape body");
    let samples = parse_samples(&scrape);

    // then: the push this test just drove landed as a whole-push success and
    // contributed at least its own two objects of each kind
    assert!(value_of(&samples, "push_total{outcome=\"ok\"}") >= 1.0);
    assert!(value_of(&samples, "objects_promoted_total{kind=\"blob\"}") >= 2.0);
    assert!(value_of(&samples, "objects_promoted_total{kind=\"tree\"}") >= 2.0);
    assert!(value_of(&samples, "objects_promoted_total{kind=\"commit\"}") >= 2.0);

    // then: the clone's fetch command was served successfully
    assert!(value_of(&samples, "fetch_total{outcome=\"ok\"}") >= 1.0);

    // then: HTTP request metrics are present for both the receive-pack and
    // upload-pack endpoints the push and clone drove
    let endpoints: HashSet<&str> = samples
        .iter()
        .filter(|sample| sample.name.starts_with("http_requests_total{"))
        .filter_map(|sample| label_value(&sample.name, "endpoint"))
        .collect();
    assert!(endpoints.contains("receive-pack"));
    assert!(endpoints.contains("upload-pack"));

    // then: /metrics and /healthz never appear in http_requests_total — the
    // endpoint label is confined to the four served git RPCs
    let allowed: HashSet<&str> = [
        "advert-upload",
        "advert-receive",
        "receive-pack",
        "upload-pack",
    ]
    .into_iter()
    .collect();
    assert!(
        endpoints.is_subset(&allowed),
        "unexpected endpoint labels: {endpoints:?}"
    );
}
