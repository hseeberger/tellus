//! The verifier of the cluster demo: it polls every node and the load balancer. Once the chaos
//! agent has been quiet long enough for the cluster to have recovered, it asserts what the cluster
//! promises: every node is a member, every node sees every other as Up, and every node can still
//! message every other.
//!
//! Violations are logged, counted and served at `/violations`; the process keeps running, since
//! this is a forever test rather than a run to completion.

use anyhow::Context;
use axum::{Json, Router, extract::State, routing::get};
use reqwest::Client;
use serde::Serialize;
use std::{
    collections::BTreeSet,
    env,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tellus_cluster_demo::{ClusterView, Phase, ProbeOutcome, ProbeReport};
use tokio::{
    fs,
    net::TcpListener,
    sync::Mutex,
    time::{Instant, sleep},
};
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const HTTP_PORT: u16 = 8080;
const CHECK_INTERVAL: Duration = Duration::from_secs(2);
const LB_INTERVAL: Duration = Duration::from_millis(500);
const HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const VIOLATIONS_KEPT: usize = 200;
const QUIET: &str = "quiet";
const UNKNOWN: &str = "unknown";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Arc::new(Config::from_env()?);
    let report = Arc::new(Mutex::new(Report::new()));
    info!(nodes = ?config.nodes, lb = %config.lb, "verifying");

    tokio::spawn(serve_http(report.clone()));
    tokio::spawn(watch_lb(config.clone(), report.clone()));

    verify(config, report).await
}

/// The verifier's configuration, all of it from the environment.
struct Config {
    nodes: Vec<String>,
    lb: String,
    chaos_state: PathBuf,
    settle: Duration,
    lb_outage: Duration,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let nodes = var("TELLUS_NODES")?
            .split(',')
            .map(|node| node.trim().to_string())
            .filter(|node| !node.is_empty())
            .collect::<Vec<_>>();
        let lb = var("TELLUS_LB")?;
        let chaos_state = var("TELLUS_CHAOS_STATE")?.into();
        let settle = seconds("TELLUS_SETTLE_SECS")?;
        let lb_outage = seconds("TELLUS_LB_OUTAGE_SECS")?;

        Ok(Self {
            nodes,
            lb,
            chaos_state,
            settle,
            lb_outage,
        })
    }
}

/// What the verifier has seen so far, served at `/status` and `/violations`.
struct Report {
    chaos: String,
    verifications: usize,
    violations: usize,
    lb_requests: usize,
    lb_failures: usize,
    longest_lb_outage_millis: u128,
    recent_violations: Vec<Violation>,
}

impl Report {
    fn new() -> Self {
        Self {
            chaos: UNKNOWN.to_string(),
            verifications: 0,
            violations: 0,
            lb_requests: 0,
            lb_failures: 0,
            longest_lb_outage_millis: 0,
            recent_violations: Vec::new(),
        }
    }

    fn violated(&mut self, detail: String) {
        error!(detail, "VIOLATION");
        self.violations += 1;
        self.recent_violations.push(Violation { at: now(), detail });
        if self.recent_violations.len() > VIOLATIONS_KEPT {
            self.recent_violations.remove(0);
        }
    }
}

#[derive(Serialize, Clone)]
struct Violation {
    at: u64,
    detail: String,
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn var(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("{name} is not set"))
}

fn seconds(name: &str) -> anyhow::Result<Duration> {
    let seconds = var(name)?
        .parse()
        .with_context(|| format!("{name} is not a number of seconds"))?;

    Ok(Duration::from_secs(seconds))
}

async fn serve_http(report: Arc<Mutex<Report>>) {
    let router = Router::new()
        .route("/status", get(status))
        .route("/violations", get(violations))
        .with_state(report);

    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, HTTP_PORT));
    match TcpListener::bind(addr).await {
        Ok(listener) => {
            if let Err(error) = axum::serve(listener, router).await {
                error!(%error, "HTTP API stopped");
            }
        }

        Err(error) => error!(%error, %addr, "cannot bind the HTTP API"),
    }
}

async fn status(State(report): State<Arc<Mutex<Report>>>) -> Json<serde_json::Value> {
    let report = report.lock().await;
    Json(serde_json::json!({
        "chaos": report.chaos,
        "verifications": report.verifications,
        "violations": report.violations,
        "lb_requests": report.lb_requests,
        "lb_failures": report.lb_failures,
        "longest_lb_outage_millis": report.longest_lb_outage_millis,
    }))
}

async fn violations(State(report): State<Arc<Mutex<Report>>>) -> Json<Vec<Violation>> {
    let report = report.lock().await;
    Json(report.recent_violations.clone())
}

/// The claim the load balancer stands for: whatever the chaos agent does to the nodes behind it,
/// a request is answered. A single failed request is the health check's detection lag, an outage
/// beyond [Config::lb_outage] is a violation.
async fn watch_lb(config: Arc<Config>, report: Arc<Mutex<Report>>) {
    let client = client();
    let url = format!("{}/cluster", config.lb);
    let mut failing_since = None;

    loop {
        let ok = client
            .get(&url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());

        let mut report = report.lock().await;
        report.lb_requests += 1;
        if ok {
            failing_since = None;
        } else {
            report.lb_failures += 1;
            let since = *failing_since.get_or_insert_with(Instant::now);
            let outage = since.elapsed();
            report.longest_lb_outage_millis =
                report.longest_lb_outage_millis.max(outage.as_millis());
            if outage > config.lb_outage {
                report.violated(format!("load balancer unavailable for {outage:?}"));
                failing_since = Some(Instant::now());
            }
        }
        drop(report);

        sleep(LB_INTERVAL).await;
    }
}

/// Verifies once per quiet window: the chaos agent's state file names the fault it is running,
/// so the cluster is only held to its promises once it has had [Config::settle] without one.
async fn verify(config: Arc<Config>, report: Arc<Mutex<Report>>) -> anyhow::Result<()> {
    let client = client();
    let mut quiet_since = Instant::now();
    let mut verified = false;

    loop {
        sleep(CHECK_INTERVAL).await;

        let chaos = chaos_state(&config.chaos_state).await;
        report.lock().await.chaos = chaos.clone();
        if chaos != QUIET {
            quiet_since = Instant::now();
            verified = false;
            continue;
        }
        if verified || quiet_since.elapsed() < config.settle {
            continue;
        }

        let violations = verify_cluster(&client, &config.nodes).await;
        let mut report = report.lock().await;
        report.verifications += 1;
        if violations.is_empty() {
            info!(quiet_for = ?quiet_since.elapsed(), "cluster verified");
        }
        for violation in violations {
            report.violated(violation);
        }
        verified = true;
    }
}

/// An unreadable state file is not quiet: until the chaos agent says what it is doing, the
/// cluster is not held to anything.
async fn chaos_state(path: &Path) -> String {
    match fs::read_to_string(path).await {
        Ok(state) => state.trim().to_string(),

        Err(error) => {
            debug!(%error, "cannot read the chaos state");
            UNKNOWN.to_string()
        }
    }
}

async fn verify_cluster(client: &Client, nodes: &[String]) -> Vec<String> {
    let mut violations = Vec::new();

    let mut views = Vec::with_capacity(nodes.len());
    for node in nodes {
        match cluster_view(client, node).await {
            Ok(view) => views.push(view),
            Err(error) => violations.push(format!("{node} did not answer: {error}")),
        }
    }
    if views.len() < nodes.len() {
        return violations;
    }

    if let Some(split) = split_brain(&views) {
        violations.push(split);
        return violations;
    }

    let expected = views.iter().map(|view| view.addr).collect::<BTreeSet<_>>();
    for view in &views {
        if view.phase != Phase::Joined {
            violations.push(format!("{} is {:?}, not joined", view.name, view.phase));
        }

        let up = view.up_addrs();
        if up != expected {
            let missing = expected.difference(&up).collect::<Vec<_>>();
            violations.push(format!("{} does not see {missing:?} as Up", view.name));
        }
    }

    for node in nodes {
        match probe_report(client, node).await {
            Ok(report) => {
                for probe in report.probes {
                    if let ProbeOutcome::Failed { error } = probe.outcome {
                        violations.push(format!(
                            "{} cannot message {}: {error}",
                            report.name, probe.addr
                        ));
                    }
                }
            }

            Err(error) => violations.push(format!("{node} did not answer a probe: {error}")),
        }
    }

    violations
}

/// Two members whose Up sets do not overlap are two clusters, which never merge on their own. It
/// is the one failure no single node's view shows, since each side looks healthy to itself. A
/// node still bootstrapping is no cluster, so its view of itself alone is not one side of one.
fn split_brain(views: &[ClusterView]) -> Option<String> {
    let joined = views
        .iter()
        .filter(|view| view.phase == Phase::Joined)
        .collect::<Vec<_>>();
    for (index, one) in joined.iter().enumerate() {
        for other in &joined[index + 1..] {
            if one.up_addrs().is_disjoint(&other.up_addrs()) {
                return Some(format!(
                    "split brain: {} sees {:?}, {} sees {:?}",
                    one.name,
                    one.up_addrs(),
                    other.name,
                    other.up_addrs()
                ));
            }
        }
    }

    None
}

async fn cluster_view(client: &Client, node: &str) -> anyhow::Result<ClusterView> {
    let view = client
        .get(format!("{node}/cluster"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(view)
}

async fn probe_report(client: &Client, node: &str) -> anyhow::Result<ProbeReport> {
    let report = client
        .get(format!("{node}/probe"))
        .timeout(PROBE_TIMEOUT)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(report)
}

fn client() -> Client {
    Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("HTTP client")
}

/// Seconds since the epoch: the verifier's own log lines carry the readable timestamp, this
/// only has to order the violations served at `/violations`.
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use crate::split_brain;
    use std::net::SocketAddr;
    use tellus::cluster::MemberState;
    use tellus_cluster_demo::{ClusterView, MemberView, Phase};

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn view(name: &str, port: u16, phase: Phase, up: &[u16]) -> ClusterView {
        ClusterView {
            name: name.to_string(),
            addr: addr(port),
            phase,
            members: up
                .iter()
                .map(|port| MemberView {
                    addr: addr(*port),
                    state: MemberState::Up,
                })
                .collect(),
        }
    }

    /// Two members which do not see each other are two clusters.
    #[test]
    fn two_disjoint_members_are_a_split_brain() {
        let views = [
            view("node1", 1, Phase::Joined, &[1, 2]),
            view("node3", 3, Phase::Joined, &[3, 4, 5]),
        ];

        assert!(split_brain(&views).is_some());
    }

    /// A node still bootstrapping lists only itself and is no cluster, so two of them are not two
    /// clusters, and neither is one beside a member.
    #[test]
    fn bootstrapping_nodes_are_not_a_split_brain() {
        let two_bootstrapping = [
            view("node1", 1, Phase::Bootstrapping, &[1]),
            view("node2", 2, Phase::Bootstrapping, &[2]),
        ];
        let one_beside_a_member = [
            view("node1", 1, Phase::Bootstrapping, &[1]),
            view("node3", 3, Phase::Joined, &[3, 4, 5]),
        ];

        assert!(split_brain(&two_bootstrapping).is_none());
        assert!(split_brain(&one_beside_a_member).is_none());
    }
}
