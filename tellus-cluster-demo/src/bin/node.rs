//! A node of the cluster demo: a tellus remoting endpoint which bootstraps via DNS or Kubernetes
//! seed discovery, registers a worker under a well known key and serves an HTTP API answering
//! what it sees of the cluster and whether it can still message every other member.
//!
//! Configured by `config/default.yaml` plus one of the `config/dns.yaml` and `config/k8s.yaml`
//! overlays, which is what chooses the discovery, plus the per-node `CFG__` environment overrides
//! [configured](https://github.com/hseeberger/configured) layers on top, see `Config`. A node
//! downed by the cluster, e.g. the minority side of a partition self-downing, exits, so the
//! orchestrator's restart mints the fresh incarnation which alone can rejoin.

use anyhow::{Context, anyhow};
use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use configured::{Case, Configured};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    convert::Infallible,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorContext, ActorSystem, Control, Incoming, ReplyTo,
    cluster::{
        self, BootstrapConfig, EndpointConfig, Key, Member, MemberState, SeedDiscovery,
        transport::{ConnectedControl, QuicConnection, QuicTransport, Transport, TransportError},
    },
};
use tellus_bootstrap_dns::{DnsSeeds, Query};
use tellus_bootstrap_k8s::{K8sSeeds, Pods};
use tellus_cluster_demo::{ClusterView, MemberView, Phase, Probe, ProbeOutcome, ProbeReport};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    sync::watch,
    task::JoinSet,
    time::{sleep, timeout},
};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const HTTP_PORT: u16 = 8080;
const WORKER_KEY: &str = "worker";
const SELF_DOWN_INTERVAL: Duration = Duration::from_secs(1);
const DOWNED_LINGER: Duration = Duration::from_secs(3);
const LEAVE_TIMEOUT: Duration = Duration::from_secs(10);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const ASK_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::load(Case::Snake).context("load configuration")?;
    let addr = config.endpoint.advertised_addr;
    start_endpoint(config.endpoint)?;

    let state = Arc::new(NodeState::new(config.node_name.clone(), addr));
    tokio::spawn(serve_http(state.clone()));
    info!(name = %config.node_name, %addr, "endpoint started, bootstrapping");

    let bootstrap = match config.seeds {
        Seeds::Dns(query) => {
            let seeds = DnsSeeds::new(query).context("DNS seed discovery")?;
            bootstrap_unless_terminated(seeds, config.bootstrap).await?
        }

        Seeds::K8s(pods) => {
            let seeds = K8sSeeds::new(pods)
                .await
                .context("Kubernetes seed discovery")?;
            bootstrap_unless_terminated(seeds, config.bootstrap).await?
        }
    };
    if matches!(bootstrap, Bootstrap::Terminated) {
        info!("terminating before having joined");
        return Ok(());
    }

    let system = ActorSystem::new(Worker { addr });
    cluster::register(&Key::new(WORKER_KEY), system.root()).context("registering the worker")?;
    state.set_phase(Phase::Joined);
    info!(name = %config.node_name, "joined the cluster");

    tokio::select! {
        () = await_self_down(addr) => {
            state.set_phase(Phase::Downed);
            error!("downed by the cluster, exiting so a restart rejoins");
            sleep(DOWNED_LINGER).await;
            Err(anyhow!("downed by the cluster"))
        }

        () = await_termination() => {
            info!("terminating, leaving the cluster");
            system.root().tell(Job::Stop);
            timeout(LEAVE_TIMEOUT, cluster::leave_on_terminated(system))
                .await
                .context("the departure was not announced within the timeout")?
                .context("leaving the cluster")
        }
    }
}

/// This node's configuration: `config/default.yaml` for everything the five nodes share, one of
/// the `config/dns.yaml` and `config/k8s.yaml` overlays for `seeds`, the `CFG__NODE_NAME` and
/// `CFG__ENDPOINT__ADVERTISED_ADDR` environment overrides for what differs per node.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node_name: String,
    seeds: Seeds,
    bootstrap: BootstrapConfig,
    endpoint: EndpointConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Seeds {
    Dns(Query),
    K8s(Pods),
}

/// What the HTTP API needs beyond the process-wide remoting endpoint.
struct NodeState {
    name: String,
    addr: SocketAddr,
    phase: watch::Sender<Phase>,
}

impl NodeState {
    fn new(name: String, addr: SocketAddr) -> Self {
        Self {
            name,
            addr,
            phase: watch::Sender::new(Phase::Bootstrapping),
        }
    }

    fn phase(&self) -> Phase {
        *self.phase.borrow()
    }

    fn set_phase(&self, phase: Phase) {
        self.phase.send_replace(phase);
    }
}

enum Bootstrap {
    Joined,
    Terminated,
}

/// The actor every node registers: it answers a ping, so probing proves messaging works, not
/// only that the member lists agree.
struct Worker {
    addr: SocketAddr,
}

impl Actor for Worker {
    type Message = Job;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(job) = incoming else {
            unreachable!("worker only receives Job")
        };

        match job {
            Job::Ping { reply_to } => {
                reply_to.reply(Pong {
                    addr: self.addr,
                    up_members: up_addrs().len(),
                });
                Ok(Control::Continue(state))
            }

            Job::Stop => Ok(Control::Stop),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Job {
    Ping { reply_to: ReplyTo<Pong> },
    Stop,
}

#[derive(Serialize, Deserialize)]
struct Pong {
    addr: SocketAddr,
    up_members: usize,
}

/// QUIC gives up on a silent address only after its own handshake timeout, tens of seconds, and
/// a partition leaves three of the four seeds silent: unbounded, one bootstrap round would
/// outlast the fault it is meant to survive.
struct BoundedConnect(QuicTransport);

impl Transport for BoundedConnect {
    type Connection = QuicConnection;

    fn data_streams(&self) -> Option<NonZeroUsize> {
        self.0.data_streams()
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        max_frame_size: usize,
    ) -> Result<ConnectedControl<QuicConnection>, TransportError> {
        match timeout(CONNECT_TIMEOUT, self.0.connect(addr, max_frame_size)).await {
            Ok(connected) => connected,
            Err(_) => Err(TransportError::other(anyhow!("connect timeout"))),
        }
    }

    async fn accept(&self, max_frame_size: usize) -> Result<QuicConnection, TransportError> {
        self.0.accept(max_frame_size).await
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// The dev transport, which does not verify certificates: this cluster runs on a private Docker
/// network, a production one would use [QuicTransport::mutual_tls].
///
/// Binding the advertised address rather than the unspecified one is what makes the dev
/// certificate cover it, since it carries the bind address's IP and peers bind an advertised
/// address to the identity proving it.
fn start_endpoint(config: EndpointConfig) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(config.advertised_addr).context("dev QUIC transport")?;
    let transport = BoundedConnect(transport);

    cluster::start_endpoint(config, transport).context("remoting endpoint")
}

async fn serve_http(state: Arc<NodeState>) {
    let router = Router::new()
        .route("/health", get(health))
        .route("/cluster", get(cluster))
        .route("/probe", get(probe))
        .with_state(state);

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

/// The load balancer's health check: a node downed by the cluster is taken out of rotation
/// before its process exits.
async fn health(State(state): State<Arc<NodeState>>) -> StatusCode {
    if state.phase() == Phase::Downed {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

async fn cluster(State(state): State<Arc<NodeState>>) -> Json<ClusterView> {
    let members = members()
        .into_iter()
        .map(|member| MemberView {
            addr: member.addr(),
            state: member.state(),
        })
        .collect();

    Json(ClusterView {
        name: state.name.clone(),
        addr: state.addr,
        phase: state.phase(),
        members,
    })
}

async fn probe(State(state): State<Arc<NodeState>>) -> Json<ProbeReport> {
    let mut tasks = JoinSet::new();
    for addr in up_addrs().into_iter().filter(|addr| *addr != state.addr) {
        tasks.spawn(async move {
            Probe {
                addr,
                outcome: probe_member(addr).await,
            }
        });
    }

    let mut probes = tasks.join_all().await;
    probes.sort_by_key(|probe| probe.addr);

    Json(ProbeReport {
        name: state.name.clone(),
        probes,
    })
}

/// Resolving the worker on every probe rather than caching it keeps the probe honest: discovery
/// crosses the network too, so a member whose lookups fail is not reported as reachable.
async fn probe_member(addr: SocketAddr) -> ProbeOutcome {
    let start = Instant::now();

    let worker = match timeout(LOOKUP_TIMEOUT, cluster::lookup(&Key::new(WORKER_KEY), addr)).await {
        Ok(Ok(worker)) => worker,

        Ok(Err(error)) => {
            return ProbeOutcome::Failed {
                error: error.to_string(),
            };
        }

        Err(_) => {
            return ProbeOutcome::Failed {
                error: "lookup timeout".to_string(),
            };
        }
    };

    match worker
        .ask(ASK_TIMEOUT, |reply_to| Job::Ping { reply_to })
        .await
    {
        Ok(pong) => ProbeOutcome::Ok {
            millis: start.elapsed().as_millis(),
            up_members: pong.up_members,
        },

        Err(error) => ProbeOutcome::Failed {
            error: error.to_string(),
        },
    }
}

async fn bootstrap_unless_terminated<D>(
    seeds: D,
    config: BootstrapConfig,
) -> anyhow::Result<Bootstrap>
where
    D: SeedDiscovery,
{
    tokio::select! {
        result = cluster::bootstrap(seeds, config) => {
            result.context("bootstrapping the cluster")?;
            Ok(Bootstrap::Joined)
        }

        () = await_termination() => Ok(Bootstrap::Terminated),
    }
}

/// A node is downed once nothing lists its address as Up anymore; its own entry cannot be told
/// apart from an earlier incarnation's, which a rejoined node's member list still carries.
async fn await_self_down(addr: SocketAddr) {
    loop {
        sleep(SELF_DOWN_INTERVAL).await;
        if !up_addrs().contains(&addr) {
            return;
        }
    }
}

async fn await_termination() {
    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut interrupt = signal(SignalKind::interrupt()).expect("SIGINT handler");

    tokio::select! {
        _ = terminate.recv() => (),
        _ = interrupt.recv() => (),
    }
}

fn up_addrs() -> BTreeSet<SocketAddr> {
    members()
        .into_iter()
        .filter(|member| member.state() == MemberState::Up)
        .map(|member| member.addr())
        .collect()
}

fn members() -> Vec<Member> {
    cluster::members().expect("remoting endpoint started")
}

#[cfg(test)]
mod tests {
    use crate::{Config, Seeds};
    use configured::{CONFIG_DIR, CONFIG_ENV_PREFIX, CONFIG_OVERLAYS, Case, Configured};
    use std::{env, net::SocketAddr, num::NonZeroU16};
    use tellus_bootstrap_dns::Query;
    use tellus_bootstrap_k8s::{Pods, Port};

    /// The shipped configuration must keep deserializing into tellus's own config types, which
    /// this demo is outside of `just all` to notice; the per-node overrides are what the compose
    /// file and the manifests set, and each overlay must contribute exactly its own discovery.
    /// Both overlays are asserted by one test, since the loader reads process-global environment
    /// variables which two tests would race on.
    #[test]
    fn the_shipped_config_loads() {
        unsafe {
            env::set_var(CONFIG_DIR, concat!(env!("CARGO_MANIFEST_DIR"), "/config"));
            env::remove_var(CONFIG_ENV_PREFIX);
            env::set_var(CONFIG_OVERLAYS, "dns");
            env::set_var("CFG__NODE_NAME", "node1");
            env::set_var("CFG__ENDPOINT__ADVERTISED_ADDR", "172.28.0.11:7878");
        }

        let config = Config::load(Case::Snake).expect("the dns overlay loads");

        assert_eq!(config.node_name, "node1");
        assert_eq!(
            config.endpoint.advertised_addr,
            "172.28.0.11:7878".parse::<SocketAddr>().expect("valid")
        );
        assert_eq!(config.bootstrap.min_peers.get(), 5);
        let Seeds::Dns(query) = config.seeds else {
            panic!("the dns overlay yields DNS discovery")
        };
        assert_eq!(
            query,
            Query::Ip {
                name: "tellus".to_string(),
                port: NonZeroU16::new(7_878).expect("7878 is not zero"),
            }
        );

        unsafe { env::set_var(CONFIG_OVERLAYS, "k8s") };

        let config = Config::load(Case::Snake).expect("the k8s overlay loads");

        let Seeds::K8s(pods) = config.seeds else {
            panic!("the k8s overlay yields Kubernetes discovery")
        };
        assert_eq!(
            pods,
            Pods {
                namespace: None,
                label_selector: "app=tellus".to_string(),
                port: Port::Number(NonZeroU16::new(7_878).expect("7878 is not zero")),
            }
        );

        unsafe { env::remove_var(CONFIG_OVERLAYS) };

        assert!(Config::load(Case::Snake).is_err());
    }
}
