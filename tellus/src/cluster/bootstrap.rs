#[cfg(feature = "serde")]
use crate::cluster::formation::FormationProviderConfig;
use crate::cluster::{
    endpoint::{self, EndpointInner, FormError},
    formation::{Formation, FormationProviderFactory, Majority},
    membership::{JoinError, JoinRound, join_round},
};
use derive_more::Debug;
use std::{
    collections::BTreeSet,
    convert::Infallible,
    error::Error,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error as ThisError;
use tokio::time::sleep;
use tracing::{debug, warn};

/// A source of seed addresses for [bootstrap], e.g. a static list ([FixedSeeds]), DNS (the
/// `tellus-bootstrap-dns` crate) or Kubernetes pods (the `tellus-bootstrap-k8s` crate).
#[trait_variant::make(Send)]
pub trait SeedDiscovery
where
    Self: Send + 'static,
{
    /// The type of the discovery's failures.
    type Error: Error + Send + Sync + 'static;

    /// Resolve the current seed addresses, this node's own included when it is discoverable;
    /// order and duplicates are irrelevant. Called repeatedly by [bootstrap], so a failure is
    /// retried, not fatal.
    async fn resolve(&mut self) -> Result<Vec<SocketAddr>, Self::Error>;
}

/// The [SeedDiscovery] for a statically configured cluster: resolves to the same fixed
/// addresses every time.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize), serde(transparent))]
pub struct FixedSeeds(Vec<SocketAddr>);

impl FixedSeeds {
    /// A discovery resolving to exactly the given addresses.
    pub fn new(seeds: Vec<SocketAddr>) -> Self {
        Self(seeds)
    }
}

impl SeedDiscovery for FixedSeeds {
    type Error = Infallible;

    async fn resolve(&mut self) -> Result<Vec<SocketAddr>, Infallible> {
        Ok(self.0.clone())
    }
}

/// Configuration for [bootstrap], deserializable with the `serde` feature: every field falls
/// back to its `DEFAULT_*` constant, and the formation provider is chosen by name, see
/// docs/cluster.md.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedBootstrapConfig")
)]
pub struct BootstrapConfig {
    /// The minimum size of the universe, the resolved addresses united with this node's own,
    /// before a decision is taken; one allows a deliberately single-node cluster. Counting the
    /// universe rather than the resolved set keeps the floor meaningful whether or not discovery
    /// happens to resolve this node's own address. Nodes deciding on disjoint discovery views can
    /// form separate clusters, see [bootstrap]; a higher minimum shrinks that window.
    ///
    /// Defaults to [BootstrapConfig::DEFAULT_MIN_PEERS].
    pub min_peers: NonZeroUsize,

    /// How long the resolved address set must stay unchanged before the join decision is taken.
    /// A new set is never settled, so zero takes the decision on the second resolution seeing the
    /// same addresses, not on the first. Defaults to [BootstrapConfig::DEFAULT_SETTLE].
    pub settle: Duration,

    /// The pause between two resolutions. Must not be zero, which would resolve in a busy loop;
    /// defaults to [BootstrapConfig::DEFAULT_RESOLVE_INTERVAL].
    pub resolve_interval: Duration,

    /// Creates the [FormationProvider] deciding whether this node forms a new cluster when no
    /// discovered address is a member of one; the default is [Majority].
    ///
    /// [FormationProvider]: crate::cluster::formation::FormationProvider
    #[debug(skip)]
    pub formation: FormationProviderFactory,
}

impl BootstrapConfig {
    /// The `min_peers` of [BootstrapConfig::new].
    pub const DEFAULT_MIN_PEERS: NonZeroUsize = NonZeroUsize::new(2).expect("2 is not zero");

    /// The `settle` of [BootstrapConfig::new].
    pub const DEFAULT_SETTLE: Duration = Duration::from_secs(3);

    /// The `resolve_interval` of [BootstrapConfig::new].
    pub const DEFAULT_RESOLVE_INTERVAL: Duration = Duration::from_secs(1);

    /// A configuration with every field taken from its `DEFAULT_*` constant, forming by
    /// [Majority].
    pub fn new() -> Self {
        Self {
            min_peers: Self::DEFAULT_MIN_PEERS,
            settle: Self::DEFAULT_SETTLE,
            resolve_interval: Self::DEFAULT_RESOLVE_INTERVAL,
            formation: Arc::new(|| Box::new(Majority)),
        }
    }
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// The [BootstrapConfig] given to [bootstrap] is invalid.
#[derive(Debug, ThisError)]
pub enum InvalidBootstrapConfig {
    /// The configured `resolve_interval` is zero.
    #[error("resolve_interval is zero")]
    ZeroResolveInterval,
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedBootstrapConfig {
    #[serde(default)]
    min_peers: Option<NonZeroUsize>,

    #[serde(default, with = "humantime_serde")]
    settle: Option<Duration>,

    #[serde(default, with = "humantime_serde")]
    resolve_interval: Option<Duration>,

    #[serde(default)]
    formation: Option<FormationProviderConfig>,
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedBootstrapConfig> for BootstrapConfig {
    type Error = InvalidBootstrapConfig;

    fn try_from(unchecked: UncheckedBootstrapConfig) -> Result<Self, Self::Error> {
        let defaults = Self::new();

        let config = Self {
            min_peers: unchecked.min_peers.unwrap_or(defaults.min_peers),
            settle: unchecked.settle.unwrap_or(defaults.settle),
            resolve_interval: unchecked
                .resolve_interval
                .unwrap_or(defaults.resolve_interval),
            formation: unchecked
                .formation
                .map_or(defaults.formation, FormationProviderConfig::factory),
        };
        validate(&config)?;

        Ok(config)
    }
}

/// The cluster cannot be bootstrapped.
#[derive(Debug, ThisError)]
pub enum BootstrapError {
    /// The given configuration is invalid.
    #[error(transparent)]
    Config(#[from] InvalidBootstrapConfig),

    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// This node's incarnation has been downed by the cluster; only a restarted process, with a
    /// fresh incarnation, can join again, so the caller should exit.
    #[error("this node has been downed")]
    Downed,
}

/// Discover the cluster and become a member of it, once: resolve seed addresses with the given
/// discovery every [resolve_interval](BootstrapConfig::resolve_interval) until the view has
/// settled, then attempt to join through every resolved address but this node's own. The view
/// has settled once the resolved set, this node's own address included, has held at least
/// [min_peers](BootstrapConfig::min_peers) addresses unchanged for
/// [settle](BootstrapConfig::settle). A started endpoint is no cluster and refuses joins, so
/// nodes starting together admit each other nowhere. When no address is a member of a cluster,
/// the [FormationProvider](crate::cluster::formation::FormationProvider) decides whether this node
/// forms one, by default at the lowest address of a majority. Requiring a majority keeps a
/// partitioned node from coming back as a cluster of its own. A resolved set holding nothing but
/// this node's own address forms a cluster of one.
///
/// Returns at once when this node is already a member, e.g. through [form](crate::cluster::form).
/// Returns once this node is a member and does not keep running. Membership owns failure
/// detection and downing from then on. [BootstrapError::Downed], possible only when rejoining
/// after this node was downed, means the caller should exit so a restart mints the fresh
/// incarnation. A resolve failure is logged and retried, like a seed which is not a member yet,
/// and restarts the settle window, since nothing was observed during it. A settled view holding
/// fewer than `min_peers` addresses is logged as well, since it blocks every decision below and
/// would otherwise be silent. There is no internal timeout, so wrap the call in
/// [timeout](tokio::time::timeout) to bound bootstrap. Nodes
/// deciding on disjoint discovery views can still form separate clusters, which never merge on
/// their own; the settle window and the `min_peers` floor are the guard, so size `min_peers` to
/// the deployment when possible.
pub async fn bootstrap<D>(mut discovery: D, config: BootstrapConfig) -> Result<(), BootstrapError>
where
    D: SeedDiscovery,
{
    validate(&config)?;

    let endpoint = endpoint::get().ok_or(BootstrapError::EndpointNotStarted)?;
    let self_addr = endpoint.node().addr();

    let mut settle = Settle::new(config.settle);
    let mut formation = (config.formation)();
    loop {
        if endpoint.downed() {
            return Err(BootstrapError::Downed);
        }
        // Nothing left to decide, and joining a cluster this node is in churns its lanes.
        if endpoint.formed() {
            return Ok(());
        }

        match discovery.resolve().await {
            Ok(resolved) => {
                let universe = universe(resolved, self_addr);
                let settled = settle.settled(&universe, Instant::now());
                let enough_peers = universe.len() >= config.min_peers.get();

                if settled && !enough_peers {
                    warn!(
                        universe = universe.len(),
                        min_peers = config.min_peers.get(),
                        "waiting for discovery to resolve more addresses"
                    );
                }

                if settled && enough_peers {
                    release_gone_pin(endpoint, &universe);

                    let seeds = seeds(&universe, self_addr);
                    let round = match join_round(endpoint, &seeds).await {
                        Ok(round) => round,

                        Err(JoinError::Downed) => return Err(BootstrapError::Downed),

                        Err(JoinError::EndpointNotStarted | JoinError::NoSeeds) => {
                            unreachable!("join_round is given the endpoint and skips no seed list")
                        }
                    };

                    match round {
                        JoinRound::Joined => {
                            debug!("bootstrap decided: joined an existing cluster");
                            return Ok(());
                        }

                        JoinRound::ClusterSeen(addr) => {
                            debug!(cluster_addr = %addr, "a cluster admitted this node, retrying it")
                        }

                        JoinRound::NoAdmission(outcomes) => {
                            let decision = Formation {
                                universe,
                                self_addr,
                                outcomes,
                            };
                            if formation.form(&decision) && form_cluster(endpoint)? {
                                return Ok(());
                            }
                        }
                    }
                }
            }

            Err(error) => {
                settle.reset();
                warn!(%error, "cannot resolve seed addresses");
            }
        }
        sleep(config.resolve_interval).await;
    }
}

fn validate(config: &BootstrapConfig) -> Result<(), InvalidBootstrapConfig> {
    if config.resolve_interval.is_zero() {
        return Err(InvalidBootstrapConfig::ZeroResolveInterval);
    }

    Ok(())
}

/// `true` once this node is a member; `false` while a join attempt could be making it a member
/// of another cluster, so the decision has to be retaken.
fn form_cluster(endpoint: &'static EndpointInner) -> Result<bool, BootstrapError> {
    match endpoint.form_cluster() {
        Ok(()) => {
            debug!("bootstrap decided: formed a cluster");
            Ok(true)
        }

        // A concurrent join or an explicit form got there first, which is the same outcome.
        Err(FormError::AlreadyFormed) => Ok(true),

        Err(FormError::Downed) => Err(BootstrapError::Downed),

        Err(FormError::JoinInFlight | FormError::ClusterPinned(_)) => {
            debug!("formation postponed, a join attempt is unresolved");
            Ok(false)
        }

        Err(FormError::EndpointNotStarted) => unreachable!("the endpoint is started"),
    }
}

fn release_gone_pin(endpoint: &'static EndpointInner, universe: &BTreeSet<SocketAddr>) {
    if let Some(released) = endpoint.release_pin_if_gone(universe) {
        debug!(cluster_addr = %released, "released the pinned cluster, discovery dropped it");
    }
}

struct Settle {
    settle: Duration,
    current: Option<(BTreeSet<SocketAddr>, Instant)>,
}

impl Settle {
    fn new(settle: Duration) -> Self {
        Self {
            settle,
            current: None,
        }
    }

    fn settled(&mut self, resolved: &BTreeSet<SocketAddr>, at: Instant) -> bool {
        match &self.current {
            Some((current, unchanged_since)) if current == resolved => {
                at.duration_since(*unchanged_since) >= self.settle
            }

            _ => {
                self.current = Some((resolved.clone(), at));
                false
            }
        }
    }

    fn reset(&mut self) {
        self.current = None;
    }
}

/// Discovery need not resolve this node's own address, so the universe adds it.
fn universe(resolved: Vec<SocketAddr>, self_addr: SocketAddr) -> BTreeSet<SocketAddr> {
    let mut universe = BTreeSet::from_iter(resolved);
    universe.insert(self_addr);
    universe
}

fn seeds(resolved: &BTreeSet<SocketAddr>, self_addr: SocketAddr) -> Vec<SocketAddr> {
    resolved
        .iter()
        .copied()
        .filter(|addr| *addr != self_addr)
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::cluster::bootstrap::{
        BootstrapConfig, FixedSeeds, InvalidBootstrapConfig, SeedDiscovery, Settle, bootstrap,
        seeds, universe, validate,
    };
    #[cfg(feature = "serde")]
    use crate::cluster::formation::{Formation, JoinOutcome};
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        time::{Duration, Instant},
    };

    #[test]
    fn test_seeds() {
        let one = addr(1);
        let two = addr(2);
        let three = addr(3);
        let resolved = BTreeSet::from_iter([three, one, two]);

        assert_eq!(seeds(&resolved, two), vec![one, three]);
        assert_eq!(seeds(&resolved, addr(9)), vec![one, two, three]);
        assert_eq!(seeds(&BTreeSet::from_iter([one]), one), vec![]);
    }

    /// A discovery omitting this node's own address still yields the full universe, so a
    /// `min_peers` sized to the deployment is met either way.
    #[test]
    fn the_universe_adds_this_node_whether_or_not_discovery_resolved_it() {
        let one = addr(1);
        let two = addr(2);
        let three = addr(3);

        assert_eq!(
            universe(vec![two, three], one),
            BTreeSet::from([one, two, three])
        );
        assert_eq!(
            universe(vec![one, two, three], one),
            BTreeSet::from([one, two, three])
        );
        assert_eq!(universe(vec![], one), BTreeSet::from([one]));
    }

    #[test]
    fn test_settle() {
        let settle_duration = Duration::from_secs(3);
        let mut settle = Settle::new(settle_duration);
        let now = Instant::now();
        let one = BTreeSet::from_iter([addr(1)]);
        let one_two = BTreeSet::from_iter([addr(1), addr(2)]);

        assert!(!settle.settled(&one, now));
        assert!(!settle.settled(&one, now + Duration::from_secs(1)));
        assert!(settle.settled(&one, now + settle_duration));

        assert!(!settle.settled(&one_two, now + settle_duration));
        assert!(!settle.settled(&one_two, now + Duration::from_secs(5)));
        assert!(settle.settled(&one_two, now + settle_duration + settle_duration));

        settle.reset();
        let observed = now + settle_duration + settle_duration + Duration::from_secs(1);
        assert!(!settle.settled(&one_two, observed));
        assert!(!settle.settled(&one_two, now + settle_duration * 3));
        assert!(settle.settled(&one_two, observed + settle_duration));
    }

    #[tokio::test]
    async fn test_fixed_seeds() {
        let mut fixed = FixedSeeds::new(vec![addr(1), addr(2)]);

        assert_eq!(fixed.resolve().await, Ok(vec![addr(1), addr(2)]));
        assert_eq!(fixed.resolve().await, Ok(vec![addr(1), addr(2)]));
    }

    /// A new configuration carries the defaults its documentation names, so a caller only has to
    /// overwrite what it wants to differ.
    #[test]
    fn a_new_config_carries_the_documented_defaults() {
        let config = BootstrapConfig::new();

        assert_eq!(config.min_peers, BootstrapConfig::DEFAULT_MIN_PEERS);
        assert_eq!(config.settle, BootstrapConfig::DEFAULT_SETTLE);
        assert_eq!(
            config.resolve_interval,
            BootstrapConfig::DEFAULT_RESOLVE_INTERVAL
        );
    }

    /// A zero resolve interval would poll discovery in a busy loop, so it is refused before
    /// anything is resolved rather than running the loop that way.
    #[tokio::test]
    async fn a_zero_resolve_interval_is_refused() {
        let mut config = BootstrapConfig::new();
        config.resolve_interval = Duration::ZERO;

        assert!(matches!(
            validate(&config),
            Err(InvalidBootstrapConfig::ZeroResolveInterval)
        ));

        let seeds = FixedSeeds::new(vec![addr(1), addr(2)]);
        assert!(
            bootstrap(seeds, config).await.is_err(),
            "bootstrap must refuse the config before it resolves anything"
        );
    }

    /// The documented config form, which a config file provides: every field is optional, and the
    /// formation provider is chosen by name and told apart by what it decides.
    #[cfg(feature = "serde")]
    #[test]
    fn a_config_deserializes_from_its_documented_form() {
        let config = serde_json::from_str::<BootstrapConfig>(
            r#"{
                "min_peers": 5,
                "settle": "500ms",
                "resolve_interval": "100ms",
                "formation": "explicit"
            }"#,
        )
        .expect("the documented config form deserializes");

        assert_eq!(config.min_peers.get(), 5);
        assert_eq!(config.settle, Duration::from_millis(500));
        assert_eq!(config.resolve_interval, Duration::from_millis(100));

        let alone = Formation {
            universe: BTreeSet::from([addr(1)]),
            self_addr: addr(1),
            outcomes: vec![(addr(2), JoinOutcome::NoCluster)],
        };
        assert!(
            !(config.formation)().form(&alone),
            "the explicit provider never forms"
        );

        let defaults = serde_json::from_str::<BootstrapConfig>("{}").expect("every field defaults");
        assert_eq!(defaults.min_peers, BootstrapConfig::DEFAULT_MIN_PEERS);
        assert_eq!(defaults.settle, BootstrapConfig::DEFAULT_SETTLE);
        assert_eq!(
            defaults.resolve_interval,
            BootstrapConfig::DEFAULT_RESOLVE_INTERVAL
        );
        assert!(
            (defaults.formation)().form(&alone),
            "the default majority provider forms at the lowest address of a majority"
        );

        assert!(
            serde_json::from_str::<BootstrapConfig>(r#"{ "min_pears": 5 }"#).is_err(),
            "a misspelled key must not be a silent default"
        );
        assert!(
            serde_json::from_str::<BootstrapConfig>(r#"{ "resolve_interval": "0s" }"#).is_err()
        );
    }

    /// A fixed seed list is a plain sequence of addresses in a config file.
    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn fixed_seeds_deserialize_from_a_sequence() {
        let mut seeds =
            serde_json::from_str::<FixedSeeds>(r#"["127.0.0.1:4242", "127.0.0.2:4242"]"#)
                .expect("a sequence of addresses deserializes");

        assert_eq!(seeds.resolve().await, Ok(vec![addr(1), addr(2)]));
    }

    fn addr(n: u8) -> SocketAddr {
        format!("127.0.0.{n}:4242").parse().expect("valid address")
    }
}
