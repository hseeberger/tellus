#[cfg(feature = "serde")]
use crate::cluster::{
    codec::CodecConfig, downing::DowningProviderConfig, failure::FailureDetectorConfig,
};
use crate::{
    ActorRef, Backoff,
    actor_context::PanicPayload,
    cluster::{
        codec::{Codec, Postcard},
        downing::{Disconnected, Downing, DowningProvider, DowningProviderFactory, KeepMajority},
        failure::{
            self, FailureDetectorFactory, Liveness, PeerLiveness, PhiAccrualFailureDetector,
        },
        frame::{Frame, RefusalReason},
        lanes::{Generation, LaneError, Lanes},
        membership::{self, DownOutcome, Member, MemberState, Membership},
        node::NodeId,
        peer::{ConnectError, DialRequest, JoinRequest, accept_loop, dial_loop, join_loop},
        reachability::{self, Reachability, WireReachability},
        receptionist::{self, PublishedChange, Receptionist, Snapshot},
        registry::{Registered, Registry, TooManyRegistrations},
        reply::{self, PendingReplies},
        state::{Changes, ClusterState},
        transport::Transport,
        watch::{WatcherTable, WireWatchTable},
    },
    sync::lock,
};
use derive_more::Debug;
use flume::Sender;
use serde::de::DeserializeOwned;
#[cfg(feature = "cluster-dev")]
use std::sync::atomic::AtomicU64;
use std::{
    cell::Cell,
    collections::{BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    num::NonZeroUsize,
    ops::ControlFlow,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    sync::{broadcast, oneshot, watch},
    task::{self, JoinHandle},
    time::{MissedTickBehavior, interval, timeout},
};
use tracing::{debug, error, info, warn};

/// Above this tokio's broadcast channel panics rather than rounding up.
const MAX_CHANGE_CAPACITY: usize = usize::MAX / 2;

static ENDPOINT: OnceLock<EndpointInner> = OnceLock::new();

thread_local! {
    static UPDATING: Cell<bool> = const { Cell::new(false) };
}

/// Configuration for [start_endpoint], deserializable with the `serde` feature: the advertised
/// address is required, every other field falls back to its `DEFAULT_*` constant, and the
/// pluggable families are chosen by name, see docs/cluster.md.
#[derive(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedEndpointConfig")
)]
pub struct EndpointConfig {
    /// The address other nodes reach this node at; together with a per-process incarnation it is
    /// the node's identity carried inside serialized references.
    pub advertised_addr: SocketAddr,

    /// The capacity of the outbound queue per peer node; messages told to a full queue are
    /// dropped as dead letters, while system frames like terminated signals bypass the capacity
    /// and are never dropped. The capacity covers the whole peer, however many streams carry it.
    /// Defaults to [EndpointConfig::DEFAULT_OUTBOUND_CAPACITY].
    pub outbound_capacity: NonZeroUsize,

    /// The upper bound on the data streams opened per peer connection, which frames are spread
    /// over by the actor they are delivered to, so a large message only delays frames towards
    /// targets hashing onto the same stream. A transport without streams lowers this to a single
    /// lane carrying everything, and `1` does the same for one with. A transport with fewer
    /// streams than this lowers it to what it reports, e.g.
    /// [QuicTransport::DATA_STREAMS](crate::cluster::transport::QuicTransport::DATA_STREAMS).
    ///
    /// All of them are opened at connection setup, so a peer admitting fewer concurrent streams
    /// fails the connection right there instead of stalling a stream later.
    ///
    /// Defaults to [EndpointConfig::DEFAULT_MAX_STREAMS_PER_PEER].
    pub max_streams_per_peer: NonZeroUsize,

    /// The maximum size of an encoded frame in bytes: an outbound frame beyond it becomes a
    /// local dead letter before reaching the transport, an inbound one is refused.
    /// [start_endpoint] refuses a value too small to hold one member of a snapshot chunk.
    /// Defaults to [EndpointConfig::DEFAULT_MAX_FRAME_SIZE].
    pub max_frame_size: NonZeroUsize,

    /// The codec encoding and decoding message payloads. Defaults to [Postcard]; from a config
    /// file one of the provided codecs is chosen by name, see docs/cluster.md.
    #[debug(skip)]
    pub codec: Arc<dyn Codec>,

    /// The interval at which the member list is gossiped to every Up member; the gossip frames
    /// double as heartbeats, and the tick also evaluates failure detection and polls the downing
    /// provider. Must not be zero; defaults to [EndpointConfig::DEFAULT_HEARTBEAT_INTERVAL].
    pub heartbeat_interval: Duration,

    /// The interval at which this node's remote watches are re-asserted with idempotent `Watch`
    /// frames, healing a `Terminated` frame lost with the watched side's connection. Must stay
    /// below the failure detection deadline, else such a loss on a pair with no other watches is
    /// healed by a false node death instead of by the watched node's answer.
    ///
    /// Must not be zero; defaults to [EndpointConfig::DEFAULT_WATCH_REFRESH_INTERVAL].
    pub watch_refresh_interval: Duration,

    /// Creates the [FailureDetector] for a member, deciding when it is locally unreachable; the
    /// default is a [PhiAccrualFailureDetector] with its defaults. [DeadlineFailureDetector]
    /// remains the deterministic choice, e.g. for tests. From a config file one of the provided
    /// detectors is chosen by name, carrying its own tuning, see docs/cluster.md.
    ///
    /// [start_endpoint] arms one detector, feeds it a burst of heartbeats with no time between
    /// them and refuses the configuration unless the member is still available after the longer
    /// of `heartbeat_interval` and `watch_refresh_interval`: every inbound frame is a heartbeat,
    /// so traffic teaches a detector the shortest intervals there are.
    ///
    /// [DeadlineFailureDetector]: crate::cluster::failure::DeadlineFailureDetector
    /// [FailureDetector]: crate::cluster::failure::FailureDetector
    /// [PhiAccrualFailureDetector]: crate::cluster::failure::PhiAccrualFailureDetector
    #[debug(skip)]
    pub failure_detector: FailureDetectorFactory,

    /// Creates the [DowningProvider] deciding when a member outside this node's connected
    /// reachability component is downed; the default is [KeepMajority] with its own default
    /// deadline, which resolves a partition towards one side, see docs/cluster.md. From a config
    /// file one of the provided providers is chosen by name, carrying its own deadline.
    ///
    /// [DowningProvider]: crate::cluster::downing::DowningProvider
    /// [KeepMajority]: crate::cluster::downing::KeepMajority
    #[debug(skip)]
    pub downing_provider: DowningProviderFactory,

    /// How long a detailed Down member is listed after it was downed. Must comfortably exceed the
    /// gossip convergence time so the detailed entry normally reaches every current member.
    /// Defaults to [EndpointConfig::DEFAULT_DOWN_RETENTION].
    pub down_retention: Duration,

    /// The maximum number of per-address Down watermarks kept and sent on connection setup; above
    /// it the oldest incarnation is evicted. Eviction bounds this endpoint's memory and its setup
    /// cost but lets a zombie at the evicted address be admitted again. See docs/cluster.md.
    ///
    /// Defaults to [EndpointConfig::DEFAULT_DOWN_WATERMARKS].
    pub down_watermarks: NonZeroUsize,

    /// How long [leave](fn@crate::cluster::leave) waits for every peer to acknowledge the
    /// announced departure before severing anyway. Bounds a shutting down process, so it is sized
    /// by the deployment's termination grace period rather than by the gossip cadence; an
    /// acknowledgement costs a round trip. Defaults to [EndpointConfig::DEFAULT_LEAVE_TIMEOUT].
    pub leave_timeout: Duration,

    /// The bounds pacing reconnection of a lost connection: the first attempt waits the minimum,
    /// each further one doubles it up to the maximum. Defaults to
    /// [EndpointConfig::DEFAULT_RECONNECT_BACKOFF_MIN] and
    /// [EndpointConfig::DEFAULT_RECONNECT_BACKOFF_MAX].
    pub reconnect_backoff: Backoff,

    /// After this many failed connection attempts an address no Up member advertises is given up
    /// and its queued messages become dead letters; a member's address is retried until the
    /// downing provider settles its fate. Giving up is not final: a later message dials again.
    /// An address which answered without speaking this protocol is refused instead, until a
    /// handshake from it proves a tellus node is there.
    ///
    /// Defaults to [EndpointConfig::DEFAULT_MAX_CONNECT_ATTEMPTS].
    pub max_connect_attempts: u32,

    /// How many changes a subscriber of [changes](crate::cluster::changes) may fall behind
    /// before it is told of a gap and handed the current state and verdict; a buffer, not a
    /// history, holding one cluster state per version. A power of two no larger than half the
    /// address space, since the channel rounds up to one. Defaults to
    /// [EndpointConfig::DEFAULT_CHANGE_CAPACITY].
    pub change_capacity: NonZeroUsize,
}

impl EndpointConfig {
    /// The `outbound_capacity` of [EndpointConfig::new].
    pub const DEFAULT_OUTBOUND_CAPACITY: NonZeroUsize =
        NonZeroUsize::new(8_192).expect("8192 is not zero");

    /// The `max_streams_per_peer` of [EndpointConfig::new].
    pub const DEFAULT_MAX_STREAMS_PER_PEER: NonZeroUsize =
        NonZeroUsize::new(16).expect("16 is not zero");

    /// The `max_frame_size` of [EndpointConfig::new].
    pub const DEFAULT_MAX_FRAME_SIZE: NonZeroUsize =
        NonZeroUsize::new(1_024 * 1_024).expect("1 MiB is not zero");

    /// The `heartbeat_interval` of [EndpointConfig::new].
    pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

    /// The `watch_refresh_interval` of [EndpointConfig::new].
    pub const DEFAULT_WATCH_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

    /// The `down_retention` of [EndpointConfig::new].
    pub const DEFAULT_DOWN_RETENTION: Duration = Duration::from_secs(300);

    /// The `down_watermarks` of [EndpointConfig::new].
    pub const DEFAULT_DOWN_WATERMARKS: NonZeroUsize =
        NonZeroUsize::new(4_096).expect("4096 is not zero");

    /// The `leave_timeout` of [EndpointConfig::new].
    pub const DEFAULT_LEAVE_TIMEOUT: Duration = Duration::from_secs(3);

    /// The minimum of the `reconnect_backoff` of [EndpointConfig::new].
    pub const DEFAULT_RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(250);

    /// The maximum of the `reconnect_backoff` of [EndpointConfig::new].
    pub const DEFAULT_RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(3);

    /// The `max_connect_attempts` of [EndpointConfig::new].
    pub const DEFAULT_MAX_CONNECT_ATTEMPTS: u32 = 8;

    /// The `change_capacity` of [EndpointConfig::new].
    pub const DEFAULT_CHANGE_CAPACITY: NonZeroUsize =
        NonZeroUsize::new(256).expect("256 is not zero");

    /// A configuration with the given advertised address, every other field taken from its
    /// `DEFAULT_*` constant: the [Postcard] codec, the [PhiAccrualFailureDetector] with its
    /// defaults and [KeepMajority] downing with its own.
    pub fn new(advertised_addr: SocketAddr) -> Self {
        Self {
            advertised_addr,
            outbound_capacity: Self::DEFAULT_OUTBOUND_CAPACITY,
            max_streams_per_peer: Self::DEFAULT_MAX_STREAMS_PER_PEER,
            max_frame_size: Self::DEFAULT_MAX_FRAME_SIZE,
            codec: Arc::new(Postcard),
            heartbeat_interval: Self::DEFAULT_HEARTBEAT_INTERVAL,
            watch_refresh_interval: Self::DEFAULT_WATCH_REFRESH_INTERVAL,
            failure_detector: Arc::new(|| Box::new(PhiAccrualFailureDetector::default())),
            downing_provider: Arc::new(|| Box::new(KeepMajority::default())),
            down_retention: Self::DEFAULT_DOWN_RETENTION,
            down_watermarks: Self::DEFAULT_DOWN_WATERMARKS,
            leave_timeout: Self::DEFAULT_LEAVE_TIMEOUT,
            reconnect_backoff: Backoff::new(
                Self::DEFAULT_RECONNECT_BACKOFF_MIN,
                Self::DEFAULT_RECONNECT_BACKOFF_MAX,
            )
            .expect("the bounds are valid"),
            max_connect_attempts: Self::DEFAULT_MAX_CONNECT_ATTEMPTS,
            change_capacity: Self::DEFAULT_CHANGE_CAPACITY,
        }
    }
}

/// The [EndpointConfig] given to [start_endpoint] is invalid.
#[derive(Debug, Error)]
pub enum InvalidEndpointConfig {
    /// The configured `max_frame_size` cannot hold the smallest chunk of a member snapshot or of
    /// a reachability snapshot, so a connection setup could not send one without the receiver
    /// refusing it.
    #[error("max_frame_size of {max_frame_size} bytes is below the minimum of {min} bytes")]
    MaxFrameSizeTooSmall {
        /// The configured size.
        max_frame_size: usize,

        /// The smallest size a snapshot chunk can respect.
        min: usize,
    },

    /// The configured `heartbeat_interval` is zero.
    #[error("heartbeat_interval is zero")]
    ZeroHeartbeatInterval,

    /// The configured `watch_refresh_interval` is zero.
    #[error("watch_refresh_interval is zero")]
    ZeroWatchRefreshInterval,

    /// The configured `change_capacity` is not a power of two, or too large for the channel.
    #[error("change_capacity of {change_capacity} is not a power of two up to {max}")]
    ChangeCapacity {
        /// The configured capacity.
        change_capacity: usize,

        /// The largest capacity the channel accepts.
        max: usize,
    },

    /// The configured failure detector declares a member unavailable within the given silence
    /// after a burst of traffic, although the heartbeat or the watch refresh interval, whichever
    /// is longer, may leave a healthy member that silent.
    #[error("failure detector does not tolerate a silence of {silence:?} after a burst of traffic")]
    FailureDetectorTooEager {
        /// The longer of the heartbeat and the watch refresh interval.
        silence: Duration,
    },
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedEndpointConfig {
    advertised_addr: SocketAddr,

    #[serde(default)]
    outbound_capacity: Option<NonZeroUsize>,

    #[serde(default)]
    max_streams_per_peer: Option<NonZeroUsize>,

    #[serde(default)]
    max_frame_size: Option<NonZeroUsize>,

    #[serde(default)]
    codec: Option<CodecConfig>,

    #[serde(default, with = "humantime_serde")]
    heartbeat_interval: Option<Duration>,

    #[serde(default, with = "humantime_serde")]
    watch_refresh_interval: Option<Duration>,

    #[serde(default)]
    failure_detector: Option<FailureDetectorConfig>,

    #[serde(default)]
    downing_provider: Option<DowningProviderConfig>,

    #[serde(default, with = "humantime_serde")]
    down_retention: Option<Duration>,

    #[serde(default)]
    down_watermarks: Option<NonZeroUsize>,

    #[serde(default, with = "humantime_serde")]
    leave_timeout: Option<Duration>,

    #[serde(default)]
    reconnect_backoff: Option<Backoff>,

    #[serde(default)]
    max_connect_attempts: Option<u32>,

    #[serde(default)]
    change_capacity: Option<NonZeroUsize>,
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedEndpointConfig> for EndpointConfig {
    type Error = InvalidEndpointConfig;

    fn try_from(unchecked: UncheckedEndpointConfig) -> Result<Self, Self::Error> {
        let defaults = Self::new(unchecked.advertised_addr);

        let config = Self {
            advertised_addr: unchecked.advertised_addr,
            outbound_capacity: unchecked
                .outbound_capacity
                .unwrap_or(defaults.outbound_capacity),
            max_streams_per_peer: unchecked
                .max_streams_per_peer
                .unwrap_or(defaults.max_streams_per_peer),
            max_frame_size: unchecked.max_frame_size.unwrap_or(defaults.max_frame_size),
            codec: unchecked.codec.map_or(defaults.codec, CodecConfig::codec),
            heartbeat_interval: unchecked
                .heartbeat_interval
                .unwrap_or(defaults.heartbeat_interval),
            watch_refresh_interval: unchecked
                .watch_refresh_interval
                .unwrap_or(defaults.watch_refresh_interval),
            failure_detector: unchecked
                .failure_detector
                .map_or(defaults.failure_detector, FailureDetectorConfig::factory),
            downing_provider: unchecked
                .downing_provider
                .map_or(defaults.downing_provider, DowningProviderConfig::factory),
            down_retention: unchecked.down_retention.unwrap_or(defaults.down_retention),
            down_watermarks: unchecked
                .down_watermarks
                .unwrap_or(defaults.down_watermarks),
            leave_timeout: unchecked.leave_timeout.unwrap_or(defaults.leave_timeout),
            reconnect_backoff: unchecked
                .reconnect_backoff
                .unwrap_or(defaults.reconnect_backoff),
            max_connect_attempts: unchecked
                .max_connect_attempts
                .unwrap_or(defaults.max_connect_attempts),
            change_capacity: unchecked
                .change_capacity
                .unwrap_or(defaults.change_capacity),
        };
        validate(&config)?;

        Ok(config)
    }
}

/// The remoting endpoint cannot be started.
#[derive(Debug, Error)]
pub enum StartError {
    /// The endpoint has already been started; there is one per process.
    #[error("remoting endpoint already started")]
    AlreadyStarted,

    /// The given configuration is invalid.
    #[error(transparent)]
    Config(#[from] InvalidEndpointConfig),
}

/// This node cannot form a cluster.
#[derive(Debug, Error)]
pub enum FormError {
    /// The remoting endpoint has not been started, see [start_endpoint].
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// This node is already a member of a cluster.
    #[error("already a member of a cluster")]
    AlreadyFormed,

    /// This node's incarnation has been downed by the cluster; only a restarted process, with a
    /// fresh incarnation, can be a member again.
    #[error("this node has been downed")]
    Downed,

    /// A join attempt is in flight, which may be making this node a member of another cluster;
    /// worth retrying once it has resolved.
    #[error("a join attempt is in flight")]
    JoinInFlight,

    /// A cluster at this address answered a join attempt and may already count this node, so
    /// nothing else may be formed until that attempt resolves; worth retrying.
    #[error("pinned to the cluster at {0}")]
    ClusterPinned(SocketAddr),
}

/// Start the process wide remoting endpoint: accept connections from the given transport and
/// dial peers on demand. The started endpoint is not a cluster yet, so nothing can join it:
/// [form] makes it a cluster of one and [join](crate::cluster::join) makes it a member of
/// another node's. References can only be serialized and deserialized after this.
///
/// # Errors
/// Fails if the endpoint has been started already, there is one per process, or if the
/// configuration is invalid.
///
/// # Panics
/// Panics if called outside of a Tokio runtime.
pub fn start_endpoint<T>(config: EndpointConfig, transport: T) -> Result<(), StartError>
where
    T: Transport,
{
    validate(&config)?;

    let (dial_request_tx, dial_request_rx) = flume::unbounded();
    let (join_request_tx, join_request_rx) = flume::unbounded();
    let data_streams = transport.data_streams().map_or(0, |streams| {
        config.max_streams_per_peer.get().min(streams.get())
    });
    let inner = EndpointInner::new(config, data_streams, dial_request_tx, join_request_tx);
    ENDPOINT
        .set(inner)
        .map_err(|_| StartError::AlreadyStarted)?;
    let endpoint = ENDPOINT.get().expect("endpoint was just set");

    let transport = Arc::new(transport);
    task::spawn(accept_loop(transport.clone(), endpoint));
    task::spawn(dial_loop(transport.clone(), dial_request_rx, endpoint));
    task::spawn(join_loop(transport, join_request_rx, endpoint));
    task::spawn(membership_loop(endpoint));
    task::spawn(watch_refresh_loop(endpoint));
    task::spawn(replication_loop(endpoint));
    Ok(())
}

/// Form a cluster of this node alone, for a deployment deciding formation itself rather than
/// leaving it to [bootstrap](fn@crate::cluster::bootstrap): a started endpoint is not a cluster, so
/// nothing can join this node until it has formed one or joined one.
///
/// # Errors
/// Fails if the endpoint is not started, if this node is already a member, if it has been
/// downed, or if a join attempt could be making it a member of another cluster right now, which
/// is transient and worth retrying.
pub fn form() -> Result<(), FormError> {
    get().ok_or(FormError::EndpointNotStarted)?.form_cluster()
}

/// Where the endpoint stands between being started and being done, see
/// [lifecycle](crate::cluster::lifecycle).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "snake_case")
)]
pub enum Lifecycle {
    /// Started, but a member of no cluster yet: joins are refused until this node forms one or
    /// joins one.
    #[default]
    Unformed,

    /// A member of a cluster, of one or of many.
    Formed,

    /// Downed or left, for good: only a restarted process rejoins.
    Downed,
}

impl Lifecycle {
    fn code(self) -> u8 {
        match self {
            Lifecycle::Unformed => 0,
            Lifecycle::Formed => 1,
            Lifecycle::Downed => 2,
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            0 => Lifecycle::Unformed,
            1 => Lifecycle::Formed,
            _ => Lifecycle::Downed,
        }
    }
}

pub(crate) struct EndpointInner {
    node: NodeId,
    config: EndpointConfig,
    registry: Arc<Registry>,
    lanes: Lanes,
    inbound_readers: tokio::sync::Mutex<HashMap<SocketAddr, InboundReader>>,
    membership: Membership,
    update_lock: Mutex<()>,
    state_tx: watch::Sender<ClusterState>,
    changes_tx: broadcast::Sender<PublishedChange>,
    refused_addrs: Mutex<HashSet<SocketAddr>>,
    lifecycle: CoordinatedLifecycle,
    leaving: AtomicBool,
    departure: OnceLock<watch::Receiver<bool>>,
    liveness: Liveness,
    reachability: Reachability,
    watchers: WatcherTable,
    wire_watches: WireWatchTable,
    pending_replies: PendingReplies,
    receptionist: Receptionist,
    join_request_tx: Sender<JoinRequest>,
    sever_tx: watch::Sender<u64>,
    #[cfg(feature = "cluster-dev")]
    dropped_terminated: AtomicU64,
}

impl EndpointInner {
    /// Leaked, since everything downstream takes the endpoint by `'static` reference, and none of
    /// the loops is started: a test spawns the one it exercises.
    #[cfg(test)]
    pub(crate) fn for_tests(
        config: EndpointConfig,
    ) -> (
        &'static EndpointInner,
        flume::Receiver<DialRequest>,
        flume::Receiver<JoinRequest>,
    ) {
        let (dial_request_tx, dial_request_rx) = flume::unbounded();
        let (join_request_tx, join_request_rx) = flume::unbounded();
        let inner = Self::new(config, 0, dial_request_tx, join_request_tx);
        (Box::leak(Box::new(inner)), dial_request_rx, join_request_rx)
    }

    pub(crate) fn node(&self) -> NodeId {
        self.node
    }

    pub(crate) fn config(&self) -> &EndpointConfig {
        &self.config
    }

    pub(crate) fn codec(&self) -> &dyn Codec {
        self.config.codec.as_ref()
    }

    pub(crate) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub(crate) fn membership(&self) -> &Membership {
        &self.membership
    }

    pub(crate) fn state_rx(&self) -> watch::Receiver<ClusterState> {
        self.state_tx.subscribe()
    }

    /// Subscribed before the snapshot is taken, so nothing published in between is missed.
    pub(crate) fn changes(&'static self) -> Changes {
        Changes::new(self.changes_tx.subscribe(), &self.receptionist)
    }

    #[cfg(test)]
    pub(crate) fn published_changes(&self) -> broadcast::Receiver<PublishedChange> {
        self.changes_tx.subscribe()
    }

    /// Lock order: `update_lock`, then `Membership::entries`, then `Reachability::state`, then
    /// the watch write, then, with all of those released, the receptionist's replicas for the
    /// channel send; the closure must not call back into `update`.
    pub(crate) fn update<R>(
        &self,
        mutate: impl FnOnce(&Membership, &Reachability) -> R,
    ) -> Updated<R> {
        let _reentrancy = UpdateGuard::enter();
        let _guard = lock(&self.update_lock);
        let result = mutate(&self.membership, &self.reachability);
        self.reachability.prune_pending(
            |node| self.membership.is_down(node),
            self.config.down_retention,
        );

        let members = self.membership.members();
        let up = members
            .iter()
            .filter(|member| member.state() == MemberState::Up)
            .map(Member::node)
            .collect::<HashSet<_>>();
        self.reachability.promote_pending(&up);
        let disconnected = self.reachability.unreachable_members(&members);
        let unreachable = disconnected
            .iter()
            .map(|(member, _)| *member)
            .collect::<Vec<_>>();
        let changed = self.state_tx.send_if_modified(|state| {
            if state.members() == members && state.unreachable() == unreachable {
                return false;
            }
            *state = ClusterState::new(
                self.node(),
                state.version() + 1,
                members.clone(),
                unreachable,
            );
            true
        });
        if changed {
            // The watch first, then the channel, so the stream never leads the current value.
            self.receptionist.publish(self.state_tx.borrow().clone());
        }

        Updated {
            result,
            members,
            disconnected,
        }
    }

    pub(crate) fn watchers(&self) -> &WatcherTable {
        &self.watchers
    }

    pub(crate) fn wire_watches(&self) -> &WireWatchTable {
        &self.wire_watches
    }

    pub(crate) fn pending_replies(&self) -> &PendingReplies {
        &self.pending_replies
    }

    pub(crate) fn receptionist(&self) -> &Receptionist {
        &self.receptionist
    }

    pub(crate) fn register_own<M>(
        &self,
        name: String,
        actor_ref: &ActorRef<M>,
    ) -> Result<(), TooManyRegistrations>
    where
        M: DeserializeOwned + Send + 'static,
    {
        let applied = match self.registry.register(name, actor_ref)? {
            Registered::Unchanged => true,

            Registered::Added {
                version,
                registration,
            } => self.receptionist.add_own(version, registration),

            Registered::Changed => false,
        };
        if !applied {
            self.apply_own_registrations();
        }
        Ok(())
    }

    pub(crate) fn registrations_snapshot(&self) -> Arc<Snapshot> {
        let registrations = self.registry.registrations();
        Arc::new(receptionist::snapshot(
            registrations.version,
            registrations.entries,
            self.config.max_frame_size.get(),
        ))
    }

    pub(crate) fn offer_snapshot(&self, peer: NodeId, snapshot: &Arc<Snapshot>) {
        self.lanes.offer_snapshot(peer.addr(), snapshot);
    }

    pub(crate) fn merge_reachability(&self, observations: &[WireReachability]) {
        self.update(|membership, reachability| {
            reachability.merge(observations, &membership.up_nodes());
        });
    }

    pub(crate) fn reconnect_backoff(&self, attempts: u32) -> Duration {
        self.config
            .reconnect_backoff
            .duration(attempts.saturating_sub(1))
    }

    pub(crate) fn send(&self, peer: NodeId, frame: Frame<'static>) -> Result<(), LaneError> {
        if self.downed() || self.leaving() {
            return Err(LaneError::NodeUnreachable(peer));
        }

        #[cfg(feature = "cluster-dev")]
        if matches!(frame, Frame::Terminated { .. }) && self.drop_terminated() {
            debug!(%peer, "dropping terminated frame, fault injection");
            return Ok(());
        }

        self.lanes.enqueue(
            peer,
            frame,
            || {
                if self.membership.is_down(peer) {
                    return Err(LaneError::NodeUnreachable(peer));
                }
                // Built before the write lock, which every send waits for: it copies every
                // registration.
                Ok(self.registrations_snapshot())
            },
            // Under the lock, since a leave collects the lanes once, under the same lock.
            || !self.leaving() && !self.is_refused(peer.addr()),
        )
    }

    pub(crate) fn track_liveness(&self, peer: NodeId) -> PeerLiveness {
        self.liveness.track(peer, &self.config.failure_detector)
    }

    pub(crate) fn refuse(&self, addr: SocketAddr) {
        lock(&self.refused_addrs).insert(addr);
    }

    /// A refusal only means whatever answered there last did not speak this protocol.
    pub(crate) fn unrefuse(&self, addr: SocketAddr) {
        lock(&self.refused_addrs).remove(&addr);
    }

    pub(crate) fn request_join(&self, request: JoinRequest) {
        if self.join_request_tx.send(request).is_err() {
            error!("dial loop gone, join will never be attempted");
            debug_assert!(false, "dial loop gone");
        }
    }

    pub(crate) fn lifecycle(&self) -> Lifecycle {
        self.lifecycle.current()
    }

    pub(crate) fn formed(&self) -> bool {
        self.lifecycle() == Lifecycle::Formed
    }

    pub(crate) fn downed(&self) -> bool {
        self.lifecycle() == Lifecycle::Downed
    }

    pub(crate) fn leaving(&self) -> bool {
        self.leaving.load(Ordering::Relaxed)
    }

    pub(crate) fn leaving_flag(&'static self) -> &'static AtomicBool {
        &self.leaving
    }

    pub(crate) async fn leave(&'static self) {
        if self.downed() {
            return;
        }

        // The departure runs in a task of its own, since a cancelled caller must not cancel it.
        let mut departed_rx = self
            .departure
            .get_or_init(|| {
                let (departed_tx, departed_rx) = watch::channel(false);
                task::spawn(async move {
                    self.depart().await;
                    departed_tx.send_replace(true);
                });
                departed_rx
            })
            .clone();
        if departed_rx.wait_for(|departed| *departed).await.is_err() {
            error!("departure task gone before this node left");
        }
    }

    pub(crate) fn form_cluster(&self) -> Result<(), FormError> {
        self.lifecycle.coordinate(Coordinator::form).map(|_| ())
    }

    /// Observed by [form]; exclusivity itself comes from the single join loop, not from this.
    /// `false` if this node is downed, which no attempt may outlive.
    pub(crate) fn enter_join(&self) -> bool {
        self.lifecycle.coordinate(Coordinator::enter_join)
    }

    /// The single commit point of a join attempt: permit release and outcome happen under one
    /// lock, so no formation slips in between; a down decided here runs its side effects once.
    pub(crate) fn finish_join(
        &self,
        addr: SocketAddr,
        result: Result<(), ConnectError>,
    ) -> Result<(), ConnectError> {
        let finished = self
            .lifecycle
            .coordinate(|coordinator| coordinator.finish_join(addr, result));

        if finished.newly_downed {
            self.sever_down();
            error!("this node has been downed, restart the process to rejoin");
        }
        finished.result
    }

    pub(crate) fn pin_cluster(&self, addr: SocketAddr) {
        self.lifecycle
            .coordinate(|coordinator| coordinator.pin(addr));
    }

    pub(crate) fn pinned_cluster(&self) -> Option<SocketAddr> {
        self.lifecycle.coordinate(|coordinator| coordinator.pinned)
    }

    /// Decided under one lock, so evidence a running attempt has gathered is never mistaken for
    /// the stale pin.
    pub(crate) fn release_pin_if_gone(
        &self,
        universe: &BTreeSet<SocketAddr>,
    ) -> Option<SocketAddr> {
        self.lifecycle
            .coordinate(|coordinator| coordinator.release_pin_if_gone(universe))
    }

    /// Honoring this Down keeps other nodes' synthesized signals true; only a restart rejoins.
    pub(crate) fn self_down(&self) {
        if self.leaving() {
            return;
        }
        if self.latch_down() {
            error!("this node has been downed, restart the process to rejoin");
        }
    }

    pub(crate) fn snapshot_frames(&self, members: Vec<Member>) -> Vec<Frame<'static>> {
        membership::snapshot_frames(members, self.config.max_frame_size.get())
    }

    pub(crate) fn reachability_snapshot_frames(&self) -> Vec<Frame<'static>> {
        let up = self.membership.up_nodes();
        reachability::snapshot_chunks(
            self.reachability.snapshot(&up),
            self.config.max_frame_size.get(),
        )
        .into_iter()
        .map(|observations| Frame::Reachability { observations })
        .collect()
    }

    /// One fan-out per tick carries everything learned since the last one, so a relayed statement
    /// costs a round rather than a flood of its own; like gossip it skips a disconnected lane,
    /// whose uncounted queues would otherwise grow.
    pub(crate) fn push_reachability(&self, observations: Vec<WireReachability>) {
        if observations.is_empty() || self.downed() {
            return;
        }

        let chunks = reachability::snapshot_chunks(observations, self.config.max_frame_size.get());
        for peer in self.membership.up_peers() {
            if self.lane_connected(peer.addr()) == Some(false) {
                continue;
            }
            if let Err(error) = chunks.iter().try_for_each(|chunk| {
                self.send(
                    peer,
                    Frame::Reachability {
                        observations: chunk.clone(),
                    },
                )
            }) {
                debug!(%peer, %error, "cannot send reachability observations");
            }
        }
    }

    /// Pushes to every Up member whose lane is live; the periodic tick covers everyone else.
    pub(crate) fn push_gossip(&self) {
        if self.downed() {
            return;
        }

        self.gossip_to_up_members(LaneFilter::Connected);
    }

    /// `None` if no lane exists; gossip skips on connectivity alone, never on suspicion.
    pub(crate) fn lane_connected(&self, addr: SocketAddr) -> Option<bool> {
        self.lanes.connected(addr)
    }

    pub(crate) fn mark_lane_disconnected(&self, addr: SocketAddr, lane_id: Generation) {
        self.lanes.disconnect(addr, lane_id);
    }

    /// Mark Down, close the lane, quiesce deliveries, then fail asks and flush synthesized signals.
    pub(crate) fn node_death(&self, peer: NodeId) {
        let outcome = self.update(|membership, _| membership.down(peer)).result;
        self.death_consequences(peer, outcome);
    }

    pub(crate) fn death_consequences(&self, peer: NodeId, outcome: DownOutcome) {
        if !outcome.member_changed && !outcome.watermark_changed {
            return;
        }

        self.flush_fenced(peer);
        if outcome.member_changed {
            self.push_gossip();
        }
    }

    pub(crate) fn flush_fenced(&self, fence: NodeId) {
        self.lanes.remove_fenced(fence);
        self.liveness.quiesce_fenced(fence);
        self.update(|_, reachability| reachability.prune_fenced(fence));
        self.pending_replies.fail_fenced(fence);
        self.receptionist.evict_fenced(fence);

        for (target, watchers) in self.watchers.take_fenced(fence) {
            for watcher in watchers {
                if let Err(error) = watcher.handle_terminated(target) {
                    debug!(watcher_id = %watcher.watcher_id(), other_id = %target, %error, "cannot send synthesized terminated signal");
                }
            }
        }

        for watch in self.wire_watches.take_fenced(fence) {
            if let Some(watcher_registry) = self.registry.watcher_registry(watch.target) {
                watcher_registry.remove(watch.wire_watcher_id);
            }
        }
        self.liveness.untrack_fenced(fence);
    }

    /// The kept incarnations are reread inside the retention, else a fresh entry is lost for good.
    pub(crate) fn untrack_idle_peers(&self) {
        self.liveness.retain_with(|| self.membership.up_nodes());
    }

    pub(crate) fn bind_lane(&self, addr: SocketAddr, lane_id: Generation, peer: NodeId) {
        self.lanes
            .bind(addr, lane_id, peer, || self.membership.is_down(peer));
    }

    /// Held from the old reader's shutdown to the new one's spawn, else two readers overlap.
    pub(crate) async fn supersede_inbound_reader<F>(&self, addr: SocketAddr, spawn_reader: F)
    where
        F: FnOnce(watch::Receiver<()>) -> JoinHandle<()>,
    {
        let mut readers = self.inbound_readers.lock().await;

        if let Some(superseded) = readers.remove(&addr) {
            let _ = superseded.shutdown_tx.send(());
            if let Err(error) = superseded.reader.await {
                warn!(peer_addr = %addr, %error, "superseded inbound reader panicked");
            }
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let reader = spawn_reader(shutdown_rx);
        readers.insert(
            addr,
            InboundReader {
                shutdown_tx,
                reader,
            },
        );
    }

    pub(crate) fn sever_rx(&self) -> watch::Receiver<u64> {
        self.sever_tx.subscribe()
    }

    #[cfg(feature = "cluster-dev")]
    pub(crate) fn sever(&self) {
        self.sever_tx.send_modify(|generation| *generation += 1);
    }

    #[cfg(feature = "cluster-dev")]
    pub(crate) fn arm_terminated_drop(&self, count: u64) {
        self.dropped_terminated.store(count, Ordering::Relaxed);
    }

    pub(crate) fn is_lane_open(&self, addr: SocketAddr, lane_id: Generation) -> bool {
        self.lanes.is_open(addr, lane_id)
    }

    /// `true` if this call removed the lane, i.e. it was still the address's current one.
    pub(crate) fn remove_lane(&self, addr: SocketAddr, lane_id: Generation) -> bool {
        self.lanes.remove(addr, lane_id)
    }

    fn new(
        config: EndpointConfig,
        data_streams: usize,
        dial_request_tx: Sender<DialRequest>,
        join_request_tx: Sender<JoinRequest>,
    ) -> Self {
        let node = NodeId::new(config.advertised_addr);
        let down_watermarks = config.down_watermarks;
        let (state_tx, _) = watch::channel(ClusterState::new(
            node,
            0,
            vec![Member::new(node, MemberState::Up)],
            Vec::new(),
        ));
        let (changes_tx, _) = broadcast::channel(config.change_capacity.get());
        let receptionist = Receptionist::new(node, state_tx.borrow().clone(), changes_tx.clone());

        let lanes = Lanes::new(data_streams, config.outbound_capacity, dial_request_tx);

        Self {
            node,
            config,
            registry: Arc::new(Registry::new()),
            lanes,
            inbound_readers: tokio::sync::Mutex::new(HashMap::new()),
            membership: Membership::new(node, down_watermarks),
            update_lock: Mutex::new(()),
            state_tx,
            changes_tx,
            refused_addrs: Mutex::new(HashSet::new()),
            lifecycle: CoordinatedLifecycle::new(),
            leaving: AtomicBool::new(false),
            departure: OnceLock::new(),
            liveness: Liveness::new(),
            reachability: Reachability::new(node),
            watchers: WatcherTable::new(),
            wire_watches: WireWatchTable::new(),
            pending_replies: PendingReplies::new(),
            receptionist,
            join_request_tx,
            sever_tx: watch::channel(0).0,
            #[cfg(feature = "cluster-dev")]
            dropped_terminated: AtomicU64::new(0),
        }
    }

    async fn depart(&self) {
        // Announce before leaving, which refuses every send, and wait for the lanes before the
        // sever, since a sever turns whatever is still queued or unacknowledged into dead letters.
        self.update(|membership, _| membership.down(self.node));
        self.announce();
        let lanes_done = self.begin_leave();

        let all_done = async {
            for lane_done in lanes_done {
                let _ = lane_done.await;
            }
        };
        if timeout(self.config.leave_timeout, all_done).await.is_err() {
            warn!("leaving with the departure not acknowledged by every peer");
        }

        self.leave_down();
    }

    /// Pushes to every Up member, a disconnected lane included: a departure is sent once.
    fn announce(&self) {
        let members = self.membership.members();
        for peer in self.membership.up_peers() {
            for frame in self.snapshot_frames(members.clone()) {
                if let Err(error) = self.send(peer, frame) {
                    debug!(%peer, %error, "cannot announce the departure");
                }
            }
        }
    }

    fn begin_leave(&self) -> Vec<oneshot::Receiver<()>> {
        self.leaving.store(true, Ordering::Relaxed);
        self.lanes.drain()
    }

    /// The same end, chosen and announced, hence logged as a departure rather than an error.
    fn leave_down(&self) {
        if self.latch_down() {
            info!("this node has left the cluster, restart the process to rejoin");
        }
    }

    fn apply_own_registrations(&self) {
        let registrations = self.registry.registrations();
        self.receptionist.apply(
            self.node,
            registrations.version,
            registrations.entries,
            true,
        );
    }

    fn gossip_to_up_members(&self, lanes: LaneFilter) {
        let members = self.membership.members();
        for peer in self.membership.up_peers() {
            if lanes.admits(self.lane_connected(peer.addr()))
                && let Err(error) = self
                    .snapshot_frames(members.clone())
                    .into_iter()
                    .try_for_each(|frame| self.send(peer, frame))
            {
                debug!(%peer, %error, "cannot send gossip");
            }
        }
    }

    fn is_refused(&self, addr: SocketAddr) -> bool {
        lock(&self.refused_addrs).contains(&addr)
    }

    /// `true` if this call ended the endpoint, so the caller logs why exactly once.
    fn latch_down(&self) -> bool {
        if !self.lifecycle.coordinate(Coordinator::down) {
            return false;
        }

        self.sever_down();
        true
    }

    fn sever_down(&self) {
        self.update(|membership, _| membership.down(self.node));
        self.sever_tx.send_modify(|generation| *generation += 1);
    }

    #[cfg(feature = "cluster-dev")]
    fn drop_terminated(&self) -> bool {
        self.dropped_terminated
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_sub(1)
            })
            .is_ok()
    }
}

pub(crate) struct Updated<R> {
    pub(crate) result: R,
    pub(crate) members: Vec<Member>,
    pub(crate) disconnected: Vec<(Member, Instant)>,
}

fn validate(config: &EndpointConfig) -> Result<(), InvalidEndpointConfig> {
    let min_frame_size = membership::MIN_FRAME_SIZE.max(reachability::MIN_FRAME_SIZE);
    if config.max_frame_size.get() < min_frame_size {
        return Err(InvalidEndpointConfig::MaxFrameSizeTooSmall {
            max_frame_size: config.max_frame_size.get(),
            min: min_frame_size,
        });
    }
    if config.heartbeat_interval.is_zero() {
        return Err(InvalidEndpointConfig::ZeroHeartbeatInterval);
    }
    if config.watch_refresh_interval.is_zero() {
        return Err(InvalidEndpointConfig::ZeroWatchRefreshInterval);
    }
    if !config.change_capacity.is_power_of_two()
        || config.change_capacity.get() > MAX_CHANGE_CAPACITY
    {
        return Err(InvalidEndpointConfig::ChangeCapacity {
            change_capacity: config.change_capacity.get(),
            max: MAX_CHANGE_CAPACITY,
        });
    }
    let silence = config.heartbeat_interval.max(config.watch_refresh_interval);
    if !failure::tolerates_silence(&config.failure_detector, silence) {
        return Err(InvalidEndpointConfig::FailureDetectorTooEager { silence });
    }

    Ok(())
}

pub(crate) fn get() -> Option<&'static EndpointInner> {
    ENDPOINT.get()
}

#[derive(Clone, Copy)]
enum LaneFilter {
    Connected,
    NotDisconnected,
}

impl LaneFilter {
    fn admits(self, connected: Option<bool>) -> bool {
        match self {
            LaneFilter::Connected => connected == Some(true),
            LaneFilter::NotDisconnected => connected != Some(false),
        }
    }
}

struct UpdateGuard;

impl UpdateGuard {
    fn enter() -> Self {
        UPDATING.with(|updating| {
            if updating.get() {
                panic!("re-entrant cluster state update, which would deadlock");
            }
            updating.set(true);
        });
        Self
    }
}

impl Drop for UpdateGuard {
    fn drop(&mut self) {
        UPDATING.with(|updating| updating.set(false));
    }
}

struct CoordinatedLifecycle {
    coordinator: Mutex<Coordinator>,
    lifecycle: AtomicU8,
}

impl CoordinatedLifecycle {
    fn new() -> Self {
        Self {
            coordinator: Mutex::new(Coordinator::default()),
            lifecycle: AtomicU8::new(Lifecycle::Unformed.code()),
        }
    }

    fn current(&self) -> Lifecycle {
        Lifecycle::from_code(self.lifecycle.load(Ordering::Relaxed))
    }

    fn coordinate<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Coordinator) -> R,
    {
        let mut coordinator = lock(&self.coordinator);
        let result = f(&mut coordinator);
        // Stored before the lock is released, else the lock free reads drift from the pin.
        self.lifecycle
            .store(coordinator.lifecycle.code(), Ordering::Relaxed);
        result
    }
}

/// Every lifecycle transition runs here, so a formation decision cannot slip between a join's
/// admission and the record of it.
#[derive(Default)]
struct Coordinator {
    lifecycle: Lifecycle,
    join_in_flight: bool,
    pinned: Option<SocketAddr>,
}

impl Coordinator {
    fn enter_join(&mut self) -> bool {
        if self.lifecycle == Lifecycle::Downed {
            return false;
        }

        self.join_in_flight = true;
        true
    }

    /// A down landing mid attempt leaves nothing to pin: the evidence would outlive the node.
    fn pin(&mut self, addr: SocketAddr) {
        if self.lifecycle != Lifecycle::Downed {
            self.pinned = Some(addr);
        }
    }

    /// Downed dominates every raw outcome, since a down may land anywhere inside the attempt.
    fn finish_join(&mut self, addr: SocketAddr, result: Result<(), ConnectError>) -> Finished {
        self.join_in_flight = false;
        if self.lifecycle == Lifecycle::Downed {
            self.pinned = None;
            return Finished {
                result: Err(ConnectError::SelfDowned),
                newly_downed: false,
            };
        }

        let mut newly_downed = false;
        let result = match result {
            Ok(()) => {
                self.pinned = None;
                self.lifecycle = Lifecycle::Formed;
                Ok(())
            }

            Err(ConnectError::Refused(RefusalReason::Down)) => {
                newly_downed = self.down();
                Err(ConnectError::Refused(RefusalReason::Down))
            }

            Err(ConnectError::Refused(RefusalReason::NoCluster)) => {
                if self.pinned == Some(addr) {
                    self.pinned = None;
                }
                Err(ConnectError::Refused(RefusalReason::NoCluster))
            }

            Err(error) => Err(error),
        };

        Finished {
            result,
            newly_downed,
        }
    }

    fn release_pin_if_gone(&mut self, universe: &BTreeSet<SocketAddr>) -> Option<SocketAddr> {
        if self.join_in_flight {
            return None;
        }

        match self.pinned {
            Some(pinned) if !universe.contains(&pinned) => {
                self.pinned = None;
                Some(pinned)
            }

            _ => None,
        }
    }

    fn form(&mut self) -> Result<Lifecycle, FormError> {
        if self.lifecycle == Lifecycle::Downed {
            return Err(FormError::Downed);
        }
        if self.join_in_flight {
            return Err(FormError::JoinInFlight);
        }
        if let Some(addr) = self.pinned {
            return Err(FormError::ClusterPinned(addr));
        }

        match self.lifecycle {
            Lifecycle::Unformed => {
                self.lifecycle = Lifecycle::Formed;
                Ok(self.lifecycle)
            }

            Lifecycle::Formed => Err(FormError::AlreadyFormed),

            Lifecycle::Downed => unreachable!("checked above"),
        }
    }

    fn down(&mut self) -> bool {
        if self.lifecycle == Lifecycle::Downed {
            return false;
        }

        self.lifecycle = Lifecycle::Downed;
        self.pinned = None;
        true
    }
}

struct Finished {
    result: Result<(), ConnectError>,
    newly_downed: bool,
}

struct InboundReader {
    shutdown_tx: watch::Sender<()>,
    reader: JoinHandle<()>,
}

async fn membership_loop(endpoint: &'static EndpointInner) {
    let mut provider = (endpoint.config.downing_provider)();
    let mut ticks = interval(endpoint.config.heartbeat_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticks.tick().await;
        if endpoint.downed() {
            break;
        }

        match catch_unwind(AssertUnwindSafe(|| {
            membership_tick(endpoint, provider.as_mut())
        })) {
            Ok(ControlFlow::Continue(())) => {}

            Ok(ControlFlow::Break(())) => break,

            Err(panic) => {
                error!(panic = %PanicPayload(panic.as_ref()), "membership tick panicked");
                provider = (endpoint.config.downing_provider)();
            }
        }
    }
}

fn membership_tick(
    endpoint: &'static EndpointInner,
    provider: &mut dyn DowningProvider,
) -> ControlFlow<()> {
    {
        endpoint.gossip_to_up_members(LaneFilter::NotDisconnected);

        let Updated {
            result: outbound,
            members,
            disconnected,
        } = endpoint.update(|membership, reachability| {
            for peer in membership.up_peers() {
                if endpoint.liveness.is_available(peer) {
                    if reachability.observe(peer, true) {
                        debug!(%peer, "member reachable again");
                    }
                } else if reachability.observe(peer, false) {
                    warn!(%peer, "member directly unreachable");
                }
            }
            reachability.take_outbound(&membership.up_nodes())
        });
        endpoint.push_reachability(outbound);

        match provider.down(&members, Disconnected::new(&disconnected), Instant::now()) {
            Downing::Members(members) => {
                if members.iter().any(|member| member.node() == endpoint.node) {
                    warn!("self down, this node named by the downing provider");
                    endpoint.self_down();
                    return ControlFlow::Break(());
                }
                for member in members {
                    warn!(%member, "node death, downed by the downing provider");
                    endpoint.node_death(member.node());
                }
            }

            Downing::SelfDown => {
                warn!("self down, decided by the downing provider");
                endpoint.self_down();
                return ControlFlow::Break(());
            }
        }

        endpoint.update(|membership, _| membership.sweep(endpoint.config.down_retention));
        endpoint.untrack_idle_peers();
        reply::send_cancellations(endpoint, endpoint.pending_replies.release_abandoned());
    }
    ControlFlow::Continue(())
}

/// The tick covers a lane which did not exist or was disconnected when a change was published.
async fn replication_loop(endpoint: &'static EndpointInner) {
    let mut changes = endpoint.registry.changes();
    let mut ticks = interval(endpoint.config.watch_refresh_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            changed = changes.changed() => {
                if changed.is_err() {
                    break;
                }
            }

            _ = ticks.tick() => {}
        }
        if endpoint.downed() {
            break;
        }

        endpoint.apply_own_registrations();
        let snapshot = endpoint.registrations_snapshot();
        for peer in endpoint.membership.up_peers() {
            endpoint.offer_snapshot(peer, &snapshot);
        }
    }
}

/// Re-asserting a watch compensates a `Terminated` frame lost with its connection; like gossip it
/// skips a disconnected lane, whose uncounted queues would otherwise grow.
async fn watch_refresh_loop(endpoint: &'static EndpointInner) {
    let mut ticks = interval(endpoint.config.watch_refresh_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticks.tick().await;
        if endpoint.downed() {
            break;
        }

        for peer in endpoint.watchers.peers() {
            if endpoint.membership.is_down(peer)
                || endpoint.lane_connected(peer.addr()) == Some(false)
            {
                continue;
            }

            for (target, watcher) in endpoint.watchers.watches(peer) {
                if let Err(error) = endpoint.send(peer, Frame::Watch { target, watcher }) {
                    debug!(%peer, actor_id = %target, %error, "cannot refresh remote watch");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::{
        ClusterState,
        downing::{Disconnected, Downing, DowningProvider},
        endpoint::{
            Coordinator, EndpointConfig, EndpointInner, FormError, InvalidEndpointConfig,
            Lifecycle, MAX_CHANGE_CAPACITY, membership_loop, validate,
        },
        failure::{Deadline, DeadlineFailureDetector},
        frame::{Frame, RefusalReason},
        lanes::LaneError,
        membership::{self, Member, MemberState},
        node::NodeId,
        peer::ConnectError,
        reachability::{self, Reachability, WireReachability},
        receptionist::{PublishedChange, Settlement, SnapshotSource},
        state::Change,
    };
    #[cfg(feature = "serde")]
    use crate::cluster::{
        codec::{Codec, Postcard},
        downing::KeepMajority,
        failure::PhiAccrual,
    };
    use std::{
        collections::{BTreeSet, HashSet},
        net::SocketAddr,
        num::NonZeroUsize,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };
    use tokio::{
        task,
        time::{sleep, timeout},
    };

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    /// Once admission has classified an address as permanently incompatible, later frames fail
    /// locally instead of opening one new lane (and therefore one new dial) per message.
    #[test]
    fn a_refused_address_is_not_redialed_per_frame() {
        let local = addr(1);
        let peer = NodeId::new(addr(2));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(local));
        endpoint.update(|membership, _| membership.add_up(peer));
        endpoint.refuse(peer.addr());

        for _ in 0..2 {
            assert!(matches!(
                endpoint.send(peer, gossip()),
                Err(LaneError::NodeUnreachable(node)) if node == peer
            ));
        }
        assert!(dial_requests.is_empty());

        endpoint.unrefuse(peer.addr());
        endpoint
            .send(peer, gossip())
            .expect("an address can be retried after it is explicitly unrefused");
        assert_eq!(dial_requests.len(), 1);
    }

    /// A push carries a statement once, so a node which was disconnected while it went out has no
    /// second chance at it; connection setup hence replays the whole table. Both an own and a
    /// relayed statement must survive that replay, else a reconnecting member keeps deriving a
    /// component the cluster has already left behind.
    #[test]
    fn a_reconnect_snapshot_transfers_established_observations() {
        let relay = NodeId::new(addr(2));
        let subject = NodeId::new(addr(3));
        let reconnecting_node = NodeId::new(addr(4));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let members = [endpoint.node(), relay, subject, reconnecting_node];
        for member in members {
            endpoint.update(|membership, _| membership.add_up(member));
        }
        let up = members.iter().copied().collect::<HashSet<_>>();

        endpoint.update(|_, reachability| {
            reachability.observe(subject, false);
            reachability.merge(
                &[WireReachability {
                    observer: relay,
                    subject,
                    version: 1,
                    reachable: false,
                }],
                &up,
            );
        });

        let observations = endpoint
            .reachability_snapshot_frames()
            .into_iter()
            .flat_map(|frame| match frame {
                Frame::Reachability { observations } => observations,
                frame => panic!("a reachability snapshot holds no {frame:?}"),
            })
            .collect::<Vec<_>>();

        // The reconnecting member saw none of the deltas and reaches the subject through either of
        // the others until the snapshot arrives.
        let reconnecting = Reachability::new(reconnecting_node);
        reconnecting.observe(subject, false);
        let all = members
            .iter()
            .map(|node| membership::Member::new(*node, membership::MemberState::Up))
            .collect::<Vec<_>>();
        assert!(reconnecting.unreachable_members(&all).is_empty());

        reconnecting.merge(&observations, &up);

        let outside = reconnecting
            .unreachable_members(&all)
            .iter()
            .map(|(member, _)| member.node())
            .collect::<Vec<_>>();
        assert_eq!(outside, vec![subject]);
    }

    /// A push skips a member whose lane is known disconnected, so a partition cannot turn every
    /// observation into another dial at the peers which just went silent; a member with no lane at
    /// all is still dialed, exactly as gossip does it.
    #[test]
    fn a_reachability_push_skips_a_disconnected_lane() {
        let local = addr(1);
        let silent = NodeId::new(addr(2));
        let fresh = NodeId::new(addr(3));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(local));
        endpoint.update(|membership, _| membership.add_up(silent));
        endpoint.update(|membership, _| membership.add_up(fresh));

        // A lane is disconnected until its dial completes, which this one never does.
        endpoint.send(silent, gossip()).expect("the lane is opened");
        let silent_lane = dial_requests.recv().expect("the silent peer is dialed");
        assert_eq!(endpoint.lane_connected(silent.addr()), Some(false));

        endpoint.push_reachability(vec![WireReachability {
            observer: NodeId::new(local),
            subject: silent,
            version: 1,
            reachable: false,
        }]);

        assert_eq!(silent_lane.control_rx.len(), 1);
        assert_eq!(dial_requests.len(), 1);
    }

    /// The value names this node, so its own entry is found by identity, not by address.
    #[test]
    fn a_state_knows_its_own_entry() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let state_rx = endpoint.state_rx();
        let state = state_rx.borrow().clone();
        assert_eq!(state.this_addr(), addr(1));
        assert_eq!(state.this_incarnation(), endpoint.node().incarnation());
        assert_eq!(
            state.this_member().map(Member::state),
            Some(MemberState::Up)
        );

        endpoint.update(|membership, _| membership.down(endpoint.node()));
        let state = state_rx.borrow().clone();
        assert_eq!(
            state.this_member().map(Member::state),
            Some(MemberState::Down)
        );
    }

    /// The published value is what `members()` answers, one version per observable change and
    /// none for a change nothing can observe.
    #[test]
    fn an_update_publishes_what_members_answers() {
        let peer = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let state_rx = endpoint.state_rx();
        assert_eq!(state_rx.borrow().version(), 0);

        assert!(
            endpoint
                .update(|membership, _| membership.add_up(peer))
                .result
        );
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 1);
        assert_eq!(state.members(), endpoint.membership().members());
        assert!(state.unreachable().is_empty());

        assert!(
            !endpoint
                .update(|membership, _| membership.add_up(peer))
                .result
        );
        assert_eq!(state_rx.borrow().version(), 1);
    }

    /// An unreachable member is published as such and leaves the set with its death, which is
    /// one version, since the Down and the derivation are published together.
    #[test]
    fn an_unreachable_member_is_published_and_cleared_by_its_death() {
        let peer = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        let state_rx = endpoint.state_rx();

        endpoint.update(|_, reachability| reachability.observe(peer, false));
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 2);
        assert_eq!(state.unreachable(), [Member::new(peer, MemberState::Up)]);

        endpoint.node_death(peer);
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 3);
        assert!(state.unreachable().is_empty());
        assert_eq!(state_of(&state, peer), Some(MemberState::Down));
    }

    /// A gossiped supersession is one value in which the superseded incarnation is already Down:
    /// no value ever holds two Up incarnations at one address.
    #[test]
    fn a_gossiped_supersession_publishes_one_consistent_value() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(older));
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        membership::on_gossip(endpoint, &[Member::new(younger, MemberState::Up)]);

        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(
            state.up().filter(|member| member.addr() == shared).count(),
            1
        );
        assert_eq!(state_of(&state, older), Some(MemberState::Down));
        assert_eq!(state_of(&state, younger), Some(MemberState::Up));
    }

    /// A self down decided by the same merge neither skips the other outcomes nor publishes
    /// twice: one value shows the superseded peer Down, the successor Up and this node Down.
    #[test]
    fn a_self_down_merge_still_applies_every_outcome() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let successor = NodeId::new(addr(1));
        endpoint.update(|membership, _| membership.add_up(older));
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        membership::on_gossip(
            endpoint,
            &[
                Member::new(younger, MemberState::Up),
                Member::new(successor, MemberState::Down),
            ],
        );

        assert!(endpoint.downed());
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(
            state.up().filter(|member| member.addr() == shared).count(),
            1
        );
        assert_eq!(state_of(&state, older), Some(MemberState::Down));
        assert_eq!(state_of(&state, younger), Some(MemberState::Up));
        assert_eq!(state_of(&state, endpoint.node()), Some(MemberState::Down));
    }

    /// A self down's fence covers this node's own observations, so pruning them can make a
    /// survivor reachable again; that happens inside the merge's update, never as a second value.
    #[test]
    fn a_self_down_merge_prunes_before_publishing() {
        let survivor = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let successor = NodeId::new(addr(1));
        endpoint.update(|membership, _| membership.add_up(survivor));
        endpoint.update(|_, reachability| reachability.observe(survivor, false));
        let state_rx = endpoint.state_rx();
        let state = state_rx.borrow().clone();
        assert_eq!(
            state.unreachable(),
            [Member::new(survivor, MemberState::Up)]
        );
        let version = state.version();

        membership::on_gossip(endpoint, &[Member::new(successor, MemberState::Down)]);

        assert!(endpoint.downed());
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert!(state.unreachable().is_empty());
        assert_eq!(state_of(&state, endpoint.node()), Some(MemberState::Down));
        assert_eq!(state_of(&state, survivor), Some(MemberState::Up));
    }

    /// A gossiped Down is applied in the same value as the rest of the merge.
    #[test]
    fn a_gossiped_down_is_applied_in_the_same_value() {
        let peer = NodeId::new(addr(2));
        let other = NodeId::new(addr(3));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        membership::on_gossip(
            endpoint,
            &[
                Member::new(peer, MemberState::Down),
                Member::new(other, MemberState::Up),
            ],
        );

        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(state_of(&state, peer), Some(MemberState::Down));
        assert_eq!(state_of(&state, other), Some(MemberState::Up));
    }

    /// Parked evidence naming a member added by the same update derives that member unreachable
    /// at once, not one tick later after a value which showed it reachable.
    #[test]
    fn parked_evidence_is_promoted_before_deriving() {
        let relay = NodeId::new(addr(2));
        let newcomer = NodeId::new(addr(3));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(relay));
        let up = HashSet::from([endpoint.node(), relay]);
        endpoint.update(|_, reachability| {
            reachability.observe(newcomer, false);
            reachability.merge(
                &[WireReachability {
                    observer: relay,
                    subject: newcomer,
                    version: 1,
                    reachable: false,
                }],
                &up,
            );
        });
        let state_rx = endpoint.state_rx();
        assert!(state_rx.borrow().unreachable().is_empty());

        endpoint.update(|membership, _| membership.add_up(newcomer));

        let state = state_rx.borrow().clone();
        assert_eq!(
            state.unreachable(),
            [Member::new(newcomer, MemberState::Up)]
        );
    }

    /// A self down publishes this node's own Down, the last value before the loops end.
    #[test]
    fn a_self_down_publishes_its_own_down() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let state_rx = endpoint.state_rx();

        endpoint.self_down();

        assert!(endpoint.downed());
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 1);
        assert_eq!(state_of(&state, endpoint.node()), Some(MemberState::Down));
    }

    /// A sweep publishes the removal, since the swept entry leaves what `members()` answers.
    #[test]
    fn a_sweep_publishes_the_removal() {
        let peer = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        endpoint.node_death(peer);
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        endpoint.update(|membership, _| membership.sweep(Duration::ZERO));

        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(state_of(&state, peer), None);
    }

    /// Versions count observable changes exactly, whichever threads make them.
    #[test]
    fn versions_count_changes_across_threads() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));

        let updates = (2..12)
            .map(|port| {
                thread::spawn(move || {
                    endpoint.update(|membership, _| membership.add_up(NodeId::new(addr(port))));
                })
            })
            .collect::<Vec<_>>();
        for update in updates {
            update.join().expect("an update does not panic");
        }

        let state = endpoint.state_rx().borrow().clone();
        assert_eq!(state.version(), 10);
        assert_eq!(state.members().len(), 11);
    }

    /// A re-entrant update panics instead of deadlocking, and the check resets for the next one.
    #[test]
    fn a_reentrant_update_panics_and_resets() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));

        let reentrant = catch_unwind(AssertUnwindSafe(|| {
            endpoint.update(|_, _| {
                endpoint.update(|_, _| ());
            });
        }));

        assert!(reentrant.is_err());
        endpoint.update(|_, _| ());
    }

    fn state_of(state: &ClusterState, node: NodeId) -> Option<MemberState> {
        state
            .members()
            .iter()
            .find(|member| member.node() == node)
            .map(Member::state)
    }

    fn gossip() -> Frame<'static> {
        Frame::Gossip {
            members: Vec::new(),
            more: false,
        }
    }

    /// A frame size below one snapshot chunk is refused at start, since a connection setup could
    /// not send its member snapshot without the receiver refusing the frame. A zero interval is
    /// refused too: both intervals drive a Tokio interval, which panics on a zero period.
    #[test]
    fn an_invalid_config_is_refused() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let required_min = membership::MIN_FRAME_SIZE.max(reachability::MIN_FRAME_SIZE);
        let mut config = EndpointConfig::new(addr);
        config.max_frame_size =
            NonZeroUsize::new(required_min - 1).expect("the minimum is above one");

        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::MaxFrameSizeTooSmall { max_frame_size, min })
                if max_frame_size == required_min - 1
                    && min == required_min
        ));

        config.max_frame_size = NonZeroUsize::new(required_min).expect("the minimum is not zero");
        assert!(validate(&config).is_ok());

        config.heartbeat_interval = Duration::ZERO;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::ZeroHeartbeatInterval)
        ));

        config.heartbeat_interval = EndpointConfig::DEFAULT_HEARTBEAT_INTERVAL;
        config.watch_refresh_interval = Duration::ZERO;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::ZeroWatchRefreshInterval)
        ));

        config.watch_refresh_interval = EndpointConfig::DEFAULT_WATCH_REFRESH_INTERVAL;
        for rejected in [3, 1 << (usize::BITS - 1)] {
            config.change_capacity = NonZeroUsize::new(rejected).expect("not zero");
            assert!(matches!(
                validate(&config),
                Err(InvalidEndpointConfig::ChangeCapacity { change_capacity, max })
                    if change_capacity == rejected && max == MAX_CHANGE_CAPACITY
            ));
        }
        for accepted in [1, 1_024] {
            config.change_capacity = NonZeroUsize::new(accepted).expect("not zero");
            assert!(validate(&config).is_ok());
        }
    }

    struct PanickingDowning(Arc<AtomicUsize>);

    impl DowningProvider for PanickingDowning {
        fn down(&mut self, _: &[Member], _: Disconnected<'_>, _: Instant) -> Downing {
            self.0.fetch_add(1, Ordering::Relaxed);
            panic!("downing provider bug");
        }
    }

    struct DowningThisNode;

    impl DowningProvider for DowningThisNode {
        fn down(&mut self, members: &[Member], _: Disconnected<'_>, _: Instant) -> Downing {
            Downing::Members(members.to_vec())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_downing_provider_is_rebuilt() {
        let (polls, built) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let mut config = EndpointConfig::new(addr(1));
        config.downing_provider = Arc::new({
            let (polls, built) = (polls.clone(), built.clone());
            move || {
                built.fetch_add(1, Ordering::Relaxed);
                Box::new(PanickingDowning(polls.clone()))
            }
        });
        let heartbeat_interval = config.heartbeat_interval;
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(config);

        let looping = task::spawn(membership_loop(endpoint));
        for _ in 0..3 {
            sleep(heartbeat_interval).await;
        }

        assert!(polls.load(Ordering::Relaxed) >= 3);
        assert!(built.load(Ordering::Relaxed) >= 3);
        assert!(!looping.is_finished());
        looping.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_provider_naming_this_node_self_downs_it() {
        let mut config = EndpointConfig::new(addr(1));
        config.downing_provider = Arc::new(|| Box::new(DowningThisNode));
        let heartbeat_interval = config.heartbeat_interval;
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(config);
        endpoint.form_cluster().expect("a fresh endpoint forms");

        let looping = task::spawn(membership_loop(endpoint));
        sleep(heartbeat_interval * 2).await;

        assert!(endpoint.downed());
        assert!(looping.is_finished());
    }

    #[tokio::test]
    async fn a_second_leave_waits_for_the_first_departure() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint
            .send(NodeId::new(addr(2)), gossip())
            .expect("a lane opens");
        let dial = dial_requests.try_recv().expect("the lane is dialed");

        let first = task::spawn(endpoint.leave());
        while !endpoint.leaving() {
            task::yield_now().await;
        }

        assert!(
            timeout(Duration::from_millis(100), endpoint.leave())
                .await
                .is_err()
        );
        assert!(!endpoint.downed());

        let _ = dial.done_tx.send(());
        first.await.expect("the first leave completes");
        assert!(endpoint.downed());
        endpoint.leave().await;
    }

    #[tokio::test]
    async fn an_aborted_leave_keeps_the_departure_running() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint
            .send(NodeId::new(addr(2)), gossip())
            .expect("a lane opens");
        let dial = dial_requests.try_recv().expect("the lane is dialed");

        let first = task::spawn(endpoint.leave());
        while !endpoint.leaving() {
            task::yield_now().await;
        }
        let second = task::spawn(endpoint.leave());
        task::yield_now().await;
        first.abort();
        assert!(first.await.is_err_and(|error| error.is_cancelled()));

        assert!(
            timeout(Duration::from_millis(100), endpoint.leave())
                .await
                .is_err()
        );
        assert!(!endpoint.downed());

        let _ = dial.done_tx.send(());
        second.await.expect("the second leave completes");
        assert!(endpoint.downed());
    }

    #[test]
    fn a_detector_too_eager_for_the_intervals_is_refused() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let mut config = EndpointConfig::new(addr);
        assert!(validate(&config).is_ok());

        let too_slow = Duration::from_secs(5);
        config.heartbeat_interval = too_slow;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::FailureDetectorTooEager { silence }) if silence == too_slow
        ));

        config.heartbeat_interval = EndpointConfig::DEFAULT_HEARTBEAT_INTERVAL;
        config.watch_refresh_interval = too_slow;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::FailureDetectorTooEager { silence }) if silence == too_slow
        ));

        config.watch_refresh_interval = EndpointConfig::DEFAULT_WATCH_REFRESH_INTERVAL;
        let deadline = Deadline::new(Duration::from_millis(500)).expect("not zero");
        config.failure_detector =
            Arc::new(move || Box::new(DeadlineFailureDetector::new(deadline)));
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::FailureDetectorTooEager { .. })
        ));

        let deadline = Deadline::new(Duration::from_secs(3)).expect("not zero");
        config.failure_detector =
            Arc::new(move || Box::new(DeadlineFailureDetector::new(deadline)));
        assert!(validate(&config).is_ok());
    }

    /// Every version reaches a subscriber which keeps up, each followed by its verdict, and a
    /// change nothing can observe sends nothing.
    #[tokio::test]
    async fn changes_stream_every_version_with_its_verdict() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let mut changes = endpoint.changes();
        let initial = endpoint.state_rx().borrow().clone();
        assert_eq!(changes.next().await, Change::State(initial));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(0, true))
        );

        let first = NodeId::new(addr(2));
        let second = NodeId::new(addr(3));
        endpoint.update(|membership, _| membership.add_up(first));
        endpoint.update(|membership, _| membership.add_up(second));
        endpoint.update(|membership, _| membership.add_up(second));

        for (version, node) in [(1, first), (2, second)] {
            match changes.next().await {
                Change::State(state) => {
                    assert_eq!(state.version(), version);
                    assert_eq!(state_of(&state, node), Some(MemberState::Up));
                }

                other => panic!("expected a state, got {other:?}"),
            }
            assert_eq!(
                changes.next().await,
                Change::Settled(Settlement::new(version, false))
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), changes.next())
                .await
                .is_err()
        );
    }

    /// A consumer may hold a borrowed cluster state while asking the receptionist, and updates
    /// keep coming: the receptionist must never wait on the watch while holding its lock, or the
    /// writer-preferring watch lock deadlocks the three of them.
    #[test]
    fn a_borrowed_state_and_a_settlement_query_never_deadlock_an_update() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let consumer = std::thread::spawn(move || {
            let state_rx = endpoint.state_rx();
            for _ in 0..2_000 {
                let borrowed = state_rx.borrow();
                let settlement = endpoint.receptionist().settlement();
                assert!(settlement.version() <= borrowed.version());
            }
        });
        let updater = std::thread::spawn(move || {
            for _ in 0..1_000 {
                let peer = NodeId::new(addr(2));
                endpoint.update(|membership, _| membership.add_up(peer));
                endpoint.update(|membership, _| membership.down(peer));
            }
        });
        std::thread::spawn(move || {
            consumer.join().expect("the consumer completes");
            updater.join().expect("the updater completes");
            let _ = done_tx.send(());
        });

        assert!(
            done_rx.recv_timeout(Duration::from_secs(30)).is_ok(),
            "the borrowing consumer and the updater deadlocked"
        );
    }

    /// Probabilistic: snapshots racing updates. A verdict for N after the state for N+1 would
    /// betray a verdict sent after the state's channel send.
    #[test]
    fn a_verdict_never_trails_the_next_state() {
        let mut config = EndpointConfig::new(addr(1));
        config.change_capacity = NonZeroUsize::new(1 << 16).expect("not zero");
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(config);
        let mut published = endpoint.published_changes();

        let snapshots = std::thread::spawn(move || {
            for _ in 0..2_000 {
                endpoint.receptionist().snapshot();
            }
        });
        for _ in 0..500 {
            let peer = NodeId::new(addr(2));
            endpoint.update(|membership, _| membership.add_up(peer));
            endpoint.update(|membership, _| membership.down(peer));
        }
        snapshots.join().expect("the snapshots complete");

        let mut last_state_version = 0;
        while let Ok(change) = published.try_recv() {
            match change {
                PublishedChange::State(state) => last_state_version = state.version(),
                PublishedChange::Settled { settlement, .. } => {
                    assert_eq!(settlement.version(), last_state_version);
                }
            }
        }
        assert_eq!(last_state_version, 1_000);
    }

    /// The documented config form, which a config file provides and which the `DEFAULT_*`
    /// constants back: the ordinary fields carry what was given, and the three pluggable ones are
    /// told apart by what they do, since they are trait objects.
    #[cfg(feature = "serde")]
    #[test]
    fn a_config_deserializes_from_its_documented_form() {
        let config = serde_json::from_str::<EndpointConfig>(
            r#"{
                "advertised_addr": "127.0.0.1:2552",
                "outbound_capacity": 128,
                "max_streams_per_peer": 4,
                "max_frame_size": 65536,
                "codec": "postcard",
                "heartbeat_interval": "500ms",
                "watch_refresh_interval": "1s",
                "failure_detector": { "deadline": "3s" },
                "downing_provider": { "down_after_deadline": { "after": "2s" } },
                "down_retention": "1m",
                "down_watermarks": 64,
                "leave_timeout": "10s",
                "reconnect_backoff": { "min": "100ms", "max": "1s" },
                "max_connect_attempts": 3,
                "change_capacity": 64
            }"#,
        )
        .expect("the documented config form deserializes");

        assert_eq!(config.advertised_addr, addr(2552));
        assert_eq!(config.outbound_capacity.get(), 128);
        assert_eq!(config.max_streams_per_peer.get(), 4);
        assert_eq!(config.max_frame_size.get(), 65_536);
        assert_eq!(config.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(config.watch_refresh_interval, Duration::from_secs(1));
        assert_eq!(config.down_retention, Duration::from_secs(60));
        assert_eq!(config.down_watermarks.get(), 64);
        assert_eq!(config.leave_timeout, Duration::from_secs(10));
        assert_eq!(config.reconnect_backoff.min(), Duration::from_millis(100));
        assert_eq!(config.reconnect_backoff.max(), Duration::from_secs(1));
        assert_eq!(config.max_connect_attempts, 3);
        assert_eq!(config.change_capacity.get(), 64);

        let message = "hello".to_string();
        assert_eq!(
            config.codec.encode(&message).expect("encodes"),
            Postcard.encode(&message).expect("encodes")
        );

        let mut detector = (config.failure_detector)();
        let now = Instant::now();
        detector.record_heartbeat(now);
        assert!(detector.is_available(now + Duration::from_secs(3)));
        assert!(!detector.is_available(now + Duration::from_secs(4)));

        let member = Member::new(NodeId::new(addr(2553)), MemberState::Up);
        let since = Instant::now();
        assert_eq!(
            (config.downing_provider)().down(
                &[member],
                Disconnected::new(&[(member, since)]),
                since + Duration::from_secs(2)
            ),
            Downing::Members(vec![member])
        );
    }

    /// Everything but the advertised address is optional and falls back to what
    /// [EndpointConfig::new] installs, so a config file names only what it changes.
    #[cfg(feature = "serde")]
    #[test]
    fn an_omitted_field_falls_back_to_its_default() {
        let config =
            serde_json::from_str::<EndpointConfig>(r#"{ "advertised_addr": "127.0.0.1:2552" }"#)
                .expect("the advertised address alone deserializes");
        let defaults = EndpointConfig::new(addr(2552));

        assert_eq!(config.advertised_addr, defaults.advertised_addr);
        assert_eq!(config.outbound_capacity, defaults.outbound_capacity);
        assert_eq!(config.max_streams_per_peer, defaults.max_streams_per_peer);
        assert_eq!(config.max_frame_size, defaults.max_frame_size);
        assert_eq!(config.heartbeat_interval, defaults.heartbeat_interval);
        assert_eq!(
            config.watch_refresh_interval,
            defaults.watch_refresh_interval
        );
        assert_eq!(config.down_retention, defaults.down_retention);
        assert_eq!(config.down_watermarks, defaults.down_watermarks);
        assert_eq!(config.leave_timeout, defaults.leave_timeout);
        assert_eq!(
            config.reconnect_backoff.min(),
            defaults.reconnect_backoff.min()
        );
        assert_eq!(
            config.reconnect_backoff.max(),
            defaults.reconnect_backoff.max()
        );
        assert_eq!(config.max_connect_attempts, defaults.max_connect_attempts);
        assert_eq!(config.change_capacity, defaults.change_capacity);

        let message = "hello".to_string();
        assert_eq!(
            config.codec.encode(&message).expect("encodes"),
            Postcard.encode(&message).expect("encodes")
        );

        let mut detector = (config.failure_detector)();
        let now = Instant::now();
        detector.record_heartbeat(now);
        assert!(
            detector.is_available(now + PhiAccrual::DEFAULT_WARMUP_DEADLINE),
            "the default detector is phi accrual, falling back to its warmup deadline"
        );
        assert!(
            !detector
                .is_available(now + PhiAccrual::DEFAULT_WARMUP_DEADLINE + Duration::from_secs(1))
        );

        let members = [
            Member::new(NodeId::new(addr(2553)), MemberState::Up),
            Member::new(NodeId::new(addr(2554)), MemberState::Up),
            Member::new(NodeId::new(addr(2555)), MemberState::Up),
        ];
        let since = Instant::now();
        let disconnected = [(members[1], since), (members[2], since)];
        assert_eq!(
            (config.downing_provider)().down(
                &members,
                Disconnected::new(&disconnected),
                since + KeepMajority::DEFAULT_AFTER
            ),
            Downing::SelfDown,
            "the default provider is keep majority, self downing the minority side"
        );
    }

    /// Deserialization goes through the same validation as [start_endpoint], so an invalid config
    /// cannot come out of a config file either; a misspelled key is an error rather than a
    /// silently applied default.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_validates_the_config() {
        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "heartbeat_interval": "0s" }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "watch_refresh_interval": "0s" }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "max_frame_size": 1 }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "leave_timeuot": "10s" }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "outbound_capacity": 128 }"#;
        assert!(
            serde_json::from_str::<EndpointConfig>(json).is_err(),
            "the advertised address is required"
        );
    }

    /// Forming is a decision, so it is taken once and only by an endpoint which is nothing yet.
    #[test]
    fn an_endpoint_forms_once() {
        let mut coordinator = Coordinator::default();

        assert!(matches!(coordinator.form(), Ok(Lifecycle::Formed)));
        assert!(matches!(coordinator.form(), Err(FormError::AlreadyFormed)));
    }

    /// A join attempt which has sent its handshake may already have been admitted somewhere,
    /// with nothing local recording it yet, so forming waits for it to resolve.
    #[test]
    fn a_join_in_flight_postpones_forming() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            ..Default::default()
        };

        assert!(matches!(coordinator.form(), Err(FormError::JoinInFlight)));

        coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert!(matches!(coordinator.form(), Ok(Lifecycle::Formed)));
    }

    /// A cluster which admitted this node may already count it, so nothing may be formed beside
    /// it however that attempt ended.
    #[test]
    fn a_pinned_cluster_postpones_forming() {
        let mut coordinator = Coordinator {
            pinned: Some(addr(1)),
            ..Default::default()
        };

        assert!(matches!(
            coordinator.form(),
            Err(FormError::ClusterPinned(_))
        ));
    }

    /// A completed join is what the pin was held for, so committing it makes a member and
    /// releases the pin; committing again is the documented snapshot refresh and stays harmless.
    #[test]
    fn a_completed_join_makes_a_member_and_releases_the_pin() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some(addr(1)),
            ..Default::default()
        };

        let finished = coordinator.finish_join(addr(1), Ok(()));
        assert!(finished.result.is_ok());
        assert!(!finished.newly_downed);
        assert_eq!(coordinator.lifecycle, Lifecycle::Formed);
        assert_eq!(coordinator.pinned, None);
        assert!(!coordinator.join_in_flight);

        let finished = coordinator.finish_join(addr(1), Ok(()));
        assert!(finished.result.is_ok());
        assert_eq!(coordinator.lifecycle, Lifecycle::Formed);
    }

    /// A down landing while the attempt ran wins over its success: the caller learns it was
    /// downed rather than joined, and the side effects, already run by the down, are not rerun.
    #[test]
    fn a_join_completing_after_a_down_reports_the_down() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            ..Default::default()
        };
        assert!(coordinator.down());

        let finished = coordinator.finish_join(addr(1), Ok(()));
        assert!(matches!(finished.result, Err(ConnectError::SelfDowned)));
        assert!(!finished.newly_downed);
        assert_eq!(coordinator.lifecycle, Lifecycle::Downed);
    }

    /// A refusal as dead downs this node in the same step which releases the permit, so no
    /// formation fits between the two, and the node is never formed in passing.
    #[test]
    fn a_refusal_as_dead_downs_atomically_with_the_release() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some(addr(1)),
            ..Default::default()
        };

        let finished =
            coordinator.finish_join(addr(1), Err(ConnectError::Refused(RefusalReason::Down)));
        assert!(matches!(
            finished.result,
            Err(ConnectError::Refused(RefusalReason::Down))
        ));
        assert!(finished.newly_downed);
        assert_eq!(coordinator.lifecycle, Lifecycle::Downed);
        assert_eq!(coordinator.pinned, None);
        assert!(!coordinator.join_in_flight);
        assert!(matches!(coordinator.form(), Err(FormError::Downed)));

        let finished =
            coordinator.finish_join(addr(1), Err(ConnectError::Refused(RefusalReason::Down)));
        assert!(!finished.newly_downed);
    }

    /// Only the pinned address answering that it is no cluster releases the pin; an answer from
    /// elsewhere, or any other failure, leaves the evidence standing.
    #[test]
    fn only_the_pinned_address_answering_no_cluster_releases_the_pin() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some(addr(1)),
            ..Default::default()
        };

        coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert_eq!(coordinator.pinned, Some(addr(1)));

        coordinator.finish_join(
            addr(2),
            Err(ConnectError::Refused(RefusalReason::NoCluster)),
        );
        assert_eq!(coordinator.pinned, Some(addr(1)));

        coordinator.finish_join(
            addr(1),
            Err(ConnectError::Refused(RefusalReason::NoCluster)),
        );
        assert_eq!(coordinator.pinned, None);
    }

    /// Discovery dropping the pinned address releases the pin, but never while an attempt runs,
    /// since that attempt may be gathering fresh evidence for the same address.
    #[test]
    fn a_pin_discovery_dropped_is_released_only_between_attempts() {
        let universe = BTreeSet::from([addr(2), addr(3)]);
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some(addr(1)),
            ..Default::default()
        };

        assert_eq!(coordinator.release_pin_if_gone(&universe), None);
        assert_eq!(coordinator.pinned, Some(addr(1)));

        coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert_eq!(coordinator.release_pin_if_gone(&universe), Some(addr(1)));
        assert_eq!(coordinator.pinned, None);

        coordinator.pinned = Some(addr(2));
        assert_eq!(coordinator.release_pin_if_gone(&universe), None);
        assert_eq!(coordinator.pinned, Some(addr(2)));
    }
    /// A down may land anywhere inside an attempt, and must win everywhere: no attempt starts on
    /// a downed node, no pin is written after it, and whatever the attempt's raw outcome, its
    /// caller learns of the down and nothing stays pinned.
    #[test]
    fn a_down_dominates_every_stage_of_a_join() {
        let mut coordinator = Coordinator::default();
        assert!(coordinator.enter_join());
        assert!(coordinator.down());
        assert!(matches!(coordinator.form(), Err(FormError::Downed)));

        coordinator.pin(addr(1));
        assert_eq!(coordinator.pinned, None);

        let finished = coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert!(matches!(finished.result, Err(ConnectError::SelfDowned)));
        assert!(!finished.newly_downed);
        assert_eq!(coordinator.pinned, None);
        assert!(!coordinator.join_in_flight);

        assert!(!coordinator.enter_join());
        assert!(matches!(coordinator.form(), Err(FormError::Downed)));
    }
}
