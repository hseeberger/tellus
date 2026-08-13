#[cfg(feature = "serde")]
use crate::cluster::{
    codec::CodecConfig, downing::DowningProviderConfig, failure::FailureDetectorConfig,
};
use crate::{
    Backoff,
    cluster::{
        codec::{Codec, Postcard},
        discovery::PendingLookups,
        downing::{Disconnected, Downing, DowningProviderFactory, KeepMajority},
        failure::{FailureDetectorFactory, Liveness, PeerLiveness, PhiAccrualFailureDetector},
        frame::{Frame, RefusalReason, StreamKey},
        membership::{self, Membership, WireMember},
        node::NodeId,
        peer::{ConnectError, DialRequest, JoinRequest, accept_loop, dial_loop, join_loop},
        reachability::{self, Reachability, WireReachability},
        registry::Registry,
        reply::PendingReplies,
        transport::Transport,
        watch::{WatcherTable, WireWatchTable},
    },
    quota::{CountedSendError, CountedSender, Quota},
    sync::{lock, read, write},
};
use derive_more::Debug;
use flume::Sender;
use std::{
    collections::{BTreeSet, HashMap, HashSet, hash_map::Entry},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    sync::watch,
    task::{self, JoinHandle},
    time::{MissedTickBehavior, interval},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

static ENDPOINT: OnceLock<EndpointInner> = OnceLock::new();

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
    /// lane carrying everything, and `1` does the same for one with.
    ///
    /// All of them are opened at connection setup, so a peer admitting fewer concurrent streams
    /// fails the connection right there instead of stalling a stream later; QUIC peers admit 100
    /// by default, which is the ceiling to stay under.
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

    /// How long [leave](fn@crate::cluster::leave) waits for the announced departure to leave the
    /// outbound queues before severing anyway. Bounds a shutting down process, so it is sized by
    /// the deployment's termination grace period rather than by the gossip cadence; a drain
    /// costs milliseconds. Defaults to [EndpointConfig::DEFAULT_LEAVE_TIMEOUT].
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
        }
    }
}

/// The [EndpointConfig] given to [start_endpoint] is invalid.
#[derive(Debug, Error)]
pub enum InvalidEndpointConfig {
    /// The configured `max_frame_size` cannot hold a member snapshot's smallest chunk, so a
    /// connection setup could not send one without the receiver refusing it.
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
/// another node's. Can only be called once per process; references can only be
/// serialized and deserialized after this.
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

/// A task and the entry it was started for must agree, so a stale task cannot close a newer one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Generation(u64);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    #[default]
    Unformed,
    Formed,
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

#[derive(Debug, Error)]
pub(crate) enum LaneError {
    #[error("node {0} unreachable")]
    NodeUnreachable(NodeId),

    #[error("outbound queue towards node {0} full")]
    OutboundQueueFull(NodeId),
}

pub(crate) struct EndpointInner {
    node: NodeId,
    data_streams: usize,
    config: EndpointConfig,
    registry: Arc<Registry>,
    lanes: RwLock<HashMap<SocketAddr, Lane>>,
    inbound_readers: tokio::sync::Mutex<HashMap<SocketAddr, InboundReader>>,
    next_generation: AtomicU64,
    membership: Membership,
    refused_addrs: Mutex<HashSet<SocketAddr>>,
    lifecycle: AtomicU8,
    coordinator: Mutex<Coordinator>,
    liveness: Liveness,
    reachability: Reachability,
    watchers: WatcherTable,
    wire_watches: WireWatchTable,
    pending_lookups: PendingLookups,
    pending_replies: PendingReplies,
    dial_request_tx: Sender<DialRequest>,
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

    pub(crate) fn watchers(&self) -> &WatcherTable {
        &self.watchers
    }

    pub(crate) fn wire_watches(&self) -> &WireWatchTable {
        &self.wire_watches
    }

    pub(crate) fn pending_lookups(&self) -> &PendingLookups {
        &self.pending_lookups
    }

    pub(crate) fn pending_replies(&self) -> &PendingReplies {
        &self.pending_replies
    }

    pub(crate) fn merge_reachability(&self, observations: &[WireReachability]) {
        self.reachability
            .merge(observations, &self.membership.up_nodes());
    }

    pub(crate) fn reconnect_backoff(&self, attempts: u32) -> Duration {
        self.config
            .reconnect_backoff
            .duration(attempts.saturating_sub(1))
    }

    /// System frames bypass the outbound capacity; a frame's [StreamKey] picks the stream, hence
    /// FIFO per recipient. A bound lane refuses any incarnation but its own.
    pub(crate) fn send(&self, peer: NodeId, frame: Frame<'static>) -> Result<(), LaneError> {
        if self.downed() {
            return Err(LaneError::NodeUnreachable(peer));
        }

        #[cfg(feature = "cluster-dev")]
        if matches!(frame, Frame::Terminated { .. }) && self.drop_terminated() {
            debug!(%peer, "dropping terminated frame, fault injection");
            return Ok(());
        }

        {
            let lanes = read(&self.lanes);
            if let Some(outbound_tx) = Self::matching_lane(&lanes, peer, &frame)? {
                return Self::try_send(outbound_tx, frame, peer);
            }
        }

        if self.membership.is_down(peer) {
            return Err(LaneError::NodeUnreachable(peer));
        }

        let mut lanes = write(&self.lanes);
        if let Some(outbound_tx) = Self::matching_lane(&lanes, peer, &frame)? {
            return Self::try_send(outbound_tx, frame, peer);
        }
        if self.is_refused(peer.addr()) {
            return Err(LaneError::NodeUnreachable(peer));
        }

        let outbound_tx = self.open_lane(&mut lanes, peer.addr(), &frame);
        Self::try_send(outbound_tx, frame, peer)
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
        Lifecycle::from_code(self.lifecycle.load(Ordering::Relaxed))
    }

    pub(crate) fn formed(&self) -> bool {
        self.lifecycle() == Lifecycle::Formed
    }

    pub(crate) fn downed(&self) -> bool {
        self.lifecycle() == Lifecycle::Downed
    }

    pub(crate) fn form_cluster(&self) -> Result<(), FormError> {
        let mut coordinator = lock(&self.coordinator);
        self.set_lifecycle(coordinator.form()?);
        Ok(())
    }

    /// Observed by [form]; exclusivity itself comes from the single join loop, not from this.
    /// `false` if this node is downed, which no attempt may outlive.
    pub(crate) fn enter_join(&self) -> bool {
        lock(&self.coordinator).enter_join()
    }

    /// The single commit point of a join attempt: permit release and outcome happen under one
    /// lock, so no formation slips in between; a down decided here runs its side effects once.
    pub(crate) fn finish_join(
        &self,
        addr: SocketAddr,
        result: Result<(), ConnectError>,
    ) -> Result<(), ConnectError> {
        let finished = {
            let mut coordinator = lock(&self.coordinator);
            let finished = coordinator.finish_join(addr, result);
            self.set_lifecycle(coordinator.lifecycle);
            finished
        };

        if finished.newly_downed {
            self.sever_down();
            error!("this node has been downed, restart the process to rejoin");
        }
        finished.result
    }

    pub(crate) fn pin_cluster(&self, addr: SocketAddr) {
        lock(&self.coordinator).pin(addr);
    }

    pub(crate) fn pinned_cluster(&self) -> Option<SocketAddr> {
        lock(&self.coordinator).pinned
    }

    /// Decided under one lock, so evidence a running attempt has gathered is never mistaken for
    /// the stale pin.
    pub(crate) fn release_pin_if_gone(
        &self,
        universe: &BTreeSet<SocketAddr>,
    ) -> Option<SocketAddr> {
        lock(&self.coordinator).release_pin_if_gone(universe)
    }

    /// Honoring this Down keeps other nodes' synthesized signals true; only a restart rejoins.
    pub(crate) fn self_down(&self) {
        if self.latch_down() {
            error!("this node has been downed, restart the process to rejoin");
        }
    }

    /// The same end, chosen and announced, hence logged as a departure rather than an error.
    pub(crate) fn leave_down(&self) {
        if self.latch_down() {
            info!("this node has left the cluster, restart the process to rejoin");
        }
    }

    /// Pushes to every Up member, a disconnected lane included: a departure is sent once.
    pub(crate) fn announce(&self) {
        let members = self.membership.snapshot();
        for peer in self.membership.up_peers() {
            for frame in self.snapshot_frames(members.clone()) {
                if let Err(error) = self.send(peer, frame) {
                    debug!(%peer, %error, "cannot announce the departure");
                }
            }
        }
    }

    pub(crate) fn snapshot_frames(&self, members: Vec<WireMember>) -> Vec<Frame<'static>> {
        membership::snapshot_frames(members, self.config.max_frame_size.get())
    }

    pub(crate) fn reachability_snapshot_frames(&self) -> Vec<Frame<'static>> {
        let up = self.membership.up_nodes();
        reachability::snapshot_frames(
            self.reachability.snapshot(&up),
            self.config.max_frame_size.get(),
        )
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

        let members = self.membership.snapshot();
        for peer in self.membership.up_peers() {
            if self.lane_connected(peer.addr()) == Some(true)
                && let Err(error) = self
                    .snapshot_frames(members.clone())
                    .into_iter()
                    .try_for_each(|frame| self.send(peer, frame))
            {
                debug!(%peer, %error, "cannot push gossip");
            }
        }
    }

    /// `None` if no lane exists; gossip skips on connectivity alone, never on suspicion.
    pub(crate) fn lane_connected(&self, addr: SocketAddr) -> Option<bool> {
        read(&self.lanes).get(&addr).map(|lane| lane.connected)
    }

    /// Every lane counts, a still dialing one included; a peer which never returns never drains.
    pub(crate) fn outbound_drained(&self) -> bool {
        read(&self.lanes).values().all(Lane::drained)
    }

    pub(crate) fn mark_lane_disconnected(&self, addr: SocketAddr, lane_id: Generation) {
        let mut lanes = write(&self.lanes);
        if let Some(lane) = lanes.get_mut(&addr)
            && lane.id == lane_id
        {
            lane.connected = false;
        }
    }

    /// Mark Down, close the lane, quiesce deliveries, then fail asks and flush synthesized signals.
    pub(crate) fn node_death(&self, peer: NodeId) {
        let down = self.membership.down(peer);
        if !down.member_changed && !down.watermark_changed {
            return;
        }

        self.flush_fenced(peer);
        if down.member_changed {
            self.push_gossip();
        }
    }

    pub(crate) fn flush_fenced(&self, fence: NodeId) {
        {
            // An unbound lane is still dialing and may already serve the successor incarnation!
            let mut lanes = write(&self.lanes);
            if lanes
                .get(&fence.addr())
                .is_some_and(|lane| lane.peer.is_some_and(|peer| fence.covers(peer)))
            {
                lanes.remove(&fence.addr());
            }
        }

        self.liveness.quiesce_fenced(fence);
        self.reachability.prune_fenced(fence);
        self.pending_lookups.fail_fenced(fence);
        self.pending_replies.fail_fenced(fence);

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

    /// The Down state must be read under the lanes lock, else a node death in between is missed.
    pub(crate) fn bind_lane(&self, addr: SocketAddr, lane_id: Generation, peer: NodeId) {
        let mut lanes = write(&self.lanes);
        let down = self.membership.is_down(peer);
        Self::bind_or_remove_lane(&mut lanes, addr, lane_id, peer, down);
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
        read(&self.lanes)
            .get(&addr)
            .is_some_and(|lane| lane.id == lane_id)
    }

    /// `true` if this call removed the lane, i.e. it was still the address's current one.
    pub(crate) fn remove_lane(&self, addr: SocketAddr, lane_id: Generation) -> bool {
        let mut lanes = write(&self.lanes);
        if lanes.get(&addr).is_some_and(|lane| lane.id == lane_id) {
            lanes.remove(&addr);
            true
        } else {
            false
        }
    }

    fn new(
        config: EndpointConfig,
        data_streams: usize,
        dial_request_tx: Sender<DialRequest>,
        join_request_tx: Sender<JoinRequest>,
    ) -> Self {
        let node = NodeId::new(config.advertised_addr);
        let down_watermarks = config.down_watermarks;

        Self {
            node,
            data_streams,
            config,
            registry: Arc::new(Registry::new()),
            lanes: RwLock::new(HashMap::new()),
            inbound_readers: tokio::sync::Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(0),
            membership: Membership::new(node, down_watermarks),
            refused_addrs: Mutex::new(HashSet::new()),
            lifecycle: AtomicU8::new(Lifecycle::Unformed.code()),
            coordinator: Mutex::new(Coordinator::default()),
            liveness: Liveness::new(),
            reachability: Reachability::new(node),
            watchers: WatcherTable::new(),
            wire_watches: WireWatchTable::new(),
            pending_lookups: PendingLookups::new(),
            pending_replies: PendingReplies::new(),
            dial_request_tx,
            join_request_tx,
            sever_tx: watch::channel(0).0,
            #[cfg(feature = "cluster-dev")]
            dropped_terminated: AtomicU64::new(0),
        }
    }

    fn is_refused(&self, addr: SocketAddr) -> bool {
        lock(&self.refused_addrs).contains(&addr)
    }

    /// Call only under the coordinator lock, else the lock free reads drift from the pin.
    fn set_lifecycle(&self, lifecycle: Lifecycle) {
        self.lifecycle.store(lifecycle.code(), Ordering::Relaxed);
    }

    /// `true` if this call ended the endpoint, so the caller logs why exactly once.
    fn latch_down(&self) -> bool {
        {
            let mut coordinator = lock(&self.coordinator);
            if !coordinator.down() {
                return false;
            }
            self.set_lifecycle(Lifecycle::Downed);
        }

        self.sever_down();
        true
    }

    fn sever_down(&self) {
        self.membership.down(self.node);
        self.sever_tx.send_modify(|generation| *generation += 1);
    }

    /// A dead peer's lane is removed rather than bound, so a successor opens a fresh one.
    fn bind_or_remove_lane(
        lanes: &mut HashMap<SocketAddr, Lane>,
        addr: SocketAddr,
        lane_id: Generation,
        peer: NodeId,
        down: bool,
    ) {
        if let Entry::Occupied(mut lane) = lanes.entry(addr)
            && lane.get().id == lane_id
        {
            if down {
                lane.remove();
            } else {
                let lane = lane.get_mut();
                lane.peer = Some(peer);
                lane.connected = true;
            }
        }
    }

    fn try_send(
        outbound_tx: &CountedSender<Frame<'static>>,
        frame: Frame<'static>,
        peer: NodeId,
    ) -> Result<(), LaneError> {
        let result = if frame.is_counted() {
            outbound_tx.try_send_counted(frame)
        } else {
            outbound_tx
                .try_send_uncounted(frame)
                .map_err(CountedSendError::from)
        };

        result.map_err(|error| match error {
            CountedSendError::Full(_) => LaneError::OutboundQueueFull(peer),
            CountedSendError::Disconnected(_) => LaneError::NodeUnreachable(peer),
        })
    }

    fn matching_lane<'a>(
        lanes: &'a HashMap<SocketAddr, Lane>,
        peer: NodeId,
        frame: &Frame,
    ) -> Result<Option<&'a CountedSender<Frame<'static>>>, LaneError> {
        match lanes.get(&peer.addr()) {
            Some(lane) if lane.peer.is_none_or(|bound| bound == peer) => {
                Ok(Some(lane.outbound_tx(frame)))
            }

            Some(_) => Err(LaneError::NodeUnreachable(peer)),

            None => Ok(None),
        }
    }

    /// One [Quota] for the whole lane, so the capacity holds however many streams share it.
    fn open_lane<'a>(
        &self,
        lanes: &'a mut HashMap<SocketAddr, Lane>,
        addr: SocketAddr,
        frame: &Frame,
    ) -> &'a CountedSender<Frame<'static>> {
        let quota = Quota::bounded(self.config.outbound_capacity);
        let lane_id = self.next_generation();

        let (control_tx, control_rx) = flume::unbounded();
        let control_tx = CountedSender::new(control_tx, quota.clone());
        let (data_tx, data_rx) = (0..self.data_streams)
            .map(|_| {
                let (data_tx, data_rx) = flume::unbounded();
                (CountedSender::new(data_tx, quota.clone()), data_rx)
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let lane = Lane {
            id: lane_id,
            peer: None,
            connected: false,
            control_tx,
            data_tx,
        };
        lanes.insert(addr, lane);

        let dial_request = DialRequest {
            addr,
            lane_id,
            control_rx,
            data_rx,
            quota,
        };
        if self.dial_request_tx.send(dial_request).is_err() {
            error!(peer_addr = %addr, "dial loop gone, lane will never be connected");
            debug_assert!(false, "dial loop gone");
        }

        lanes
            .get(&addr)
            .expect("lane was just inserted")
            .outbound_tx(frame)
    }

    fn next_generation(&self) -> Generation {
        Generation(self.next_generation.fetch_add(1, Ordering::Relaxed))
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

    Ok(())
}

pub(crate) fn get() -> Option<&'static EndpointInner> {
    ENDPOINT.get()
}

struct Lane {
    id: Generation,
    peer: Option<NodeId>,
    connected: bool,
    control_tx: CountedSender<Frame<'static>>,
    data_tx: Vec<CountedSender<Frame<'static>>>,
}

impl Lane {
    /// The control queue counts too: a departure rides it and a leave waits for both.
    fn drained(&self) -> bool {
        self.control_tx.is_empty() && self.data_tx.iter().all(CountedSender::is_empty)
    }

    fn outbound_tx(&self, frame: &Frame) -> &CountedSender<Frame<'static>> {
        let data_tx = frame
            .stream_key()
            .zip(NonZeroUsize::new(self.data_tx.len()))
            .map(|(key, streams)| &self.data_tx[stream_index(key, streams)]);

        data_tx.unwrap_or(&self.control_tx)
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

/// The mapping only has to agree with itself: nothing about it travels the wire.
fn stream_index(key: StreamKey, streams: NonZeroUsize) -> usize {
    let value = match key {
        StreamKey::Actor(actor) => Uuid::from(actor).as_u128() as u64,
        StreamKey::Nonce(nonce) => nonce.as_u64(),
    };
    value as usize % streams
}

/// One tick does all periodic membership work; the downing provider lives here, so it needs no
/// locking. A self downed node stops the loop for good.
async fn membership_loop(endpoint: &'static EndpointInner) {
    let mut provider = (endpoint.config.downing_provider)();
    let mut ticks = interval(endpoint.config.heartbeat_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticks.tick().await;
        if endpoint.downed() {
            break;
        }

        let members = endpoint.membership.snapshot();
        for peer in endpoint.membership.up_peers() {
            if endpoint.lane_connected(peer.addr()) == Some(false) {
                continue;
            }
            if let Err(error) = endpoint
                .snapshot_frames(members.clone())
                .into_iter()
                .try_for_each(|frame| endpoint.send(peer, frame))
            {
                debug!(%peer, %error, "cannot send gossip");
            }
        }

        for peer in endpoint.membership.up_peers() {
            if endpoint.liveness.is_available(peer) {
                if endpoint.reachability.observe(peer, true) {
                    debug!(%peer, "member reachable again");
                }
            } else if endpoint.reachability.observe(peer, false) {
                warn!(%peer, "member directly unreachable");
            }
        }

        let up = endpoint.membership.up_nodes();
        endpoint.reachability.promote_pending(&up);
        endpoint.push_reachability(endpoint.reachability.take_outbound(&up));

        let members = endpoint.membership.members();
        let disconnected = endpoint.reachability.unreachable_members(&members);
        match provider.down(&members, Disconnected::new(&disconnected), Instant::now()) {
            Downing::Members(members) => {
                for member in members {
                    warn!(%member, "node death, downed by the downing provider");
                    endpoint.node_death(member.node());
                }
            }

            Downing::SelfDown => {
                warn!("self down, decided by the downing provider");
                endpoint.self_down();
                break;
            }
        }

        endpoint.membership.sweep(endpoint.config.down_retention);
        endpoint.untrack_idle_peers();
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
    #[cfg(feature = "serde")]
    use crate::cluster::{
        codec::{Codec, Postcard},
        downing::{Disconnected, Downing, KeepMajority},
        failure::PhiAccrual,
        membership::{Member, MemberState},
    };
    use crate::{
        ActorId,
        cluster::{
            discovery::Nonce,
            endpoint::{
                Coordinator, EndpointConfig, EndpointInner, FormError, Generation,
                InvalidEndpointConfig, Lane, LaneError, Lifecycle, stream_index, validate,
            },
            frame::{Frame, RefusalReason, StreamKey},
            membership,
            node::NodeId,
            peer::ConnectError,
            reachability::{self, Reachability, WireReachability},
        },
        quota::{CountedSender, Quota},
    };
    use flume::Receiver;
    #[cfg(feature = "serde")]
    use std::time::Instant;
    use std::{
        borrow::Cow,
        collections::{BTreeSet, HashMap, HashSet},
        net::SocketAddr,
        num::NonZeroUsize,
        sync::atomic::AtomicU64,
        time::Duration,
    };

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn lane(
        streams: usize,
    ) -> (
        Lane,
        Receiver<Frame<'static>>,
        Vec<Receiver<Frame<'static>>>,
    ) {
        lane_with_quota(streams, Quota::unbounded())
    }

    /// Sequential nonces spread round-robin without a general-purpose hash, while a key always
    /// maps to the same stream.
    #[test]
    fn nonce_streams_use_the_nonce_value_directly() {
        let streams = NonZeroUsize::new(8).expect("eight is nonzero");
        let next = AtomicU64::new(0);
        let indices = (0..8)
            .map(|_| stream_index(StreamKey::Nonce(Nonce::mint(&next)), streams))
            .collect::<Vec<_>>();
        assert_eq!(indices, (0..8).collect::<Vec<_>>());
    }

    /// Once admission has classified an address as permanently incompatible, later frames fail
    /// locally instead of opening one new lane (and therefore one new dial) per message.
    #[test]
    fn a_refused_address_is_not_redialed_per_frame() {
        let local = addr(1);
        let peer = NodeId::new(addr(2));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(local));
        endpoint.membership().add_up(peer);
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
            endpoint.membership().add_up(member);
        }
        let up = members.iter().copied().collect::<HashSet<_>>();

        endpoint.reachability.observe(subject, false);
        endpoint.reachability.merge(
            &[WireReachability {
                observer: relay,
                subject,
                version: 1,
                reachable: false,
            }],
            &up,
        );

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
        endpoint.membership().add_up(silent);
        endpoint.membership().add_up(fresh);

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

    fn lane_with_quota(
        streams: usize,
        quota: Quota,
    ) -> (
        Lane,
        Receiver<Frame<'static>>,
        Vec<Receiver<Frame<'static>>>,
    ) {
        let (control_tx, control_rx) = flume::unbounded();
        let control_tx = CountedSender::new(control_tx, quota.clone());
        let (data_tx, data_rx) = (0..streams)
            .map(|_| {
                let (data_tx, data_rx) = flume::unbounded();
                (CountedSender::new(data_tx, quota.clone()), data_rx)
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let lane = Lane {
            id: Generation(0),
            peer: None,
            connected: false,
            control_tx,
            data_tx,
        };
        (lane, control_rx, data_rx)
    }

    fn send(lane: &Lane, frame: Frame<'static>) {
        lane.outbound_tx(&frame)
            .try_send_uncounted(frame)
            .expect("the queue is open");
    }

    fn gossip() -> Frame<'static> {
        Frame::Gossip {
            members: Vec::new(),
            more: false,
        }
    }

    fn message(target: ActorId) -> Frame<'static> {
        Frame::Message {
            target,
            reply_tags: Vec::new(),
            payload: Cow::Borrowed(&[]),
        }
    }

    fn try_send(lane: &Lane, frame: Frame<'static>, peer: NodeId) -> Result<(), LaneError> {
        let outbound_tx = lane.outbound_tx(&frame);
        EndpointInner::try_send(outbound_tx, frame, peer)
    }

    /// A lane bound to one incarnation refuses frames for any other, so a frame for a dead
    /// incarnation never rides its successor's connection.
    #[test]
    fn a_frame_for_another_incarnation_is_refused() {
        let (mut lane, _control_rx, _data_rx) = lane(0);
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let peer = NodeId::new(addr);
        lane.peer = Some(peer);
        let lanes = HashMap::from([(addr, lane)]);

        assert!(matches!(
            EndpointInner::matching_lane(&lanes, peer, &gossip()),
            Ok(Some(_))
        ));

        let successor = NodeId::new(addr);
        assert!(matches!(
            EndpointInner::matching_lane(&lanes, successor, &gossip()),
            Err(LaneError::NodeUnreachable(_))
        ));

        let unknown = NodeId::new("127.0.0.1:2553".parse().expect("valid address"));
        assert!(matches!(
            EndpointInner::matching_lane(&lanes, unknown, &gossip()),
            Ok(None)
        ));
    }

    /// A leave waits for the control queue, which carries its departure, and for the data queues,
    /// which carry the terminated signals it must stay behind; a lane is drained only once every
    /// one of them is empty.
    #[test]
    fn a_lane_is_drained_once_every_queue_is_empty() {
        let (lane, control_rx, data_rx) = lane(2);
        let target = ActorId::new();

        assert!(lane.drained(), "a fresh lane has nothing queued");

        send(&lane, gossip());
        assert!(!lane.drained());
        let _ = control_rx.recv().expect("the queue holds the gossip");
        assert!(lane.drained());

        send(&lane, message(target));
        assert!(!lane.drained());
        for data_rx in &data_rx {
            while data_rx.recv_timeout(Duration::ZERO).is_ok() {}
        }
        assert!(lane.drained());
    }

    /// A transport without data streams puts every frame on the control stream, which is one
    /// ordered lane per peer carrying everything: the guarantees hold there by the same argument,
    /// not as a special case.
    #[test]
    fn without_data_streams_everything_rides_the_control_stream() {
        let (lane, control_rx, _data_rx) = lane(0);
        let (target, watcher) = (ActorId::new(), ActorId::new());

        assert!(lane.data_tx.is_empty(), "the lane has no data streams");

        send(
            &lane,
            Frame::Message {
                target,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[]),
            },
        );
        send(&lane, Frame::Terminated { target, watcher });
        send(&lane, Frame::Watch { target, watcher });
        send(&lane, gossip());

        assert_eq!(control_rx.len(), 4);
    }

    /// A terminated signal rides its watcher's stream, the one the messages to that watcher ride:
    /// that shared queue is the whole mechanism behind the ordering guarantee.
    #[test]
    fn a_terminated_signal_shares_its_watchers_stream() {
        let streams = NonZeroUsize::new(8).expect("8 is not zero");
        let (lane, control_rx, data_rx) = lane(streams.get());
        let watcher = ActorId::new();

        send(
            &lane,
            Frame::Message {
                target: watcher,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[]),
            },
        );
        send(
            &lane,
            Frame::Terminated {
                target: ActorId::new(),
                watcher,
            },
        );

        assert_eq!(
            data_rx[stream_index(StreamKey::Actor(watcher), streams)].len(),
            2
        );
        assert!(control_rx.is_empty());
    }

    /// Per-node frames stay on the control stream even where data streams exist: only frames
    /// delivered to an actor have a stream to pick.
    #[test]
    fn per_node_frames_stay_on_the_control_stream() {
        let (lane, control_rx, data_rx) = lane(8);
        let (target, watcher) = (ActorId::new(), ActorId::new());

        send(&lane, Frame::Watch { target, watcher });
        send(&lane, Frame::Unwatch { target, watcher });
        send(&lane, gossip());

        assert_eq!(control_rx.len(), 3);
        assert!(data_rx.iter().all(Receiver::is_empty));
    }

    /// Message frames are counted against the outbound capacity and become dead letters once it
    /// is exhausted, while system frames bypass the count: a terminated signal must never be
    /// dropped for want of capacity. A queued frame holds its slot until the receiver dequeues
    /// it, so the second message is refused although the first is still in the queue.
    #[test]
    fn messages_are_counted_against_the_outbound_capacity_and_system_frames_bypass_it() {
        let capacity = NonZeroUsize::new(1).expect("1 is not zero");
        let (lane, control_rx, _data_rx) = lane_with_quota(0, Quota::bounded(capacity));
        let (target, watcher) = (ActorId::new(), ActorId::new());
        let peer = NodeId::new("127.0.0.1:2552".parse().expect("valid address"));

        assert!(try_send(&lane, message(target), peer).is_ok());
        assert!(matches!(
            try_send(&lane, message(target), peer),
            Err(LaneError::OutboundQueueFull(full)) if full == peer
        ));

        assert!(try_send(&lane, Frame::Terminated { target, watcher }, peer).is_ok());
        assert!(try_send(&lane, Frame::Watch { target, watcher }, peer).is_ok());
        assert!(try_send(&lane, gossip(), peer).is_ok());

        assert_eq!(control_rx.len(), 4);
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
                "max_connect_attempts": 3
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

    /// A send towards a peer whose lane is gone is unreachable, not full: the two failures steer
    /// different callers, dead-lettering versus giving up on the node.
    #[test]
    fn a_disconnected_lane_reports_the_node_unreachable() {
        let capacity = NonZeroUsize::new(1).expect("1 is not zero");
        let (lane, control_rx, _data_rx) = lane_with_quota(0, Quota::bounded(capacity));
        let target = ActorId::new();
        let peer = NodeId::new("127.0.0.1:2552".parse().expect("valid address"));
        drop(control_rx);

        assert!(matches!(
            try_send(&lane, message(target), peer),
            Err(LaneError::NodeUnreachable(unreachable)) if unreachable == peer
        ));
        assert!(matches!(
            try_send(&lane, gossip(), peer),
            Err(LaneError::NodeUnreachable(unreachable)) if unreachable == peer
        ));
    }

    /// A dial which lost the race against node death removes its lane instead of binding it to
    /// the dead incarnation, so a send to the successor opens a fresh lane rather than being
    /// refused until the dead connection breaks.
    #[test]
    fn binding_a_tombstoned_peer_removes_the_lane() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let peer = NodeId::new(addr);
        let (lane, _control_rx, _data_rx) = lane(0);
        let mut lanes = HashMap::from([(addr, lane)]);

        EndpointInner::bind_or_remove_lane(&mut lanes, addr, Generation(0), peer, true);

        assert!(lanes.is_empty());
    }

    /// Binding a live peer names it on the lane, and a lane under another ID or address stays
    /// untouched either way: it belongs to a successor dial, which a stale task must not close.
    #[test]
    fn binding_names_the_peer_and_spares_other_lanes() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let peer = NodeId::new(addr);
        let (lane, _control_rx, _data_rx) = lane(0);
        let mut lanes = HashMap::from([(addr, lane)]);

        EndpointInner::bind_or_remove_lane(&mut lanes, addr, Generation(0), peer, false);
        assert_eq!(lanes.get(&addr).and_then(|lane| lane.peer), Some(peer));

        EndpointInner::bind_or_remove_lane(
            &mut lanes,
            addr,
            Generation(1),
            NodeId::new(addr),
            true,
        );
        assert_eq!(lanes.get(&addr).and_then(|lane| lane.peer), Some(peer));

        let other_addr = "127.0.0.1:2553".parse().expect("valid address");
        EndpointInner::bind_or_remove_lane(&mut lanes, other_addr, Generation(0), peer, true);
        assert_eq!(lanes.len(), 1);
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
