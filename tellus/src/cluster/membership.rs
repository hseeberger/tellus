use crate::{
    cluster::{
        endpoint::{self, EndpointInner},
        formation::JoinOutcome,
        frame::{Frame, RefusalReason},
        node::NodeId,
        peer::{ConnectError, JoinRequest},
    },
    sync::{read, write},
};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::RwLock,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{sync::oneshot, time::sleep};
use tracing::{debug, warn};

/// A member's two-point state lattice: `Up < Down`, with removal represented by absence.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MemberState {
    /// A member: heartbeated, gossiped, connectable.
    #[display("up")]
    Up,

    /// Dead for good: its handshakes are refused and sends towards it fail, which is what makes
    /// the synthesized terminated signals true.
    #[display("down")]
    Down,
}

/// A member of the cluster as this node sees it.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
#[display("{node} ({state})")]
pub struct Member {
    node: NodeId,
    state: MemberState,
}

impl Member {
    /// The address the member advertises.
    pub fn addr(&self) -> SocketAddr {
        self.node.addr()
    }

    /// The member's state.
    pub fn state(&self) -> MemberState {
        self.state
    }

    pub(crate) fn new(node: NodeId, state: MemberState) -> Self {
        Self { node, state }
    }

    pub(crate) fn node(&self) -> NodeId {
        self.node
    }
}

/// A cluster which cannot be joined.
#[derive(Debug, Error)]
pub enum JoinError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// No seed addresses other than this node's own were given, and joining never forms.
    #[error("no seed addresses")]
    NoSeeds,

    /// This node's incarnation has been downed by the cluster; only a restarted process, with a
    /// fresh incarnation, can join again.
    #[error("this node has been downed")]
    Downed,
}

/// The members cannot be listed.
#[derive(Debug, Error)]
pub enum MembersError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,
}

/// A member cannot be downed.
#[derive(Debug, Error)]
pub enum DownError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// No Up member advertises this address; during bootstrap gossip may not have carried that
    /// node's join yet, hence worth retrying.
    #[error("no Up member at {0}")]
    NotAMember(SocketAddr),

    /// The address is this node's own. A node ends its own membership with
    /// [leave](fn@crate::cluster::leave), which announces the departure instead of staging a death.
    #[error("cannot down this node, leave instead")]
    ThisNode,
}

/// Join the cluster one of the seed addresses is a member of, which is one of the two ways this
/// node becomes a member of anything: a started endpoint is no cluster, and only
/// [form](crate::cluster::form) or [bootstrap](fn@crate::cluster::bootstrap) turns it into one. A
/// cluster is hence the transitive closure of joins, and independent clusters must not share
/// seeds.
///
/// The seeds are tried in order, with the reconnect backoff between rounds, until one admits
/// this node: there is no internal timeout, so wrap the call in [timeout](tokio::time::timeout)
/// to bound it. A seed which is a member of no cluster refuses, so a seed list shared by nodes
/// which are all still starting forms nothing on its own. This node's own address is skipped in
/// every position, and a seed list naming nothing else fails as [NoSeeds](JoinError::NoSeeds).
/// Once a cluster has admitted this node, only that cluster is tried, even if the attempt failed
/// before its member snapshot arrived: no other seed can be proven to be that same cluster.
/// Joining again when already a member refreshes the member snapshot and is harmless.
pub async fn join(seeds: &[SocketAddr]) -> Result<(), JoinError> {
    let endpoint = endpoint::get().ok_or(JoinError::EndpointNotStarted)?;
    if seeds.iter().all(|seed| *seed == endpoint.node().addr()) {
        return Err(JoinError::NoSeeds);
    }

    let mut attempts = 0u32;
    loop {
        if let JoinRound::Joined = join_round(endpoint, seeds).await? {
            return Ok(());
        }

        attempts += 1;
        sleep(endpoint.reconnect_backoff(attempts)).await;
    }
}

/// What one pass over the seeds saw, which is what a formation decision is taken on.
pub(crate) enum JoinRound {
    Joined,

    /// The cluster at this address may count this node already, so only it may be tried and
    /// nothing may be formed until the attempt resolves.
    ClusterSeen(SocketAddr),

    NoAdmission(Vec<(SocketAddr, JoinOutcome)>),
}

/// One pass over the seeds, this node's own address skipped in every position: a formed node
/// dialing itself would admit itself and never reach the real seeds.
pub(crate) async fn join_round(
    endpoint: &'static EndpointInner,
    seeds: &[SocketAddr],
) -> Result<JoinRound, JoinError> {
    let seeds = match endpoint.pinned_cluster() {
        Some(pinned) => vec![pinned],

        None => seeds
            .iter()
            .copied()
            .filter(|seed| *seed != endpoint.node().addr())
            .collect(),
    };

    let mut outcomes = Vec::with_capacity(seeds.len());
    for seed in seeds {
        if endpoint.downed() {
            return Err(JoinError::Downed);
        }

        match join_via(endpoint, seed).await {
            Ok(()) => return Ok(JoinRound::Joined),

            Err(ConnectError::Refused(RefusalReason::Down) | ConnectError::SelfDowned) => {
                return Err(JoinError::Downed);
            }

            Err(ConnectError::Refused(RefusalReason::NoCluster)) => {
                outcomes.push((seed, JoinOutcome::NoCluster));
            }

            Err(error) => {
                debug!(seed_addr = %seed, %error, "cannot join via seed");
                if let Some(pinned) = endpoint.pinned_cluster() {
                    return Ok(JoinRound::ClusterSeen(pinned));
                }
                outcomes.push((seed, JoinOutcome::Unavailable));
            }
        }
    }

    Ok(JoinRound::NoAdmission(outcomes))
}

/// The members of the cluster as this node sees them, this node included, ordered by address and
/// incarnation. Down members are listed until their retention expires.
pub fn members() -> Result<Vec<Member>, MembersError> {
    endpoint::get()
        .map(|endpoint| endpoint.membership().members())
        .ok_or(MembersError::EndpointNotStarted)
}

/// Down the Up member at the given address, running the node death sequence: pending asks towards
/// it fail, its watchers receive synthesized terminated signals, and its incarnation is refused
/// from then on.
///
/// # Errors
/// Fails if the endpoint is not started, if no Up member advertises the address, or if the
/// address is this node's own, which [leave](fn@crate::cluster::leave) is for.
pub fn down(addr: SocketAddr) -> Result<(), DownError> {
    let endpoint = endpoint::get().ok_or(DownError::EndpointNotStarted)?;

    let node = endpoint
        .membership()
        .up_member_at(addr)
        .ok_or(DownError::NotAMember(addr))?;
    if node == endpoint.node() {
        return Err(DownError::ThisNode);
    }

    warn!(peer = %node, "node death, downed manually");
    endpoint.node_death(node);

    Ok(())
}

/// Entry state is a monotone lattice per incarnation (`Up < Down`), so gossip merge converges.
pub(crate) struct Membership {
    node: NodeId,
    entries: RwLock<Entries>,
}

impl Membership {
    pub(crate) fn new(node: NodeId, down_watermarks: NonZeroUsize) -> Self {
        let mut members = HashMap::new();
        members.insert(
            node,
            MemberEntry {
                state: MemberState::Up,
                changed_at: Instant::now(),
            },
        );

        Self {
            node,
            entries: RwLock::new(Entries {
                members,
                down: HashMap::new(),
                down_watermarks,
            }),
        }
    }

    pub(crate) fn is_up(&self, node: NodeId) -> bool {
        read(&self.entries)
            .members
            .get(&node)
            .is_some_and(|entry| entry.state == MemberState::Up)
    }

    pub(crate) fn is_down(&self, node: NodeId) -> bool {
        read(&self.entries).is_down(node)
    }

    pub(crate) fn up_member_at(&self, addr: SocketAddr) -> Option<NodeId> {
        read(&self.entries)
            .members
            .iter()
            .find(|(node, entry)| node.addr() == addr && entry.state == MemberState::Up)
            .map(|(node, _)| *node)
    }

    pub(crate) fn has_up_member_at(&self, addr: SocketAddr) -> bool {
        self.up_member_at(addr).is_some()
    }

    pub(crate) fn up_peers(&self) -> Vec<NodeId> {
        read(&self.entries)
            .members
            .iter()
            .filter(|(node, entry)| entry.state == MemberState::Up && **node != self.node)
            .map(|(node, _)| *node)
            .collect()
    }

    pub(crate) fn up_nodes(&self) -> HashSet<NodeId> {
        read(&self.entries)
            .members
            .iter()
            .filter(|(_, entry)| entry.state == MemberState::Up)
            .map(|(node, _)| *node)
            .collect()
    }

    pub(crate) fn members(&self) -> Vec<Member> {
        let mut members = read(&self.entries)
            .members
            .iter()
            .map(|(node, entry)| Member::new(*node, entry.state))
            .collect::<Vec<_>>();
        members.sort_by_key(|member| (member.node.addr(), member.node.incarnation()));
        members
    }

    pub(crate) fn snapshot(&self) -> Vec<WireMember> {
        read(&self.entries)
            .members
            .iter()
            .map(|(node, entry)| WireMember {
                node: *node,
                state: entry.state,
            })
            .collect()
    }

    /// Only connection setup may send this: watermarks in the heartbeat would grow forever.
    pub(crate) fn handshake_snapshot(&self) -> Vec<WireMember> {
        let entries = read(&self.entries);
        entries
            .members
            .iter()
            .map(|(node, entry)| WireMember {
                node: *node,
                state: entry.state,
            })
            .chain(
                entries
                    .down
                    .values()
                    .filter(|node| !entries.members.contains_key(node))
                    .map(|node| WireMember {
                        node: *node,
                        state: MemberState::Down,
                    }),
            )
            .collect()
    }

    /// `true` if the entry is new; a Down entry stays Down, the lattice never moves backwards.
    pub(crate) fn add_up(&self, node: NodeId) -> bool {
        let mut entries = write(&self.entries);
        if entries.is_down(node) {
            return false;
        }

        match entries.members.entry(node) {
            Entry::Occupied(_) => false,

            Entry::Vacant(entry) => {
                entry.insert(MemberEntry {
                    state: MemberState::Up,
                    changed_at: Instant::now(),
                });
                true
            }
        }
    }

    pub(crate) fn down(&self, node: NodeId) -> DownOutcome {
        {
            let mut entries = write(&self.entries);
            let watermark_changed = entries.merge_down_watermark(node);
            let member_changed = entries
                .members
                .get_mut(&node)
                .filter(|entry| entry.state == MemberState::Up)
                .is_some_and(|entry| {
                    entry.state = MemberState::Down;
                    entry.changed_at = Instant::now();
                    true
                });
            DownOutcome {
                member_changed,
                watermark_changed,
            }
        }
    }

    pub(crate) fn merge(&self, members: &[WireMember]) -> Merge {
        let mut merge = Merge::default();
        let mut entries = write(&self.entries);

        for member in members {
            match member.state {
                MemberState::Down => {
                    if entries.merge_down_watermark(member.node) {
                        merge.to_flush.push(member.node);
                    }
                    if member.node.addr() == self.node.addr()
                        && self.node.incarnation() <= member.node.incarnation()
                    {
                        merge.self_down = true;
                    } else {
                        merge.to_down.extend(
                            entries
                                .members
                                .iter()
                                .filter(|(node, entry)| {
                                    member.node.covers(**node) && entry.state == MemberState::Up
                                })
                                .map(|(node, _)| *node),
                        );
                    }
                }

                MemberState::Up => {
                    if member.node == self.node
                        || entries.is_down(member.node)
                        || entries.members.contains_key(&member.node)
                    {
                        continue;
                    }

                    let superseded = entries
                        .members
                        .iter()
                        .find(|(node, entry)| {
                            node.addr() == member.node.addr() && entry.state == MemberState::Up
                        })
                        .map(|(node, _)| *node);
                    if let Some(superseded) = superseded {
                        if superseded.incarnation() < member.node.incarnation() {
                            if superseded == self.node {
                                merge.self_down = true;
                                return merge;
                            }

                            merge.to_down.push(superseded);
                        } else {
                            merge.to_down.push(member.node);
                            continue;
                        }
                    }

                    entries.members.insert(
                        member.node,
                        MemberEntry {
                            state: MemberState::Up,
                            changed_at: Instant::now(),
                        },
                    );
                    merge.new_up.push(member.node);
                }
            }
        }

        merge
    }

    /// Drops detailed Down entries older than the retention; their address watermarks remain.
    pub(crate) fn sweep(&self, retention: Duration) {
        let now = Instant::now();
        let mut entries = write(&self.entries);
        entries.members.retain(|_, entry| {
            let expired = entry.state == MemberState::Down
                && now.duration_since(entry.changed_at) >= retention;
            !expired
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WireMember {
    pub(crate) node: NodeId,
    pub(crate) state: MemberState,
}

impl WireMember {
    /// The largest encoding of one member, pinned by a test.
    pub(crate) const MAX_ENCODED_LEN: usize = 40;

    /// Headroom for the `Gossip` envelope: its variant tag, length varint and `more` flag.
    const ENVELOPE_LEN: usize = 16;
}

/// The smallest `max_frame_size` [snapshot_frames] can respect, one member plus its envelope.
pub(crate) const MIN_FRAME_SIZE: usize = WireMember::MAX_ENCODED_LEN + WireMember::ENVELOPE_LEN;

/// Chunks a snapshot into `Gossip` frames, all but the last marked `more`; arrival order is free.
pub(crate) fn snapshot_frames(
    members: Vec<WireMember>,
    max_frame_size: usize,
) -> Vec<Frame<'static>> {
    let per_chunk = max_frame_size
        .saturating_sub(WireMember::ENVELOPE_LEN)
        .div_euclid(WireMember::MAX_ENCODED_LEN)
        .max(1);
    if members.len() <= per_chunk {
        return vec![Frame::Gossip {
            members,
            more: false,
        }];
    }

    let chunks = members.len().div_ceil(per_chunk);
    members
        .chunks(per_chunk)
        .enumerate()
        .map(|(index, chunk)| Frame::Gossip {
            members: chunk.to_vec(),
            more: index + 1 < chunks,
        })
        .collect()
}

/// What [Membership::merge] asks its caller to do outside the membership lock.
#[derive(Default)]
pub(crate) struct Merge {
    pub(crate) new_up: Vec<NodeId>,
    pub(crate) to_down: Vec<NodeId>,
    pub(crate) to_flush: Vec<NodeId>,
    pub(crate) self_down: bool,
}

pub(crate) struct DownOutcome {
    pub(crate) member_changed: bool,
    pub(crate) watermark_changed: bool,
}

/// The merge's consequences run out here, where no membership lock is held.
pub(crate) fn on_gossip(endpoint: &'static EndpointInner, members: &[WireMember]) {
    let merge = endpoint.membership().merge(members);

    if merge.self_down {
        for peer in merge.to_flush {
            endpoint.flush_fenced(peer);
        }
        endpoint.self_down();
        return;
    }

    let changed = !merge.new_up.is_empty() || !merge.to_down.is_empty();
    for node in merge.new_up {
        // Called for the arming side effect; no inbound delivery to gate here.
        let _ = endpoint.track_liveness(node);
    }
    for node in &merge.to_down {
        warn!(peer = %node, "node death, downed via gossip");
        endpoint.node_death(*node);
    }
    for peer in merge.to_flush {
        if !merge.to_down.contains(&peer) {
            endpoint.flush_fenced(peer);
        }
    }

    if changed {
        endpoint.push_gossip();
    }
}

struct Entries {
    members: HashMap<NodeId, MemberEntry>,
    /// The greatest Down incarnation per address.
    down: HashMap<SocketAddr, NodeId>,
    down_watermarks: NonZeroUsize,
}

impl Entries {
    fn is_down(&self, node: NodeId) -> bool {
        self.down
            .get(&node.addr())
            .is_some_and(|down| down.covers(node))
    }

    /// Reports a newly learned death even if the watermark loses the cap: its state needs flushing.
    fn merge_down_watermark(&mut self, node: NodeId) -> bool {
        let changed = match self.down.entry(node.addr()) {
            Entry::Occupied(mut entry) if entry.get().incarnation() < node.incarnation() => {
                entry.insert(node);
                true
            }

            Entry::Occupied(_) => false,

            Entry::Vacant(entry) => {
                entry.insert(node);
                true
            }
        };

        while self.down.len() > self.down_watermarks.get() {
            let oldest = self
                .down
                .iter()
                .min_by_key(|(_, node)| node.incarnation())
                .map(|(addr, _)| *addr)
                .expect("a map above its cap is not empty");
            self.down.remove(&oldest);
        }
        changed
    }
}

struct MemberEntry {
    state: MemberState,
    changed_at: Instant,
}

async fn join_via(endpoint: &'static EndpointInner, addr: SocketAddr) -> Result<(), ConnectError> {
    let (result_tx, result_rx) = oneshot::channel();
    endpoint.request_join(JoinRequest { addr, result_tx });

    match result_rx.await {
        Ok(result) => result,
        Err(_) => Err(ConnectError::Closed),
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::{
        frame::Frame,
        membership::{MIN_FRAME_SIZE, MemberState, Membership, WireMember, snapshot_frames},
        node::NodeId,
    };
    use std::{net::SocketAddr, num::NonZeroUsize, time::Duration};

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn wire(node: NodeId, state: MemberState) -> WireMember {
        WireMember { node, state }
    }

    /// The member list starts with this node Up, which is what the endpoint gossips and counts
    /// once it is a member; being a member at all is the endpoint's lifecycle, not this.
    #[test]
    fn a_new_membership_lists_this_node_up() {
        let node = NodeId::new(addr(1));
        let membership = Membership::new(node, NonZeroUsize::MAX);

        assert!(membership.is_up(node));
        assert_eq!(membership.members().len(), 1);
        assert!(membership.up_peers().is_empty());
    }

    /// The lattice never moves backwards: Down wins over Up, once Down always Down, and merging
    /// is idempotent, so duplicated or reordered gossip cannot flip a state.
    #[test]
    fn down_is_terminal_and_merge_is_idempotent() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        let peer = NodeId::new(addr(2));

        assert!(membership.add_up(peer));
        assert!(!membership.add_up(peer));
        assert!(membership.down(peer).member_changed);
        assert!(!membership.down(peer).member_changed);
        assert!(!membership.add_up(peer));
        assert!(membership.is_down(peer));

        let merge = membership.merge(&[wire(peer, MemberState::Up)]);
        assert!(merge.new_up.is_empty());
        assert!(merge.to_down.is_empty());

        let merge = membership.merge(&[wire(peer, MemberState::Down)]);
        assert!(merge.to_down.is_empty());
    }

    /// Merge folds unknown Down entries into the watermark map without detailed membership.
    #[test]
    fn merge_applies_up_and_folds_downs_into_the_watermark() {
        let node = NodeId::new(addr(1));
        let membership = Membership::new(node, NonZeroUsize::MAX);
        let (up, dead) = (NodeId::new(addr(2)), NodeId::new(addr(3)));

        let merge = membership.merge(&[
            wire(up, MemberState::Up),
            wire(dead, MemberState::Down),
            wire(node, MemberState::Up),
        ]);
        assert_eq!(merge.new_up, vec![up]);
        assert!(merge.to_down.is_empty());
        assert_eq!(merge.to_flush, vec![dead]);
        assert!(!merge.self_down);
        assert!(membership.is_up(up));
        assert!(membership.is_down(dead));
        assert!(
            !membership
                .members()
                .iter()
                .any(|member| member.node == dead)
        );

        let merge = membership.merge(&[wire(node, MemberState::Down)]);
        assert!(merge.self_down);
    }

    /// A departure needs no wire change: the leaving node moves its own entry to Down, the
    /// ordinary snapshot carries it, and the receiver hands it back as an ordinary node death.
    #[test]
    fn a_departure_is_gossiped_down_and_merged_as_a_node_death() {
        let leaver = NodeId::new(addr(1));
        let membership = Membership::new(leaver, NonZeroUsize::MAX);
        assert!(membership.down(leaver).member_changed);

        let snapshot = membership.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].state, MemberState::Down);

        let peer = Membership::new(NodeId::new(addr(2)), NonZeroUsize::MAX);
        peer.add_up(leaver);

        let merge = peer.merge(&snapshot);
        assert_eq!(merge.to_down, vec![leaver]);
        assert!(merge.new_up.is_empty());
        assert!(!merge.self_down);
    }

    /// Two Up incarnations at one address resolve towards the younger one, whichever order they
    /// are learned in: both sides of a gossip exchange converge on the same member.
    #[test]
    fn merge_supersedes_towards_the_younger_incarnation() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);
        assert!(older.incarnation() < younger.incarnation());

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        assert!(membership.add_up(older));
        let merge = membership.merge(&[wire(younger, MemberState::Up)]);
        assert_eq!(merge.to_down, vec![older]);
        assert!(membership.is_up(younger));

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        assert!(membership.add_up(younger));
        let merge = membership.merge(&[wire(older, MemberState::Up)]);
        assert_eq!(merge.to_down, vec![older]);
        assert!(membership.is_up(younger));
        assert!(!membership.is_up(older));
    }

    /// This node's own address takes part in supersession: a predecessor gossiped Up without its
    /// tombstone, e.g. by a seed which never learned of it, is downed. Adopted instead, it would
    /// be a phantom member this node would dial and resolve lookups to.
    #[test]
    fn merge_downs_a_predecessor_at_this_nodes_address() {
        let shared = addr(1);
        let predecessor = NodeId::new(shared);
        let node = NodeId::new(shared);
        assert!(predecessor.incarnation() < node.incarnation());

        let membership = Membership::new(node, NonZeroUsize::MAX);

        let merge = membership.merge(&[wire(predecessor, MemberState::Up)]);

        assert_eq!(merge.to_down, vec![predecessor]);
        assert!(merge.new_up.is_empty());
        assert!(!merge.self_down);
        assert!(!membership.is_up(predecessor));
        assert!(membership.up_peers().is_empty());
    }

    /// The same rule the other way round: a younger incarnation at this node's address makes this
    /// node the stale one, which is what every other node concludes about it, so it downs itself
    /// rather than staying Up against a cluster which has moved on.
    #[test]
    fn merge_self_downs_against_a_successor_at_this_nodes_address() {
        let shared = addr(1);
        let node = NodeId::new(shared);
        let successor = NodeId::new(shared);
        assert!(node.incarnation() < successor.incarnation());

        let membership = Membership::new(node, NonZeroUsize::MAX);

        let merge = membership.merge(&[wire(successor, MemberState::Up)]);

        assert!(merge.self_down);
    }

    /// Retention forgets the entry but keeps its watermark, which leaves the periodic snapshot.
    #[test]
    fn retention_sweeps_down_entries_but_keeps_the_watermark() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        let peer = NodeId::new(addr(2));
        membership.add_up(peer);
        membership.down(peer);
        assert!(membership.is_down(peer));

        membership.sweep(Duration::from_secs(3600));
        assert!(membership.is_down(peer));
        assert!(
            membership
                .snapshot()
                .contains(&wire(peer, MemberState::Down))
        );

        membership.sweep(Duration::ZERO);
        assert!(membership.is_down(peer));
        assert_eq!(membership.members().len(), 1);
        assert_eq!(
            membership.snapshot(),
            vec![wire(membership.node, MemberState::Up)]
        );

        let handshake = membership.handshake_snapshot();
        assert_eq!(handshake.len(), 2);
        assert!(handshake.contains(&wire(peer, MemberState::Down)));
        assert!(handshake.contains(&wire(membership.node, MemberState::Up)));
    }

    /// A watermark fences its incarnation and every predecessor, but admits a restart.
    #[test]
    fn a_down_watermark_fences_older_incarnations_and_admits_a_restart() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        let stale = NodeId::new(addr(2));
        let downed = NodeId::new(addr(2));
        let restarted = NodeId::new(addr(2));

        assert!(stale.incarnation() < downed.incarnation());
        assert!(downed.incarnation() < restarted.incarnation());
        assert!(membership.add_up(downed));
        assert!(membership.down(downed).member_changed);
        membership.sweep(Duration::ZERO);

        assert!(membership.is_down(stale));
        assert!(membership.is_down(downed));
        assert!(!membership.add_up(stale));
        assert!(!membership.add_up(downed));
        assert!(membership.add_up(restarted));
        assert!(membership.is_up(restarted));
    }

    /// A newer Down watermark fences and terminates an older local Up incarnation.
    #[test]
    fn a_newer_down_watermark_terminates_an_older_up_member() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        let older = NodeId::new(addr(2));
        let newer = NodeId::new(addr(2));
        assert!(older.incarnation() < newer.incarnation());
        assert!(membership.add_up(older));

        let merge = membership.merge(&[wire(newer, MemberState::Down)]);

        assert_eq!(merge.to_down, vec![older]);
        assert_eq!(merge.to_flush, vec![newer]);
        assert!(membership.is_down(older));
        assert!(membership.is_down(newer));
        assert!(membership.down(older).member_changed);
        assert!(!membership.is_up(older));
        assert!(
            membership
                .handshake_snapshot()
                .contains(&wire(newer, MemberState::Down))
        );
    }

    /// The widest member must fit the bound the chunking divides by, else a chunk oversizes.
    #[test]
    fn a_wire_member_never_encodes_beyond_the_chunking_bound() {
        let widest = WireMember {
            node: NodeId::new(
                "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                    .parse()
                    .expect("valid address"),
            ),
            state: MemberState::Down,
        };

        let len = postcard::experimental::serialized_size(&widest).expect("member size");

        assert!(
            len <= WireMember::MAX_ENCODED_LEN,
            "{len} exceeds the bound"
        );
    }

    /// Chunking bounds every frame, loses no member, keeps the order, and marks every chunk but
    /// the last `more`, which is what stops a joiner reading a truncated snapshot.
    #[test]
    fn chunking_bounds_each_gossip_frame_and_marks_all_but_the_last() {
        let members = (0..100)
            .map(|port| wire(NodeId::new(addr(port + 1)), MemberState::Down))
            .collect::<Vec<_>>();
        let max_frame_size = 1_000;

        let frames = snapshot_frames(members.clone(), max_frame_size);

        assert!(frames.len() > 1);
        let mut rejoined = Vec::<WireMember>::new();
        for (index, frame) in frames.iter().enumerate() {
            let len = frame.encoded_len().expect("frame size");
            assert!(len <= max_frame_size, "{len} exceeds {max_frame_size}");
            let Frame::Gossip { members, more } = frame else {
                panic!("not a gossip frame");
            };
            assert_eq!(*more, index + 1 < frames.len());
            rejoined.extend(members.iter().copied());
        }
        assert_eq!(rejoined, members);

        let single = snapshot_frames(members, 1_000_000);
        assert_eq!(single.len(), 1);
        assert!(matches!(single[0], Frame::Gossip { more: false, .. }));
        assert!(matches!(
            snapshot_frames(Vec::new(), 1).as_slice(),
            [Frame::Gossip { members, more: false }] if members.is_empty()
        ));
    }

    /// At the smallest size [start_endpoint](crate::cluster::start_endpoint) admits, a chunk of the
    /// widest possible members still encodes within it, so the floor of one member per chunk
    /// cannot oversize.
    #[test]
    fn the_minimum_frame_size_holds_a_chunk_of_the_widest_members() {
        let widest = NodeId::new(
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                .parse()
                .expect("valid address"),
        );
        let members = vec![wire(widest, MemberState::Down); 10];

        let frames = snapshot_frames(members, MIN_FRAME_SIZE);

        assert!(frames.len() > 1);
        for frame in &frames {
            let len = frame.encoded_len().expect("frame size");
            assert!(len <= MIN_FRAME_SIZE, "{len} exceeds {MIN_FRAME_SIZE}");
        }
    }

    /// A node absent past the retention learns the death from the connection snapshot alone.
    #[test]
    fn a_swept_fence_reaches_an_absent_node_through_the_handshake_snapshot() {
        let downed = NodeId::new(addr(2));
        let cluster = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        cluster.add_up(downed);
        cluster.down(downed);
        cluster.sweep(Duration::ZERO);

        let absent = Membership::new(NodeId::new(addr(3)), NonZeroUsize::MAX);
        absent.add_up(downed);

        let merge = absent.merge(&cluster.snapshot());
        assert!(merge.to_down.is_empty());
        assert!(merge.to_flush.is_empty());
        assert!(absent.is_up(downed));

        let merge = absent.merge(&cluster.handshake_snapshot());

        assert_eq!(merge.to_down, vec![downed]);
        assert_eq!(merge.to_flush, vec![downed]);
        assert!(absent.is_down(downed));
        assert!(absent.down(downed).member_changed);
        assert!(!absent.is_up(downed));
    }

    /// A covering watermark at this node's address self-downs the endpoint.
    #[test]
    fn merge_self_downs_against_a_down_watermark_at_its_address() {
        let shared = addr(1);
        let node = NodeId::new(shared);
        let successor = NodeId::new(shared);
        assert!(node.incarnation() < successor.incarnation());
        let membership = Membership::new(node, NonZeroUsize::MAX);

        let merge = membership.merge(&[wire(successor, MemberState::Down)]);

        assert!(merge.self_down);
        assert_eq!(merge.to_flush, vec![successor]);
        assert!(membership.is_down(node));
        assert!(
            membership
                .handshake_snapshot()
                .contains(&wire(successor, MemberState::Down))
        );
    }
}
