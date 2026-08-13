use crate::{
    cluster::{
        endpoint::{self, EndpointInner},
        formation::JoinOutcome,
        frame::{Frame, RefusalReason},
        node::{Incarnation, NodeId},
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

/// A member of the cluster as this node sees it. It serializes flat, `{addr, incarnation, state}`,
/// which is also how gossip carries it.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "MemberRepr", from = "MemberRepr")]
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

    /// The incarnation of the process at that address, which tells a restarted member apart from
    /// its predecessor.
    pub fn incarnation(&self) -> Incarnation {
        self.node.incarnation()
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

impl Member {
    /// The largest encoding of one member, pinned by a test.
    pub(crate) const MAX_ENCODED_LEN: usize = 40;

    /// Headroom for the `Gossip` envelope: its variant tag, length varint and `more` flag.
    const ENVELOPE_LEN: usize = 16;
}

#[derive(Serialize, Deserialize)]
struct MemberRepr {
    addr: SocketAddr,
    incarnation: Incarnation,
    state: MemberState,
}

impl From<Member> for MemberRepr {
    fn from(member: Member) -> Self {
        Self {
            addr: member.addr(),
            incarnation: member.incarnation(),
            state: member.state(),
        }
    }
}

impl From<MemberRepr> for Member {
    fn from(repr: MemberRepr) -> Self {
        Self::new(NodeId::from_parts(repr.addr, repr.incarnation), repr.state)
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
/// every position. Once a cluster has admitted this node, only that cluster is tried, even if the
/// attempt failed before its member snapshot arrived: no other seed can be proven to be that same
/// cluster. Joining again when already a member refreshes the member snapshot and is harmless.
///
/// # Errors
/// Fails if the endpoint is not started, if the seeds name nothing but this node's own address,
/// or if this node has been downed, which only a restarted process recovers from.
pub async fn join(seeds: &[SocketAddr]) -> Result<(), JoinError> {
    let endpoint = endpoint::get().ok_or(JoinError::EndpointNotStarted)?;
    if seeds.iter().all(|seed| *seed == endpoint.node().addr()) {
        return Err(JoinError::NoSeeds);
    }

    let mut attempts = 0u32;
    loop {
        match join_round(endpoint, seeds).await {
            JoinRound::Joined => return Ok(()),
            JoinRound::Downed => return Err(JoinError::Downed),
            JoinRound::ClusterSeen(_) | JoinRound::NoAdmission(_) => {}
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

    Downed,
}

/// One pass over the seeds, this node's own address skipped in every position: a formed node
/// dialing itself would admit itself and never reach the real seeds.
pub(crate) async fn join_round(
    endpoint: &'static EndpointInner,
    seeds: &[SocketAddr],
) -> JoinRound {
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
            return JoinRound::Downed;
        }

        match join_via(endpoint, seed).await {
            Ok(()) => return JoinRound::Joined,

            Err(ConnectError::Refused(RefusalReason::Down) | ConnectError::SelfDowned) => {
                return JoinRound::Downed;
            }

            Err(ConnectError::Refused(RefusalReason::NoCluster)) => {
                outcomes.push((seed, JoinOutcome::NoCluster));
            }

            Err(error) => {
                debug!(seed_addr = %seed, %error, "cannot join via seed");
                if let Some(pinned) = endpoint.pinned_cluster() {
                    return JoinRound::ClusterSeen(pinned);
                }
                outcomes.push((seed, JoinOutcome::Unavailable));
            }
        }
    }

    JoinRound::NoAdmission(outcomes)
}

/// The members of the cluster as this node sees them, this node included, ordered by address and
/// incarnation. A member this node saw Up is listed Down until its retention expires; one which
/// was Down before this node learned of it is kept only as its address's fence, hence not listed.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn members() -> Result<Vec<Member>, MembersError> {
    endpoint::get()
        .map(|endpoint| endpoint.state_rx().borrow().members().to_vec())
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
        members.insert(node, MemberEntry::up());

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
        self.while_up(node, |up| up)
    }

    pub(crate) fn is_down(&self, node: NodeId) -> bool {
        read(&self.entries).is_down(node)
    }

    /// Runs `f` with the node's Up state while no membership change can land, so what `f`
    /// stores or publishes is consistent with the state it was given.
    pub(crate) fn while_up<R>(&self, node: NodeId, f: impl FnOnce(bool) -> R) -> R {
        let entries = read(&self.entries);
        let up = entries
            .members
            .get(&node)
            .is_some_and(|entry| entry.state == MemberState::Up);
        f(up)
    }

    pub(crate) fn up_member_at(&self, addr: SocketAddr) -> Option<NodeId> {
        read(&self.entries).up_at(addr)
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

    /// Only connection setup may send this: watermarks in the heartbeat would grow forever.
    pub(crate) fn handshake_snapshot(&self) -> Vec<Member> {
        let entries = read(&self.entries);
        entries
            .members
            .iter()
            .map(|(node, entry)| Member::new(*node, entry.state))
            .chain(
                entries
                    .down
                    .values()
                    .filter(|node| !entries.members.contains_key(node))
                    .map(|node| Member::new(*node, MemberState::Down)),
            )
            .collect()
    }

    pub(crate) fn admit_join(&self, node: NodeId) -> JoinAdmission {
        let mut entries = write(&self.entries);
        if entries.is_down(node) {
            return JoinAdmission::Dead;
        }
        if entries.members.contains_key(&node) {
            return JoinAdmission::AlreadyUp;
        }

        let superseded = match entries.up_at(node.addr()) {
            Some(previous) if previous == self.node => {
                return if previous.incarnation() < node.incarnation() {
                    JoinAdmission::SupersedesThisNode
                } else {
                    JoinAdmission::Stale {
                        outcome: entries.down(node),
                    }
                };
            }

            Some(previous) if node.incarnation() < previous.incarnation() => {
                return JoinAdmission::Stale {
                    outcome: entries.down(node),
                };
            }

            Some(previous) => Some((previous, entries.down(previous))),

            None => None,
        };

        entries.members.insert(node, MemberEntry::up());
        JoinAdmission::Admitted { superseded }
    }

    #[cfg(test)]
    pub(crate) fn add_up(&self, node: NodeId) -> bool {
        let mut entries = write(&self.entries);
        if entries.is_down(node) {
            return false;
        }

        match entries.members.entry(node) {
            Entry::Occupied(_) => false,

            Entry::Vacant(entry) => {
                entry.insert(MemberEntry::up());
                true
            }
        }
    }

    pub(crate) fn down(&self, node: NodeId) -> DownOutcome {
        write(&self.entries).down(node)
    }

    pub(crate) fn merge(&self, members: &[Member]) -> Merge {
        let mut entries = write(&self.entries);
        // No Down before the scan ends, else a later explicit Down of the same node is not flushed.
        let scan = entries.scan(self.node, members);
        let downed = scan
            .to_down
            .iter()
            .map(|node| (*node, entries.down(*node)))
            .collect();
        if scan.self_down {
            entries.down(self.node);
        }

        Merge {
            new_up: scan.new_up,
            downed,
            to_flush: scan.to_flush,
            self_down: scan.self_down,
        }
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

/// The smallest `max_frame_size` [snapshot_frames] can respect, one member plus its envelope.
pub(crate) const MIN_FRAME_SIZE: usize = Member::MAX_ENCODED_LEN + Member::ENVELOPE_LEN;

/// Chunks a snapshot into `Gossip` frames, all but the last marked `more`; arrival order is free.
pub(crate) fn snapshot_frames(members: Vec<Member>, max_frame_size: usize) -> Vec<Frame<'static>> {
    let per_chunk = max_frame_size
        .saturating_sub(Member::ENVELOPE_LEN)
        .div_euclid(Member::MAX_ENCODED_LEN)
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

/// What [Membership::merge] changed, for its caller to act on outside the membership lock.
pub(crate) struct Merge {
    pub(crate) new_up: Vec<NodeId>,
    pub(crate) downed: Vec<(NodeId, DownOutcome)>,
    pub(crate) to_flush: Vec<NodeId>,
    pub(crate) self_down: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct DownOutcome {
    pub(crate) member_changed: bool,
    pub(crate) watermark_changed: bool,
}

pub(crate) enum JoinAdmission {
    Admitted {
        superseded: Option<(NodeId, DownOutcome)>,
    },
    AlreadyUp,
    Dead,
    Stale {
        outcome: DownOutcome,
    },
    SupersedesThisNode,
}

/// The merge's consequences run out here, where no membership lock is held.
pub(crate) fn on_gossip(endpoint: &'static EndpointInner, members: &[Member]) {
    let merge = endpoint
        .update(|membership, reachability| {
            let merge = membership.merge(members);
            for fence in merge
                .downed
                .iter()
                .map(|(node, _)| node)
                .chain(&merge.to_flush)
            {
                reachability.prune_fenced(*fence);
            }
            merge
        })
        .result;

    if merge.self_down {
        for peer in merge.to_flush {
            endpoint.flush_fenced(peer);
        }
        endpoint.self_down();
        return;
    }

    let changed = !merge.new_up.is_empty() || !merge.downed.is_empty();
    for node in merge.new_up {
        // Called for the arming side effect; no inbound delivery to gate here.
        let _ = endpoint.track_liveness(node);
        endpoint.receptionist().origin_up(node);
    }
    for &(node, outcome) in &merge.downed {
        warn!(peer = %node, "node death, downed via gossip");
        endpoint.death_consequences(node, outcome);
    }
    for peer in merge.to_flush {
        if !merge.downed.iter().any(|(node, _)| *node == peer) {
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
    fn scan(&mut self, this_node: NodeId, members: &[Member]) -> Scan {
        let mut scan = Scan::default();

        for member in members {
            match member.state {
                MemberState::Down => {
                    if self.merge_down_watermark(member.node) {
                        scan.to_flush.push(member.node);
                    }
                    if member.node.addr() == this_node.addr()
                        && this_node.incarnation() <= member.node.incarnation()
                    {
                        scan.self_down = true;
                    } else {
                        scan.to_down.extend(
                            self.members
                                .iter()
                                .filter(|(node, entry)| {
                                    member.node.covers(**node) && entry.state == MemberState::Up
                                })
                                .map(|(node, _)| *node),
                        );
                    }
                }

                MemberState::Up => {
                    if member.node == this_node
                        || self.is_down(member.node)
                        || self.members.contains_key(&member.node)
                    {
                        continue;
                    }

                    if let Some(superseded) = self.up_at(member.node.addr()) {
                        if superseded.incarnation() < member.node.incarnation() {
                            if superseded == this_node {
                                scan.self_down = true;
                                return scan;
                            }

                            scan.to_down.push(superseded);
                        } else {
                            scan.to_down.push(member.node);
                            continue;
                        }
                    }

                    self.members.insert(member.node, MemberEntry::up());
                    scan.new_up.push(member.node);
                }
            }
        }

        scan
    }

    fn down(&mut self, node: NodeId) -> DownOutcome {
        let watermark_changed = self.merge_down_watermark(node);
        let member_changed = self
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

    fn is_down(&self, node: NodeId) -> bool {
        self.down
            .get(&node.addr())
            .is_some_and(|down| down.covers(node))
    }

    fn up_at(&self, addr: SocketAddr) -> Option<NodeId> {
        self.members
            .iter()
            .find(|(node, entry)| node.addr() == addr && entry.state == MemberState::Up)
            .map(|(node, _)| *node)
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

#[derive(Default)]
struct Scan {
    new_up: Vec<NodeId>,
    to_down: Vec<NodeId>,
    to_flush: Vec<NodeId>,
    self_down: bool,
}

impl MemberEntry {
    fn up() -> Self {
        Self {
            state: MemberState::Up,
            changed_at: Instant::now(),
        }
    }
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
    use crate::{
        ActorId, MailboxCapacity,
        cluster::{
            endpoint::{EndpointConfig, EndpointInner},
            frame::Frame,
            membership::{
                JoinAdmission, MIN_FRAME_SIZE, Member, MemberState, Membership, Merge, on_gossip,
                snapshot_frames,
            },
            node::NodeId,
        },
        mailbox::make_mailbox,
        watch::Watcher,
    };
    use std::{net::SocketAddr, num::NonZeroUsize, time::Duration};

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
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

        let merge = membership.merge(&[Member::new(peer, MemberState::Up)]);
        assert!(merge.new_up.is_empty());
        assert!(merge.downed.is_empty());

        let merge = membership.merge(&[Member::new(peer, MemberState::Down)]);
        assert!(merge.downed.is_empty());
    }

    /// Merge folds unknown Down entries into the watermark map without detailed membership.
    #[test]
    fn merge_applies_up_and_folds_downs_into_the_watermark() {
        let node = NodeId::new(addr(1));
        let membership = Membership::new(node, NonZeroUsize::MAX);
        let (up, dead) = (NodeId::new(addr(2)), NodeId::new(addr(3)));

        let merge = membership.merge(&[
            Member::new(up, MemberState::Up),
            Member::new(dead, MemberState::Down),
            Member::new(node, MemberState::Up),
        ]);
        assert_eq!(merge.new_up, vec![up]);
        assert!(merge.downed.is_empty());
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

        let merge = membership.merge(&[Member::new(node, MemberState::Down)]);
        assert!(merge.self_down);
    }

    /// A departure needs no wire change: the leaving node moves its own entry to Down, the
    /// ordinary snapshot carries it, and the receiver hands it back as an ordinary node death.
    #[test]
    fn a_departure_is_gossiped_down_and_merged_as_a_node_death() {
        let leaver = NodeId::new(addr(1));
        let membership = Membership::new(leaver, NonZeroUsize::MAX);
        assert!(membership.down(leaver).member_changed);

        let snapshot = membership.members();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].state, MemberState::Down);

        let peer = Membership::new(NodeId::new(addr(2)), NonZeroUsize::MAX);
        peer.add_up(leaver);

        let merge = peer.merge(&snapshot);
        assert_eq!(downed_nodes(&merge), vec![leaver]);
        assert!(merge.new_up.is_empty());
        assert!(!merge.self_down);
    }

    /// An older incarnation learned after the younger one at its address is downed rather than
    /// adopted, so both sides of a gossip exchange converge on the same member.
    #[test]
    fn merge_supersedes_towards_the_younger_incarnation() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);
        assert!(older.incarnation() < younger.incarnation());

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        assert!(membership.add_up(younger));
        let merge = membership.merge(&[Member::new(older, MemberState::Up)]);
        assert_eq!(downed_nodes(&merge), vec![older]);
        assert!(membership.is_up(younger));
        assert!(!membership.is_up(older));
    }

    #[test]
    fn a_join_supersedes_towards_the_younger_incarnation() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        assert!(matches!(
            membership.admit_join(older),
            JoinAdmission::Admitted { superseded: None }
        ));
        assert!(matches!(
            membership.admit_join(younger),
            JoinAdmission::Admitted { superseded: Some((superseded, outcome)) }
                if superseded == older && outcome.member_changed
        ));
        assert!(membership.is_up(younger));
        assert!(membership.is_down(older));

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        assert!(matches!(
            membership.admit_join(younger),
            JoinAdmission::Admitted { superseded: None }
        ));
        assert!(matches!(
            membership.admit_join(older),
            JoinAdmission::Stale { outcome } if outcome.watermark_changed
        ));
        assert!(membership.is_up(younger));
        assert!(membership.is_down(older));
        assert!(matches!(membership.admit_join(older), JoinAdmission::Dead));
        assert!(matches!(
            membership.admit_join(younger),
            JoinAdmission::AlreadyUp
        ));
    }

    #[test]
    fn a_join_at_this_nodes_address_never_adds_a_second_incarnation() {
        let shared = addr(1);
        let older = NodeId::new(shared);
        let node = NodeId::new(shared);
        let younger = NodeId::new(shared);

        let membership = Membership::new(node, NonZeroUsize::MAX);
        assert!(matches!(
            membership.admit_join(younger),
            JoinAdmission::SupersedesThisNode
        ));
        assert!(matches!(
            membership.admit_join(older),
            JoinAdmission::Stale { .. }
        ));
        assert!(membership.is_up(node));
        assert!(!membership.is_up(younger));
        assert!(!membership.is_up(older));
    }

    /// This node's own address takes part in supersession: a predecessor gossiped Up without its
    /// tombstone, e.g. by a seed which never learned of it, is downed. Adopted instead, it would
    /// be a phantom member this node would dial and list as a registration origin.
    #[test]
    fn merge_downs_a_predecessor_at_this_nodes_address() {
        let shared = addr(1);
        let predecessor = NodeId::new(shared);
        let node = NodeId::new(shared);
        assert!(predecessor.incarnation() < node.incarnation());

        let membership = Membership::new(node, NonZeroUsize::MAX);

        let merge = membership.merge(&[Member::new(predecessor, MemberState::Up)]);

        assert_eq!(downed_nodes(&merge), vec![predecessor]);
        assert!(merge.new_up.is_empty());
        assert!(!merge.self_down);
        assert!(!membership.is_up(predecessor));
        assert!(membership.up_peers().is_empty());
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
                .members()
                .contains(&Member::new(peer, MemberState::Down))
        );

        membership.sweep(Duration::ZERO);
        assert!(membership.is_down(peer));
        assert_eq!(membership.members().len(), 1);
        assert_eq!(
            membership.members(),
            vec![Member::new(membership.node, MemberState::Up)]
        );

        let handshake = membership.handshake_snapshot();
        assert_eq!(handshake.len(), 2);
        assert!(handshake.contains(&Member::new(peer, MemberState::Down)));
        assert!(handshake.contains(&Member::new(membership.node, MemberState::Up)));
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

        let merge = membership.merge(&[Member::new(newer, MemberState::Down)]);

        assert_eq!(outcomes(&merge), vec![(older, (true, false))]);
        assert_eq!(merge.to_flush, vec![newer]);
        assert!(membership.is_down(older));
        assert!(membership.is_down(newer));
        assert!(!membership.down(older).member_changed);
        assert!(!membership.is_up(older));
        assert!(
            membership
                .handshake_snapshot()
                .contains(&Member::new(newer, MemberState::Down))
        );
    }

    /// The widest member must fit the bound the chunking divides by, else a chunk oversizes.
    #[test]
    fn a_member_never_encodes_beyond_the_chunking_bound() {
        let widest = Member {
            node: NodeId::new(
                "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                    .parse()
                    .expect("valid address"),
            ),
            state: MemberState::Down,
        };

        let len = postcard::experimental::serialized_size(&widest).expect("member size");

        assert!(len <= Member::MAX_ENCODED_LEN, "{len} exceeds the bound");
    }

    /// Chunking bounds every frame, loses no member, keeps the order, and marks every chunk but
    /// the last `more`, which is what stops a joiner reading a truncated snapshot.
    #[test]
    fn chunking_bounds_each_gossip_frame_and_marks_all_but_the_last() {
        let members = (0..100)
            .map(|port| Member::new(NodeId::new(addr(port + 1)), MemberState::Down))
            .collect::<Vec<_>>();
        let max_frame_size = 1_000;

        let frames = snapshot_frames(members.clone(), max_frame_size);

        assert!(frames.len() > 1);
        let mut rejoined = Vec::<Member>::new();
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
        let members = vec![Member::new(widest, MemberState::Down); 10];

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

        let merge = absent.merge(&cluster.members());
        assert!(merge.downed.is_empty());
        assert!(merge.to_flush.is_empty());
        assert!(absent.is_up(downed));

        let merge = absent.merge(&cluster.handshake_snapshot());

        assert_eq!(outcomes(&merge), vec![(downed, (true, false))]);
        assert_eq!(merge.to_flush, vec![downed]);
        assert!(absent.is_down(downed));
        assert!(!absent.down(downed).member_changed);
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

        let merge = membership.merge(&[Member::new(successor, MemberState::Down)]);

        assert!(merge.self_down);
        assert_eq!(merge.to_flush, vec![successor]);
        assert!(membership.is_down(node));
        assert!(
            membership
                .handshake_snapshot()
                .contains(&Member::new(successor, MemberState::Down))
        );
    }

    fn outcomes(merge: &Merge) -> Vec<(NodeId, (bool, bool))> {
        merge
            .downed
            .iter()
            .map(|(node, outcome)| (*node, (outcome.member_changed, outcome.watermark_changed)))
            .collect()
    }

    fn downed_nodes(merge: &Merge) -> Vec<NodeId> {
        merge.downed.iter().map(|(node, _)| *node).collect()
    }

    fn superseded_pair() -> (Membership, NodeId, NodeId) {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX);
        let predecessor = NodeId::new(addr(2));
        let successor = NodeId::new(addr(2));
        assert!(predecessor.incarnation() < successor.incarnation());
        assert!(membership.add_up(predecessor));
        (membership, predecessor, successor)
    }

    #[test]
    fn a_merged_supersession_downs_the_predecessor_after_the_scan() {
        let (membership, predecessor, successor) = superseded_pair();

        let merge = membership.merge(&[Member::new(successor, MemberState::Up)]);
        let downed = outcomes(&merge);

        assert_eq!(merge.new_up, vec![successor]);
        assert_eq!(downed, vec![(predecessor, (true, true))]);
        assert!(merge.to_flush.is_empty());
        assert!(!merge.self_down);
        assert!(membership.is_down(predecessor));
        assert!(membership.is_up(successor));
    }

    #[test]
    fn an_explicit_down_after_a_supersession_still_fences_the_predecessor() {
        let (membership, predecessor, successor) = superseded_pair();

        let merge = membership.merge(&[
            Member::new(successor, MemberState::Up),
            Member::new(predecessor, MemberState::Down),
        ]);
        let downed = outcomes(&merge);

        assert_eq!(merge.new_up, vec![successor]);
        assert_eq!(
            downed,
            vec![(predecessor, (true, false)), (predecessor, (false, false))]
        );
        assert_eq!(merge.to_flush, vec![predecessor]);
        assert!(!merge.self_down);
    }

    #[test]
    fn a_self_down_with_a_supersession_still_fences_the_predecessor() {
        let (membership, predecessor, successor) = superseded_pair();
        let node = membership.node;

        let merge = membership.merge(&[
            Member::new(successor, MemberState::Up),
            Member::new(predecessor, MemberState::Down),
            Member::new(node, MemberState::Down),
        ]);
        let downed = outcomes(&merge);

        assert_eq!(
            downed,
            vec![(predecessor, (true, false)), (predecessor, (false, false))]
        );
        assert_eq!(merge.to_flush, vec![predecessor, node]);
        assert!(merge.self_down);
        assert!(membership.is_down(node));
        assert!(membership.is_down(predecessor));
    }

    /// A younger incarnation at this node's address makes this node the stale one, which is what
    /// every other node concludes about it, so it downs itself rather than staying Up against a
    /// cluster which has moved on, and still downs what the snapshot superseded before.
    #[test]
    fn a_self_down_on_the_early_return_still_downs_the_earlier_supersession() {
        let (membership, predecessor, successor) = superseded_pair();
        let node = membership.node;
        let node_successor = NodeId::new(node.addr());

        let merge = membership.merge(&[
            Member::new(successor, MemberState::Up),
            Member::new(node_successor, MemberState::Up),
        ]);
        let downed = outcomes(&merge);

        assert_eq!(merge.new_up, vec![successor]);
        assert_eq!(downed, vec![(predecessor, (true, true))]);
        assert!(merge.to_flush.is_empty());
        assert!(merge.self_down);
        assert!(membership.is_down(node));
        assert!(!membership.is_up(node_successor));
    }

    #[test]
    fn a_self_down_snapshot_runs_no_effect_for_a_superseded_member() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let predecessor = NodeId::new(addr(2));
        let successor = NodeId::new(addr(2));
        let witness = NodeId::new(addr(3));
        endpoint.update(|membership, _| {
            membership.add_up(predecessor);
            membership.add_up(witness);
        });

        endpoint
            .send(
                witness,
                Frame::Gossip {
                    members: Vec::new(),
                    more: false,
                },
            )
            .expect("the lane is opened");
        let witness_lane = dial_requests.try_recv().expect("the witness is dialed");
        endpoint.bind_lane(witness.addr(), witness_lane.lane_id, witness);
        while witness_lane.control_rx.try_recv().is_ok() {}

        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let (incoming_rx, _watcher_registration) = mailbox.split();
        endpoint.watchers().add(
            predecessor,
            ActorId::new(),
            Watcher::new(ActorId::new(), mailbox_handle.terminated_handler()),
        );
        let version = endpoint.state_rx().borrow().version();

        on_gossip(
            endpoint,
            &[
                Member::new(successor, MemberState::Up),
                Member::new(endpoint.node(), MemberState::Down),
            ],
        );

        let state = endpoint.state_rx().borrow().clone();
        assert_eq!(state.version(), version + 1);
        let down = state
            .members()
            .iter()
            .filter(|member| member.state() == MemberState::Down)
            .map(|member| member.node)
            .collect::<Vec<_>>();
        assert!(down.contains(&predecessor));
        assert!(down.contains(&endpoint.node()));
        assert!(incoming_rx.is_empty());
        assert!(witness_lane.control_rx.try_recv().is_err());
    }
}
