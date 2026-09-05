use crate::{
    cluster::{
        frame::Frame,
        membership::{Member, MemberState},
        node::NodeId,
    },
    sync::lock,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

/// One observer's latest statement about one direct peer. Versions are owned by the observer;
/// relays only merge and forward them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WireReachability {
    pub(crate) observer: NodeId,
    pub(crate) subject: NodeId,
    pub(crate) version: u64,
    pub(crate) reachable: bool,
}

impl WireReachability {
    /// Two maximum-width IPv6 node identities plus the version and state, pinned by a test.
    const MAX_ENCODED_LEN: usize = 96;
    const ENVELOPE_LEN: usize = 16;
}

pub(crate) const MIN_FRAME_SIZE: usize =
    WireReachability::MAX_ENCODED_LEN + WireReachability::ENVELOPE_LEN;

/// Records parked for a membership which has not caught up yet, bounding what an ill-behaved peer
/// can make this node hold.
const MAX_PENDING: usize = 4_096;

pub(crate) struct Reachability {
    node: NodeId,
    next_version: AtomicU64,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    observations: HashMap<(NodeId, NodeId), WireReachability>,
    pending: HashMap<(NodeId, NodeId), WireReachability>,
    forward: HashMap<(NodeId, NodeId), WireReachability>,
    unreachable_since: HashMap<NodeId, Instant>,
}

impl State {
    /// Records the statement unless a newer one for the same edge is already known, queueing it
    /// for the next push; the version alone decides, so a relay never reorders an observer.
    fn admit(&mut self, record: WireReachability) -> bool {
        let key = (record.observer, record.subject);
        if self
            .observations
            .get(&key)
            .is_some_and(|current| current.version >= record.version)
        {
            return false;
        }

        self.observations.insert(key, record);
        self.forward.insert(key, record);
        true
    }

    /// Capped, since a peer can name any node: past the cap a new edge is dropped rather than
    /// evicting one which is only waiting for gossip.
    fn park(&mut self, record: WireReachability) {
        let key = (record.observer, record.subject);
        match self.pending.get(&key).map(|current| current.version) {
            Some(version) if version < record.version => {
                self.pending.insert(key, record);
            }

            Some(_) => {}

            None if self.pending.len() < MAX_PENDING => {
                self.pending.insert(key, record);
            }

            None => {}
        }
    }
}

impl Reachability {
    pub(crate) fn new(node: NodeId) -> Self {
        Self {
            node,
            next_version: AtomicU64::new(1),
            state: Mutex::new(State::default()),
        }
    }

    /// Returns whether this direct edge changed, queueing the new statement for the next push.
    pub(crate) fn observe(&self, subject: NodeId, reachable: bool) -> bool {
        let mut state = lock(&self.state);
        let previous = state
            .observations
            .get(&(self.node, subject))
            .map(|record| record.reachable);
        if previous.unwrap_or(true) == reachable {
            return false;
        }

        state.admit(WireReachability {
            observer: self.node,
            subject,
            version: self.next_version.fetch_add(1, Ordering::Relaxed),
            reachable,
        })
    }

    /// Merges peer statements, parking the ones naming a node which is not a member here yet:
    /// their observer resends only to its own peers, so a record which arrived through a relay has
    /// no other way back once the relay has deduplicated it.
    pub(crate) fn merge(&self, records: &[WireReachability], up: &HashSet<NodeId>) {
        let mut state = lock(&self.state);
        for record in records.iter().copied() {
            if record.observer == record.subject || record.observer == self.node {
                continue;
            }
            if up.contains(&record.observer) && up.contains(&record.subject) {
                state.admit(record);
            } else {
                state.park(record);
            }
        }
    }

    /// Admits the parked records whose endpoints have become members, which queues them for the
    /// next push and hence resumes the relay that dropped them.
    pub(crate) fn promote_pending(&self, up: &HashSet<NodeId>) {
        let mut state = lock(&self.state);
        let ready = state
            .pending
            .iter()
            .filter(|(_, record)| up.contains(&record.observer) && up.contains(&record.subject))
            .map(|(key, record)| (*key, *record))
            .collect::<Vec<_>>();

        for (key, record) in ready {
            state.pending.remove(&key);
            state.admit(record);
        }
    }

    /// Everything to push this tick, drained: the statements accepted since the last push plus
    /// this node's standing unreachable ones, which are re-asserted so a peer that never received
    /// one at all still converges.
    pub(crate) fn take_outbound(&self, up: &HashSet<NodeId>) -> Vec<WireReachability> {
        let mut state = lock(&self.state);
        let mut outbound = take(&mut state.forward);
        outbound.retain(|_, record| up.contains(&record.observer) && up.contains(&record.subject));
        for record in state.observations.values() {
            if record.observer == self.node && !record.reachable && up.contains(&record.subject) {
                outbound.insert((record.observer, record.subject), *record);
            }
        }

        sorted(outbound.into_values().collect())
    }

    pub(crate) fn snapshot(&self, up: &HashSet<NodeId>) -> Vec<WireReachability> {
        let records = lock(&self.state)
            .observations
            .values()
            .filter(|record| up.contains(&record.observer) && up.contains(&record.subject))
            .copied()
            .collect();

        sorted(records)
    }

    pub(crate) fn prune_fenced(&self, fence: NodeId) {
        let mut state = lock(&self.state);
        let covered = |(observer, subject): &(NodeId, NodeId)| {
            fence.covers(*observer) || fence.covers(*subject)
        };
        state.observations.retain(|edge, _| !covered(edge));
        state.pending.retain(|edge, _| !covered(edge));
        state.forward.retain(|edge, _| !covered(edge));
        state
            .unreachable_since
            .retain(|node, _| !fence.covers(*node));
    }

    /// Members outside this node's component are what a downing provider sees as unreachable.
    pub(crate) fn unreachable_members(&self, members: &[Member]) -> Vec<(Member, Instant)> {
        let up = members
            .iter()
            .filter(|member| member.state() == MemberState::Up)
            .map(Member::node)
            .collect::<HashSet<_>>();
        let mut state = lock(&self.state);
        let mut connected = HashSet::from([self.node]);
        let mut queue = VecDeque::from([self.node]);
        while let Some(from) = queue.pop_front() {
            for to in up.iter().copied() {
                if connected.contains(&to) || edge_is_down(&state.observations, from, to) {
                    continue;
                }
                connected.insert(to);
                queue.push_back(to);
            }
        }

        let now = Instant::now();
        state
            .unreachable_since
            .retain(|node, _| up.contains(node) && !connected.contains(node));
        for node in up.iter().filter(|node| !connected.contains(node)) {
            state.unreachable_since.entry(*node).or_insert(now);
        }

        members
            .iter()
            .filter_map(|member| {
                state
                    .unreachable_since
                    .get(&member.node())
                    .map(|since| (*member, *since))
            })
            .collect()
    }
}

fn sorted(mut records: Vec<WireReachability>) -> Vec<WireReachability> {
    records.sort_by_key(|record| {
        (
            record.observer.addr(),
            record.observer.incarnation(),
            record.subject.addr(),
            record.subject.incarnation(),
        )
    });
    records
}

fn edge_is_down(
    observations: &HashMap<(NodeId, NodeId), WireReachability>,
    left: NodeId,
    right: NodeId,
) -> bool {
    observations
        .get(&(left, right))
        .is_some_and(|record| !record.reachable)
        || observations
            .get(&(right, left))
            .is_some_and(|record| !record.reachable)
}

pub(crate) fn snapshot_frames(
    records: Vec<WireReachability>,
    max_frame_size: usize,
) -> Vec<Frame<'static>> {
    snapshot_chunks(records, max_frame_size)
        .into_iter()
        .map(|observations| Frame::Reachability { observations })
        .collect()
}

/// Chunked once, so a push fanning out to every peer splits the records only once; a [Frame]
/// owns its records, hence the clone per peer.
pub(crate) fn snapshot_chunks(
    records: Vec<WireReachability>,
    max_frame_size: usize,
) -> Vec<Vec<WireReachability>> {
    if records.is_empty() {
        return Vec::new();
    }
    let per_chunk = max_frame_size
        .saturating_sub(WireReachability::ENVELOPE_LEN)
        .div_euclid(WireReachability::MAX_ENCODED_LEN)
        .max(1);
    records
        .chunks(per_chunk)
        .map(|chunk| chunk.to_vec())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{MAX_PENDING, MIN_FRAME_SIZE, Reachability, WireReachability, snapshot_frames};
    use crate::{
        cluster::{
            downing::{Disconnected, Downing, DowningProvider, KeepMajority},
            membership::{Member, MemberState},
            node::NodeId,
        },
        sync::lock,
    };
    use std::{collections::HashSet, net::SocketAddr, time::Duration};

    fn node(port: u16) -> NodeId {
        NodeId::new(SocketAddr::from(([127, 0, 0, 1], port)))
    }

    fn members(nodes: &[NodeId]) -> Vec<Member> {
        nodes
            .iter()
            .map(|node| Member::new(*node, MemberState::Up))
            .collect()
    }

    fn up(nodes: &[NodeId]) -> HashSet<NodeId> {
        nodes.iter().copied().collect()
    }

    /// Merging plus the tick's promotion, which is what a node actually applies per round.
    fn merge_all(table: &Reachability, records: &[WireReachability], nodes: &[NodeId]) {
        table.merge(records, &up(nodes));
        table.promote_pending(&up(nodes));
    }

    fn observe(table: &Reachability, subject: NodeId, reachable: bool) -> WireReachability {
        assert!(table.observe(subject, reachable), "the edge must change");
        table
            .take_outbound(&up(&[table.node, subject]))
            .into_iter()
            .find(|record| record.subject == subject)
            .expect("the new statement is queued for the next push")
    }

    /// A broken direct A-B edge still leaves one component through C, so neither endpoint may
    /// manufacture the other's tombstone.
    #[test]
    fn a_single_broken_link_stays_connected_through_a_third_member() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let c = Reachability::new(nodes[2]);
        let records = [observe(&a, nodes[1], false), observe(&b, nodes[0], false)];
        for table in [&a, &b, &c] {
            merge_all(table, &records, &nodes);
            assert!(table.unreachable_members(&members(&nodes)).is_empty());
        }
    }

    /// A genuine A | B,C split yields the same two components at every node after relaying.
    #[test]
    fn a_partition_is_derived_from_the_shared_graph() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let c = Reachability::new(nodes[2]);
        let records = [
            observe(&a, nodes[1], false),
            observe(&a, nodes[2], false),
            observe(&b, nodes[0], false),
            observe(&c, nodes[0], false),
        ];
        for table in [&a, &b, &c] {
            merge_all(table, &records, &nodes);
        }

        let all_members = members(&nodes);
        let unreachable_a = a.unreachable_members(&all_members);
        let outside_a = unreachable_a
            .iter()
            .copied()
            .map(|(member, _)| member.node())
            .collect::<HashSet<_>>();
        assert_eq!(outside_a, HashSet::from([nodes[1], nodes[2]]));
        let after = Duration::from_secs(10);
        let at = unreachable_a[0].1 + after;
        assert_eq!(
            KeepMajority::new(after).down(&all_members, Disconnected::new(&unreachable_a), at),
            Downing::SelfDown
        );
        for table in [&b, &c] {
            let unreachable = table.unreachable_members(&all_members);
            let outside = unreachable
                .iter()
                .copied()
                .map(|(member, _)| member.node())
                .collect::<HashSet<_>>();
            assert_eq!(outside, HashSet::from([nodes[0]]));
            assert_eq!(
                KeepMajority::new(after).down(
                    &all_members,
                    Disconnected::new(&unreachable),
                    unreachable[0].1 + after
                ),
                Downing::Members(vec![all_members[0]])
            );
        }
    }

    /// A record which arrives through a relay is the case the observer's own resend cannot repair:
    /// the observer only ever pushes to its own peers, and the relay deduplicates the resend by
    /// version. Parking it until membership catches up is what keeps the graph from staying wrong,
    /// and the promoted record must be queued again so the relay continues past this node.
    #[test]
    fn a_relayed_record_parked_while_membership_lags_is_promoted_and_relayed_on() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let b_to_c = observe(&b, nodes[2], false);
        let a_to_c = observe(&a, nodes[2], false);

        let lagging = up(&[nodes[0], nodes[1]]);
        a.merge(&[b_to_c], &lagging);
        a.promote_pending(&lagging);
        assert!(a.take_outbound(&lagging).is_empty());
        assert!(a.unreachable_members(&members(&nodes)).is_empty());

        a.promote_pending(&up(&nodes));

        assert_eq!(a.take_outbound(&up(&nodes)), vec![a_to_c, b_to_c]);
        let outside = a
            .unreachable_members(&members(&nodes))
            .iter()
            .map(|(member, _)| member.node())
            .collect::<Vec<_>>();
        assert_eq!(outside, vec![nodes[2]]);
    }

    /// A push carries what was learned since the last one, plus this node's own standing failures,
    /// which are re-asserted so a peer that received none of them at all still converges. A
    /// relayed record is forwarded once; a recovery revokes its failure rather than standing.
    #[test]
    fn a_push_drains_what_was_learned_and_re_asserts_own_failures() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let all = up(&nodes);

        let a_to_b = observe(&a, nodes[1], false);
        observe(&a, nodes[2], false);
        observe(&a, nodes[2], true);
        let b_to_c = observe(&b, nodes[2], false);
        a.merge(&[b_to_c], &all);

        assert_eq!(a.take_outbound(&all), vec![a_to_b, b_to_c]);
        assert_eq!(a.take_outbound(&all), vec![a_to_b]);
    }

    /// A peer can name any node, so what this node parks for it is capped.
    #[test]
    fn the_pending_table_is_bounded() {
        let local = node(1);
        let table = Reachability::new(local);
        let observer = node(9_000);
        let records = (0..MAX_PENDING + 8)
            .map(|version| WireReachability {
                observer,
                subject: NodeId::new(
                    format!("127.0.0.1:{}", 10_000 + version)
                        .parse()
                        .expect("valid address"),
                ),
                version: version as u64,
                reachable: false,
            })
            .collect::<Vec<_>>();

        table.merge(&records, &up(&[local]));

        assert_eq!(lock(&table.state).pending.len(), MAX_PENDING);
    }

    /// A newer recovery wins even if an old failure is relayed afterwards.
    #[test]
    fn versions_make_recovery_monotone() {
        let nodes = [node(1), node(2)];
        let owner = Reachability::new(nodes[0]);
        let replica = Reachability::new(nodes[1]);
        let failed = observe(&owner, nodes[1], false);
        let recovered = observe(&owner, nodes[1], true);
        merge_all(&replica, &[recovered, failed], &nodes);

        assert!(replica.unreachable_members(&members(&nodes)).is_empty());
        assert_eq!(replica.snapshot(&HashSet::from(nodes)), vec![recovered]);
    }

    /// The conservative fixed-size bound keeps every reachability snapshot chunk within the
    /// endpoint's configured frame limit.
    #[test]
    fn reachability_snapshots_respect_the_frame_limit() {
        let widest = || {
            NodeId::new(
                "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                    .parse()
                    .expect("valid address"),
            )
        };
        let record = WireReachability {
            observer: widest(),
            subject: widest(),
            version: u64::MAX,
            reachable: false,
        };
        assert!(
            postcard::experimental::serialized_size(&record).expect("record size")
                <= WireReachability::MAX_ENCODED_LEN
        );
        for frame in snapshot_frames(vec![record; 10], MIN_FRAME_SIZE) {
            assert!(frame.encode_into(Vec::new()).expect("frame encodes").len() <= MIN_FRAME_SIZE);
        }
    }
}
