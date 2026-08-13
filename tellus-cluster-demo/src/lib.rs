//! The JSON a demo node's HTTP API answers, shared with the verifier consuming it.

#![warn(missing_docs)]

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, net::SocketAddr};
use tellus::cluster::MemberState;

/// What one node sees of the cluster, answered by `GET /cluster`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterView {
    /// The node's name, e.g. `node1`.
    pub name: String,

    /// The address the node advertises to the cluster.
    pub addr: SocketAddr,

    /// How far the node is along joining the cluster.
    pub phase: Phase,

    /// The member list as this node sees it, one entry per member incarnation, so a restarted
    /// node's address is listed twice until the Down entry's retention expires.
    pub members: Vec<MemberView>,
}

impl ClusterView {
    /// The addresses this node lists as Up.
    pub fn up_addrs(&self) -> BTreeSet<SocketAddr> {
        self.members
            .iter()
            .filter(|member| member.state == MemberState::Up)
            .map(|member| member.addr)
            .collect()
    }
}

/// How far a node is along joining the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// The endpoint is started and seed discovery is running, but this node has not joined yet.
    Bootstrapping,

    /// A member of the cluster, with the worker registered under its key.
    Joined,

    /// Downed by the cluster: the process exits, so a restart rejoins with a fresh incarnation.
    Downed,
}

/// One member of the cluster as one node sees it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MemberView {
    /// The address the member advertises.
    pub addr: SocketAddr,

    /// The member's state.
    pub state: MemberState,
}

/// What one node's messaging to every other member yields, answered by `GET /probe`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProbeReport {
    /// The probing node's name.
    pub name: String,

    /// One entry per Up member other than this node.
    pub probes: Vec<Probe>,
}

/// A message round trip to the worker of one member.
#[derive(Debug, Serialize, Deserialize)]
pub struct Probe {
    /// The probed member's address.
    pub addr: SocketAddr,

    /// What the round trip yielded.
    pub outcome: ProbeOutcome,
}

/// The result of one probe.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeOutcome {
    /// The worker answered.
    Ok {
        /// The round trip time in milliseconds, discovery included.
        millis: u128,

        /// How many members the answering node lists as Up.
        up_members: usize,
    },

    /// The worker could not be resolved or did not answer.
    Failed {
        /// What went wrong.
        error: String,
    },
}
