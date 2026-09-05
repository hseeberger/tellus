use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

/// Address plus per-process incarnation, so a restart is distinguishable from its predecessor.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[display("{addr}#{incarnation}")]
pub(crate) struct NodeId {
    addr: SocketAddr,
    incarnation: Incarnation,
}

impl NodeId {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            incarnation: Incarnation::new(),
        }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    pub(crate) fn covers(&self, other: NodeId) -> bool {
        self.addr == other.addr && other.incarnation <= self.incarnation
    }
}

/// Ordered: UUIDv7 is time based, so the younger incarnation on a host compares greater.
#[derive(
    Debug, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub(crate) struct Incarnation(Uuid);

impl Incarnation {
    fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::node::NodeId;
    use std::cmp::Ordering;

    /// Supersession resolves towards the younger incarnation by comparing incarnations, so one
    /// minted later must compare greater.
    #[test]
    fn a_younger_incarnation_compares_greater() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let older = NodeId::new(addr).incarnation();
        let younger = NodeId::new(addr).incarnation();

        assert!(older < younger);
        assert!(younger > older);
        assert_eq!(older.cmp(&older), Ordering::Equal);
    }

    /// A node's textual identity is address plus incarnation, so two runs at the same address are
    /// distinguishable in a log line.
    #[test]
    fn a_node_displays_address_and_incarnation() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let node = NodeId::new(addr);
        let restarted = NodeId::new(addr);

        assert_eq!(
            node.to_string(),
            format!("127.0.0.1:1234#{}", node.incarnation())
        );
        assert_ne!(node.to_string(), restarted.to_string());
    }
}
