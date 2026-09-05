//! Deciding whether this node forms a new cluster when a [bootstrap](fn@crate::cluster::bootstrap)
//! round found none: [FormationProvider] is what
//! [BootstrapConfig::formation](crate::cluster::BootstrapConfig::formation) takes, the default is
//! [Majority], whose lowest-address rule stops a downed minority from re-forming inside a
//! partition.

use std::{collections::BTreeSet, fmt::Debug, net::SocketAddr, sync::Arc};

/// Creates the [FormationProvider] for a [bootstrap](fn@crate::cluster::bootstrap) run.
pub type FormationProviderFactory = Arc<dyn Fn() -> Box<dyn FormationProvider> + Send + Sync>;

/// Decides whether this node forms a new cluster, asked by
/// [bootstrap](fn@crate::cluster::bootstrap) once a round of join attempts has been admitted
/// nowhere; the default is [Majority].
///
/// A started endpoint is not a cluster, so forming one is a decision, and the wrong one splits
/// the cluster in two. A node forming while it is merely partitioned from the others comes back
/// as a cluster of its own, and clusters never merge. See docs/cluster.md.
pub trait FormationProvider
where
    Self: Send + 'static,
{
    /// Whether this node forms a cluster now.
    fn form(&mut self, formation: &Formation) -> bool;
}

/// What one round of join attempts saw, next to the addresses it was taken over.
#[derive(Debug, Clone)]
pub struct Formation {
    /// Every address discovery resolved, this node's own included: the denominator of any
    /// counting rule and the set the lowest address is taken from. Deciding on the universe
    /// rather than on who answered is what keeps a partitioned node from forming.
    pub universe: BTreeSet<SocketAddr>,

    /// This node's own address.
    pub self_addr: SocketAddr,

    /// What each attempted address answered. Addresses of the universe which were not attempted
    /// are absent, e.g. while this node is pinned to a cluster, so a rule must count against the
    /// universe rather than against this.
    pub outcomes: Vec<(SocketAddr, JoinOutcome)>,
}

impl Formation {
    /// This node holds the lowest address of the universe, hence the only one which may form.
    pub fn is_lowest(&self) -> bool {
        self.universe.first() == Some(&self.self_addr)
    }

    /// How many addresses of the universe are known to be no cluster, this node included, since
    /// a node running bootstrap is none itself.
    pub fn no_cluster_count(&self) -> usize {
        1 + self
            .outcomes
            .iter()
            .filter(|(_, outcome)| *outcome == JoinOutcome::NoCluster)
            .count()
    }
}

/// What one join attempt against one address yielded, short of admitting this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOutcome {
    /// It answered that it is a member of no cluster, so it counts towards forming one.
    NoCluster,

    /// It did not answer, or answered something which is not an admission: unreachable, but also
    /// a refused identity, a protocol mismatch, an undecodable frame. Nothing about whether a
    /// cluster exists there follows from it.
    Unavailable,
}

/// The default [FormationProvider]: forms once a strict majority of the universe, this node
/// included, is known to be no cluster, and only at the lowest address of the universe.
///
/// The lowest address makes the former unique, so nodes deciding on the same universe cannot
/// form two clusters. The majority is what keeps a partitioned minority from forming one at all,
/// which is the failure this exists for: its nodes are downed, restart, and would otherwise come
/// back as a cluster of their own. Both conditions are on the universe rather than on who
/// answered, so a node which cannot reach the lowest address forms nothing; formation liveness
/// is hence a requirement on discovery, to stop resolving nodes which are gone.
#[derive(Debug, Default)]
pub struct Majority;

impl FormationProvider for Majority {
    fn form(&mut self, formation: &Formation) -> bool {
        formation.is_lowest() && formation.no_cluster_count() * 2 > formation.universe.len()
    }
}

/// The strict [FormationProvider]: forms only once every address of the universe is known to be
/// no cluster, and only at the lowest of them. Any one unreachable address blocks a cold start,
/// so no cluster is formed while any part of the universe is unaccounted for.
#[derive(Debug, Default)]
pub struct Unanimous;

impl FormationProvider for Unanimous {
    fn form(&mut self, formation: &Formation) -> bool {
        formation.is_lowest() && formation.no_cluster_count() == formation.universe.len()
    }
}

/// The [FormationProvider] which never forms: the cluster comes into being through
/// [form](crate::cluster::form), called by the deployment, and bootstrap only ever joins it.
#[derive(Debug, Default)]
pub struct Explicit;

impl FormationProvider for Explicit {
    fn form(&mut self, _: &Formation) -> bool {
        false
    }
}

#[cfg(feature = "serde")]
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FormationProviderConfig {
    Majority,
    Unanimous,
    Explicit,
}

#[cfg(feature = "serde")]
impl FormationProviderConfig {
    pub(crate) fn factory(self) -> FormationProviderFactory {
        match self {
            Self::Majority => Arc::new(|| Box::new(Majority)),
            Self::Unanimous => Arc::new(|| Box::new(Unanimous)),
            Self::Explicit => Arc::new(|| Box::new(Explicit)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::cluster::formation::FormationProviderConfig;
    use crate::cluster::formation::{
        Explicit, Formation, FormationProvider, JoinOutcome, Majority, Unanimous,
    };
    use std::{collections::BTreeSet, net::SocketAddr};

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn formation(self_port: u16, ports: &[u16], outcomes: &[(u16, JoinOutcome)]) -> Formation {
        let mut universe = ports
            .iter()
            .map(|port| addr(*port))
            .collect::<BTreeSet<_>>();
        universe.insert(addr(self_port));

        Formation {
            universe,
            self_addr: addr(self_port),
            outcomes: outcomes
                .iter()
                .map(|(port, outcome)| (addr(*port), *outcome))
                .collect(),
        }
    }

    #[test]
    fn majority_forms_at_the_lowest_address_of_a_settled_universe() {
        let formation = formation(
            1,
            &[1, 2, 3, 4, 5],
            &[
                (2, JoinOutcome::NoCluster),
                (3, JoinOutcome::NoCluster),
                (4, JoinOutcome::NoCluster),
                (5, JoinOutcome::NoCluster),
            ],
        );

        assert!(Majority.form(&formation));
    }

    /// The whole point: the minority side of a partition, restarted, must not come back as a
    /// cluster of its own, however completely it can see itself.
    #[test]
    fn majority_refuses_a_partitioned_minority() {
        let formation = formation(
            1,
            &[1, 2, 3, 4, 5],
            &[
                (2, JoinOutcome::NoCluster),
                (3, JoinOutcome::Unavailable),
                (4, JoinOutcome::Unavailable),
                (5, JoinOutcome::Unavailable),
            ],
        );

        assert!(!Majority.form(&formation));
    }

    /// Only one node may form, else two of the same majority form two clusters.
    #[test]
    fn majority_refuses_every_address_but_the_lowest() {
        let formation = formation(
            2,
            &[1, 2, 3, 4, 5],
            &[
                (1, JoinOutcome::NoCluster),
                (3, JoinOutcome::NoCluster),
                (4, JoinOutcome::NoCluster),
                (5, JoinOutcome::NoCluster),
            ],
        );

        assert!(!Majority.form(&formation));
    }

    /// The lowest address is taken from the universe, not from who answered, so a node which
    /// cannot reach it waits rather than electing itself.
    #[test]
    fn majority_refuses_while_the_lowest_address_is_unavailable() {
        let formation = formation(
            2,
            &[1, 2, 3, 4, 5],
            &[
                (1, JoinOutcome::Unavailable),
                (3, JoinOutcome::NoCluster),
                (4, JoinOutcome::NoCluster),
                (5, JoinOutcome::NoCluster),
            ],
        );

        assert!(!Majority.form(&formation));
    }

    /// A round which asked only the pinned address cannot reach a majority of the universe.
    #[test]
    fn majority_refuses_a_round_which_asked_one_address() {
        let formation = formation(1, &[1, 2, 3, 4, 5], &[(2, JoinOutcome::NoCluster)]);

        assert!(!Majority.form(&formation));
    }

    /// A discovery resolving nothing but this node is a cluster of one, deliberately.
    #[test]
    fn majority_forms_a_single_node_cluster() {
        let formation = formation(1, &[], &[]);

        assert!(Majority.form(&formation));
    }

    /// Discovery need not resolve this node's own address, so the universe adds it.
    #[test]
    fn majority_counts_a_universe_the_local_address_is_missing_from() {
        let formation = formation(1, &[2, 3], &[(2, JoinOutcome::NoCluster)]);

        assert!(Majority.form(&formation));
    }

    #[test]
    fn unanimous_forms_only_once_every_address_answered() {
        let all = formation(
            1,
            &[1, 2, 3],
            &[(2, JoinOutcome::NoCluster), (3, JoinOutcome::NoCluster)],
        );
        let one_missing = formation(
            1,
            &[1, 2, 3],
            &[(2, JoinOutcome::NoCluster), (3, JoinOutcome::Unavailable)],
        );

        assert!(Unanimous.form(&all));
        assert!(!Unanimous.form(&one_missing));
    }

    #[test]
    fn explicit_never_forms() {
        let formation = formation(1, &[], &[]);

        assert!(!Explicit.form(&formation));
    }

    /// The provider is chosen by name in a config file; the built providers are told apart by
    /// what they decide, since all three are trait objects.
    #[cfg(feature = "serde")]
    #[test]
    fn a_formation_provider_is_selected_by_name() {
        let majority = formation(
            1,
            &[1, 2, 3],
            &[(2, JoinOutcome::NoCluster), (3, JoinOutcome::Unavailable)],
        );

        let config = serde_json::from_str::<FormationProviderConfig>(r#""majority""#)
            .expect("the majority provider is valid");
        assert!(config.factory()().form(&majority));

        let config = serde_json::from_str::<FormationProviderConfig>(r#""unanimous""#)
            .expect("the unanimous provider is valid");
        assert!(!config.factory()().form(&majority));

        let config = serde_json::from_str::<FormationProviderConfig>(r#""explicit""#)
            .expect("the explicit provider is valid");
        assert!(!config.factory()().form(&majority));

        assert!(serde_json::from_str::<FormationProviderConfig>(r#""majorty""#).is_err());
    }
}
