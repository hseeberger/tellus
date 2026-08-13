use derive_more::{Display, Into};
use uuid::Uuid;

/// A unique actor ID: a UUID v7, so IDs are time-ordered by creation. With the `cluster` feature
/// it is serializable, e.g. to carry it in messages crossing node boundaries.
#[derive(Debug, Display, Clone, Copy, Into, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "cluster", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorId(Uuid);

impl ActorId {
    pub(crate) fn new() -> Self {
        Self(Uuid::now_v7())
    }
}
