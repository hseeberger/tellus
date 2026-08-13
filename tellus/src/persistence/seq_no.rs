use derive_more::Display;

/// A position in an event stream: either the sequence number of a stored event, gapless and
/// starting at 0, or the position at which the next event is appended, which is [SeqNo::ZERO] for
/// an empty stream. Replay resumes at a position, never after one, so no adjustment is needed
/// anywhere between a snapshot, a read and an append.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SeqNo(u64);

impl SeqNo {
    /// The position of an empty stream and the sequence number of the first event.
    pub const ZERO: Self = Self::new(0);

    /// Create a sequence number, e.g. from a store's column value.
    pub const fn new(seq_no: u64) -> Self {
        Self(seq_no)
    }

    /// The position directly after this one.
    pub fn succ(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The position after appending `count` events at this one.
    pub fn advanced_by(self, count: usize) -> Self {
        Self(self.0.saturating_add(count as u64))
    }

    /// This position as a `u64`, for stores mapping it onto a column type.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::seq_no::SeqNo;

    /// Advancing by one is the successor, and advancing by a count lands where that many
    /// successive appends would.
    #[test]
    fn succ_and_advanced_by_agree() {
        assert_eq!(SeqNo::ZERO.succ(), SeqNo::new(1));
        assert_eq!(SeqNo::ZERO.advanced_by(1), SeqNo::ZERO.succ());
        assert_eq!(SeqNo::new(2).advanced_by(3), SeqNo::new(5));
    }

    /// Advancing past the end saturates: a wrapped position would silently reorder a stream.
    #[test]
    fn advancing_saturates_instead_of_wrapping() {
        assert_eq!(SeqNo::new(u64::MAX).succ(), SeqNo::new(u64::MAX));
        assert_eq!(SeqNo::new(u64::MAX).advanced_by(3), SeqNo::new(u64::MAX));
    }
}
