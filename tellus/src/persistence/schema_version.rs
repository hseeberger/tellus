use derive_more::Display;

/// The schema version of a stored payload, written alongside it and checked on read: a payload
/// whose version the current code neither knows nor upcasts is rejected rather than decoded with
/// the current shape. It is a number of its own, never interchangeable with a
/// [SeqNo](crate::SeqNo) or any other value travelling with the same record.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersion(u16);

impl SchemaVersion {
    /// Create a schema version. It is `const` so it can define
    /// [Versioned::VERSION](crate::Versioned::VERSION).
    pub const fn new(schema_version: u16) -> Self {
        Self(schema_version)
    }

    /// This schema version as a `u16`, for stores mapping it onto a column type and for matching
    /// on it in a [Versioned::decode](crate::Versioned::decode) override.
    pub fn as_u16(self) -> u16 {
        self.0
    }
}
