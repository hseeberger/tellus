// include! only inside a wrapper module, else these use items collide with the includer's!

use std::{
    collections::HashMap,
    convert::Infallible,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tellus::{
    AppendError, EncodedEvent, EncodedSnapshot, EventStore, PersistenceId, SeqNo, SnapshotStore,
    StoredEvent, StoredSnapshot,
};

/// An in-memory [EventStore] and [SnapshotStore]: clones share the same streams and snapshots.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    streams: Arc<Mutex<HashMap<PersistenceId, Vec<StoredEvent>>>>,
    snapshots: Arc<Mutex<HashMap<PersistenceId, StoredSnapshot>>>,
}

impl EventStore for InMemoryStore {
    type Error = Infallible;

    async fn append(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        let mut streams = self.streams.lock().expect("streams lock poisoned");
        let stream = streams.entry(id.clone()).or_default();
        if SeqNo::new(stream.len() as u64) != next_seq_no {
            return Err(AppendError::Conflict);
        }

        for (n, event) in events.into_iter().enumerate() {
            stream.push(StoredEvent {
                seq_no: next_seq_no.advanced_by(n),
                event,
            });
        }

        Ok(())
    }

    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        let streams = self.streams.lock().expect("streams lock poisoned");
        let events = streams
            .get(id)
            .map(|stream| {
                // Gapless from 0 by construction (append rejects any other next sequence
                // number), so the index equals the sequence number.
                let start = (from_seq_no.as_u64() as usize).min(stream.len());
                let end = start.saturating_add(limit.get()).min(stream.len());
                stream[start..end].to_vec()
            })
            .unwrap_or_default();

        Ok(events)
    }
}

impl SnapshotStore for InMemoryStore {
    type Error = Infallible;

    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        self.snapshots
            .lock()
            .expect("snapshots lock poisoned")
            .insert(
                id.clone(),
                StoredSnapshot {
                    next_seq_no,
                    snapshot,
                },
            );

        Ok(())
    }

    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        let snapshot = self
            .snapshots
            .lock()
            .expect("snapshots lock poisoned")
            .get(id)
            .cloned();

        Ok(snapshot)
    }
}
