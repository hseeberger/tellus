//! Validates the contract suite itself against a minimal in-memory store: with no shipped
//! reference implementation, this proves the checks are runnable and pass against a store which
//! follows the contract by construction.

#![cfg(feature = "persistence-tests")]

mod in_memory_store {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/in_memory_store.rs"
    ));
}

use in_memory_store::InMemoryStore;

#[tokio::test]
async fn event_store_contract() {
    tellus::persistence_tests::event_store_contract(InMemoryStore::default()).await;
}

#[tokio::test]
async fn snapshot_store_contract() {
    tellus::persistence_tests::snapshot_store_contract(InMemoryStore::default()).await;
}

#[tokio::test]
async fn snapshot_with_event_tail() {
    let store = InMemoryStore::default();
    tellus::persistence_tests::snapshot_with_event_tail(store.clone(), store).await;
}
