//! Validates the contract suite itself against [InMemoryStore]: the checks must be runnable and
//! must pass against a store which follows the contract by construction.

#![cfg(all(feature = "persistence-tests", feature = "persistence-in-memory"))]

use tellus::InMemoryStore;

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
