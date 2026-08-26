# tellus-persistence-postgres

PostgreSQL-backed stores for [tellus](../tellus) persistence: `PostgresStore` implements both
`EventStore` and `SnapshotStore` over one shared connection pool, so a single value, cheap to
clone, wires a whole `Persistence`.

For the contract these stores implement, from replay equals live execution to fencing and schema
evolution, see [docs/persistence.md](../docs/persistence.md).

## Schema

Events live in the `events` table with the primary key `(entity_type, entity_id, seq_no)`,
snapshots in `snapshots` keyed by `(entity_type, entity_id)`, only the latest one retained. The
schema is [`migrations/0001_events_and_snapshots.sql`](migrations/0001_events_and_snapshots.sql),
embedded in the crate and applied via `PostgresStore::migrate`.

Appends are a single atomic, conditional statement: a stale expected next sequence number, below or
above the actual one, fails with `AppendError::Conflict` and leaves the stream untouched, which is
tellus's fencing guarantee. An empty append inserts nothing but is still fenced.

## Examples

Both recipes start the `postgres` service of the repository's
[docker-compose.yaml](../docker-compose.yaml); the database is addressed via `DATABASE_URL`,
defaulting to that service. `docker compose down` stops it, `down -v` resets the data.

- [`event_sourced_counter`](examples/event_sourced_counter.rs): a counter surviving process
  restarts; every run recovers the count by replay, increments it once and prints the new count, so
  repeated runs count on:

  ```shell
  just run-examples-event-sourced-counter
  ```

- [`event_sourced_supervision`](examples/event_sourced_supervision.rs): the flaky loader of
  tellus's plain supervision example, now event-sourced, so the restart after a toxic value
  recovers the count by replay instead of starting over at 0:

  ```shell
  just run-examples-event-sourced-supervision
  ```

## Tests

[`tests/store.rs`](tests/store.rs) runs the contract test suite which ships with `tellus` behind
its `persistence-tests` feature against a real server, started via testcontainers, hence Docker is
required. Image tag and credentials are read out of the repository's
[docker-compose.yaml](../docker-compose.yaml) at compile time via
[`composed`](https://crates.io/crates/composed), so there is one place to change either.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
