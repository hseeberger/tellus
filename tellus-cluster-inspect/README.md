# tellus-cluster-inspect

An [axum](https://crates.io/crates/axum) `Router` which lets you look into the
[tellus](../tellus) cluster node it runs in. Mount it into your application's own HTTP server:

```rust
let inspect = tellus_cluster_inspect::router(InspectConfig::default())?;
let app = Router::new().nest("/inspect", inspect);
```

It reveals the cluster's topology, so mount it behind whatever protects the rest of your server;
it brings no authentication of its own and binds no port.

This package is not published yet, like the rest of the workspace; it is part of `just all`.

## What it serves

- `GET /cluster`: a `Snapshot` as JSON: the node's current `ClusterState` (version, this node's
  address and incarnation, the member list with each member's address, incarnation and state, and
  the unreachable set), the endpoint's `Lifecycle` (`unformed`, `formed`, `downed`) and the
  receptionist's `Settlement` (whether every Up member's registrations are what lookups answer
  from, with the version it judged).
- `GET /events`: a stream of server-sent events, one `Stamped` JSON object per message with the
  unix milliseconds it was recorded at and an `InspectEvent` tagged by `kind`:
  - `hello` opens every stream: the log's `started_at` and whether the stream `resumed` right
    after the client's `Last-Event-ID`.
  - `state`: the whole `ClusterState` at a new version, never a delta.
  - `settled`: the receptionist's verdict flipped.
  - `gap`: the node's own subscriber fell behind by `dropped` changes; the current state and
    verdict follow.

  These are the items of `tellus::cluster::changes`, recorded in publication order, so a verdict
  never precedes the state it judges and versions are consecutive except across a gap.

## Resuming

Every logged message carries an `id` in the form `{started_at}:{seq}`. A client which reconnects
with that id in `Last-Event-ID` continues right after it, provided the log (`events_kept`
messages, 1024 by default) still retains that position; the hello then says `resumed: true`.

Otherwise the hello says `resumed: false` and the client is handed a recovery: the retained
history oldest first, without ids, then the current state and the current verdict, of which only
the last carries an id. So a client that drops in the middle of the recovery holds no position and
recovers again, and one that got through resumes from a position at which everything newer than
the pair follows. A changed `started_at` between two hellos means the node restarted.

## Configuration

`InspectConfig` has one field, `events_kept`, the size of the log. It is not part of the node's
`EndpointConfig`, since it is a property of this router, not of the endpoint.
