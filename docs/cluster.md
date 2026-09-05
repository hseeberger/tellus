# Clustered remoting

This document explains how tellus remoting (the `cluster` feature) works and which of the core
guarantees from [actors.md](actors.md) carry over the network, which weaken, and why. The
implementation lives in [`tellus/src/cluster`](../tellus/src/cluster), whose public API is flat
for what every node calls, with one submodule per pluggable family (`transport`, `codec`,
`downing`, `failure`, `formation`), and [`tellus-cluster-demo`](../tellus-cluster-demo) runs five
nodes of it under continuous crashes, departures and partitions.

## Overview

Remoting makes actors on different nodes message each other through the same API as local ones,
and every set of nodes doing so is a *cluster*.

`ActorRef` is location transparent: it implements `Serialize` and `Deserialize`, both failing with
`RefError::EndpointNotStarted` until the endpoint is started. Message types embed reference fields,
e.g. `reply_to: ActorRef<Reply>`, and work unchanged no matter where their counterpart lives.

Each process runs at most one remoting endpoint, started via `cluster::start_endpoint` with a
`Transport` (the provided one is QUIC via quinn, TLS included) and an `EndpointConfig`. The
separate `cluster-dev` feature adds `QuicTransport::dev`, which skips certificate verification and
hence exists only where it is asked for. With the `serde` feature `EndpointConfig`,
`BootstrapConfig` and `QuicConfig` are deserializable. Each pluggable family is chosen by the name
of one of its provided implementations (`codec: postcard`, `failure_detector: { phi_accrual: {} }`,
`downing_provider: { keep_majority: { after: 10s } }`, `formation: majority`); a custom
implementation is still assigned in code. An invalid `EndpointConfig` or `BootstrapConfig` is
refused by deserialization just as it is by `start_endpoint` and `cluster::bootstrap`. A
`QuicConfig` is plain data: its server name and the PEM files its paths name are validated by
`QuicTransport::from_config`, which reads them. Deserializing one is hence not yet a check that the
node can present an identity.

Membership is not optional. A started endpoint is *not* a cluster and admits nobody,
`cluster::form` makes it a cluster of one, and `cluster::join` makes it a member of the cluster one
of its seed addresses is in. Only members message each other: a handshake from a node which is
neither a member nor joining is refused, and so is a join towards a node which is a member of
nothing. The member list is gossiped, so every member learns every other from one seed address,
and a node shutting down cleanly announces its own departure with `cluster::leave` instead of
leaving the cluster to detect its silence.

Bootstrap goes through joining and discovery: `cluster::register` names a local actor and
`cluster::lookup` resolves that name at a member's address. The seed addresses themselves can be
discovered instead of configured: `cluster::bootstrap` resolves them through a `SeedDiscovery` and
joins once the view settles, see Membership. `serialize_ref` and `deserialize_ref` remain for
exchanging a reference out of band, e.g. through configuration; the bytes name the message type, so
a wrong-typed resolution is refused like a mistyped lookup. Every further reference travels inside
messages.

Death watch works across nodes through the ordinary `ActorContext::watch`, with the weakened
contract below. Request-response works across nodes too: `ReplyTo` is serializable like a
reference, so `ActorRef::ask` and `ActorContext::reply_to` work unchanged against remote actors,
with the `NoReply` detection weakened as described below.

A node's identity is its advertised address plus an *incarnation*, a UUID minted exactly once per
process start, so a restarted node is distinguishable from its predecessor and no process can ever
resurrect an incarnation.

## Membership

A member's state is a two point lattice per incarnation: `Up`, then possibly `Down`, and removal
is absence. A detailed Down is terminal and doubles as the incarnation's tombstone; its address
can also retain a watermark that fences it and its predecessors. A restarted node returns
as a fresh incarnation, never by leaving Down. That monotonicity makes the member list a
state-based CRDT: merging is union by node with the larger state per incarnation, hence
commutative, associative and idempotent, so duplicated, reordered or crossed gossip cannot corrupt
it, and no versioning or anti-entropy machinery is needed.

*Joining*: `cluster::join` tries its seed addresses in order with the reconnect backoff between
rounds, no internal timeout, until one admits the joiner. A join rides one dedicated connection:
the handshake names the joiner with `Join` intent, the seed admits it as Up and answers with its
member snapshot, which the joiner merges as *one* snapshot once its last chunk has arrived. A
connection lost mid stream hence leaves no fragment of a cluster the joiner never joined. Every
further frame rides ordinary lanes. Only a node which is itself a member admits a join; a node
which is not answers `NoCluster`, which is retryable, so a seed list shared by nodes that are all
still starting forms nothing on its own. The joiner's own address is skipped in every position. A
cluster is the transitive closure of joins; independent clusters must not share seeds, since any
gossip exchange merges them.

Joining is exclusive and, once a cluster has answered, pinned. At most one join attempt runs per
endpoint, because two overlapping ones could be admitted by two different clusters before either
is recorded, leaving both counting this node; further callers queue. An attempt is abandoned if
its caller goes away while it waits for the permit or for the transport to connect, since nothing
has been sent by then. From the `Join` handshake on it runs to its conclusion, since the peer may
already count this node from that moment. Once a seed's handshake, address and identity
have been validated, that address is *pinned*: it is the only one tried afterwards, and nothing
may be formed beside it, because no other seed can be proven to be that same cluster. The pin is
released by a completed join, by that address answering `NoCluster`, by this node being downed, or
by bootstrap seeing the address leave a newly settled discovery view.

*Forming*: a cluster comes into being by decision, never as a side effect. `cluster::form` makes
this node a cluster of one for a deployment which decides for itself; it is refused if this node
is already a member, has been downed, or has a join attempt or a pinned cluster outstanding.
Otherwise `cluster::bootstrap` decides through a `FormationProvider`, a pluggable trait like the
`DowningProvider`; its provided implementations are `Majority` (the default), `Unanimous` and
`Explicit`. The provider sees the *universe*, every resolved address united with this node's own,
and what each attempted address answered. `Majority` forms once a strict majority of the universe,
this node included, is known to be no cluster, and only at the lowest address of the universe. The
lowest-address condition ensures that only one node forms for a shared universe. The majority
stops a partitioned minority from re-forming. Both conditions are on the universe rather than on
who answered, so a node which cannot reach the lowest address forms nothing. Formation liveness is
hence a requirement on discovery, which must stop resolving nodes that are gone. A reduced view
must still satisfy `min_peers` for the next-lowest address to inherit the role.

*Bootstrap*: `cluster::bootstrap` turns the seed addresses from configuration into discovery. A
`SeedDiscovery` resolves them; the provided ones are `FixedSeeds` in tellus, SRV and A/AAAA
records in the [`tellus-bootstrap-dns`](../tellus-bootstrap-dns) crate and pods listed by label
selector in the [`tellus-bootstrap-k8s`](../tellus-bootstrap-k8s) crate. The *universe* is the
resolved set united with this node's own address. Bootstrap polls the discovery until the universe
has held at least `min_peers` addresses unchanged for the settle window. It then attempts to join
through every resolved address but this node's own, lowest first, and asks the `FormationProvider`
when none of them admitted it. It returns as soon as this node is a member, its own formation
included, and at once when it already is: it does not keep running, membership owns failure
detection and downing from then on. `min_peers` counts the universe while the formation provider
counts what its addresses answered. That distinction keeps a restarted node from re-forming inside
a partition: discovery is unaffected by a partition, reachability is not. Resolve failures are
retried and restart the settle window, since nothing was observed during them. There is no
internal timeout, so the call is bounded by the caller's timeout. `Downed` is fatal per process
and means the caller should exit, letting the restart mint the fresh incarnation. What does this
not prevent? Nodes deciding on *disjoint* discovery views can still form separate clusters, which
never merge on their own, since the formation rule is only as good as the universe it is taken
over. The settle window and the `min_peers` floor shrink that window, so size `min_peers` to the
deployment when possible.

*Gossip*: every `heartbeat_interval` each node pushes its full member list to every Up member, and
additionally whenever the list changes locally; the acceptor of every connection sends its
snapshot as the first frame behind the handshake. Gossip frames double as heartbeats, so there is
no separate ping. A member with a currently disconnected lane is skipped, keyed on transport
connectivity, never on suspicion, so the uncounted system-frame queues stay bounded while a member
is retried and two healthy nodes can never talk each other into mutual silence. A state change
hence reaches every member with a live path within one gossip round per hop of connectivity;
convergence is eventual, not agreed, see downing below. Direct-link reachability rides the same
tick: an edge change is versioned by its observer, and every statement a node mints, accepts from a
peer or promotes is queued and fanned out once per tick, deduplicated by version at every hop. A
statement is *merged* the moment it arrives, so this node's own decisions never wait, and only its
*forwarding* costs a round. Reachability hence spreads one hop per tick exactly as membership does,
and a burst of edge changes costs one push per member per tick rather than one fan-out per record.
Like gossip, a push skips a disconnected lane. A statement naming a node which is not a member here
yet cannot be merged, and its observer resends only to its own peers, so a relayed one would be lost
for good. Such a record is therefore parked in a bounded table and admitted once membership catches
up, which resumes the relay past this node. Connection setup sends the full observation table,
and every tick re-asserts this node's own standing unreachable statements, which together repair
a record lost with a connection rather than dropped on arrival.

Batching bounds the frames, not the replication. Every node forwards every statement it accepts to
every Up peer, the one it came from included, so a single edge change still costs O(n^2) copies of
that record across the cluster, and a partition breaking e edges costs O(e * n^2). That is an
accepted bound, not one this design removes: it is the same order as the membership push, and a
smaller fan-out would trade the deterministic spread that downing agreement rests on for a
probabilistic one. Reachability is free while a cluster is healthy, since nothing is then queued or
re-asserted; membership is not, since every node pushes its whole list to every peer each tick,
which is O(n^2) frames and O(n^3) copied entries per second across the cluster. Both are
deliberately simple for the cluster sizes this targets, and neither is meant for hundreds of
nodes.

*Refusal*: an inbound `Member`-intent handshake from a node which is not an Up member is answered
with a refusal the dialer treats as retryable, since gossip may not have carried that node's
join everywhere yet; the backoff retry succeeds once it has. A dead incarnation's handshake
is refused finally. The outbound direction is optimistic: a send towards a node this endpoint does
not know yet, e.g. a reference which arrived ahead of the gossip naming its node, dials anyway and
lets the far side judge; only a send towards a Down incarnation is refused locally.

*Supersession*: a join from an address an Up member already advertises proves the old process is
gone, so the old incarnation is downed and the new one admitted. Where both directions race, the
younger incarnation wins: UUIDv7 incarnations order by minting time, assuming the restarting
host's clock did not regress. The direct join handshake stays the primary path; the ordering only
makes the gossip merge convergent.

*Leaving*: `cluster::leave` announces a departure instead of leaving it to be detected: this node
moves its own entry to Down, pushes that member list to every Up member, waits until the
announcement has left its outbound queues, and severs. It needs no protocol of its own, since a
departure is nothing but this node's own entry taking the one step the lattice allows. A member
receiving it hence runs the ordinary node death sequence right away rather than after failure
detection plus downing: pending asks fail, watchers are signaled, the incarnation is refused. The
announcement goes to every Up member, a lane still being dialed included: it is the last thing
this endpoint sends and can hence not grow a queue the way the periodic gossip would. The wait for
it to drain is bounded by `leave_timeout`. A member which is gone already costs that timeout and
then learns the departure from another member's gossip, or, with no live path at all, from its own
failure detection. Leaving is terminal per incarnation exactly as being downed is, so the process
is expected to exit, and only a restart rejoins.

*Self down*: a Down watermark at a node's own address can cover its incarnation. When gossip or a
final refusal tells the node so, it honors the verdict: it logs at error level, severs every
connection, and refuses everything from then on; `cluster::join` fails as `Downed`. Only a
restarted process, with a fresh incarnation, can rejoin. Honoring the verdict keeps the
synthesized signals other nodes flushed true. A leave is the same sever and the same latch, chosen
by this node and announced first, hence logged as the departure it is rather than at error level.

*Retention*: a detailed Down entry is dropped `down_retention` after this node learned of it, but
its address keeps the greatest Down UUIDv7 incarnation as a compact watermark. That watermark
refuses the dead process and every predecessor while allowing a restart, whose incarnation compares
greater. The retention window covers frames and gossip still naming the detailed entry in flight;
after it expires, `cluster::members` omits the dead incarnation while the fence stays.

Watermarks ride connection setup, not the heartbeat. The periodic gossip carries the member list
alone, so it stays the size of the cluster however many addresses the cluster has outlived.
Connection setup exchanges snapshots carrying the watermarks too. The acceptor sends its snapshot
behind its handshake, and the dialer sends its own over the freshly bound lane. That is enough to
converge, because a node absent longer than the retention has to reconnect before it can say
anything, and either side of that reconnect hands the other its fences. A snapshot is split into
as many `Gossip` frames as `max_frame_size` demands, every one but the last marked `more` so a
joiner reads the whole of it; the merge, being a CRDT union, absorbs the chunks in any order. A
`max_frame_size` too small to hold one member and its envelope is refused by
`cluster::start_endpoint`, since no chunking could keep such a snapshot within it.
`down_watermarks` caps the set, evicting the oldest incarnation first, which bounds both the memory
an endpoint spends on outlived addresses and the time a connection setup takes to hand them over.
An evicted address falls back to the behavior it had before watermarks, where its zombie is refused
as an unknown member rather than told it is dead.

The fence relies on UUIDv7 incarnations being monotone at an address. In particular, a host whose
clock moves backwards across a restart can mint an incarnation below the watermark and be refused
until an operator intervenes.

`cluster::members` lists the membership as this node sees it, `cluster::down` downs a member by
address, e.g. for an operator who knows a node is gone for good, and `cluster::leave` leaves the
cluster. A process whose lifetime is one actor system's lifetime calls
`cluster::leave_on_terminated` in place of `ActorSystem::terminated`: waiting for the tree first is
what puts the departure behind every terminated signal the tree owes remote watchers.

[`examples/cluster.rs`](../tellus/examples/cluster.rs) runs all of this in four processes: joining
through one seed address, converging on a member list nothing was configured with, a killed node
downed by every survivor into a synthesized terminated signal, and, at the end, the survivors
leaving, which shows both ways out of a cluster in one run.

## The wire model

All frames from one node towards another ride one lane: a set of outbound queues drained by one
connection. Sending enqueues synchronously at `tell` time; the receiving endpoint injects into
local mailboxes in arrival order. The queues are unbounded underneath with a reservation counter
in front, exactly like a local mailbox. One counter serves the whole lane, so `outbound_capacity`
means the same thing however many queues share it. Ordinary messages and replies are subject to
the capacity; system frames (watch registration, terminated signals, reply-dropped notifications,
gossip) bypass it but ride the same queues, since a terminated signal must never be dropped and
must never overtake.

A lane is not one queue but a *control* queue plus a bounded pool of *data* queues, one per stream
of the connection, `max_streams_per_peer` of them at most. A frame delivered to an actor picks its
stream by hashing that actor's ID; every other frame rides the control stream. So FIFO holds per
recipient rather than per node, and a large message only delays frames towards recipients hashing
onto the same stream. The mapping never travels the wire: the receiving side dispatches by the
target named in the frame, so only the sender has to agree with itself.

That the terminated signal names its *watcher* is what carries the ordering guarantee across this
split. A `Terminated` frame hashes onto the watcher's stream, the same one the messages the dying
actor sent that watcher ride, so the shared queue orders them exactly as one lane used to. It
costs one frame per watcher rather than one per node. A `Reply` frame names the actor it is
*delivered to* for the same reason: it rides that actor's stream, behind whatever the responder
told it before replying. A reply no actor awaits, the answer to an `ask`, names none and keys on
its nonce instead, which spreads such replies over the data streams rather than putting a user
payload on the control stream.

How many data streams a lane gets is a property of the transport, not an assumption: `Transport`
reports it, QUIC offers as many as configured, and a transport without streams reports zero. At
zero every frame rides the control stream, which is one ordered lane per peer carrying everything.
The guarantees hold there by the same argument rather than as a special case, which keeps the
abstraction implementable over a stream-less transport such as TCP.

All data streams are opened when the connection is established, not on first use. A peer admitting
fewer concurrent streams than this node opens then fails the connection at setup, into the
ordinary reconnect path. Opened on demand, a stream that was never granted would instead stall
whichever queue happened to hash onto it, silently and forever, since these streams live as long
as the connection.

A lost connection is reconnected with exponential backoff; frames queued while the link is down
are delivered after the reconnect, in order. There is no replay of frames already handed to a dead
connection: delivery stays at-most-once, per-sender FIFO becomes "in order, with gaps". An address
no Up member advertises is given up after `max_connect_attempts` failed attempts, turning its
queued frames into dead letters; a member's address is retried until the downing provider settles
its fate. Giving up is not final: a later message dials again. A connection's reader is aborted
before the next one is dialed, so two readers for one peer can never interleave a frame buffered
on the dead connection behind a frame from the new one.

A lane belongs to an address but serves one incarnation: the handshake names the peer it is
connected to, and from then on a frame addressed to any other incarnation at that address is a
local dead letter rather than traffic written onto its successor's connection. A reference which
outlived the node it names hence fails fast, close to the sender, instead of being dropped as an
unknown target on the far side. Frames already sitting in the lane's queues when a reconnect's
handshake reveals the successor are the exception: they drain onto the new connection and die on
the far side as unknown targets, at-most-once either way.

## Guarantees

- **The tell contract extends verbatim.** Fire-and-forget, at-most-once; an unreachable node, a
  full outbound queue, a message encoding beyond `max_frame_size` and an undecodable payload all
  become logged dead letters. Undecodable includes a payload whose embedded reference names an
  actor of the receiving node which has already terminated: the whole message is the dead letter
  then, unlike a local tell to a terminated actor, which costs only itself.
- **Per-sender FIFO holds** for messages from one sender to one target, "with gaps" across
  reconnects as above.
- **Remote death watch has two tiers**, indistinguishable in the API:
  - A *real termination*, delivered over the wire, keeps the full local contract: the
    terminated signal arrives behind all messages the terminated actor delivered to the
    watcher, and it proves the actor's destructors have run. This is because the wire watcher
    fires inside the target's local termination sequence and its `Terminated` frame rides the
    same queue as the messages the actor sent that watcher before, the one their shared
    recipient hashes onto.
  - A *synthesized signal*, flushed when the watched actor's member is downed, proves none of
    that: the actor may be alive across a network partition and its destructors may never run.
    It guarantees exactly one thing, made true by construction rather than observation:
    **after the signal, no message from that actor is ever delivered through this endpoint
    again.** The node death sequence marks the incarnation Down and stops all delivery from it
    before the signals are flushed, and a handshake from a Down incarnation is refused.
    Distinguishing a crashed node from an unreachable one is impossible in an asynchronous
    network; this weakening is fundamental, not an implementation gap.
  - A *clean leave* keeps the real tier as the normal case rather than as a promise:
    `cluster::leave_on_terminated` announces the departure only once the local tree has
    terminated, so every `Terminated` frame it owes is queued first and the drain waits for
    those queues too. The announcement rides the control stream while the signals ride their
    watchers' data streams, though, and those are not ordered against each other, so a peer may
    process the departure first and synthesize instead. Exactly once survives either way: the
    node death sequence takes the watcher entries, so a real signal arriving second finds none
    and fires nothing.
- **Watching a dead incarnation signals immediately.** A watch on a node already Down fires the
  synthesized signal right away. A node merely not known yet does not: gossip converges per hop,
  and sending to such a node works, so firing on it would break the one promise the synthesized
  signal makes. A node which never becomes a member is answered instead when its lane is given up,
  together with the asks that lane fails.
- **Unwatch stays absolute.** It is enforced on the watcher's side in the run loop, which does not
  know or care whether a signal came from the local or a remote actor.
- **Watching is race-free.** A `Watch` for an already terminated (or never bound) actor is
  answered with an immediate terminated signal by the watched node, mirroring the local atomic
  registration close.
- **Request-response crosses nodes through the same API.** Serializing a `ReplyTo` moves the reply
  destination into a nonce-keyed pending table on its origin node; the receiving node gets a proxy
  whose reply rides a `Reply` frame back to the origin, and dropping the proxy without a reply
  sends a `ReplyDropped` frame instead. An ask still resolves exactly once, at latest at its
  timeout, and a `reply_to` reply stays FIFO with the responder's other messages to the asker,
  since the `Reply` frame names that actor and hence rides its stream. The `NoReply` detection
  weakens to best-effort.

  `ReplyDropped` is fire-and-forget: one lost with its connection resolves the ask by its timeout,
  since a reply is not idempotent and nothing reply-related is replayed on a reconnect. An ask
  cancelled before that releases its entry itself, so a lost notification costs it nothing.
  Downing a member fails every pending ask towards it as `NoReply`, after the tombstone and
  quiesce, so such a `NoReply` is never followed by its reply. Every member is heartbeated, so a
  vanished node's asks are settled by the downing provider, watched or not. Giving up a stale
  lane, one towards an address no Up member advertises anymore, fails the asks stamped with its
  peers the same way. If the transport reports the loss too late, the ask timeout still resolves
  the request.

  A message frame names its payload's reply destinations next to the payload, so a request the
  receiving node dead-letters undecoded, e.g. towards a meanwhile terminated actor, is answered
  with `ReplyDropped` for each of them: such an ask resolves as `NoReply` rather than by its
  timeout. A request this node itself drops, one whose framed size exceeds `max_frame_size` or one
  lost with its connection, releases the same destinations locally, since such a frame never
  reaches its peer as a whole. Only the peer a request was sent to settles it. A pending entry is
  stamped with that peer before its frame is queued, and a `Reply` or `ReplyDropped` frame naming
  a nonce stamped with anybody else is dropped as a dead letter, as is a `LookupReply` from a
  member the lookup did not go to. Nonces are unique to this node, not to the member naming one.

  An ask which ends without its reply, by its timeout or by its future being dropped, releases its
  own reply destination, the one no actor awaits, so an ask which nothing answers costs its entry
  only until its timeout, not until the node dies. A destination the request carried for an actor
  is that actor's to wait for and stays. An actor-origin request has no timeout at all, so its
  entry is instead bounded by the life of the actor awaiting the reply: that actor's termination
  evicts it, through the same watcher mechanism the reference registry evicts routes with. A
  `ReplyTo` may be forwarded to a third node and each hop chains its reply through the previous
  one; the downing eviction covers each hop's next node only, the timeout covers the rest.

## Discovery

The first reference cannot travel inside a message, so it is resolved by name instead:
`cluster::register(&Key::new("worker-pool"), actor_ref)` names an actor of this node, and
`cluster::lookup(&Key::new("worker-pool"), addr).await` resolves that name at the Up member
advertising `addr`. A lookup messages a member like any other frame would; joining is the one door
into the cluster, so an address which is not a member's answers `NotAMember` locally.

A `Key<M>` carries the message type next to the name, and the type travels as the name its
compiler spells it, so a key naming the wrong type is refused as `TypeMismatch` rather than
resolved into a reference which drops every message told to it. That comparison assumes both nodes
are built from the same source, which is the assumption the wire format already makes.

Discovery has these properties:

- **It is a point query, not a directory.** A lookup names one address; nothing but the member
  list is gossiped, and a node knows only what it registered itself. Resolution composes with
  whatever names addresses already, DNS or an orchestrator, rather than duplicating it.
- **`NotAMember` and `NotFound` are ordinary bootstrap answers**: the former while the node at the
  address has not joined yet, the latter while it has joined but not registered anything. There is
  no internal timeout: callers wrap a lookup in `tokio::time::timeout` and retry both.
- **More than one actor may hold one key**, and a point lookup answers with one of them. The
  registry hence already behaves like a receptionist: a cluster-wide directory could be built on
  top of it without changing the key type.
- **A registration lives as long as the actor.** Naming an actor binds it exactly as serializing a
  reference to it does, and the same eviction drops both once it terminates.
- **The answer names an incarnation**, since the reply carries the responder's node identity, so a
  resolved reference is the same kind of reference as one which arrived inside a message and stops
  working when that node is replaced.

Nothing here assumes a name lives on exactly one node, and node identity stays out of `Key`, so a
cluster-wide lookup or a listing subscription is an addition rather than a change.

## Failure detection and node death

Every Up member is heartbeated: the periodic gossip is the heartbeat, and every inbound frame
counts as one. A pluggable `FailureDetector` per member turns silence into a local *unreachable*
mark, cleared again by the next heartbeat. The default is the phi accrual detector, which learns
each member's inter-arrival distribution and turns one threshold into a per-member deadline. The
deadline detector remains the deterministic choice, e.g. for tests. Direct unreachability is
an observation, not member state: its versioned edge is relayed so every connected member can
derive the same reachability graph.

Downing is a separate decision, made by the pluggable `DowningProvider`, polled every tick with
the members outside this node's connected component, passed as `Disconnected` rather than as a
plain unreachability mark. An edge is absent if either endpoint reports the other unreachable; an
absent direct edge does not split a component while an alternate path exists. A member is hence
downed no earlier than every remaining member has reported it unreachable and that statement has
been relayed here: the time to down is the slowest detector plus one flood, not this node's own
detector. Its verdict is either the members to down or a *self-down*, which is what lets a
provider resolve a partition instead of only observing one.
Downing runs the node death sequence: mark Down (the tombstone), close the lane, wait out
in-flight deliveries, fail the pending asks, flush the synthesized signals. A new incarnation
joining at a member's address downs the old incarnation the same way, as does `cluster::down`, and
so does a member's announced departure; a self-down severs every connection instead, exactly as an
incarnation which learns through gossip that it was downed elsewhere.

The default is `KeepMajority` with a ten second deadline: once every component-unreachable member
has been unreachable that long, the side seeing a strict majority of the Up members downs the
others, any other side downs itself, and an even split goes to the side holding the member with
the lowest address. Exactly one side of a partition hence survives, provided the member lists
agreed before it. Two sides which never gossiped a new member to each other can both count
themselves the majority; deciding from gossiped membership rather than from an external arbiter
cannot rule that out. `DownAfterDeadline` is the unilateral alternative, downing whatever is
unreachable past a deadline: each side of a partition downs the other and one cluster becomes two,
each honestly satisfying its own synthesized-signal contract. It is a development and testing
choice, where downing on nothing but a deadline is what makes a node death reproducible.

What no provider removes is the fundamental part: a crashed node and an unreachable one are
indistinguishable, so a surviving side downs members which may well still be running, and a
minority keeps working until it notices and downs itself. Either way a downed node rejoins only as
a restarted process, and the synthesized signals stay true for the endpoint which flushed them,
which is exactly what the weaker tier of the watch contract promises.

The watch protocol is self-healing without any redelivery machinery: `Watch` frames are re-sent
after every reconnect and re-asserted every `watch_refresh_interval` (registration is idempotent
on the watched node). A `Watch` or `Terminated` frame lost with a broken connection is hence
compensated on the next connection or the next refresh. A re-sent `Watch` for a meanwhile
terminated actor is answered with `Terminated` right away. The refresh covers the loss a reconnect
cannot see: a `Terminated` frame lost with the *watched* side's connection while the watcher's own
lane never broke. The healed answer still reflects a real termination, delayed by at most the
refresh interval, which stays below the failure detection deadline, so a quiet pair heals by
answer rather than by a false node death. The downing provider covers the remaining case of a node
that never comes back.

## Trust

TLS is mandatory for QUIC, but server-only TLS authenticates just the dialed side: any client able
to reach the port can complete a handshake. Production deployments hence use mutual TLS via
`QuicTransport::mutual_tls`: every node presents one certificate as both its server and its client
identity, verified against the cluster's certificate authority, so a stranger cannot complete a
connection and reaches neither the protocol nor the membership. Issuing and renewing the
certificates is established automation, e.g. cert-manager or SPIFFE/SPIRE on Kubernetes.

An authenticated node is additionally bound to the address it advertises: where the transport
proves a peer identity, the admission requires the advertised IP among the certificate's IP
address subject alternative names, in both directions, and drops a mismatch without an answer. An
authenticated-but-lying node can hence not claim another member's address and have the healthy
node there downed. This has one consequence for issuance: member certificates must carry the
node's advertised IP as an iPAddress SAN, since DNS names are not resolved and hence not matched.
Only mutual TLS closes the gap, because without a client certificate the accepting side sees no
identity to check.
`QuicTransport::dev` mints its certificate with the bind IP, so the check holds in development
too, unverified as everything under that transport. One boundary remains documented rather than
closed: certificates are read once at startup, so a renewal takes a process restart until hot
rotation via quinn's server config swap is added.

## Limitations

- One remoting endpoint per process; node identity is address based.
- One connection per direction between a pair of nodes, each dialed on first use. Head-of-line
  blocking is bounded rather than gone: recipients hashing onto the same stream still delay each
  other, and a pool of `max_streams_per_peer` streams cannot separate an unbounded number of
  recipients.
- Downing decides from gossiped membership and versioned reachability, not from an external
  arbiter: the default `KeepMajority` resolves a partition towards one side, but two sides whose
  member lists disagreed before the split can both count themselves the majority, and
  `DownAfterDeadline` splits the cluster by design. Downed members must restart to rejoin; the
  split-brain problem itself is fundamental.
- Gossip pushes the full member list to every member every heartbeat interval: O(n^2) frames and
  O(n^3) copied entries per second across the cluster, fine for the cluster sizes this targets, not
  for hundreds of nodes.
- Deriving death from connectivity leaves a broken link between two members which both stay in the
  component unresolved: nothing is downed, and every message between exactly those two fails for
  as long as the link is broken. That is deliberate, since the alternative is letting one endpoint
  of a working cluster manufacture the other's tombstone, but it is a degraded state no provider
  currently ends. It shows up as a `member directly unreachable` warning which never turns into a
  downing.
- A leave is announced, not confirmed: the drain proves the departure left this node's queues, not
  that a peer received it. A member which is unreachable at that moment still exits through
  relayed gossip or, with no live path at all, through failure detection and downing. A departure
  lost in the last milliseconds before the sever costs the fast path rather than correctness. A
  leave can also overtake the terminated signals it was queued behind, which costs a watcher the
  real tier for the synthesized one.
- Bootstrap decides from what discovery shows: nodes acting on disjoint discovery views can form
  separate clusters, which never merge on their own; the settle window, the `min_peers` floor and
  the formation provider's majority shrink that window, they do not close it. A discovery which
  answers partially counts here too: a view missing an address throughout the settle window is
  what the majority is computed against. Formation fails closed, which costs liveness: while the
  lowest address of the universe does not answer, nothing forms, and a discovery which keeps
  resolving nodes that are gone never lets a cold start finish.
- A join attempt which sent its `Join` handshake and got no answer at all leaves an ambiguity: the
  peer may have admitted this node before failing to reply, and nothing local records it, since a
  cluster is pinned only once its answer has been validated. The join permit covers the attempt
  itself, so nothing forms while it runs, but a later formation can still race that far side's
  failure detection. Pinning on the ambiguity instead would wedge bootstrap behind any slow seed,
  which costs more than the ambiguity does.
- Detailed Down entries are remembered for `down_retention`; their greatest incarnation per
  address remains as a watermark which refuses zombie processes while admitting a newer restart.
  Watermarks are sent on connection setup rather than in the heartbeat and capped by
  `down_watermarks`, so an address evicted from a long churning cluster falls back to the
  pre-watermark behavior.
- The identity binding matches IP address SANs only; a deployment whose certificates carry nothing
  but DNS names must issue them with the advertised IPs added.
- An address which answered without speaking this protocol is not dialed again, so a
  misconfiguration costs one round of attempts rather than one per message. A tellus node coming
  up there recovers by dialing this node, whose successful handshake lifts the refusal; a tellus
  node which never dials back stays unreachable from here.
