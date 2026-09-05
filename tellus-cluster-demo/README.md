# tellus-cluster-demo

A five node tellus cluster which runs forever under continuous chaos, on Docker Compose or on
Kubernetes.

The nodes find each other through seed discovery, DNS or the Kubernetes API, a chaos agent
crashes one node or partitions two nodes off the other three every cycle, a load balancer in
front of the nodes stays available throughout, and a verifier holds the cluster to its promises
once every quiet window. Both stacks run the same two binaries and the same verifier, and prove
the same things; what differs is who starts the containers and how a fault is injected.

## Running it

Docker Compose, with DNS discovery:

```sh
just cluster-demo-up      # build the image and start everything
just cluster-demo-logs    # follow all logs, or `just cluster-demo-logs node1 chaos`
just cluster-demo-status  # one node's view through the balancer, plus the verifier's counters
just cluster-demo-down    # stop everything and drop the volumes
```

Kubernetes, in a [kind](https://kind.sigs.k8s.io) cluster of its own, with either discovery:

```sh
just cluster-demo-k8s-up        # dns discovery; `just cluster-demo-k8s-up k8s` for the other
just cluster-demo-k8s-logs      # follow all logs, or `just cluster-demo-k8s-logs tellus-2`
just cluster-demo-k8s-status    # the same view and counters as above
just cluster-demo-k8s-down      # delete the cluster
```

It needs `kind` v0.33 or later and `kubectl`, and it serves the same URLs on the same ports, so
everything below applies to both stacks. Only one of them can run at a time, since they share
those ports.

- `http://localhost:8080/cluster`: what one node, whichever the load balancer picks, sees of the
  cluster. Repeated calls during a partition show the two sides disagreeing, and then agreeing
  again.
- `http://localhost:8080/probe`: what that node's messaging to every other member yields, a
  discovery lookup plus an ask per member. Membership converging while asks fail would be a bug
  the member list alone hides.
- `http://localhost:8091/cluster` through `http://localhost:8095/cluster`: the same, but one
  named node rather than whichever the load balancer picks, which you want while the cluster is
  split.
- `http://localhost:8081/status`: the verifier's counters, including the load balancer's
  availability and the current chaos action.
- `http://localhost:8081/violations`: what the cluster failed to deliver, empty on a healthy run.

## The parts

Under Docker Compose:

- **The nodes** (`src/bin/node.rs`), five containers with static addresses on the `cluster`
  network, all sharing the network alias `tellus`, so Docker's DNS answers that one name with all
  five addresses. The `seeds` section names that one name, `DnsSeeds` resolves it and
  `cluster::bootstrap` joins through it, so nothing but the name is configured. Each node is on a
  second network, `edge`, carrying nothing but HTTP: a partitioned node stays reachable for the load
  balancer and can report that it sees everyone else as unreachable and has downed itself.
- **The load balancer** (`Caddyfile`), health checking all five nodes every two seconds, so a
  crashed or self-downed node leaves the rotation before it stops answering.
- **The chaos agent** (`chaos.sh`), rotating through three faults: `pumba kill` for a crash,
  `pumba netem loss --percent 100` in both directions between the two sides for a partition, and
  a SIGTERM for a clean departure, which the cluster handles through `leave` rather than through
  failure detection. A container signalled through the Docker API counts as manually stopped, so
  no restart policy brings it back and the agent starts it again itself. It names the fault it is
  running in a state file, which lets the verifier tell a legitimate disagreement from a
  violation.
- **The verifier** (`src/bin/verifier.rs`), polling the load balancer twice a second, and once
  per quiet window checking that every node is joined, that every node sees all five as Up, and
  that every node can message every other one.

On Kubernetes the nodes are a StatefulSet of five behind a headless service, the load balancer is
a service rather than Caddy, a partition is a pair of NetworkPolicies the agent switches on by
labelling pods, and the agent shares a pod with the verifier so the two still meet over a file.
[`k8s/README.md`](k8s/README.md) explains each of those, including why the nodes are a
StatefulSet when a Deployment is the natural default. The verifier is the same binary with the
same configuration, and is unaware of which stack it runs on.

## What it shows

- **Bootstrap**: five nodes starting at once resolve one DNS name to each other, none of them
  admits anyone until one forms, the lowest address does, and the rest join it. A restarted node
  rejoins through the same name.
- **Departure versus death**: a node stopped with SIGTERM announces its departure and is downed
  within a gossip round, while a killed one has to be detected. Each rotation hence shows both
  ways out of a cluster next to each other.
- **Crash detection**: a killed node's silence is noticed by the survivors' failure detectors and
  turned into a death by their downing providers, each on its own clock.
- **Partition resolution**: the majority downs the minority, and the minority, which cannot know
  it is the minority until it counts, downs itself. `KeepMajority` is what makes the two sides
  agree without talking to each other.
- **Restart to rejoin**: a downed node is dead for good, so the node process exits on its own,
  and the restart policy brings it back with a fresh incarnation the cluster admits.
- **Availability under all of it**: the load balancer answers throughout, which is the point of a
  cluster whose parts keep failing.

## What it found

The first soak run turned up a failure mode the tests did not cover: **a partition left two
clusters behind, permanently**.

The minority side self-downed, as `KeepMajority` promises. Its nodes then exited and restarted, as
a downed node must, and bootstrapped again while the partition was still up. Discovery is
unaffected by a partition, so all five addresses resolved and `min_peers` was satisfied, and
`join` took the first *reachable* seed, which was the node's own partition mate. The minority came
back as a cluster of its own, and when the partition healed the two did not merge, because nothing
merges clusters.

```
16:56:42  node1, node2  self down, decided by the downing provider
16:56:48  node1         joined the cluster        # each other's, not the majority's
16:57:41  partition healed
17:00     node1, node2 see 2 members; node3, node4, node5 see 3
```

`min_peers` did not prevent it: it counts resolved addresses, which the partition does not touch,
not the members of the cluster actually joined. Whether one partition ended this way was a race on
seed order, so the split accumulated over a run: after three partitions the five nodes here sat in
two live clusters, `{11, 12, 15}` and `{13, 14}`.

It is fixed. A started endpoint is no longer a cluster: it admits nobody until it has formed one
or joined one, and forming is a decision taken by a `FormationProvider` over everything discovery
resolved rather than a side effect of seed order. The same partition now leaves the minority
bootstrapping until the heal, after which it rejoins the majority. The verifier keeps checking for
it by name, since that is the failure no single node's view shows.

## Configuration

[`config/default.yaml`](config/default.yaml) carries everything the five nodes share, loaded with
[configured](https://github.com/hseeberger/configured) into tellus's own `BootstrapConfig` and
`EndpointConfig`. The compose file and the manifests add only what differs per node, as the
`CFG__NODE_NAME` and `CFG__ENDPOINT__ADVERTISED_ADDR` overrides configured layers on top. An
override is spelled `CFG__<SECTION>__<KEY>`, with double underscores between segments.

Which discovery a node uses is the one thing the default file does not carry. `CONFIG_OVERLAYS`
names [`config/dns.yaml`](config/dns.yaml) or [`config/k8s.yaml`](config/k8s.yaml), and that
overlay contributes the whole `seeds` section: overlays merge key by key, so a `seeds` in the
default file would leave a node configured for both at once, which deserializes into neither. A
node started without the variable does not start at all, which is the right answer to a node
which was never told how to find the others.

The relevant settings are:

- `bootstrap.min_peers` (5): how many addresses discovery must resolve, unchanged for the settle
  window, before a node joins. Sizing it to the deployment keeps five simultaneously starting
  nodes from forming two clusters which never merge. In exchange, no node can bootstrap while
  fewer than five addresses resolve, so a restarting node waits until all five containers are
  running again. That suits a fixed size cluster and not an elastic one.
- `QUIET_SECS`, `PARTITION_SECS`, `RECOVERY_SECS`, `DEAD_SECS` under Compose only, (chaos) and
  `TELLUS_SETTLE_SECS` (verifier): the verifier only checks after the chaos agent has been quiet
  for the settle window, so the settle window must stay below the quiet window, and the quiet
  window must be long enough for a restarted node to have rejoined.
- The `endpoint` section spells out the pluggable provider defaults, the adaptive phi accrual
  detector and `KeepMajority` after ten seconds of unreachability, so what the demo shows is what
  a deployment gets, visibly rather than implied. Every other `EndpointConfig` field keeps its
  default implicitly.

## Caveats

- The nodes wrap the transport in a two second connect timeout. QUIC gives up on a silent address
  only after its own handshake timeout, tens of seconds, and a partition leaves three of the four
  seeds silent, so an unbounded bootstrap round would outlast the fault it is meant to survive.
- The nodes use `QuicTransport::dev`, which does not verify certificates. That lets the demo run
  without a certificate authority; a deployment uses `QuicTransport::mutual_tls`, which
  additionally binds each peer's advertised address to its certificate's IP SANs.
- Both the nodes and the containers pumba touches need `NET_ADMIN` and `iproute2`, since pumba
  installs its tc rules inside the target container. Compose only.
- A node self-downing mid-partition exits, so its container restarts without pumba's rules, which
  ends that node's side of the partition early. The fault is a container level one, and the
  demo's timings are sized so the interesting part has happened by then. Compose only: on
  Kubernetes the fault is a label on the pod, which a restarting container keeps.
- The kind cluster lowers the kubelet's restart backoff to two seconds. A node downed by the
  cluster exits on purpose, which the kubelet cannot tell from a crash loop, and its default
  backoff of up to five minutes would stall a demo whose point is continuous chaos. A real
  deployment keeps the default, so a self-downed node there is away for longer.
- The Kubernetes partition cuts with a NetworkPolicy, which drops what crosses rather than losing
  it in transit the way pumba's packet loss does. It is a cleaner fault than the network gives
  you, and it is why the Compose stack keeps pumba rather than being retired. A policy also only
  applies to connections which are new when it arrives, so the fault needs a privileged helper to
  forget the cluster's existing flows before it cuts anything at all; `k8s/README.md` explains
  that, and it is the most surprising thing in this demo.
- The Kubernetes crash fault has no fixed dead window. Deleting a pod means the StatefulSet
  replaces it at once, and the replacement usually comes up on a different address, so the
  crashed member is gone for good and has to be detected before the cluster can converge. Compose
  needs its `DEAD_SECS` only because a container comes back at the same static address.
