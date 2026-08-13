//! Clustered remoting integration: a client node in this process runs scenarios against server
//! nodes in child processes. The scenarios prove reference serialization, discovery by name and
//! address, replies through `reply_to`, per-sender FIFO across the wire and per-target streams (a
//! bulk message towards one actor does not delay messages towards others). They also prove the
//! remote death watch contract: a real termination signals behind all delivered messages, every
//! watcher of one actor gets its own signal, watching an already terminated actor signals
//! immediately, unwatch suppresses the signal, and a killed node yields a synthesized signal via
//! failure detection.
//!
//! The reconnect path is covered by an oversize message dead-lettering locally while its lane
//! stays usable, by a mid-stream sever proving per-sender FIFO stays "in order, with gaps" over
//! the reconnected lane, by a terminated frame dropped on the watched node being healed by the
//! watch refresh, and by a node killed under a watch and restarted at its old address. In that
//! last case the tombstone kills the old incarnation, not the address, and discovery plus FIFO
//! work against the new one.
//!
//! Request-response crosses nodes through the serializable `ReplyTo`. A remote `ask` resolves
//! with the reply, a reply stays FIFO with the responder's other messages to the asker, a
//! forwarded `ReplyTo` chains its reply over two hops, and a `ReplyTo` serialized and resolved on
//! its own node comes home (and refuses a second serialization). An ask resolves as `NoReply`
//! rather than by timeout when the responder drops its `ReplyTo` and when the reply is oversize.
//! A request targeting a terminated remote actor resolves as `NoReply` too, through the frame's
//! reply tags. A request beyond `max_frame_size` fails its ask at the send, likewise instead of by
//! timeout. Killing a node holding a `ReplyTo` fails the pending ask as `NoReply` via failure
//! detection, and killing an unwatched node holding a `ReplyTo` fails the pending ask as `NoReply`
//! once the member is downed.
//!
//! Every spawned node joins the cluster through the client as its seed; scenarios hence also
//! exercise the join handshake, the member snapshot and the supersession of a restarted
//! incarnation on every bootstrap.

use anyhow::{Context, bail};
use derive_more::{Deref, DerefMut};
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    convert::Infallible,
    env, fs,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, UdpSocket},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorContext, ActorId, ActorRef, ActorSystem, AskError, Control, Incoming, ReplyTo,
    cluster::{
        self, BootstrapConfig, DownError, EndpointConfig, FixedSeeds, JoinError, Key, Member,
        MemberState,
        downing::{Disconnected, DownAfterDeadline, Downing, DowningProvider},
        failure::{Deadline, DeadlineFailureDetector},
        transport::{ConnectedControl, QuicConnection, QuicTransport, Transport, TransportError},
    },
};
use tokio::{
    runtime::Runtime,
    time::{sleep, timeout},
};

const ROLE_ENV: &str = "TELLUS_REMOTING_ROLE";
const ADDR_ENV: &str = "TELLUS_REMOTING_ADDR";
const SEEDS_ENV: &str = "TELLUS_REMOTING_SEEDS";
const BOOTSTRAP_ENV: &str = "TELLUS_REMOTING_BOOTSTRAP";
const CERT_ENV: &str = "TELLUS_REMOTING_CERT";
const KEY_ENV: &str = "TELLUS_REMOTING_KEY";
const ROOTS_ENV: &str = "TELLUS_REMOTING_ROOTS";
const REF_PREFIX: &str = "REF ";
const JOIN_PREFIX: &str = "JOIN ";
const MTLS_SERVER_NAME: &str = "tellus";
const MTLS_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const CONVERGENCE_POLL: Duration = Duration::from_millis(200);
const LEAVE_DEADLINE: Duration = Duration::from_secs(2);
const ECHO_KEY: &str = "echo";
const PINGS: u32 = 100;
const STREAMED: u32 = 50;
const WATCHERS: usize = 2;
const BULK_TARGETS: usize = 8;
const BULKS: u32 = 8;
const BULK_ACKNOWLEDGEMENTS: u32 = BULKS + BULK_TARGETS as u32 - 1;
const BULK_PAYLOAD: usize = 512 * 1_024;
const OVERSIZE_PAYLOAD: usize = 2 * 1_024 * 1_024;
const SEVER_BULKS: u32 = 300;
const SEVER_PAYLOAD: usize = 32 * 1_024;
const SEVER_DELAY: Duration = Duration::from_millis(10);
const MARKER_ATTEMPTS: usize = 10;
const MARKER_RETRY_DELAY: Duration = Duration::from_millis(500);
const RESTART_LOOKUP_ATTEMPTS: usize = 20;
const RESTART_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const RESTART_LOOKUP_DELAY: Duration = Duration::from_millis(250);
const ASKS: u32 = 10;
const FORWARD_SEQ: u32 = 11;
const ROUND_TRIP_SEQ: u32 = 7;
const TIMEOUT: Duration = Duration::from_secs(30);
const GIVE_UP_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const UNWATCH_GRACE: Duration = Duration::from_millis(500);
const DEAD_LETTER_GRACE: Duration = Duration::from_millis(300);
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const BENCH_PINGS: u32 = 50_000;
const BENCH_WINDOW: u32 = 4_096;
const BENCH_SERIAL: u32 = 2_000;
const BENCH_BULKS: u32 = 2_000;
const BENCH_BULK_WINDOW: u32 = 64;
const BENCH_BULK_PAYLOAD: usize = 64 * 1_024;

type Receiver = mpsc::Receiver<TestEvent>;

/// This node's advertised address, the seed every spawned node joins through: the scenarios run
/// against a star shaped cluster around the client, which gossip completes into a mesh.
static SELF_ADDR: OnceLock<SocketAddr> = OnceLock::new();

fn main() -> anyhow::Result<()> {
    match env::var(ROLE_ENV).as_deref() {
        Ok(role) if role == Role::Echo.as_str() => echo_node(),
        Ok(role) if role == Role::Keeper.as_str() => keeper_node(),
        Ok(role) if role == Role::Mutual.as_str() => mutual_node(),
        Ok(role) if role == Role::SelfDown.as_str() => self_down_node(),
        Ok(role) if role == Role::Leave.as_str() => leave_node(),
        Ok(role) if role == Role::Bench.as_str() => bench_client(),
        Ok(role) => bail!("unknown role {role}"),
        Err(_) => client(),
    }
}

/// The role a spawned copy of this process plays; the closed set keeps a spawn site and the
/// dispatch in `main` from drifting apart, where a typo would spawn a second client.
#[derive(Debug, Clone, Copy)]
enum Role {
    Echo,
    Keeper,
    Mutual,
    SelfDown,
    Leave,
    Bench,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Echo => "echo",
            Role::Keeper => "keeper",
            Role::Mutual => "mutual",
            Role::SelfDown => "self-down",
            Role::Leave => "leave",
            Role::Bench => "bench",
        }
    }
}

/// The echo node: replies to every ping, stops on `Stop`.
fn echo_node() -> anyhow::Result<()> {
    serve(EchoServer, ECHO_KEY)
}

/// The keeper node: spawns a fresh streamer child per `Spawn` and hands its reference out.
fn keeper_node() -> anyhow::Result<()> {
    serve(Keeper, "keeper")
}

/// The mutual TLS node, with certificates from the paths named by [CERT_ENV], [KEY_ENV] and
/// [ROOTS_ENV]: forms a cluster of one when started without seeds, staying up until killed; with
/// seeds it joins, prints the verdict and exits, so the client can assert which certificates
/// the cluster admits.
fn mutual_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let bind_addr = env::var(ADDR_ENV).context("bind address")?;
        let cert = fs::read(env::var(CERT_ENV).context("certificate path")?)?;
        let key = fs::read(env::var(KEY_ENV).context("key path")?)?;
        let ca = fs::read(env::var(ROOTS_ENV).context("roots path")?)?;

        let mut roots = RootCertStore::empty();
        roots.add(CertificateDer::from(ca))?;
        let transport = QuicTransport::mutual_tls(
            bind_addr.parse().context("bind address")?,
            vec![CertificateDer::from(cert)],
            PrivateKeyDer::Pkcs8(key.into()),
            roots,
            MTLS_SERVER_NAME,
        )?;
        let addr = transport.local_addr()?;
        cluster::start_endpoint(test_config(addr), transport)?;

        match env::var(SEEDS_ENV) {
            Ok(seeds) => {
                let seeds = seeds
                    .split(',')
                    .map(str::parse)
                    .collect::<Result<Vec<SocketAddr>, _>>()?;
                let verdict = match timeout(MTLS_JOIN_TIMEOUT, cluster::join(&seeds)).await {
                    Ok(Ok(())) => "ok",
                    Ok(Err(_)) => "refused",
                    Err(_) => "timeout",
                };
                println!("{JOIN_PREFIX}{verdict}");
                std::io::stdout().flush()?;
                Ok(())
            }

            Err(_) => {
                cluster::form().context("forming the cluster")?;
                println!("{JOIN_PREFIX}seed");
                std::io::stdout().flush()?;
                std::future::pending::<anyhow::Result<()>>().await
            }
        }
    })
}

/// The self downing node: it joins the cluster with a downing provider which gives way as soon as
/// it has a peer, then tries to rejoin until it is refused and prints that verdict, so the client
/// can assert what a provider's `SelfDown` does to the node deciding it.
fn self_down_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let transport = QuicTransport::dev("127.0.0.1:0".parse()?)?;
        let addr = transport.local_addr()?;
        let mut config = test_config(addr);
        config.downing_provider = Arc::new(|| Box::new(SelfDownWithPeers));
        cluster::start_endpoint(config, TimeoutTransport(transport))?;

        let seeds = parse_addrs(&env::var(SEEDS_ENV).context("seed addresses")?)?;
        timeout(TIMEOUT, cluster::join(&seeds))
            .await
            .context("joining the cluster")??;

        let downed = async {
            loop {
                match cluster::join(&seeds).await {
                    Ok(()) => sleep(CONVERGENCE_POLL).await,

                    Err(JoinError::Downed) => return anyhow::Ok(()),

                    Err(error) => return Err(error.into()),
                }
            }
        };
        timeout(TIMEOUT, downed)
            .await
            .context("self down within the timeout")??;

        println!("{JOIN_PREFIX}downed");
        std::io::stdout().flush()?;
        Ok(())
    })
}

/// Gives way as soon as this node has a peer, which is the verdict a quorum aware provider
/// reaches on the losing side of a partition; a partition itself is not stageable here.
struct SelfDownWithPeers;

impl DowningProvider for SelfDownWithPeers {
    fn down(&mut self, members: &[Member], _: Disconnected<'_>, _: Instant) -> Downing {
        if members.len() > 1 {
            Downing::SelfDown
        } else {
            Downing::Members(Vec::new())
        }
    }
}

/// The leaving node: an echo server which announces its departure once its actor system has
/// terminated, instead of exiting silently and leaving the cluster to detect the silence.
fn leave_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        start_endpoint().await?;

        let system = ActorSystem::new(EchoServer);
        let bytes = cluster::serialize_ref(system.root())?;
        println!("{REF_PREFIX}{}", hex_encode(&bytes));
        std::io::stdout().flush()?;

        timeout(TIMEOUT, cluster::leave_on_terminated(system))
            .await
            .context("leaving the cluster")??;
        Ok(())
    })
}

/// Serves its root both ways: registered under a key for discovery and printed as a serialized
/// reference, so the scenarios can bootstrap either way against the same node.
fn serve<A>(actor: A, name: &str) -> anyhow::Result<()>
where
    A: Actor + Send + 'static,
    A::Message: DeserializeOwned + Send + 'static,
    A::State: Send + 'static,
{
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        start_endpoint().await?;

        let system = ActorSystem::new(actor);
        cluster::register(&Key::new(name), system.root())?;
        let bytes = cluster::serialize_ref(system.root())?;
        println!("{REF_PREFIX}{}", hex_encode(&bytes));
        std::io::stdout().flush()?;

        timeout(TIMEOUT, system.terminated())
            .await
            .context("server actor system termination")??;
        Ok(())
    })
}

struct EchoServer;

impl Actor for EchoServer {
    type Message = Request;
    type State = Vec<ReplyTo<Reply>>;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(Vec::new())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Request::Ping { seq, reply_to }) => {
                reply_to.tell(Reply { seq });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Ask { seq, reply_to }) => {
                reply_to.reply(Reply { seq });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::AskThenTell {
                marker_to,
                reply_to,
            }) => {
                marker_to.tell(AskerMessage::Marker);
                reply_to.reply(Reply { seq: 0 });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Ignore { .. }) => Ok(Control::Continue(state)),

            Incoming::Message(Request::AskOversize { reply_to, .. }) => {
                reply_to.reply(Reply { seq: 0 });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::AskOversizeReply { reply_to }) => {
                reply_to.reply(BulkReply {
                    payload: vec![0; OVERSIZE_PAYLOAD],
                });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Hold { reply_to }) => {
                state.push(reply_to);
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Forward { to, reply_to }) => {
                to.tell(ForwardedRequest { reply_to });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Members { reply_to }) => {
                let own = *SELF_ADDR.get().expect("endpoint started");
                let up = cluster::members()
                    .expect("endpoint started")
                    .iter()
                    .filter(|member| member.state() == MemberState::Up)
                    .map(|member| member.addr())
                    .collect();
                reply_to.reply(MemberAddrs { own, up });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Request {
    Ping {
        seq: u32,
        reply_to: ActorRef<Reply>,
    },

    Ask {
        seq: u32,
        reply_to: ReplyTo<Reply>,
    },

    AskThenTell {
        marker_to: ActorRef<AskerMessage>,
        reply_to: ReplyTo<Reply>,
    },

    Ignore {
        reply_to: ReplyTo<Reply>,
    },

    AskOversize {
        payload: Vec<u8>,
        reply_to: ReplyTo<Reply>,
    },

    AskOversizeReply {
        reply_to: ReplyTo<BulkReply>,
    },

    Hold {
        reply_to: ReplyTo<Reply>,
    },

    Forward {
        to: ActorRef<ForwardedRequest>,
        reply_to: ReplyTo<Reply>,
    },

    Members {
        reply_to: ReplyTo<MemberAddrs>,
    },

    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
struct Reply {
    seq: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct BulkReply {
    payload: Vec<u8>,
}

/// A node's own address next to the Up members it sees, so the client can tell whose view it is
/// asserting on.
#[derive(Debug, Serialize, Deserialize)]
struct MemberAddrs {
    own: SocketAddr,
    up: Vec<SocketAddr>,
}

#[derive(Serialize, Deserialize)]
enum AskerMessage {
    Marker,
    Answer(Reply),
}

#[derive(Serialize, Deserialize)]
struct ForwardedRequest {
    reply_to: ReplyTo<Reply>,
}

struct Keeper;

impl Actor for Keeper {
    type Message = KeeperMessage;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(KeeperMessage::Spawn { reply_to }) => {
                let child = context.spawn(Streamer);
                reply_to.tell(ClientEvent::Child(child));
                Ok(Control::Continue(state))
            }

            Incoming::Message(KeeperMessage::DropTerminated { count, reply_to }) => {
                assert!(
                    cluster::drop_terminated_frames(count),
                    "endpoint not started"
                );
                reply_to.tell(ClientEvent::Armed);
                Ok(Control::Continue(state))
            }

            Incoming::Message(KeeperMessage::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum KeeperMessage {
    Spawn {
        reply_to: ActorRef<ClientEvent>,
    },

    DropTerminated {
        count: u64,
        reply_to: ActorRef<ClientEvent>,
    },

    Stop,
}

/// Streams `count` numbered messages to `reply_to` and stops on `Go`, so its terminated signal
/// must arrive behind all of them; acknowledges a `Bulk` with its sequence number and keeps
/// running, so a payload's size only ever delays the stream it rides.
struct Streamer;

impl Actor for Streamer {
    type Message = StreamerMessage;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(StreamerMessage::Go { count, reply_to }) => {
                for seq in 0..count {
                    reply_to.tell(ClientEvent::Streamed(seq));
                }
                Ok(Control::Stop)
            }

            Incoming::Message(StreamerMessage::Bulk { seq, reply_to, .. }) => {
                reply_to.tell(ClientEvent::Bulked(seq));
                Ok(Control::Continue(state))
            }

            Incoming::Message(StreamerMessage::Ask { reply_to }) => {
                reply_to.reply(Reply { seq: 0 });
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum StreamerMessage {
    Go {
        count: u32,
        reply_to: ActorRef<ClientEvent>,
    },

    Bulk {
        seq: u32,
        payload: Vec<u8>,
        reply_to: ActorRef<ClientEvent>,
    },

    Ask {
        reply_to: ReplyTo<Reply>,
    },
}

#[derive(Serialize, Deserialize)]
enum ClientEvent {
    Child(ActorRef<StreamerMessage>),
    Streamed(u32),
    Bulked(u32),
    Armed,
    Probe,
}

enum TestEvent {
    Watching,
    Bulking,
    Child(ActorRef<StreamerMessage>),
    StreamedAll,
    Done(Result<(), String>),
}

fn client() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    runtime.block_on(start_endpoint())?;

    echo_scenario(&runtime).context("echo scenario")?;
    discovery_scenario(&runtime).context("discovery scenario")?;

    let mut keeper_process = KillOnDrop(spawn_node(Role::Keeper)?);
    let keeper = resolve_ref::<KeeperMessage>(&mut keeper_process)?;

    ordered_termination_scenario(&runtime, &keeper).context("ordered termination scenario")?;
    watch_terminated_scenario(&runtime, &keeper).context("watch terminated scenario")?;
    unwatch_scenario(&runtime, &keeper).context("unwatch scenario")?;
    two_watchers_scenario(&runtime, &keeper).context("two watchers scenario")?;
    head_of_line_scenario(&runtime, &keeper).context("head of line scenario")?;
    oversize_scenario(&runtime, &keeper).context("oversize scenario")?;
    sever_scenario(&runtime, &keeper).context("sever scenario")?;
    lost_terminated_scenario(&runtime, &keeper).context("lost terminated scenario")?;
    dead_target_ask_scenario(&runtime, &keeper).context("dead target ask scenario")?;

    keeper.tell(KeeperMessage::Stop);
    expect_exit(&mut keeper_process, "keeper")?;

    let mut ask_echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let ask_echo = resolve_ref::<Request>(&mut ask_echo_process)?;

    remote_ask_scenario(&runtime, &ask_echo).context("remote ask scenario")?;
    reply_to_fifo_scenario(&runtime, &ask_echo).context("reply to fifo scenario")?;
    forwarded_reply_scenario(&runtime, &ask_echo).context("forwarded reply scenario")?;
    reply_serde_scenario(&runtime).context("reply serde scenario")?;

    ask_echo.tell(Request::Stop);
    expect_exit(&mut ask_echo_process, "ask echo")?;

    ask_node_death_scenario(&runtime).context("ask node death scenario")?;
    ask_down_scenario(&runtime).context("ask down scenario")?;

    node_death_scenario(&runtime).context("node death scenario")?;
    relayed_reachability_scenario(&runtime).context("relayed reachability scenario")?;
    restart_scenario(&runtime).context("restart scenario")?;

    join_convergence_scenario(&runtime).context("join convergence scenario")?;
    bootstrap_scenario(&runtime).context("bootstrap scenario")?;
    down_scenario(&runtime).context("down scenario")?;
    leave_scenario(&runtime).context("leave scenario")?;
    self_down_scenario().context("self down scenario")?;
    non_member_scenario(&runtime).context("non member scenario")?;
    identity_binding_scenario().context("identity binding scenario")?;

    Ok(())
}

/// Measures the remote hot paths against fresh echo and keeper nodes and prints the numbers:
/// windowed pipelined round trips, serial round trip latency and bulk payload throughput. Not a
/// regression gate; run via `just bench-cluster` and compare before and after a change.
fn bench_client() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    runtime.block_on(start_endpoint())?;

    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let pipelined = bench_echo(&runtime, &echo, BENCH_PINGS, BENCH_WINDOW)?;
    println!(
        "pipelined: {:.0} round trips/s ({BENCH_PINGS} pings, window {BENCH_WINDOW})",
        f64::from(BENCH_PINGS) / pipelined.as_secs_f64()
    );

    let serial = bench_echo(&runtime, &echo, BENCH_SERIAL, 1)?;
    println!(
        "serial: {:.1} us/round trip ({BENCH_SERIAL} pings)",
        serial.as_secs_f64() * 1e6 / f64::from(BENCH_SERIAL)
    );

    let mut keeper_process = KillOnDrop(spawn_node(Role::Keeper)?);
    let keeper = resolve_ref::<KeeperMessage>(&mut keeper_process)?;

    let bulk = bench_bulk(&runtime, &keeper)?;
    let mebibytes = f64::from(BENCH_BULKS) * BENCH_BULK_PAYLOAD as f64 / (1024.0 * 1024.0);
    println!(
        "bulk: {:.1} MiB/s ({BENCH_BULKS} bulks of {} KiB, window {BENCH_BULK_WINDOW})",
        mebibytes / bulk.as_secs_f64(),
        BENCH_BULK_PAYLOAD / 1024
    );

    echo.tell(Request::Stop);
    keeper.tell(KeeperMessage::Stop);
    expect_exit(&mut echo_process, "bench echo")?;
    expect_exit(&mut keeper_process, "bench keeper")?;
    Ok(())
}

fn bench_echo(
    runtime: &Runtime,
    echo: &ActorRef<Request>,
    total: u32,
    window: u32,
) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let event_rx = run_client(runtime, "bench echo client termination", |event_tx| {
        BenchEcho {
            server: echo.clone(),
            total,
            window,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;
    Ok(started.elapsed())
}

fn bench_bulk(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let event_rx = run_client(runtime, "bench bulk client termination", |event_tx| {
        BenchBulk {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;
    Ok(started.elapsed())
}

/// Round trip and per-sender FIFO: ordered pings, ordered replies. The reference bytes must
/// resolve as the type they were serialized for and be refused as any other. Then the node is
/// killed and told anyway, and a second node is served afterwards: only that further round trip
/// proves the dead letters left the endpoint usable, which the sends alone cannot show.
fn echo_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let bytes = ref_bytes(&mut echo_process)?;
    let echo = cluster::deserialize_ref::<Request>(&bytes).context("server reference")?;

    let mistyped = cluster::deserialize_ref::<Reply>(&bytes);
    if !matches!(mistyped, Err(cluster::RefError::TypeMismatch)) {
        bail!("reference of another message type resolved: {mistyped:?}");
    }

    let event_rx = run_client(runtime, "echo client termination", |event_tx| EchoClient {
        server: echo.clone(),
        event_tx,
    })?;
    expect_done(&event_rx)?;

    expect_exit(&mut echo_process, "echo")?;

    runtime.block_on(async {
        echo.tell(Request::Stop);
        sleep(DEAD_LETTER_GRACE).await;
        echo.tell(Request::Stop);
    });

    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let event_rx = run_client(
        runtime,
        "echo client termination after dead letters",
        |event_tx| EchoClient {
            server: echo,
            event_tx,
        },
    )?;
    expect_done(&event_rx)?;

    expect_exit(&mut echo_process, "echo")?;
    Ok(())
}

/// Discovery: a key registered on another node resolves into a working reference, given only that
/// node's address. The lookup starts before the node exists and retries `NotAMember` until the
/// node has joined, so bootstrap order does not matter, by retry rather than by a parked lookup.
/// A wrong name and a wrong message type are distinguished, since only the first is worth
/// retrying.
fn discovery_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut echo_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);

    let echo = lookup_echo_with_retries(runtime, addr).context("lookup before the node is up")?;

    let missing = runtime.block_on(async {
        let key = Key::<Request>::new("no-such-name");
        timeout(TIMEOUT, cluster::lookup(&key, addr)).await
    })?;
    if !matches!(missing, Err(cluster::LookupError::NotFound)) {
        bail!("unregistered name resolved to {missing:?}");
    }

    let mistyped = runtime.block_on(async {
        let key = Key::<Reply>::new(ECHO_KEY);
        timeout(TIMEOUT, cluster::lookup(&key, addr)).await
    })?;
    if !matches!(mistyped, Err(cluster::LookupError::TypeMismatch)) {
        bail!("key of another message type resolved to {mistyped:?}");
    }

    let event_rx = run_client(runtime, "discovered echo client termination", |event_tx| {
        EchoClient {
            server: echo,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;

    expect_exit(&mut echo_process, "discovered echo")?;
    Ok(())
}

/// A watched remote actor streams `STREAMED` messages and stops: the terminated signal must
/// arrive behind all of them, in order.
fn ordered_termination_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "ordered watch client termination", |event_tx| {
        OrderedWatch {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;

    let TestEvent::Child(_) = recv(&event_rx)? else {
        bail!("no child reference from the ordered watch client");
    };
    expect_done(&event_rx)
}

/// Watching an already terminated remote actor must still deliver the terminated signal, and
/// must do so via the watched node's immediate answer, since the node is alive and heartbeating.
/// The subject is terminated by this scenario itself, so the precondition is asserted where it is
/// relied upon rather than inherited from another scenario.
fn watch_terminated_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let dead_child = terminated_child(runtime, keeper).context("terminated child")?;

    let event_rx = run_client(runtime, "watch terminated client termination", |event_tx| {
        WatchTerminated {
            child: dead_child,
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// A reference to a remote actor which has provably terminated: it is only returned after its
/// terminated signal has been received.
fn terminated_child(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<ActorRef<StreamerMessage>> {
    let event_rx = run_client(runtime, "terminate child client termination", |event_tx| {
        TerminateChild {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;

    let TestEvent::Child(child) = recv(&event_rx)? else {
        bail!("no terminated child reference");
    };
    expect_done(&event_rx)?;
    Ok(child)
}

/// After unwatch no terminated signal may be received, even though the remote actor terminates.
fn unwatch_scenario(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<()> {
    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(UnwatchClient {
        keeper: keeper.clone(),
        event_tx,
    });

    match recv(&event_rx)? {
        TestEvent::StreamedAll => {}
        TestEvent::Done(result) => bail!("done before probe: {result:?}"),
        _ => bail!("unexpected event from the unwatch client"),
    }
    thread::sleep(UNWATCH_GRACE);
    system.root().tell(ClientEvent::Probe);

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("unwatch client termination")??;
    expect_done(&event_rx)
}

/// Two watchers on this node watching one remote actor must each receive a terminated signal:
/// the wire watch is per watcher, so one registration on the watched node owes two signals.
fn two_watchers_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "two watchers client termination", |event_tx| {
        TwoWatchers {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;

    for _ in 0..WATCHERS {
        expect_done(&event_rx)?;
    }
    Ok(())
}

/// A large message towards one remote actor must not delay messages towards others: the busy
/// target's stream is not the one they ride. Told first and by far the largest, the busy target's
/// acknowledgement would be the first to arrive over a single lane.
fn head_of_line_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "head of line client termination", |event_tx| {
        HeadOfLine {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// A message encoding beyond `max_frame_size` becomes a local dead letter instead of tearing down
/// the lane: an empty bulk told right behind it to the same target, riding the same stream, must
/// still be acknowledged, and the oversize one must never be.
fn oversize_scenario(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "oversize client termination", |event_tx| {
        Oversize {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// Severing every connection mid-stream loses frames but never reorders or duplicates them: the
/// acknowledgements arrive strictly ascending, i.e. "in order, with gaps", and a marker told
/// after the sever arrives over the reconnected lane, proving it survived with its queue. This
/// also exercises inbound reader supersession: the reconnected connection's reader takes over
/// from the severed one.
fn sever_scenario(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<()> {
    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(SeverFifo {
        keeper: keeper.clone(),
        event_tx,
    });

    match recv(&event_rx)? {
        TestEvent::Bulking => {}
        _ => bail!("unexpected event from the sever client"),
    }

    thread::sleep(SEVER_DELAY);
    if !cluster::sever_connections() {
        bail!("cannot sever, endpoint not started");
    }

    let mut done = None;
    for _ in 0..MARKER_ATTEMPTS {
        system.root().tell(ClientEvent::Probe);
        match event_rx.recv_timeout(MARKER_RETRY_DELAY) {
            Ok(TestEvent::Done(result)) => {
                done = Some(result);
                break;
            }

            Ok(_) => bail!("unexpected event from the sever client"),

            Err(_) => {}
        }
    }
    match done {
        Some(Ok(())) => {}
        Some(Err(message)) => bail!(message),
        None => bail!("no marker acknowledgement after the sever"),
    }

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("sever client termination")??;
    Ok(())
}

/// A terminated frame dropped on the watched node (fault injection) must still reach the watcher:
/// the periodic watch refresh re-asserts the watch, and the watched node answers a watch for a
/// meanwhile terminated actor with `Terminated`.
fn lost_terminated_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "lost terminated client termination", |event_tx| {
        LostTerminated {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// An ask towards a terminated remote actor is dead-lettered on the receiving node without being
/// decoded, so no proxy exists whose drop could answer; the reply tags riding the message frame
/// let that node answer nonetheless, resolving the ask as `NoReply` rather than by timeout.
fn dead_target_ask_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let dead_child = terminated_child(runtime, keeper).context("terminated child")?;

    let asked =
        runtime.block_on(dead_child.ask(TIMEOUT, |reply_to| StreamerMessage::Ask { reply_to }));
    if !matches!(asked, Err(AskError::NoReply)) {
        bail!("ask towards a terminated actor resolved to {asked:?}");
    }
    Ok(())
}

/// A remote `ask` resolves with the reply; a responder dropping its `ReplyTo` resolves the ask as
/// `NoReply` via the reply-dropped notification rather than by timeout; a request beyond
/// `max_frame_size` fails the ask at the send and an oversize reply resolves it as `NoReply` via
/// the reply-dropped notification, both instead of by timeout.
fn remote_ask_scenario(runtime: &Runtime, echo: &ActorRef<Request>) -> anyhow::Result<()> {
    for seq in 0..ASKS {
        let reply = runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Ask { seq, reply_to }))
            .context("ask")?;
        if reply.seq != seq {
            bail!("reply {} instead of {seq}", reply.seq);
        }
    }

    let ignored = runtime.block_on(echo.ask(TIMEOUT, |reply_to| Request::Ignore { reply_to }));
    if !matches!(ignored, Err(AskError::NoReply)) {
        bail!("ignored ask resolved to {ignored:?}");
    }

    let oversize = runtime.block_on(echo.ask(TIMEOUT, |reply_to| Request::AskOversize {
        payload: vec![0; OVERSIZE_PAYLOAD],
        reply_to,
    }));
    if !matches!(oversize, Err(AskError::TooLarge { .. })) {
        bail!("oversize ask resolved to {oversize:?}");
    }

    let oversize_reply =
        runtime.block_on(echo.ask(TIMEOUT, |reply_to| Request::AskOversizeReply { reply_to }));
    if !matches!(oversize_reply, Err(AskError::NoReply)) {
        bail!("oversize reply ask resolved to {oversize_reply:?}");
    }
    Ok(())
}

/// A reply created by `reply_to` stays FIFO with the responder's other messages to the asker: the
/// server tells a marker and then replies, so the marker must arrive first.
fn reply_to_fifo_scenario(runtime: &Runtime, echo: &ActorRef<Request>) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "ask then tell client termination", |event_tx| {
        AskThenTellClient {
            server: echo.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// A `ReplyTo` forwarded to a third actor still resolves its ask: the echo node re-serializes it
/// towards a responder on the client node, chaining the reply over two hops.
fn forwarded_reply_scenario(runtime: &Runtime, echo: &ActorRef<Request>) -> anyhow::Result<()> {
    let system = ActorSystem::new(Responder);
    let responder = system.root().clone();

    let reply = runtime
        .block_on(echo.ask(TIMEOUT, |reply_to| Request::Forward {
            to: responder,
            reply_to,
        }))
        .context("forwarded ask")?;
    if reply.seq != FORWARD_SEQ {
        bail!("forwarded reply {} instead of {FORWARD_SEQ}", reply.seq);
    }

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("responder termination")??;
    Ok(())
}

/// A `ReplyTo` serialized and resolved on its own node comes home as the original destination,
/// and a second serialization of the same value is refused.
fn reply_serde_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "serde round trip client termination", |event_tx| {
        SerdeRoundTrip { event_tx }
    })?;
    expect_done(&event_rx)
}

/// Killing a node holding a `ReplyTo` fails the pending ask as `NoReply` via failure detection,
/// next to the synthesized terminated signal, rather than leaving it to its timeout. A probe ask
/// behind the hold proves the hold arrived before the node is killed.
fn ask_node_death_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: echo.clone(),
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the death watch client"),
    }

    let (probe_tx, probe_rx) = mpsc::channel();
    let held = {
        let echo = echo.clone();
        runtime.spawn(async move {
            let hold = echo.ask(TIMEOUT, |reply_to| Request::Hold { reply_to });
            let probe = async {
                let probe = echo
                    .ask(TIMEOUT, |reply_to| Request::Ask { seq: 0, reply_to })
                    .await;
                let _ = probe_tx.send(probe);
            };
            let (hold, ()) = tokio::join!(hold, probe);
            hold
        })
    };

    let probe = probe_rx.recv_timeout(TIMEOUT).context("probe reply")?;
    if !matches!(probe, Ok(Reply { seq: 0 })) {
        bail!("probe ask resolved to {probe:?}");
    }

    echo_process.0.kill().context("killing the echo process")?;
    expect_done(&event_rx)?;

    let held = runtime.block_on(held).context("held ask task")?;
    if !matches!(held, Err(AskError::NoReply)) {
        bail!("held ask resolved to {held:?}");
    }

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("death watch client termination")??;
    Ok(())
}

/// Killing a node nothing watches still fails the pending ask as `NoReply`: every member is
/// heartbeated, so downing, not the ask's timeout, settles asks towards a dead member. The
/// connections are severed after the kill, since noticing the loss is the transport's business
/// and not what this scenario proves; a probe ask behind the hold proves the hold arrived before
/// the node is killed.
fn ask_down_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let (probe_tx, probe_rx) = mpsc::channel();
    let held = {
        let echo = echo.clone();
        runtime.spawn(async move {
            let hold = echo.ask(GIVE_UP_TIMEOUT, |reply_to| Request::Hold { reply_to });
            let probe = async {
                let probe = echo
                    .ask(TIMEOUT, |reply_to| Request::Ask { seq: 0, reply_to })
                    .await;
                let _ = probe_tx.send(probe);
            };
            let (hold, ()) = tokio::join!(hold, probe);
            hold
        })
    };

    let probe = probe_rx.recv_timeout(TIMEOUT).context("probe reply")?;
    if !matches!(probe, Ok(Reply { seq: 0 })) {
        bail!("probe ask resolved to {probe:?}");
    }

    echo_process.0.kill().context("killing the echo process")?;
    if !cluster::sever_connections() {
        bail!("cannot sever, endpoint not started");
    }

    let held = runtime.block_on(held).context("held ask task")?;
    if !matches!(held, Err(AskError::NoReply)) {
        bail!("held ask resolved to {held:?}");
    }
    Ok(())
}

/// Killing the watched actor's node must yield a synthesized terminated signal via failure
/// detection.
fn node_death_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut keeper_process = KillOnDrop(spawn_node(Role::Keeper)?);
    let keeper = resolve_ref::<KeeperMessage>(&mut keeper_process)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: keeper,
        event_tx,
    });

    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the node death client"),
    }
    keeper_process
        .0
        .kill()
        .context("killing the keeper process")?;

    expect_done(&event_rx)?;
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("node death client termination")??;
    Ok(())
}

/// Downing derives from the shared reachability graph, not from this node's own detector: in a
/// three node cluster the client alone cannot put the killed node outside its component, because
/// the surviving node is still a path to it. The synthesized signal hence only arrives once the
/// survivor's observation has been relayed here, and the survivor itself stays Up throughout.
fn relayed_reachability_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut witness_process = KillOnDrop(spawn_node(Role::Echo)?);
    let witness = resolve_ref::<Request>(&mut witness_process)?;
    let mut victim_process = KillOnDrop(spawn_node(Role::Echo)?);
    let victim = resolve_ref::<Request>(&mut victim_process)?;
    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;

    let deadline = Instant::now() + TIMEOUT;
    let (witness_addr, victim_addr) = loop {
        let witness_view = runtime
            .block_on(witness.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("witness members ask")?;
        let victim_view = runtime
            .block_on(victim.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("victim members ask")?;

        let all = [self_addr, witness_view.own, victim_view.own];
        let converged =
            |view: &MemberAddrs| all.iter().all(|member_addr| view.up.contains(member_addr));
        if converged(&witness_view) && converged(&victim_view) {
            break (witness_view.own, victim_view.own);
        }
        if Instant::now() >= deadline {
            bail!("members did not converge: {witness_view:?} versus {victim_view:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    };

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: victim,
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the relayed reachability client"),
    }
    victim_process
        .0
        .kill()
        .context("killing the victim process")?;

    expect_done(&event_rx)?;
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("relayed reachability client termination")??;

    let members = cluster::members().context("members")?;
    if !members
        .iter()
        .any(|member| member.addr() == victim_addr && member.state() == MemberState::Down)
    {
        bail!("the killed node is not Down in {members:?}");
    }
    if !members
        .iter()
        .any(|member| member.addr() == witness_addr && member.state() == MemberState::Up)
    {
        bail!("the surviving node is not Up in {members:?}");
    }

    witness.tell(Request::Stop);
    expect_exit(&mut witness_process, "relayed reachability witness")?;
    Ok(())
}

/// A node restarted at its old address is a new incarnation. The client first talks to the old
/// one, which binds the lane, then watches it and kills it. Failure detection then tombstones the
/// old incarnation and severs the lane. A retried lookup against the restarted node must resolve a
/// working reference, and full per-sender FIFO must hold on the fresh lane. That proves that a
/// tombstone kills an incarnation, never its address.
fn restart_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut echo_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let event_rx = run_client(runtime, "ping once client termination", |event_tx| {
        PingOnce {
            server: echo.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: echo,
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the death watch client"),
    }
    echo_process.0.kill().context("killing the echo process")?;
    expect_done(&event_rx)?;
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("death watch client termination")??;

    let mut restarted_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);

    let echo =
        lookup_echo_with_retries(runtime, addr).context("no reference from the restarted node")?;

    let event_rx = run_client(runtime, "echo client after restart", |event_tx| {
        EchoClient {
            server: echo,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;

    expect_exit(&mut restarted_process, "restarted echo")?;
    Ok(())
}

/// Two children seeded only on the client converge on the full member list: gossip carries each
/// child's join to the other, so every node, asked for its own view, names all three Up members
/// without ever having been configured with more than one address.
fn join_convergence_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut first_process = KillOnDrop(spawn_node(Role::Echo)?);
    let first = resolve_ref::<Request>(&mut first_process)?;
    let mut second_process = KillOnDrop(spawn_node(Role::Echo)?);
    let second = resolve_ref::<Request>(&mut second_process)?;
    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;

    let deadline = Instant::now() + TIMEOUT;
    let (first_view, second_view) = loop {
        let first_view = runtime
            .block_on(first.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("first members ask")?;
        let second_view = runtime
            .block_on(second.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("second members ask")?;

        let all = [self_addr, first_view.own, second_view.own];
        let converged =
            |view: &MemberAddrs| all.iter().all(|member_addr| view.up.contains(member_addr));
        if converged(&first_view) && converged(&second_view) {
            break (first_view, second_view);
        }
        if Instant::now() >= deadline {
            bail!("members did not converge: {first_view:?} versus {second_view:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    };

    let members = cluster::members().context("members")?;
    for child_addr in [first_view.own, second_view.own] {
        if !members
            .iter()
            .any(|member| member.addr() == child_addr && member.state() == MemberState::Up)
        {
            bail!("client does not see {child_addr} as Up in {members:?}");
        }
    }

    first.tell(Request::Stop);
    second.tell(Request::Stop);
    expect_exit(&mut first_process, "first convergence echo")?;
    expect_exit(&mut second_process, "second convergence echo")?;
    Ok(())
}

/// Every node bootstraps from the identical address list instead of being told whom to join:
/// each resolves the list, waits for it to settle and joins through the lowest address other
/// than its own, which keeps the join graph connected, so all three nodes end up in one cluster
/// whichever of them happens to hold the lowest address.
fn bootstrap_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;
    let first_addr = reserved_addr()?;
    let second_addr = reserved_addr()?;
    let seeds = vec![self_addr, first_addr, second_addr];

    let mut first_process = KillOnDrop(spawn_bootstrap_node(Role::Echo, first_addr, &seeds)?);
    let mut second_process = KillOnDrop(spawn_bootstrap_node(Role::Echo, second_addr, &seeds)?);
    runtime.block_on(bootstrap_cluster(seeds))?;

    let first = resolve_ref::<Request>(&mut first_process)?;
    let second = resolve_ref::<Request>(&mut second_process)?;

    let deadline = Instant::now() + TIMEOUT;
    loop {
        let first_view = runtime
            .block_on(first.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("first members ask")?;
        let second_view = runtime
            .block_on(second.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("second members ask")?;

        let all = [self_addr, first_addr, second_addr];
        let converged =
            |view: &MemberAddrs| all.iter().all(|member_addr| view.up.contains(member_addr));
        let members = cluster::members().context("members")?;
        let client_converged = all.iter().all(|member_addr| {
            members
                .iter()
                .any(|member| member.addr() == *member_addr && member.state() == MemberState::Up)
        });
        if converged(&first_view) && converged(&second_view) && client_converged {
            break;
        }
        if Instant::now() >= deadline {
            bail!("bootstrap did not converge: {first_view:?} versus {second_view:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    }

    first.tell(Request::Stop);
    second.tell(Request::Stop);
    expect_exit(&mut first_process, "first bootstrap echo")?;
    expect_exit(&mut second_process, "second bootstrap echo")?;
    Ok(())
}

/// Downing a live node this endpoint merely messages: the pending ask fails as `NoReply`, the
/// watcher receives the synthesized signal although the node is alive, the member is listed
/// Down, and the retention forgets the entry; a fresh incarnation joining at the same address
/// afterwards proves the fence binds the incarnation, never the address. Downing this node's
/// own address, or one no member advertises, is refused with its own error, so a caller can tell
/// either apart from a down which took effect.
fn down_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut echo_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: echo.clone(),
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the death watch client"),
    }

    let (probe_tx, probe_rx) = mpsc::channel();
    let held = {
        let echo = echo.clone();
        runtime.spawn(async move {
            let hold = echo.ask(TIMEOUT, |reply_to| Request::Hold { reply_to });
            let probe = async {
                let probe = echo
                    .ask(TIMEOUT, |reply_to| Request::Ask { seq: 0, reply_to })
                    .await;
                let _ = probe_tx.send(probe);
            };
            let (hold, ()) = tokio::join!(hold, probe);
            hold
        })
    };
    let probe = probe_rx.recv_timeout(TIMEOUT).context("probe reply")?;
    if !matches!(probe, Ok(Reply { seq: 0 })) {
        bail!("probe ask resolved to {probe:?}");
    }

    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;
    match cluster::down(self_addr) {
        Err(DownError::ThisNode) => {}
        other => bail!("downing this node's own address resolved to {other:?}"),
    }
    match cluster::down(reserved_addr()?) {
        Err(DownError::NotAMember(_)) => {}
        other => bail!("downing an address no member advertises resolved to {other:?}"),
    }

    cluster::down(addr).context("downing the echo node")?;

    expect_done(&event_rx)?;
    let held = runtime.block_on(held).context("held ask task")?;
    if !matches!(held, Err(AskError::NoReply)) {
        bail!("held ask resolved to {held:?}");
    }
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("death watch client termination")??;

    let members = cluster::members().context("members")?;
    if !members
        .iter()
        .any(|member| member.addr() == addr && member.state() == MemberState::Down)
    {
        bail!("downed member not listed Down in {members:?}");
    }

    let deadline = Instant::now() + TIMEOUT;
    while cluster::members()
        .context("members")?
        .iter()
        .any(|member| member.addr() == addr)
    {
        if Instant::now() >= deadline {
            bail!("down entry at {addr} not forgotten");
        }
        thread::sleep(CONVERGENCE_POLL);
    }

    echo_process.0.kill().context("killing the downed echo")?;
    let restarted_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);
    let echo =
        lookup_echo_with_retries(runtime, addr).context("no reference from the rejoined node")?;

    let event_rx = run_client(runtime, "ping once client after rejoin", |event_tx| {
        PingOnce {
            server: echo,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;
    drop(restarted_process);
    Ok(())
}

/// A node which leaves announces it: every member runs the ordinary node death sequence for it
/// right away, well inside the window in which failure detection plus downing could not have
/// concluded anything, and the watcher of an actor on it is signaled.
fn leave_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut process = KillOnDrop(spawn_node_at(Role::Leave, Some(addr))?);
    let echo = resolve_ref::<Request>(&mut process)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: echo.clone(),
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the death watch client"),
    }

    let stopped = Instant::now();
    echo.tell(Request::Stop);
    while !cluster::members()
        .context("members")?
        .iter()
        .any(|member| member.addr() == addr && member.state() == MemberState::Down)
    {
        if stopped.elapsed() >= LEAVE_DEADLINE {
            bail!("the leaving member at {addr} was not downed within {LEAVE_DEADLINE:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    }

    expect_done(&event_rx)?;
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("death watch client termination")??;
    expect_exit(&mut process, "leaving node")
}

/// A `SelfDown` verdict from the downing provider runs the node death sequence on the node which
/// decides it: the endpoint severs everything and refuses to rejoin, so the losing side of a
/// partition gives way instead of downing the side it cannot reach.
fn self_down_scenario() -> anyhow::Result<()> {
    let mut process = KillOnDrop(spawn_node(Role::SelfDown)?);

    expect_join_line(&mut process, "downed")?;
    expect_exit(&mut process, "self down")
}

/// A node which never joined is not a member, however alive it is: a lookup at its address
/// answers `NotAMember` right away.
fn non_member_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut lone_process = KillOnDrop(spawn_lone_node(Role::Echo, addr)?);
    let _bytes = ref_bytes(&mut lone_process)?;

    let looked_up = runtime.block_on(async {
        let key = Key::<Request>::new(ECHO_KEY);
        timeout(TIMEOUT, cluster::lookup(&key, addr)).await
    })?;
    if !matches!(looked_up, Err(cluster::LookupError::NotAMember(_))) {
        bail!("lookup at a non-member resolved to {looked_up:?}");
    }
    Ok(())
}

/// Under mutual TLS a node's certificate must prove the address it advertises: a joiner whose
/// certificate carries its IP is admitted, one whose certificate proves no address is dropped
/// without an answer and its join runs into its timeout. The nodes form their own cluster with
/// their own certificate authority; the dev-transport client is not part of it.
fn identity_binding_scenario() -> anyhow::Result<()> {
    let scratch = tempfile::tempdir().context("mutual TLS scratch directory")?;
    let dir = scratch.path();

    let ca_key = rcgen::KeyPair::generate().context("ca key")?;
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).context("ca parameters")?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).context("ca certificate")?;
    let ca_path = dir.join("ca.der");
    fs::write(&ca_path, ca_cert.der()).context("writing the ca certificate")?;

    let mint = |name: &str, sans: Vec<String>| -> anyhow::Result<(PathBuf, PathBuf)> {
        let key = rcgen::KeyPair::generate().context("node key")?;
        let params = rcgen::CertificateParams::new(sans).context("node parameters")?;
        let cert = params
            .signed_by(&key, &ca_cert, &ca_key)
            .context("node certificate")?;
        let cert_path = dir.join(format!("{name}.der"));
        let key_path = dir.join(format!("{name}.key"));
        fs::write(&cert_path, cert.der()).context("writing the node certificate")?;
        fs::write(&key_path, key.serialize_der()).context("writing the node key")?;
        Ok((cert_path, key_path))
    };

    let seed_addr = reserved_addr()?;
    let (seed_cert, seed_key) = mint(
        "seed",
        vec![MTLS_SERVER_NAME.to_string(), seed_addr.ip().to_string()],
    )?;
    let mut seed_process = KillOnDrop(spawn_mutual_node(
        seed_addr, &seed_cert, &seed_key, &ca_path, None,
    )?);
    expect_join_line(&mut seed_process, "seed")?;

    let good_addr = reserved_addr()?;
    let (good_cert, good_key) = mint(
        "good",
        vec![MTLS_SERVER_NAME.to_string(), good_addr.ip().to_string()],
    )?;
    let mut good_process = KillOnDrop(spawn_mutual_node(
        good_addr,
        &good_cert,
        &good_key,
        &ca_path,
        Some(seed_addr),
    )?);
    expect_join_line(&mut good_process, "ok")?;
    expect_exit(&mut good_process, "proven mutual TLS joiner")?;

    let bad_addr = reserved_addr()?;
    let (bad_cert, bad_key) = mint("bad", vec![MTLS_SERVER_NAME.to_string()])?;
    let mut bad_process = KillOnDrop(spawn_mutual_node(
        bad_addr,
        &bad_cert,
        &bad_key,
        &ca_path,
        Some(seed_addr),
    )?);
    expect_join_line(&mut bad_process, "timeout")?;
    expect_exit(&mut bad_process, "unproven mutual TLS joiner")?;
    Ok(())
}

struct EchoClient {
    server: ActorRef<Request>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for EchoClient {
    type Message = Reply;
    type State = u32;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        for seq in 0..PINGS {
            self.server.tell(Request::Ping {
                seq,
                reply_to: context.self_ref().clone(),
            });
        }
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        expected: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Reply { seq }) = incoming else {
            return Ok(Control::Continue(expected));
        };

        if seq != expected {
            let _ = self.event_tx.send(TestEvent::Done(Err(format!(
                "reply {seq} instead of {expected}"
            ))));
            return Ok(Control::Stop);
        }

        let next = expected + 1;
        if next == PINGS {
            let _ = self.event_tx.send(TestEvent::Done(Ok(())));
            self.server.tell(Request::Stop);
            Ok(Control::Stop)
        } else {
            Ok(Control::Continue(next))
        }
    }
}

/// Sends one `AskThenTell` and asserts the marker arrives before the reply.
struct AskThenTellClient {
    server: ActorRef<Request>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for AskThenTellClient {
    type Message = AskerMessage;
    type State = bool;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.server.tell(Request::AskThenTell {
            marker_to: context.self_ref().clone(),
            reply_to: context.reply_to(AskerMessage::Answer),
        });
        Ok(false)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        marker_seen: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, marker_seen) {
            (Incoming::Message(AskerMessage::Marker), false) => Ok(Control::Continue(true)),

            (Incoming::Message(AskerMessage::Answer(_)), true) => {
                let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                Ok(Control::Stop)
            }

            (Incoming::Message(AskerMessage::Answer(_)), false) => {
                let _ = self
                    .event_tx
                    .send(TestEvent::Done(Err("reply before the marker".to_string())));
                Ok(Control::Stop)
            }

            (Incoming::Message(AskerMessage::Marker), true) => {
                let _ = self
                    .event_tx
                    .send(TestEvent::Done(Err("second marker".to_string())));
                Ok(Control::Stop)
            }

            (Incoming::Terminated(_), marker_seen) => Ok(Control::Continue(marker_seen)),
        }
    }
}

/// Answers a forwarded request and stops.
struct Responder;

impl Actor for Responder {
    type Message = ForwardedRequest;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ForwardedRequest { reply_to }) => {
                reply_to.reply(Reply { seq: FORWARD_SEQ });
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

/// Serializes its own `ReplyTo`, asserts a second serialization is refused, resolves the bytes on
/// its own node and replies through the resolved destination into its own mailbox.
struct SerdeRoundTrip {
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for SerdeRoundTrip {
    type Message = AskerMessage;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let fail = |message: String| {
            let _ = self.event_tx.send(TestEvent::Done(Err(message)));
            context.self_ref().tell(AskerMessage::Marker);
        };

        let reply_to = context.reply_to(AskerMessage::Answer);
        let bytes = match serde_json::to_vec(&reply_to) {
            Ok(bytes) => bytes,
            Err(error) => {
                fail(format!("serializing the reply destination: {error}"));
                return Ok(());
            }
        };

        if serde_json::to_vec(&reply_to).is_ok() {
            fail("a second serialization of the reply destination succeeded".to_string());
            return Ok(());
        }

        match serde_json::from_slice::<ReplyTo<Reply>>(&bytes) {
            Ok(reply_to) => reply_to.reply(Reply {
                seq: ROUND_TRIP_SEQ,
            }),

            Err(error) => fail(format!("resolving the reply destination bytes: {error}")),
        }
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(AskerMessage::Answer(Reply { seq })) => {
                let result = if seq == ROUND_TRIP_SEQ {
                    Ok(())
                } else {
                    Err(format!("reply {seq} instead of {ROUND_TRIP_SEQ}"))
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            Incoming::Message(AskerMessage::Marker) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

struct OrderedWatch {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for OrderedWatch {
    type Message = ClientEvent;
    type State = OrderedWatchState;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(OrderedWatchState::AwaitingChild)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let fail = |message: String| {
            let _ = self.event_tx.send(TestEvent::Done(Err(message)));
            Ok(Control::Stop)
        };

        match (incoming, state) {
            (Incoming::Message(ClientEvent::Child(child)), OrderedWatchState::AwaitingChild) => {
                context.watch(&child);
                child.tell(StreamerMessage::Go {
                    count: STREAMED,
                    reply_to: context.self_ref().clone(),
                });
                let child_id = child.actor_id();
                let _ = self.event_tx.send(TestEvent::Child(child));
                Ok(Control::Continue(OrderedWatchState::Streaming {
                    child_id,
                    next: 0,
                }))
            }

            (
                Incoming::Message(ClientEvent::Streamed(seq)),
                OrderedWatchState::Streaming { child_id, next },
            ) => {
                if seq != next {
                    return fail(format!("streamed {seq} instead of {next}"));
                }
                Ok(Control::Continue(OrderedWatchState::Streaming {
                    child_id,
                    next: next + 1,
                }))
            }

            (Incoming::Terminated(id), OrderedWatchState::Streaming { child_id, next }) => {
                if id != child_id {
                    return fail(format!("terminated signal for unexpected actor {id}"));
                }
                if next != STREAMED {
                    return fail(format!(
                        "terminated signal after {next} of {STREAMED} messages"
                    ));
                }
                let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                Ok(Control::Stop)
            }

            _ => fail("unexpected incoming".to_string()),
        }
    }
}

/// Spawns a streamer on the keeper's node, watches it, stops it and only reports its reference
/// once the terminated signal has arrived.
struct TerminateChild {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for TerminateChild {
    type Message = ClientEvent;
    type State = Option<ActorRef<StreamerMessage>>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                context.watch(&child);
                child.tell(StreamerMessage::Go {
                    count: 0,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(Some(child)))
            }

            Incoming::Terminated(id) => {
                let result = match &state {
                    Some(child) if child.actor_id() == id => Ok(()),
                    _ => Err(format!("terminated signal for unexpected actor {id}")),
                };

                if let Some(child) = state {
                    let _ = self.event_tx.send(TestEvent::Child(child));
                }
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            _ => Ok(Control::Continue(state)),
        }
    }
}

enum OrderedWatchState {
    AwaitingChild,
    Streaming { child_id: ActorId, next: u32 },
}

struct WatchTerminated {
    child: ActorRef<StreamerMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for WatchTerminated {
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.child);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(id) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if id == self.child.actor_id() {
            Ok(())
        } else {
            Err(format!("terminated signal for unexpected actor {id}"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

struct UnwatchClient {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for UnwatchClient {
    type Message = ClientEvent;
    type State = u32;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(0)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        received: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                context.watch(&child);
                context.unwatch(&child);
                child.tell(StreamerMessage::Go {
                    count: STREAMED,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(received))
            }

            Incoming::Message(ClientEvent::Streamed(_)) => {
                let received = received + 1;
                if received == STREAMED {
                    let _ = self.event_tx.send(TestEvent::StreamedAll);
                }
                Ok(Control::Continue(received))
            }

            Incoming::Message(ClientEvent::Bulked(_) | ClientEvent::Armed) => {
                Ok(Control::Continue(received))
            }

            Incoming::Message(ClientEvent::Probe) => {
                let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                Ok(Control::Stop)
            }

            Incoming::Terminated(id) => {
                let _ = self.event_tx.send(TestEvent::Done(Err(format!(
                    "terminated signal for {id} despite unwatch"
                ))));
                Ok(Control::Stop)
            }
        }
    }
}

/// Spawns a remote actor, has [WATCHERS] local actors watch it and stops it; it terminates only
/// once all of them have seen their signal.
struct TwoWatchers {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for TwoWatchers {
    type Message = ClientEvent;
    type State = usize;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(0)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        watching: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                for _ in 0..WATCHERS {
                    let watcher = context.spawn(ChildWatcher {
                        child: child.clone(),
                        event_tx: self.event_tx.clone(),
                    });
                    context.watch(&watcher);
                }

                child.tell(StreamerMessage::Go {
                    count: 0,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(WATCHERS))
            }

            Incoming::Terminated(_) => {
                let watching = watching - 1;
                if watching == 0 {
                    Ok(Control::Stop)
                } else {
                    Ok(Control::Continue(watching))
                }
            }

            _ => Ok(Control::Continue(watching)),
        }
    }
}

struct ChildWatcher {
    child: ActorRef<StreamerMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for ChildWatcher {
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.child);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(id) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if id == self.child.actor_id() {
            Ok(())
        } else {
            Err(format!("terminated signal for unexpected actor {id}"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

/// Bulk messages to one of [BULK_TARGETS] remote actors, a tiny one to each of the others, all
/// told in that order: the first acknowledgement must come from one of the others.
struct HeadOfLine {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for HeadOfLine {
    type Message = ClientEvent;
    type State = HeadOfLineState;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        for _ in 0..BULK_TARGETS {
            self.keeper.tell(KeeperMessage::Spawn {
                reply_to: context.self_ref().clone(),
            });
        }
        Ok(HeadOfLineState::AwaitingTargets(Vec::new()))
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (
                Incoming::Message(ClientEvent::Child(child)),
                HeadOfLineState::AwaitingTargets(mut targets),
            ) => {
                targets.push(child);
                if targets.len() < BULK_TARGETS {
                    return Ok(Control::Continue(HeadOfLineState::AwaitingTargets(targets)));
                }

                let payload = vec![0; BULK_PAYLOAD];
                for seq in 0..BULKS {
                    targets[0].tell(StreamerMessage::Bulk {
                        seq,
                        payload: payload.clone(),
                        reply_to: context.self_ref().clone(),
                    });
                }
                for (index, target) in targets.iter().enumerate().skip(1) {
                    let index = u32::try_from(index).expect("the target index fits");
                    target.tell(StreamerMessage::Bulk {
                        seq: BULKS + index,
                        payload: Vec::new(),
                        reply_to: context.self_ref().clone(),
                    });
                }

                Ok(Control::Continue(HeadOfLineState::Bulking {
                    acknowledged: 0,
                    ahead: 0,
                    overtaken: false,
                }))
            }

            (
                Incoming::Message(ClientEvent::Bulked(seq)),
                HeadOfLineState::Bulking {
                    acknowledged,
                    ahead,
                    overtaken,
                },
            ) => {
                let overtaken = overtaken || seq >= BULKS;
                let ahead = ahead + u32::from(!overtaken);

                let acknowledged = acknowledged + 1;
                if acknowledged < BULK_ACKNOWLEDGEMENTS {
                    return Ok(Control::Continue(HeadOfLineState::Bulking {
                        acknowledged,
                        ahead,
                        overtaken,
                    }));
                }

                let result = if ahead < BULKS {
                    Ok(())
                } else {
                    Err(format!(
                        "all {ahead} bulk messages were acknowledged before any other target"
                    ))
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            (_, state) => Ok(Control::Continue(state)),
        }
    }
}

/// `ahead` counts the bulk target's acknowledgements arriving before any other target's, which is
/// all of them when one lane carries everything and the bulk messages were told first.
enum HeadOfLineState {
    AwaitingTargets(Vec<ActorRef<StreamerMessage>>),

    Bulking {
        acknowledged: u32,
        ahead: u32,
        overtaken: bool,
    },
}

/// Tells the streamer a bulk beyond `max_frame_size` and an empty one right behind it, expecting
/// only the empty one to be acknowledged.
struct Oversize {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for Oversize {
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                child.tell(StreamerMessage::Bulk {
                    seq: 0,
                    payload: vec![0; OVERSIZE_PAYLOAD],
                    reply_to: context.self_ref().clone(),
                });
                child.tell(StreamerMessage::Bulk {
                    seq: 1,
                    payload: Vec::new(),
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(state))
            }

            Incoming::Message(ClientEvent::Bulked(seq)) => {
                let result = if seq == 1 {
                    Ok(())
                } else {
                    Err(format!("oversize bulk {seq} was acknowledged"))
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            _ => Ok(Control::Continue(state)),
        }
    }
}

/// Keeps `window` pings outstanding towards the echo server until `total` replies arrived; a
/// window of one measures serial latency, a large one pipelined throughput.
struct BenchEcho {
    server: ActorRef<Request>,
    total: u32,
    window: u32,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for BenchEcho {
    type Message = Reply;
    type State = BenchState;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let sent = self.window.min(self.total);
        for seq in 0..sent {
            self.server.tell(Request::Ping {
                seq,
                reply_to: context.self_ref().clone(),
            });
        }
        Ok(BenchState { sent, received: 0 })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Reply { .. }) = incoming else {
            return Ok(Control::Continue(state));
        };

        state.received += 1;
        if state.sent < self.total {
            self.server.tell(Request::Ping {
                seq: state.sent,
                reply_to: context.self_ref().clone(),
            });
            state.sent += 1;
        }

        if state.received == self.total {
            let _ = self.event_tx.send(TestEvent::Done(Ok(())));
            Ok(Control::Stop)
        } else {
            Ok(Control::Continue(state))
        }
    }
}

struct BenchState {
    sent: u32,
    received: u32,
}

/// Keeps [BENCH_BULK_WINDOW] bulks of [BENCH_BULK_PAYLOAD] bytes outstanding towards a streamer
/// until [BENCH_BULKS] acknowledgements arrived.
struct BenchBulk {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for BenchBulk {
    type Message = ClientEvent;
    type State = Option<BenchBulkState>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (Incoming::Message(ClientEvent::Child(child)), None) => {
                let sent = BENCH_BULK_WINDOW.min(BENCH_BULKS);
                for seq in 0..sent {
                    child.tell(StreamerMessage::Bulk {
                        seq,
                        payload: vec![0; BENCH_BULK_PAYLOAD],
                        reply_to: context.self_ref().clone(),
                    });
                }
                Ok(Control::Continue(Some(BenchBulkState {
                    child,
                    counts: BenchState { sent, received: 0 },
                })))
            }

            (Incoming::Message(ClientEvent::Bulked(_)), Some(mut state)) => {
                state.counts.received += 1;
                if state.counts.sent < BENCH_BULKS {
                    state.child.tell(StreamerMessage::Bulk {
                        seq: state.counts.sent,
                        payload: vec![0; BENCH_BULK_PAYLOAD],
                        reply_to: context.self_ref().clone(),
                    });
                    state.counts.sent += 1;
                }

                if state.counts.received == BENCH_BULKS {
                    let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                    Ok(Control::Stop)
                } else {
                    Ok(Control::Continue(Some(state)))
                }
            }

            (_, state) => Ok(Control::Continue(state)),
        }
    }
}

struct BenchBulkState {
    child: ActorRef<StreamerMessage>,
    counts: BenchState,
}

/// Fires a burst of bulks at the streamer and validates every acknowledgement arrives strictly
/// ascending; a `Probe` tells one further marker bulk, whose acknowledgement completes the run.
struct SeverFifo {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for SeverFifo {
    type Message = ClientEvent;
    type State = Option<SeverFifoState>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (Incoming::Message(ClientEvent::Child(child)), None) => {
                let payload = vec![0; SEVER_PAYLOAD];
                for seq in 0..SEVER_BULKS {
                    child.tell(StreamerMessage::Bulk {
                        seq,
                        payload: payload.clone(),
                        reply_to: context.self_ref().clone(),
                    });
                }
                let _ = self.event_tx.send(TestEvent::Bulking);
                Ok(Control::Continue(Some(SeverFifoState {
                    child,
                    last: None,
                    next_marker: SEVER_BULKS,
                })))
            }

            (Incoming::Message(ClientEvent::Probe), Some(mut state)) => {
                state.child.tell(StreamerMessage::Bulk {
                    seq: state.next_marker,
                    payload: Vec::new(),
                    reply_to: context.self_ref().clone(),
                });
                state.next_marker += 1;
                Ok(Control::Continue(Some(state)))
            }

            (Incoming::Message(ClientEvent::Bulked(seq)), Some(mut state)) => {
                if state.last.is_some_and(|last| seq <= last) {
                    let _ = self.event_tx.send(TestEvent::Done(Err(format!(
                        "acknowledgement {seq} after {:?}",
                        state.last
                    ))));
                    return Ok(Control::Stop);
                }
                if seq >= SEVER_BULKS {
                    let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                    return Ok(Control::Stop);
                }
                state.last = Some(seq);
                Ok(Control::Continue(Some(state)))
            }

            (_, state) => Ok(Control::Continue(state)),
        }
    }
}

struct SeverFifoState {
    child: ActorRef<StreamerMessage>,
    last: Option<u32>,
    next_marker: u32,
}

/// Watches the streamer, arms the keeper's endpoint to drop the next terminated frame and only
/// then stops the streamer: the signal must arrive nonetheless, through the watch refresh.
struct LostTerminated {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for LostTerminated {
    type Message = ClientEvent;
    type State = Option<ActorRef<StreamerMessage>>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                context.watch(&child);
                self.keeper.tell(KeeperMessage::DropTerminated {
                    count: 1,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(Some(child)))
            }

            Incoming::Message(ClientEvent::Armed) => {
                if let Some(child) = &state {
                    child.tell(StreamerMessage::Go {
                        count: 0,
                        reply_to: context.self_ref().clone(),
                    });
                }
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(id) => {
                let result = match &state {
                    Some(child) if child.actor_id() == id => Ok(()),
                    _ => Err(format!("terminated signal for unexpected actor {id}")),
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            _ => Ok(Control::Continue(state)),
        }
    }
}

/// Watches its subject, reports that it is watching and reports the terminated signal, whether
/// real or synthesized.
struct DeathWatch<N> {
    subject: ActorRef<N>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl<N> Actor for DeathWatch<N>
where
    N: Send + 'static,
{
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.subject);
        let _ = self.event_tx.send(TestEvent::Watching);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(id) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if id == self.subject.actor_id() {
            Ok(())
        } else {
            Err(format!("terminated signal for unexpected actor {id}"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

/// One ping, one reply: proves the lane round trips and leaves the server running.
struct PingOnce {
    server: ActorRef<Request>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for PingOnce {
    type Message = Reply;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.server.tell(Request::Ping {
            seq: 0,
            reply_to: context.self_ref().clone(),
        });
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Reply { seq }) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if seq == 0 {
            Ok(())
        } else {
            Err(format!("reply {seq} instead of 0"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

/// A lookup loop for bootstrap: retries `NotAMember` while the node there has not joined yet,
/// `NotFound` while it has not registered yet, and unreachability while it is still coming up.
fn lookup_echo_with_retries(
    runtime: &Runtime,
    addr: SocketAddr,
) -> anyhow::Result<ActorRef<Request>> {
    let mut last_error = None;
    for _ in 0..RESTART_LOOKUP_ATTEMPTS {
        let looked_up = runtime.block_on(async {
            let key = Key::<Request>::new(ECHO_KEY);
            timeout(RESTART_LOOKUP_TIMEOUT, cluster::lookup(&key, addr)).await
        });
        match looked_up {
            Ok(Ok(reference)) => return Ok(reference),

            Ok(Err(error)) => last_error = Some(anyhow::Error::new(error)),

            Err(elapsed) => last_error = Some(anyhow::Error::new(elapsed)),
        }
        runtime.block_on(sleep(RESTART_LOOKUP_DELAY));
    }

    Err(match last_error {
        Some(error) => error.context("no reference within the lookup attempts"),
        None => anyhow::Error::msg("no reference within the lookup attempts"),
    })
}

/// Run a client actor to completion and hand back the events it reported.
fn run_client<A, F>(runtime: &Runtime, what: &'static str, actor: F) -> anyhow::Result<Receiver>
where
    A: Actor + Send + 'static,
    A::Message: Send + 'static,
    A::State: Send + 'static,
    F: FnOnce(mpsc::Sender<TestEvent>) -> A,
{
    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(actor(event_tx));

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context(what)??;
    Ok(event_rx)
}

/// Start this process's remoting endpoint, on the address named by [ADDR_ENV] or otherwise an OS
/// chosen port, and join the cluster through the seeds named by [SEEDS_ENV], if any, or by
/// bootstrapping from the address list named by [BOOTSTRAP_ENV]. It must run inside a Tokio
/// runtime.
async fn start_endpoint() -> anyhow::Result<SocketAddr> {
    let bind_addr = env::var(ADDR_ENV).unwrap_or_else(|_| "127.0.0.1:0".to_string());
    let transport = QuicTransport::dev(bind_addr.parse()?)?;
    let addr = transport.local_addr()?;
    cluster::start_endpoint(test_config(addr), TimeoutTransport(transport))?;
    let _ = SELF_ADDR.set(addr);

    if let Ok(seeds) = env::var(SEEDS_ENV) {
        timeout(TIMEOUT, cluster::join(&parse_addrs(&seeds)?))
            .await
            .context("joining the cluster")??;
    } else if let Ok(seeds) = env::var(BOOTSTRAP_ENV) {
        bootstrap_cluster(parse_addrs(&seeds)?).await?;
    } else {
        // A started endpoint is no cluster: the client is the seed the spawned nodes join, so it
        // is the one node here which forms rather than joins.
        cluster::form().context("forming the cluster")?;
    }
    Ok(addr)
}

/// Deterministic failure detection for the scenarios: a fixed deadline instead of the adaptive
/// default, unilateral downing on a short deadline instead of the keep majority default, and a
/// short retention so downed members do not pile up across scenarios. The keep majority default's
/// even split tie break would self down this process whenever the killed node holds the lower
/// address.
fn test_config(addr: SocketAddr) -> EndpointConfig {
    let deadline = Deadline::new(Duration::from_secs(3)).expect("3s is not zero");

    let mut config = EndpointConfig::new(addr);
    config.failure_detector = Arc::new(move || Box::new(DeadlineFailureDetector::new(deadline)));
    config.downing_provider = Arc::new(|| Box::new(DownAfterDeadline::new(Duration::from_secs(2))));
    config.down_retention = Duration::from_secs(5);
    config
}

fn parse_addrs(addrs: &str) -> anyhow::Result<Vec<SocketAddr>> {
    addrs
        .split(',')
        .map(|addr| addr.parse().context("address list"))
        .collect()
}

/// Bootstrap into the cluster the given addresses form; every node of a bootstrap scenario runs
/// the identical list, its own address included, with a settle window short enough for the
/// scenario timeouts.
async fn bootstrap_cluster(seeds: Vec<SocketAddr>) -> anyhow::Result<()> {
    let config = BootstrapConfig {
        min_peers: NonZeroUsize::new(seeds.len()).expect("the seed list is not empty"),
        settle: Duration::from_millis(500),
        resolve_interval: Duration::from_millis(100),
        ..BootstrapConfig::new()
    };
    timeout(TIMEOUT, cluster::bootstrap(FixedSeeds::new(seeds), config))
        .await
        .context("bootstrapping the cluster")??;
    Ok(())
}

/// QUIC abandons a dial towards a silent address only after its 30 s handshake timeout, which
/// would stretch a lane's give-up over minutes; every scenario waiting on failed dials needs
/// each attempt bounded instead.
struct TimeoutTransport(QuicTransport);

impl Transport for TimeoutTransport {
    type Connection = QuicConnection;

    fn data_streams(&self) -> Option<NonZeroUsize> {
        self.0.data_streams()
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        max_frame_size: usize,
    ) -> Result<ConnectedControl<QuicConnection>, TransportError> {
        match timeout(CONNECT_TIMEOUT, self.0.connect(addr, max_frame_size)).await {
            Ok(connected) => connected,
            Err(_) => Err(TransportError::other("connect timed out")),
        }
    }

    async fn accept(&self, max_frame_size: usize) -> Result<QuicConnection, TransportError> {
        self.0.accept(max_frame_size).await
    }
}

/// An address the OS has just handed out and nothing holds anymore, so the node about to be
/// spawned can bind it: naming a node before it exists is what a lookup has to survive.
fn reserved_addr() -> anyhow::Result<SocketAddr> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    let addr = socket.local_addr()?;
    drop(socket);
    Ok(addr)
}

fn spawn_node(role: Role) -> anyhow::Result<Child> {
    spawn_node_at(role, None)
}

fn spawn_node_at(role: Role, bind_addr: Option<SocketAddr>) -> anyhow::Result<Child> {
    let seed = SELF_ADDR.get().context("endpoint not started")?;
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, role.as_str())
        .env(SEEDS_ENV, seed.to_string())
        .stdout(Stdio::piped());
    if let Some(bind_addr) = bind_addr {
        command.env(ADDR_ENV, bind_addr.to_string());
    }

    let child = command
        .spawn()
        .with_context(|| format!("{} process", role.as_str()))?;
    Ok(child)
}

/// A node bootstrapping from the given address list instead of joining a configured seed.
fn spawn_bootstrap_node(
    role: Role,
    bind_addr: SocketAddr,
    seeds: &[SocketAddr],
) -> anyhow::Result<Child> {
    let seeds = seeds
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, role.as_str())
        .env(ADDR_ENV, bind_addr.to_string())
        .env(BOOTSTRAP_ENV, seeds)
        .stdout(Stdio::piped());

    let child = command
        .spawn()
        .with_context(|| format!("bootstrap {} process", role.as_str()))?;
    Ok(child)
}

/// A node started without seeds: a cluster of its own, which the client's cluster must refuse.
fn spawn_lone_node(role: Role, bind_addr: SocketAddr) -> anyhow::Result<Child> {
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, role.as_str())
        .env(ADDR_ENV, bind_addr.to_string())
        .stdout(Stdio::piped());

    let child = command
        .spawn()
        .with_context(|| format!("lone {} process", role.as_str()))?;
    Ok(child)
}

fn spawn_mutual_node(
    bind_addr: SocketAddr,
    cert: &Path,
    key: &Path,
    roots: &Path,
    seed: Option<SocketAddr>,
) -> anyhow::Result<Child> {
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, Role::Mutual.as_str())
        .env(ADDR_ENV, bind_addr.to_string())
        .env(CERT_ENV, cert)
        .env(KEY_ENV, key)
        .env(ROOTS_ENV, roots)
        .stdout(Stdio::piped());
    if let Some(seed) = seed {
        command.env(SEEDS_ENV, seed.to_string());
    }

    let child = command.spawn().context("mutual TLS process")?;
    Ok(child)
}

fn resolve_ref<M>(child: &mut Child) -> anyhow::Result<ActorRef<M>>
where
    M: Serialize + Send + 'static,
{
    let bytes = ref_bytes(child)?;
    cluster::deserialize_ref(&bytes).context("server reference")
}

fn ref_bytes(child: &mut Child) -> anyhow::Result<Vec<u8>> {
    let line = stdout_line(child, REF_PREFIX).context("server reference")?;
    hex_decode(&line)
}

fn expect_join_line(child: &mut Child, expected: &str) -> anyhow::Result<()> {
    let verdict = stdout_line(child, JOIN_PREFIX).context("join verdict")?;
    if verdict != expected {
        bail!("join verdict {verdict} instead of {expected}");
    }
    Ok(())
}

/// The bootstrap read must be bounded like every other wait in this file: a node which hangs
/// before printing the expected line must fail its scenario rather than hang the whole suite.
fn stdout_line(child: &mut Child, prefix: &'static str) -> anyhow::Result<String> {
    let stdout = child.stdout.take().context("server process stdout")?;
    let (line_tx, line_rx) = mpsc::channel();

    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    if let Some(line) = line.strip_prefix(prefix) {
                        let _ = line_tx.send(Ok(line.to_string()));
                        return;
                    }
                }

                Err(error) => {
                    let _ = line_tx.send(Err(anyhow::Error::from(error)));
                    return;
                }
            }
        }
        let _ = line_tx.send(Err(anyhow::Error::msg(
            "no expected line on the server process stdout",
        )));
    });

    line_rx
        .recv_timeout(TIMEOUT)
        .context("server line within the timeout")?
}

fn recv(event_rx: &mpsc::Receiver<TestEvent>) -> anyhow::Result<TestEvent> {
    event_rx.recv_timeout(TIMEOUT).context("test event")
}

fn expect_done(event_rx: &mpsc::Receiver<TestEvent>) -> anyhow::Result<()> {
    match recv(event_rx)? {
        TestEvent::Done(Ok(())) => Ok(()),
        TestEvent::Done(Err(message)) => bail!(message),
        _ => bail!("unexpected event instead of done"),
    }
}

/// Waiting without checking the exit status is what this helper makes impossible: a node which
/// died reporting an error must fail its scenario.
fn expect_exit(child: &mut Child, what: &str) -> anyhow::Result<()> {
    let status = wait_with_timeout(child, TIMEOUT)?;
    if !status.success() {
        bail!("{what} process exited with {status}");
    }
    Ok(())
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) -> anyhow::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            bail!("server process still running after {timeout:?}");
        }
        thread::sleep(EXIT_POLL_INTERVAL);
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(hex: &str) -> anyhow::Result<Vec<u8>> {
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        bail!("odd length hex encoded reference");
    }

    pairs
        .iter()
        .map(|pair| {
            let pair = str::from_utf8(pair).context("hex encoded reference")?;
            u8::from_str_radix(pair, 16).context("hex encoded reference")
        })
        .collect()
}

#[derive(Deref, DerefMut)]
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}
