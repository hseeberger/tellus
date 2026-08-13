use crate::{
    cluster::{
        discovery,
        endpoint::{EndpointInner, Generation},
        failure::PeerLiveness,
        frame::{Frame, Handshake, HandshakeError, HandshakeIntent, RefusalReason},
        membership,
        node::NodeId,
        reply,
        transport::{
            ConnectedControl, Connection, FrameReceiver, FrameSender, PeerIdentity, Transport,
            TransportError,
        },
        watch,
    },
    quota::Quota,
};
use flume::Receiver;
use std::{iter, net::SocketAddr, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    select,
    sync::oneshot,
    task::{self, JoinSet},
    time::{Instant, sleep, timeout},
};
use tracing::{debug, error, warn};

#[cfg(test)]
use crate::ActorId;
#[cfg(test)]
use std::{
    collections::HashSet,
    sync::{LazyLock, Mutex},
};

/// Every dead letter this module logs, so a test can prove one happened.
#[cfg(test)]
static DEAD_LETTERS: LazyLock<Mutex<HashSet<ActorId>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(crate) struct DialRequest {
    pub(crate) addr: SocketAddr,
    pub(crate) lane_id: Generation,
    pub(crate) control_rx: Receiver<Frame<'static>>,
    pub(crate) data_rx: Vec<Receiver<Frame<'static>>>,
    pub(crate) quota: Quota,
}

pub(crate) struct JoinRequest {
    pub(crate) addr: SocketAddr,
    pub(crate) result_tx: oneshot::Sender<Result<(), ConnectError>>,
}

/// A transport failure or a silent peer is transient, everything else is final.
#[derive(Debug, Error)]
pub(crate) enum ConnectError {
    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error("connection closed before handshake")]
    Closed,

    #[error("timed out opening {0} data streams")]
    DataStreams(usize),

    #[error("timed out exchanging handshakes")]
    HandshakeTimeout,

    #[error("first frame is not a handshake")]
    NotAHandshake,

    #[error("cannot decode handshake frame")]
    Decode(#[from] postcard::Error),

    #[error(transparent)]
    Handshake(#[from] HandshakeError),

    #[error("peer refused the handshake: {0}")]
    Refused(RefusalReason),

    #[error("peer identity does not cover its advertised address")]
    Identity,

    #[error("node dialed at {dialed} advertises {advertised}")]
    AddressMismatch {
        advertised: SocketAddr,
        dialed: SocketAddr,
    },

    #[error("this node has been downed")]
    SelfDowned,

    #[error("join attempt abandoned by its caller")]
    Abandoned,

    #[error("dead node incarnation")]
    Dead,
}

impl ConnectError {
    /// A refusal as an unknown member is retryable: gossip may not have reached it yet. So is
    /// one from a node which is no cluster yet, until it has formed one or joined one.
    fn is_retryable(&self) -> bool {
        match self {
            ConnectError::Transport(TransportError::FrameTooLarge { .. }) => false,

            ConnectError::Transport(_)
            | ConnectError::Closed
            | ConnectError::DataStreams(_)
            | ConnectError::HandshakeTimeout
            | ConnectError::Refused(RefusalReason::UnknownMember)
            | ConnectError::Refused(RefusalReason::NoCluster) => true,

            ConnectError::NotAHandshake
            | ConnectError::Decode(_)
            | ConnectError::Handshake(_)
            | ConnectError::Refused(RefusalReason::Down)
            | ConnectError::Identity
            | ConnectError::AddressMismatch { .. }
            | ConnectError::SelfDowned
            | ConnectError::Abandoned
            | ConnectError::Dead => false,
        }
    }
}

pub(crate) async fn accept_loop<T>(transport: Arc<T>, endpoint: &'static EndpointInner)
where
    T: Transport,
{
    loop {
        match transport
            .accept(endpoint.config().max_frame_size.get())
            .await
        {
            Ok(connection) => {
                task::spawn(run_accepted(connection, endpoint));
            }

            Err(error) => {
                error!(%error, "remoting endpoint cannot accept connections");
                break;
            }
        }
    }
}

pub(crate) async fn dial_loop<T>(
    transport: Arc<T>,
    dial_request_rx: Receiver<DialRequest>,
    endpoint: &'static EndpointInner,
) where
    T: Transport,
{
    while let Ok(request) = dial_request_rx.recv_async().await {
        task::spawn(run_peer(transport.clone(), request, endpoint));
    }
}

/// Attempts run here rather than spawned, so only one is ever in flight: two could be admitted
/// by two different clusters before either is recorded, leaving both counting this node.
pub(crate) async fn join_loop<T>(
    transport: Arc<T>,
    join_request_rx: Receiver<JoinRequest>,
    endpoint: &'static EndpointInner,
) where
    T: Transport,
{
    while let Ok(request) = join_request_rx.recv_async().await {
        run_join(transport.clone(), request, endpoint).await;
    }
}

/// The data streams are ordered as their queues were, which is how [EndpointInner::send] indexes.
struct Connected<C>
where
    C: Connection,
{
    frame_senders: Vec<C::Sender>,
    frame_receiver: C::Receiver,
    peer: NodeId,
    identity: Option<PeerIdentity>,
}

enum WriterEnd {
    LaneClosed,
    ConnectionLost,
}

/// [ReadEnd::Poisoned] demands the connection's end, [ReadEnd::Closed] only this stream's.
enum ReadEnd {
    Closed,
    Poisoned,
}

/// A spoofed identity gets no answer, so the connection is dropped silently rather than refused
/// with a message.
enum Admission {
    Admit(PeerLiveness),
    Refuse(RefusalReason),
    Drop,
}

/// The handshake is bounded by the heartbeat interval, so an unauthenticated dialer cannot pin
/// this task; the admission decides before the reply handshake.
async fn run_accepted<C>(connection: C, endpoint: &'static EndpointInner)
where
    C: Connection,
{
    let establishing = timeout(endpoint.config().heartbeat_interval, async {
        let (mut frame_sender, mut frame_receiver) = match connection.accept_control().await {
            Ok(halves) => halves,
            Err(error) => {
                warn!(%error, "cannot open inbound connection");
                return None;
            }
        };

        let (peer, intent) = match recv_handshake(&mut frame_receiver).await {
            Ok(handshaked) => handshaked,
            Err(error) => {
                warn!(%error, "cannot receive inbound handshake");
                return None;
            }
        };

        let liveness = match admit_inbound(peer, intent, connection.peer_identity(), endpoint) {
            Admission::Admit(liveness) => liveness,

            Admission::Refuse(reason) => {
                match send_frame(&mut frame_sender, &Frame::Refused { reason }).await {
                    // Returning drops the connection, which discards the refusal before the
                    // transport flushed it; the peer's own close proves it read the reason.
                    Ok(()) => {
                        let _ = frame_receiver.recv().await;
                    }

                    Err(error) => debug!(%peer, %error, "cannot send refusal"),
                }
                return None;
            }

            Admission::Drop => return None,
        };

        if let Err(error) =
            send_handshake(&mut frame_sender, endpoint.node(), HandshakeIntent::Member).await
        {
            warn!(%peer, %error, "cannot send outbound handshake");
            return None;
        }
        for frame in endpoint.snapshot_frames(endpoint.membership().handshake_snapshot()) {
            if let Err(error) = send_frame(&mut frame_sender, &frame).await {
                warn!(%peer, %error, "cannot send member snapshot");
                return None;
            }
        }
        for frame in endpoint.reachability_snapshot_frames() {
            if let Err(error) = send_frame(&mut frame_sender, &frame).await {
                warn!(%peer, %error, "cannot send reachability snapshot");
                return None;
            }
        }

        Some((frame_sender, frame_receiver, peer, liveness))
    })
    .await;

    let established = match establishing {
        Ok(established) => established,

        Err(_) => {
            debug!("timed out establishing inbound connection");
            return;
        }
    };
    let Some((frame_sender, frame_receiver, peer, liveness)) = established else {
        return;
    };
    debug!(%peer, "inbound connection established");
    endpoint.unrefuse(peer.addr());

    endpoint
        .supersede_inbound_reader(peer.addr(), |shutdown_rx| {
            task::spawn(read_streams(
                connection,
                frame_receiver,
                frame_sender,
                peer,
                liveness,
                endpoint,
                shutdown_rx,
            ))
        })
        .await;
}

/// The [JoinSet] is awaited on every exit, else a successor reader overlaps a delivery draining
/// here; a poisoned stream ends the whole connection.
async fn read_streams<C>(
    connection: C,
    control_rx: C::Receiver,
    frame_sender: C::Sender,
    peer: NodeId,
    liveness: PeerLiveness,
    endpoint: &'static EndpointInner,
    mut shutdown_rx: tokio::sync::watch::Receiver<()>,
) where
    C: Connection,
{
    let mut sever_rx = endpoint.sever_rx();

    let mut readers = JoinSet::new();
    readers.spawn(read_frames(control_rx, peer, liveness.clone(), endpoint));

    loop {
        select! {
            biased;

            _ = shutdown_rx.changed() => break,

            _ = sever_rx.changed() => break,

            joined = readers.join_next() => match joined {
                Some(Ok(ReadEnd::Poisoned)) | None => break,
                Some(_) => {}
            },

            accepted = connection.accept_data() => match accepted {
                Ok(Some(data_rx)) => {
                    readers.spawn(read_frames(data_rx, peer, liveness.clone(), endpoint));
                }

                Ok(None) => break,

                Err(error) => {
                    debug!(%peer, %error, "no further inbound stream");
                    break;
                }
            },
        }
    }

    readers.shutdown().await;
    drop(frame_sender);
}

/// A reader must be aborted and awaited before the next dial, else two readers overlap; watches
/// are re-sent on every connection, pending lookups only on a reconnect.
async fn run_peer<T>(transport: Arc<T>, request: DialRequest, endpoint: &'static EndpointInner)
where
    T: Transport,
{
    let DialRequest {
        addr,
        lane_id,
        control_rx,
        data_rx,
        quota,
    } = request;
    let streams = data_rx.len();
    let outbound_rx = iter::once(control_rx).chain(data_rx).collect::<Vec<_>>();
    let mut sever_rx = endpoint.sever_rx();
    let mut attempts = 0u32;
    let mut reconnected = false;

    loop {
        match connect(transport.as_ref(), addr, streams, endpoint).await {
            Ok(connected) => {
                let Connected {
                    frame_senders,
                    frame_receiver,
                    peer,
                    identity,
                } = connected;
                let liveness = match admit_outbound(peer, addr, identity, endpoint) {
                    Ok(liveness) => liveness,

                    Err(error) => {
                        warn!(%peer, %error, "refusing the outbound connection");
                        if !matches!(error, ConnectError::SelfDowned) {
                            endpoint.refuse(addr);
                        }
                        break;
                    }
                };
                let reader = task::spawn(read_frames(frame_receiver, peer, liveness, endpoint));

                endpoint.bind_lane(addr, lane_id, peer);
                let connected_at = Instant::now();
                debug!(%peer, "outbound connection established");

                // The heartbeat omits the watermarks, so this is the only path carrying this
                // side's fences to the acceptor!
                for frame in endpoint.snapshot_frames(endpoint.membership().handshake_snapshot()) {
                    if let Err(error) = endpoint.send(peer, frame) {
                        warn!(%peer, %error, "cannot send member snapshot");
                    }
                }
                for frame in endpoint.reachability_snapshot_frames() {
                    if let Err(error) = endpoint.send(peer, frame) {
                        warn!(%peer, %error, "cannot send reachability snapshot");
                    }
                }

                // Watch frames lost with a connection must be re-sent here; idempotent remotely.
                for (target, watcher) in endpoint.watchers().watches(peer) {
                    if let Err(error) = endpoint.send(peer, Frame::Watch { target, watcher }) {
                        warn!(%peer, actor_id = %target, %error, "cannot re-establish remote watch");
                    }
                }
                if reconnected {
                    for frame in endpoint.pending_lookups().frames(peer) {
                        if let Err(error) = endpoint.send(peer, frame) {
                            warn!(peer_addr = %addr, %error, "cannot re-send lookup");
                        }
                    }
                }
                reconnected = true;

                let end = select! {
                    end = write_streams(
                        frame_senders,
                        &outbound_rx,
                        &quota,
                        addr,
                        endpoint.config().max_frame_size.get(),
                    ) => end,

                    _ = sever_rx.changed() => WriterEnd::ConnectionLost,
                };
                endpoint.mark_lane_disconnected(addr, lane_id);
                reader.abort();
                let _ = reader.await;

                if matches!(end, WriterEnd::LaneClosed) {
                    break;
                }

                // One heartbeat is the shortest lifetime proving the peer answered here.
                if connected_at.elapsed() >= endpoint.config().heartbeat_interval {
                    attempts = 0;
                } else {
                    attempts += 1;

                    if !backoff_or_give_up(endpoint, addr, attempts).await {
                        break;
                    }
                }
            }

            Err(ConnectError::Refused(RefusalReason::Down)) => {
                endpoint.self_down();
                break;
            }

            Err(error) if !error.is_retryable() => {
                warn!(peer_addr = %addr, %error, "giving up connecting to node for good");
                endpoint.refuse(addr);
                break;
            }

            Err(error) => {
                attempts += 1;
                debug!(peer_addr = %addr, attempts, %error, "cannot connect to node");

                if !backoff_or_give_up(endpoint, addr, attempts).await {
                    break;
                }
            }
        }

        if !endpoint.is_lane_open(addr, lane_id) {
            break;
        }
    }

    if endpoint.remove_lane(addr, lane_id) {
        endpoint.pending_lookups().fail_addr(addr);
        endpoint.pending_replies().fail_addr(addr);
        watch::fail_watchers_at(endpoint, addr);
    }
    for outbound_rx in outbound_rx {
        drain_dead_letters(outbound_rx, addr).await;
    }
}

/// A member's address is retried forever: downing, not the attempt count, settles its fate.
async fn backoff_or_give_up(endpoint: &EndpointInner, addr: SocketAddr, attempts: u32) -> bool {
    if !endpoint.membership().has_up_member_at(addr)
        && attempts >= endpoint.config().max_connect_attempts
    {
        warn!(peer_addr = %addr, "giving up connecting to node");
        return false;
    }

    sleep(endpoint.reconnect_backoff(attempts)).await;
    true
}

async fn connect<T>(
    transport: &T,
    addr: SocketAddr,
    streams: usize,
    endpoint: &EndpointInner,
) -> Result<Connected<T::Connection>, ConnectError>
where
    T: Transport,
{
    let ConnectedControl {
        connection,
        mut control_tx,
        control_rx: mut frame_receiver,
    } = transport
        .connect(addr, endpoint.config().max_frame_size.get())
        .await?;

    let (peer, _) = exchange_handshakes(
        &mut control_tx,
        &mut frame_receiver,
        endpoint.node(),
        HandshakeIntent::Member,
        endpoint.config().heartbeat_interval,
    )
    .await?;

    let mut frame_senders = vec![control_tx];
    frame_senders.extend(open_data_streams(&connection, streams, endpoint).await?);

    let identity = connection.peer_identity();
    Ok(Connected {
        frame_senders,
        frame_receiver,
        peer,
        identity,
    })
}

/// Bounded, so a peer which connects and never speaks cannot pin this lane's dial forever.
async fn exchange_handshakes<S, R>(
    frame_sender: &mut S,
    frame_receiver: &mut R,
    node: NodeId,
    intent: HandshakeIntent,
    within: Duration,
) -> Result<(NodeId, HandshakeIntent), ConnectError>
where
    R: FrameReceiver,
    S: FrameSender,
{
    let exchange = async {
        send_handshake(frame_sender, node, intent).await?;
        recv_handshake(frame_receiver).await
    };

    match timeout(within, exchange).await {
        Ok(peer) => peer,
        Err(_) => Err(ConnectError::HandshakeTimeout),
    }
}

/// Opened before the lane carries anything, so a peer admitting fewer streams fails here.
async fn open_data_streams<C>(
    connection: &C,
    streams: usize,
    endpoint: &EndpointInner,
) -> Result<Vec<C::Sender>, ConnectError>
where
    C: Connection,
{
    let open = async {
        let mut frame_senders = Vec::with_capacity(streams);
        for _ in 0..streams {
            frame_senders.push(connection.open_data().await?);
        }
        Ok::<_, ConnectError>(frame_senders)
    };

    match timeout(endpoint.config().heartbeat_interval, open).await {
        Ok(frame_senders) => frame_senders,

        Err(_) => Err(ConnectError::DataStreams(streams)),
    }
}

/// One writer per stream, sharing the lane's quota; the first one to end ends them all.
async fn write_streams<S>(
    frame_senders: Vec<S>,
    outbound_rx: &[Receiver<Frame<'static>>],
    quota: &Quota,
    addr: SocketAddr,
    max_frame_size: usize,
) -> WriterEnd
where
    S: FrameSender,
{
    debug_assert_eq!(
        frame_senders.len(),
        outbound_rx.len(),
        "one writer per queue"
    );

    let mut writers = JoinSet::new();
    for (frame_sender, outbound_rx) in frame_senders.into_iter().zip(outbound_rx) {
        writers.spawn(write_frames(
            frame_sender,
            outbound_rx.clone(),
            quota.clone(),
            addr,
            max_frame_size,
        ));
    }

    let end = match writers.join_next().await {
        Some(Ok(end)) => end,
        Some(Err(_)) | None => WriterEnd::ConnectionLost,
    };

    // Await the aborted siblings, else a successor lane starts while a dequeued frame has still
    // not been dead-lettered.
    writers.shutdown().await;
    end
}

/// The reservation is released on dequeue, before the send, else an abort leaks a slot.
async fn write_frames<S>(
    mut frame_sender: S,
    outbound_rx: Receiver<Frame<'static>>,
    quota: Quota,
    addr: SocketAddr,
    max_frame_size: usize,
) -> WriterEnd
where
    S: FrameSender,
{
    let mut buffer = Vec::new();

    loop {
        let Ok(frame) = outbound_rx.recv_async().await else {
            return WriterEnd::LaneClosed;
        };
        let mut in_flight = InFlightFrame::new(frame, addr);
        if in_flight.frame().is_counted() {
            quota.unreserve();
        }

        buffer = match in_flight.frame().encode_into(buffer) {
            Ok(bytes) => bytes,

            Err(error) => {
                warn!(peer_addr = %addr, %error, "cannot encode frame");
                buffer = Vec::new();
                continue;
            }
        };

        // An oversize frame must never reach the transport: the receiver's refusal kills the
        // connection!
        if buffer.len() > max_frame_size {
            in_flight.discard_oversize();
            continue;
        }

        if let Err(error) = frame_sender.send(&buffer).await {
            warn!(peer_addr = %addr, %error, "connection lost");
            return WriterEnd::ConnectionLost;
        }
        in_flight.disarm();
    }
}

/// Each delivery holds the peer's gate and rechecks the tombstone, in arrival order.
async fn read_frames<R>(
    mut frame_receiver: R,
    peer: NodeId,
    liveness: PeerLiveness,
    endpoint: &'static EndpointInner,
) -> ReadEnd
where
    R: FrameReceiver,
{
    loop {
        let bytes = match frame_receiver.recv().await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                debug!(%peer, "connection closed by peer");
                return ReadEnd::Closed;
            }
            Err(error) => {
                debug!(%peer, %error, "connection lost");
                return ReadEnd::Closed;
            }
        };

        let frame = match Frame::from_bytes(bytes) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(%peer, %error, "closing connection, cannot decode frame");
                return ReadEnd::Poisoned;
            }
        };

        liveness.record_heartbeat();

        // Handle gossip before entering the gate: its merge may run a node death, which
        // write-locks a gate!
        if let Frame::Gossip { members, .. } = &frame {
            membership::on_gossip(endpoint, members);
            if endpoint.membership().is_down(peer) {
                debug!(%peer, "closing connection to a dead node incarnation");
                return ReadEnd::Poisoned;
            }
            continue;
        }

        let _guard = liveness.enter();
        if endpoint.membership().is_down(peer) {
            debug!(%peer, "closing connection to a dead node incarnation");
            return ReadEnd::Poisoned;
        }

        match frame {
            Frame::Message {
                target,
                reply_tags,
                payload,
            } => {
                if let Err(error) = endpoint
                    .registry()
                    .deliver(target, &payload, endpoint.codec())
                {
                    warn!(%peer, actor_id = %target, %error, "dead letter");
                    reply::on_undeliverable(endpoint, peer, &reply_tags);
                }
            }

            Frame::Watch { target, watcher } => watch::on_watch(endpoint, peer, target, watcher),

            Frame::Unwatch { target, watcher } => {
                watch::on_unwatch(endpoint, peer, target, watcher)
            }

            Frame::Terminated { target, watcher } => {
                watch::on_terminated(endpoint, peer, target, watcher)
            }

            Frame::Lookup { nonce, key } => discovery::on_lookup(endpoint, peer, nonce, key),

            Frame::LookupReply { nonce, result } => {
                discovery::on_lookup_reply(endpoint, peer, nonce, result)
            }

            Frame::Reply {
                nonce,
                recipient: _,
                payload,
            } => reply::on_reply(endpoint, peer, nonce, &payload),

            Frame::ReplyDropped {
                nonce,
                recipient: _,
            } => reply::on_reply_dropped(endpoint, peer, nonce),

            Frame::Gossip { .. } => unreachable!("gossip is handled before the gate"),

            // Merged now, so this node's own decisions see it at once; forwarding waits for the
            // next membership tick, which fans out everything learned since the last one.
            Frame::Reachability { observations } => endpoint.merge_reachability(&observations),

            Frame::Handshake(_) | Frame::Refused { .. } => {
                warn!(%peer, "closing connection, unexpected establishment frame");
                return ReadEnd::Poisoned;
            }
        }
    }
}

/// The identity check comes first, so an unproven address reaches nothing, not even the
/// supersession; the Down check precedes any admission.
fn admit_inbound(
    peer: NodeId,
    intent: HandshakeIntent,
    identity: Option<PeerIdentity>,
    endpoint: &'static EndpointInner,
) -> Admission {
    if endpoint.downed() {
        return Admission::Drop;
    }
    if !identity_binds(peer, identity) {
        warn!(%peer, "dropping connection, advertised address not among the certificate's IP addresses");
        return Admission::Drop;
    }
    if endpoint.membership().is_down(peer) {
        warn!(%peer, "refusing connection, dead node incarnation");
        return Admission::Refuse(RefusalReason::Down);
    }

    match intent {
        HandshakeIntent::Member => {
            if endpoint.membership().is_up(peer) {
                Admission::Admit(endpoint.track_liveness(peer))
            } else {
                debug!(%peer, "refusing connection, not (yet) a member");
                Admission::Refuse(RefusalReason::UnknownMember)
            }
        }

        HandshakeIntent::Join => {
            if !endpoint.formed() {
                debug!(%peer, "refusing join, this node is not a member of any cluster");
                return Admission::Refuse(RefusalReason::NoCluster);
            }

            if let Some(previous) = endpoint.membership().up_member_at(peer.addr())
                && previous != peer
            {
                if previous.incarnation() < peer.incarnation() {
                    warn!(%peer, %previous, "node death, address superseded by a joining incarnation");
                    endpoint.node_death(previous);
                } else {
                    warn!(%peer, %previous, "refusing join, stale incarnation for its address");
                    endpoint.node_death(peer);
                    return Admission::Refuse(RefusalReason::Down);
                }
            }

            if endpoint.membership().add_up(peer) {
                debug!(%peer, "member joined");
                endpoint.push_gossip();
            }
            Admission::Admit(endpoint.track_liveness(peer))
        }
    }
}

/// The dial side's counterpart of [admit_inbound]; membership is the responder's own bookkeeping.
fn admit_outbound(
    peer: NodeId,
    dialed: SocketAddr,
    identity: Option<PeerIdentity>,
    endpoint: &EndpointInner,
) -> Result<PeerLiveness, ConnectError> {
    if endpoint.downed() {
        return Err(ConnectError::SelfDowned);
    }
    if peer.addr() != dialed {
        return Err(ConnectError::AddressMismatch {
            advertised: peer.addr(),
            dialed,
        });
    }
    if !identity_binds(peer, identity) {
        return Err(ConnectError::Identity);
    }
    if endpoint.membership().is_down(peer) {
        return Err(ConnectError::Dead);
    }

    Ok(endpoint.track_liveness(peer))
}

fn identity_binds(peer: NodeId, identity: Option<PeerIdentity>) -> bool {
    identity.is_none_or(|identity| identity.ip_addresses.contains(&peer.addr().ip()))
}

async fn run_join<T>(transport: Arc<T>, request: JoinRequest, endpoint: &'static EndpointInner)
where
    T: Transport,
{
    let JoinRequest {
        addr,
        mut result_tx,
    } = request;
    if result_tx.is_closed() {
        debug!(seed_addr = %addr, "dropping join request, its caller is gone");
        return;
    }

    if !endpoint.enter_join() {
        let _ = result_tx.send(Err(ConnectError::SelfDowned));
        return;
    }
    let result = try_join(transport.as_ref(), addr, &mut result_tx, endpoint).await;
    let result = endpoint.finish_join(addr, result);
    let _ = result_tx.send(result);
}

/// One connection for the whole join, outside the lane machinery and discarded afterwards.
///
/// Cancellable until the Join handshake goes out; from there the peer may already count this
/// node, so the attempt runs to its conclusion.
async fn try_join<T>(
    transport: &T,
    addr: SocketAddr,
    result_tx: &mut oneshot::Sender<Result<(), ConnectError>>,
    endpoint: &'static EndpointInner,
) -> Result<(), ConnectError>
where
    T: Transport,
{
    let connected = select! {
        connected = transport.connect(addr, endpoint.config().max_frame_size.get()) => connected?,

        () = result_tx.closed() => return Err(ConnectError::Abandoned),
    };
    let ConnectedControl {
        connection,
        mut control_tx,
        control_rx: mut frame_receiver,
    } = connected;

    let (peer, _) = exchange_handshakes(
        &mut control_tx,
        &mut frame_receiver,
        endpoint.node(),
        HandshakeIntent::Join,
        endpoint.config().heartbeat_interval,
    )
    .await?;

    if peer.addr() != addr {
        return Err(ConnectError::AddressMismatch {
            advertised: peer.addr(),
            dialed: addr,
        });
    }
    if !identity_binds(peer, connection.peer_identity()) {
        warn!(%peer, "refusing join, advertised address not among the certificate's IP addresses");
        return Err(ConnectError::Identity);
    }
    if endpoint.membership().is_down(peer) {
        return Err(ConnectError::Dead);
    }

    // The answer proves a cluster admitted this node, whether or not its snapshot arrives.
    endpoint.pin_cluster(addr);

    let members = timeout(endpoint.config().heartbeat_interval, async {
        let mut members = Vec::new();
        loop {
            let frame = frame_receiver
                .recv()
                .await?
                .ok_or(ConnectError::Closed)
                .and_then(|bytes| Frame::from_bytes(bytes).map_err(ConnectError::from))?;

            match frame {
                Frame::Gossip {
                    members: page,
                    more,
                } => {
                    members.extend(page);
                    if !more {
                        return Ok(members);
                    }
                }

                Frame::Refused { reason } => return Err(ConnectError::Refused(reason)),

                _ => return Err(ConnectError::NotAHandshake),
            }
        }
    })
    .await
    .map_err(|_| ConnectError::HandshakeTimeout)??;

    // Applied as one snapshot: a connection lost mid stream must leave no fragment behind.
    membership::on_gossip(endpoint, &members);
    Ok(())
}

async fn send_handshake<S>(
    frame_sender: &mut S,
    node: NodeId,
    intent: HandshakeIntent,
) -> Result<(), TransportError>
where
    S: FrameSender,
{
    send_frame(
        frame_sender,
        &Frame::Handshake(Handshake::new(node, intent)),
    )
    .await
}

async fn send_frame<S>(frame_sender: &mut S, frame: &Frame<'_>) -> Result<(), TransportError>
where
    S: FrameSender,
{
    let bytes = frame
        .encode_into(Vec::new())
        .map_err(TransportError::other)?;
    frame_sender.send(&bytes).await
}

async fn recv_handshake<R>(
    frame_receiver: &mut R,
) -> Result<(NodeId, HandshakeIntent), ConnectError>
where
    R: FrameReceiver,
{
    let bytes = frame_receiver.recv().await?.ok_or(ConnectError::Closed)?;

    match Frame::from_bytes(bytes)? {
        Frame::Handshake(handshake) => Ok(handshake.validate()?),
        Frame::Refused { reason } => Err(ConnectError::Refused(reason)),
        _ => Err(ConnectError::NotAHandshake),
    }
}

async fn drain_dead_letters(outbound_rx: Receiver<Frame<'static>>, addr: SocketAddr) {
    while let Ok(frame) = outbound_rx.recv_async().await {
        dead_letter(frame, addr, "node unreachable");
    }
}

fn dead_letter(frame: Frame<'static>, addr: SocketAddr, reason: &'static str) {
    discard_reply_tags(&frame);

    if let Frame::Message { target, .. } = frame {
        #[cfg(test)]
        crate::sync::lock(&DEAD_LETTERS).insert(target);
        warn!(actor_id = %target, peer_addr = %addr, reason, "dead letter");
    }
}

fn oversize_dead_letter(frame: &Frame<'_>, addr: SocketAddr) {
    match frame {
        Frame::Message { target, .. } => {
            warn!(actor_id = %*target, peer_addr = %addr, "dead letter, frame exceeds the maximum frame size");
        }

        _ => warn!(peer_addr = %addr, "dropping a frame exceeding the maximum frame size"),
    }
}

/// A request dropped here never reaches its peer, so its asks resolve now, not on timeout.
fn discard_reply_tags(frame: &Frame<'_>) {
    if let Frame::Message { reply_tags, .. } = frame {
        reply::discard_replies(reply_tags);
    }
}

/// Owns a dequeued frame until the transport accepts it. Aborting a sibling writer drops this
/// guard, which gives locally lost frames the same dead-letter and reply cleanup as a send error.
struct InFlightFrame {
    frame: Option<Frame<'static>>,
    addr: SocketAddr,
}

impl InFlightFrame {
    fn new(frame: Frame<'static>, addr: SocketAddr) -> Self {
        Self {
            frame: Some(frame),
            addr,
        }
    }

    fn frame(&self) -> &Frame<'static> {
        self.frame.as_ref().expect("in-flight frame is armed")
    }

    fn discard_oversize(&mut self) {
        let frame = self.frame.take().expect("in-flight frame is armed");
        oversize_dead_letter(&frame, self.addr);
        discard_reply_tags(&frame);
    }

    fn disarm(&mut self) {
        self.frame = None;
    }
}

impl Drop for InFlightFrame {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            dead_letter(frame, self.addr, "frame never reached the transport");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId,
        cluster::{
            endpoint::{EndpointConfig, EndpointInner, FormError},
            frame::{Frame, Handshake, HandshakeError, HandshakeIntent, RefusalReason},
            membership::{self, JoinError, JoinRound, MemberState, WireMember},
            node::NodeId,
            peer::{
                Admission, ConnectError, DialRequest, JoinRequest, WriterEnd, admit_inbound,
                exchange_handshakes, identity_binds, join_loop, recv_handshake, write_frames,
                write_streams,
            },
            transport::{
                ConnectedControl, Connection, FrameReceiver, FrameSender, PeerIdentity, Transport,
                TransportError,
            },
        },
        quota::Quota,
    };
    use std::{
        borrow::Cow,
        collections::VecDeque,
        future::pending,
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
        sync::{Arc, mpsc},
        time::Duration,
    };
    use tokio::{
        sync::{
            Notify,
            mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
            oneshot,
        },
        task::{self, yield_now},
        time::timeout,
    };

    struct FakeReceiver {
        frames: VecDeque<Vec<u8>>,
        current: Vec<u8>,
    }

    impl FakeReceiver {
        fn new(frames: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                current: Vec::new(),
            }
        }
    }

    impl FrameReceiver for FakeReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            match self.frames.pop_front() {
                Some(frame) => {
                    self.current = frame;
                    Ok(Some(&self.current))
                }

                None => Ok(None),
            }
        }
    }

    /// Receives nothing, ever: the wire shape of a peer which connects and stays silent.
    struct SilentReceiver;

    impl FrameReceiver for SilentReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            std::future::pending().await
        }
    }

    struct RecordingSender(mpsc::Sender<Vec<u8>>);

    impl FrameSender for RecordingSender {
        async fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
            let _ = self.0.send(frame.to_vec());
            Ok(())
        }
    }

    enum CoordinatedSender {
        Block(Arc<Notify>),
        FailAfter(Arc<Notify>),
    }

    impl FrameSender for CoordinatedSender {
        async fn send(&mut self, _: &[u8]) -> Result<(), TransportError> {
            match self {
                CoordinatedSender::Block(started) => {
                    started.notify_one();
                    pending().await
                }
                CoordinatedSender::FailAfter(started) => {
                    started.notified().await;
                    Err(TransportError::other("connection lost"))
                }
            }
        }
    }

    fn peer() -> NodeId {
        NodeId::new("127.0.0.1:1234".parse().expect("valid address"))
    }

    fn identity(ip_addresses: impl IntoIterator<Item = IpAddr>) -> PeerIdentity {
        PeerIdentity {
            dns_names: Vec::new(),
            ip_addresses: ip_addresses.into_iter().collect(),
        }
    }

    /// A transport which proves no identity binds nothing: without mutual TLS there is no
    /// certificate to bind the advertised address against, and admission falls to the other
    /// checks.
    #[test]
    fn an_unproven_identity_binds_every_address() {
        assert!(identity_binds(peer(), None));
    }

    /// The advertised address must appear among the certificate's IP addresses, which is what
    /// stops a peer holding a valid certificate from claiming an address it was not issued for.
    #[test]
    fn an_identity_binds_only_its_own_addresses() {
        let peer = peer();
        let own = peer.addr().ip();
        let other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        assert!(identity_binds(peer, Some(identity([own]))));
        assert!(identity_binds(peer, Some(identity([other, own]))));

        assert!(!identity_binds(peer, Some(identity([other]))));
        assert!(!identity_binds(peer, Some(identity([]))));
    }

    /// Only the IP addresses bind: a certificate naming the peer in DNS alone proves nothing
    /// about the address it advertises, since that is what the lane connects back to.
    #[test]
    fn dns_names_do_not_bind_an_address() {
        let identity = PeerIdentity {
            dns_names: vec!["node.example.com".to_string()],
            ip_addresses: Vec::new(),
        };

        assert!(!identity_binds(peer(), Some(identity)));
    }

    /// A transport failure, a peer closing early, a peer staying silent or a refusal as an
    /// unknown member (gossip may not have converged yet) is worth another attempt, while a peer
    /// which does not speak this protocol never becomes one and a Down refusal is final.
    #[test]
    fn only_transient_failures_are_retried() {
        assert!(ConnectError::Transport(TransportError::other("boom")).is_retryable());
        assert!(ConnectError::Closed.is_retryable());
        assert!(ConnectError::DataStreams(1).is_retryable());
        assert!(ConnectError::HandshakeTimeout.is_retryable());
        assert!(ConnectError::Refused(RefusalReason::UnknownMember).is_retryable());

        assert!(
            !ConnectError::Transport(TransportError::FrameTooLarge { len: 2, max: 1 })
                .is_retryable()
        );

        assert!(!ConnectError::NotAHandshake.is_retryable());
        assert!(!ConnectError::Decode(postcard::Error::DeserializeUnexpectedEnd).is_retryable());
        assert!(!ConnectError::Handshake(HandshakeError::Magic(0)).is_retryable());
        assert!(!ConnectError::Handshake(HandshakeError::ProtocolVersion(u16::MAX)).is_retryable());
        assert!(!ConnectError::Refused(RefusalReason::Down).is_retryable());
    }

    /// The dial side's handshake wait is bounded like the accept side's: a peer which connects
    /// and never speaks times out into the retry path instead of pinning the lane's dial forever.
    #[tokio::test(start_paused = true)]
    async fn a_silent_peer_times_out_the_handshake() {
        let (sent_tx, _sent_rx) = mpsc::channel();
        let mut sender = RecordingSender(sent_tx);
        let mut receiver = SilentReceiver;

        let exchanged = exchange_handshakes(
            &mut sender,
            &mut receiver,
            peer(),
            HandshakeIntent::Member,
            Duration::from_secs(1),
        )
        .await;

        assert!(matches!(exchanged, Err(ConnectError::HandshakeTimeout)));
    }

    /// A peer answering within the deadline is handshaked normally, so the bound costs the happy
    /// path nothing but the timer.
    #[tokio::test]
    async fn handshakes_exchange_within_the_deadline() {
        let announced = peer();
        let bytes = Frame::Handshake(Handshake::new(announced, HandshakeIntent::Member))
            .encode_into(Vec::new())
            .expect("handshake encodes");
        let (sent_tx, sent_rx) = mpsc::channel();
        let mut sender = RecordingSender(sent_tx);
        let mut receiver = FakeReceiver::new([bytes]);

        let (exchanged, intent) = exchange_handshakes(
            &mut sender,
            &mut receiver,
            peer(),
            HandshakeIntent::Member,
            Duration::from_secs(1),
        )
        .await
        .expect("handshakes are exchanged");

        assert_eq!(exchanged, announced);
        assert_eq!(intent, HandshakeIntent::Member);
        assert_eq!(sent_rx.try_iter().count(), 1);
    }

    /// An oversize frame slipping past the send-time check dies in the writer, the last check on
    /// the path: it never reaches the transport, whose receiver's refusal would kill the
    /// connection, and the frames behind it still ride the stream.
    #[tokio::test]
    async fn an_oversize_frame_dies_in_the_writer() {
        let (outbound_tx, outbound_rx) = flume::unbounded();
        outbound_tx
            .send(Frame::Message {
                target: ActorId::new(),
                reply_tags: Vec::new(),
                payload: Cow::Owned(vec![0; 64]),
            })
            .expect("the queue is open");
        outbound_tx
            .send(Frame::Gossip {
                members: Vec::new(),
                more: false,
            })
            .expect("the queue is open");
        drop(outbound_tx);

        let (sent_tx, sent_rx) = mpsc::channel();
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let end = write_frames(
            RecordingSender(sent_tx),
            outbound_rx,
            Quota::unbounded(),
            addr,
            32,
        )
        .await;

        assert!(matches!(end, WriterEnd::LaneClosed));
        let sent = sent_rx.try_iter().collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0],
            Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .encode_into(Vec::new())
            .expect("gossip encodes")
        );
    }

    /// The first failed stream aborts its siblings. A sibling already suspended in transport send
    /// has dequeued its frame, so its in-flight guard must still turn that frame into a dead
    /// letter.
    #[tokio::test]
    async fn aborting_a_sibling_writer_dead_letters_its_in_flight_frame() {
        let target = ActorId::new();
        crate::sync::lock(&super::DEAD_LETTERS).remove(&target);
        let (blocked_tx, blocked_rx) = flume::unbounded();
        blocked_tx
            .send(Frame::Message {
                target,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[]),
            })
            .expect("blocked queue is open");
        let (failed_tx, failed_rx) = flume::unbounded();
        failed_tx
            .send(Frame::Gossip {
                members: Vec::new(),
                more: false,
            })
            .expect("failed queue is open");
        let started = Arc::new(Notify::new());
        let receivers = [blocked_rx, failed_rx];

        let end = write_streams(
            vec![
                CoordinatedSender::Block(started.clone()),
                CoordinatedSender::FailAfter(started),
            ],
            &receivers,
            &Quota::unbounded(),
            "127.0.0.1:1234".parse().expect("valid address"),
            1024,
        )
        .await;
        assert!(matches!(end, WriterEnd::ConnectionLost));
        assert!(crate::sync::lock(&super::DEAD_LETTERS).remove(&target));
    }

    #[tokio::test]
    async fn handshake_is_accepted_from_a_tellus_node() {
        let peer = peer();
        let bytes = Frame::Handshake(Handshake::new(peer, HandshakeIntent::Join))
            .encode_into(Vec::new())
            .expect("handshake encodes");
        let mut receiver = FakeReceiver::new([bytes]);

        let (handshaked, intent) = recv_handshake(&mut receiver)
            .await
            .expect("handshake is accepted");
        assert_eq!(handshaked, peer);
        assert_eq!(intent, HandshakeIntent::Join);
    }

    #[tokio::test]
    async fn a_closed_connection_is_reported_as_such() {
        let mut receiver = FakeReceiver::new([]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::Closed)
        ));
    }

    #[tokio::test]
    async fn a_first_frame_other_than_a_handshake_is_rejected() {
        let bytes = Frame::Gossip {
            members: Vec::new(),
            more: false,
        }
        .encode_into(Vec::new())
        .expect("gossip encodes");
        let mut receiver = FakeReceiver::new([bytes]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::NotAHandshake)
        ));
    }

    /// A refusal in place of the reply handshake surfaces as its reason, so the dialer can tell
    /// a not-yet-converged member (retry) from its own death (final).
    #[tokio::test]
    async fn a_refused_handshake_surfaces_its_reason() {
        let bytes = Frame::Refused {
            reason: RefusalReason::Down,
        }
        .encode_into(Vec::new())
        .expect("refusal encodes");
        let mut receiver = FakeReceiver::new([bytes]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::Refused(RefusalReason::Down))
        ));
    }

    #[tokio::test]
    async fn an_undecodable_first_frame_is_rejected() {
        let mut receiver = FakeReceiver::new([vec![0xff; 8]]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::Decode(_))
        ));
    }
    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    /// A transport handing every dial to the test, which answers it with a connection it scripts
    /// or never answers at all.
    struct ScriptedTransport(UnboundedSender<Dial>);

    struct Dial {
        addr: SocketAddr,
        answer: oneshot::Sender<ConnectedControl<ScriptedConnection>>,
    }

    impl Transport for ScriptedTransport {
        type Connection = ScriptedConnection;

        fn data_streams(&self) -> Option<NonZeroUsize> {
            None
        }

        async fn connect(
            &self,
            addr: SocketAddr,
            _: usize,
        ) -> Result<ConnectedControl<ScriptedConnection>, TransportError> {
            let (answer, answer_rx) = oneshot::channel();
            self.0
                .send(Dial { addr, answer })
                .expect("the test holds the dials");
            answer_rx
                .await
                .map_err(|_| TransportError::other(io::Error::other("dial dropped")))
        }

        async fn accept(&self, _: usize) -> Result<ScriptedConnection, TransportError> {
            pending().await
        }
    }

    struct ScriptedConnection;

    impl Connection for ScriptedConnection {
        type Sender = ChannelSender;
        type Receiver = ChannelReceiver;

        async fn accept_control(&self) -> Result<(ChannelSender, ChannelReceiver), TransportError> {
            unreachable!("the dialing side opens the control stream")
        }

        async fn open_data(&self) -> Result<ChannelSender, TransportError> {
            unreachable!("a transport without data streams")
        }

        async fn accept_data(&self) -> Result<Option<ChannelReceiver>, TransportError> {
            pending().await
        }
    }

    struct ChannelSender(UnboundedSender<Vec<u8>>);

    impl FrameSender for ChannelSender {
        async fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
            self.0
                .send(frame.to_vec())
                .map_err(|_| TransportError::other(io::Error::other("stream closed")))
        }
    }

    struct ChannelReceiver {
        frames: UnboundedReceiver<Vec<u8>>,
        current: Vec<u8>,
    }

    impl FrameReceiver for ChannelReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            match self.frames.recv().await {
                Some(frame) => {
                    self.current = frame;
                    Ok(Some(&self.current))
                }

                None => Ok(None),
            }
        }
    }

    /// The test's end of a scripted connection: what the joiner sent and the frames to answer
    /// with. Dropping it cuts the connection.
    struct Acceptor {
        from_joiner: UnboundedReceiver<Vec<u8>>,
        to_joiner: UnboundedSender<Vec<u8>>,
    }

    impl Acceptor {
        fn connected() -> (ConnectedControl<ScriptedConnection>, Acceptor) {
            let (to_joiner, joiner_rx) = unbounded_channel();
            let (joiner_tx, from_joiner) = unbounded_channel();
            let connected = ConnectedControl {
                connection: ScriptedConnection,
                control_tx: ChannelSender(joiner_tx),
                control_rx: ChannelReceiver {
                    frames: joiner_rx,
                    current: Vec::new(),
                },
            };

            (
                connected,
                Acceptor {
                    from_joiner,
                    to_joiner,
                },
            )
        }

        async fn joiner_handshake(&mut self) -> HandshakeIntent {
            let bytes = self
                .from_joiner
                .recv()
                .await
                .expect("the joiner sends a handshake");
            match Frame::from_bytes(&bytes).expect("a frame") {
                Frame::Handshake(handshake) => handshake.validate().expect("a valid handshake").1,
                _ => panic!("not a handshake"),
            }
        }

        fn admit(&self, addr: SocketAddr) {
            self.send(&Frame::Handshake(Handshake::new(
                NodeId::new(addr),
                HandshakeIntent::Member,
            )));
        }

        fn gossip(&self, members: Vec<WireMember>, more: bool) {
            self.send(&Frame::Gossip { members, more });
        }

        fn refuse(&self, reason: RefusalReason) {
            self.send(&Frame::Refused { reason });
        }

        fn send(&self, frame: &Frame<'_>) {
            self.to_joiner
                .send(frame.encode_into(Vec::new()).expect("frame encodes"))
                .expect("the joiner reads");
        }
    }

    /// A leaked endpoint with its join loop running against a scripted transport. The lane dial
    /// requests must stay receivable, since applying a snapshot opens lanes towards the members.
    struct Joining {
        endpoint: &'static EndpointInner,
        dials: UnboundedReceiver<Dial>,
        _lane_dials: flume::Receiver<DialRequest>,
    }

    fn joining(port: u16) -> Joining {
        let (endpoint, lane_dials, join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(port)));
        let (dial_tx, dials) = unbounded_channel();
        task::spawn(join_loop(
            Arc::new(ScriptedTransport(dial_tx)),
            join_requests,
            endpoint,
        ));

        Joining {
            endpoint,
            dials,
            _lane_dials: lane_dials,
        }
    }

    /// The first verdict of forming once no join attempt is in flight anymore.
    async fn form_once_settled(endpoint: &'static EndpointInner) -> Result<(), FormError> {
        timeout(Duration::from_secs(5), async {
            loop {
                match endpoint.form_cluster() {
                    Err(FormError::JoinInFlight) => yield_now().await,
                    verdict => return verdict,
                }
            }
        })
        .await
        .expect("the join attempt finishes")
    }

    /// A node which is no cluster admits nobody, so nothing can join it before it has formed or
    /// joined; once formed, it admits.
    #[tokio::test]
    async fn an_unformed_endpoint_refuses_joins_as_no_cluster() {
        let (endpoint, _lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let joiner = NodeId::new(addr(2));

        assert!(matches!(
            admit_inbound(joiner, HandshakeIntent::Join, None, endpoint),
            Admission::Refuse(RefusalReason::NoCluster)
        ));

        endpoint.form_cluster().expect("an unformed endpoint forms");
        assert!(matches!(
            admit_inbound(joiner, HandshakeIntent::Join, None, endpoint),
            Admission::Admit(_)
        ));
    }

    /// A snapshot cut before its last chunk is applied not at all, and the address stays pinned:
    /// the cluster admitted this node, so it may count it already.
    #[tokio::test]
    async fn a_truncated_snapshot_is_cluster_seen_and_applies_nothing() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        assert_eq!(dial.addr, seed);
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        assert_eq!(acceptor.joiner_handshake().await, HandshakeIntent::Join);
        acceptor.admit(seed);
        acceptor.gossip(
            vec![WireMember {
                node: NodeId::new(addr(9)),
                state: MemberState::Up,
            }],
            true,
        );
        drop(acceptor);

        let round = round
            .await
            .expect("the round completes")
            .expect("not downed");
        assert!(matches!(round, JoinRound::ClusterSeen(pinned) if pinned == seed));
        assert_eq!(endpoint.pinned_cluster(), Some(seed));
        assert!(!endpoint.formed());
        let members = endpoint.membership().members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].addr(), addr(1));
        assert_eq!(members[0].state(), MemberState::Up);
        assert!(matches!(
            endpoint.form_cluster(),
            Err(FormError::ClusterPinned(pinned)) if pinned == seed
        ));
    }

    /// The caller going away after the cluster answered cannot discard what the endpoint learned:
    /// the attempt runs on, and the pin it earned stands once it has finished.
    #[tokio::test]
    async fn a_cancelled_caller_cannot_discard_earned_evidence() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);
        let (result_tx, result_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: seed,
            result_tx,
        });

        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        drop(result_rx);
        drop(acceptor);

        assert!(matches!(
            form_once_settled(endpoint).await,
            Err(FormError::ClusterPinned(pinned)) if pinned == seed
        ));
        assert!(!endpoint.formed());
    }

    /// A connect which never resolves is abandoned with its caller, since nothing has been sent
    /// yet, and releases the permit; a connect which is still awaited holds it.
    #[tokio::test]
    async fn a_hung_connect_is_released_with_its_caller() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let (result_tx, result_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: addr(2),
            result_tx,
        });
        let dial = joining.dials.recv().await.expect("a dial");

        assert!(matches!(
            endpoint.form_cluster(),
            Err(FormError::JoinInFlight)
        ));

        drop(result_rx);
        assert!(form_once_settled(endpoint).await.is_ok());
        drop(dial);
    }

    /// Attempts are exclusive: the second is not even dialed until the first has resolved.
    #[tokio::test]
    async fn a_second_join_waits_for_the_first() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, _second_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: addr(2),
            result_tx: first_tx,
        });
        endpoint.request_join(JoinRequest {
            addr: addr(3),
            result_tx: second_tx,
        });

        let first = joining.dials.recv().await.expect("the first dial");
        assert_eq!(first.addr, addr(2));
        assert!(
            timeout(Duration::from_millis(100), joining.dials.recv())
                .await
                .is_err(),
            "the second attempt dialed while the first was in flight"
        );

        let (connected, mut acceptor) = Acceptor::connected();
        assert!(first.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.refuse(RefusalReason::NoCluster);
        assert!(matches!(
            first_rx.await,
            Ok(Err(ConnectError::Refused(RefusalReason::NoCluster)))
        ));

        let second = joining.dials.recv().await.expect("the second dial");
        assert_eq!(second.addr, addr(3));
    }

    /// A complete snapshot which lists this incarnation as Down downs this node, and the join
    /// reports that rather than success.
    #[tokio::test]
    async fn a_snapshot_downing_this_incarnation_returns_downed() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        acceptor.gossip(
            vec![
                WireMember {
                    node: NodeId::new(seed),
                    state: MemberState::Up,
                },
                WireMember {
                    node: endpoint.node(),
                    state: MemberState::Down,
                },
            ],
            false,
        );

        assert!(matches!(
            round.await.expect("the round completes"),
            Err(JoinError::Downed)
        ));
        assert!(endpoint.downed());
        assert!(matches!(endpoint.form_cluster(), Err(FormError::Downed)));
    }
}
