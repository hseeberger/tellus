//! The driver establishing connections between endpoints: [Transport] accepts and dials,
//! [Connection] splits into a [FrameSender] and a [FrameReceiver] per stream, and [PeerIdentity]
//! carries what mutual TLS proved about the peer. The provided implementation is
//! [QuicTransport]; one is passed to
//! [start_endpoint](crate::cluster::start_endpoint) per process.

mod quic;

pub use crate::cluster::transport::quic::{
    QuicConfig, QuicConnection, QuicFrameReceiver, QuicFrameSender, QuicTransport,
    QuicTransportError,
};

use std::{
    error::Error,
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
};
use thiserror::Error as ThisError;

/// A driver establishing connections between remoting endpoints; the provided one is
/// [QuicTransport].
#[trait_variant::make(Send)]
pub trait Transport
where
    Self: Send + Sync + 'static,
{
    /// The type of the connections established by this transport.
    type Connection: Connection;

    /// The number of data streams this transport can open per connection, `None` for a transport
    /// without streams, which makes every frame ride the control stream. Queried once when the
    /// endpoint starts, so it must not vary between connections.
    fn data_streams(&self) -> Option<NonZeroUsize>;

    /// Connect to the node listening at the given address, establishing the connection and
    /// opening its bidirectional control stream, whose halves are returned alongside. Frames
    /// larger than `max_frame_size` bytes are refused on every stream of the connection.
    ///
    /// Must bound itself: a silent address which never resolves stalls the join attempt holding
    /// it, and joins run one at a time per endpoint. [QuicTransport]
    /// inherits QUIC's handshake timeout, tens of seconds, which a deployment wanting a seed list
    /// walked briskly wraps in a shorter one.
    async fn connect(
        &self,
        addr: SocketAddr,
        max_frame_size: usize,
    ) -> Result<ConnectedControl<Self::Connection>, TransportError>;

    /// Wait for the next inbound connection and establish it; frames larger than
    /// `max_frame_size` bytes are refused on every stream of the connection. An error means the
    /// transport can accept no further connections at all; the failure of one inbound connection
    /// must be handled internally instead of being returned.
    async fn accept(&self, max_frame_size: usize) -> Result<Self::Connection, TransportError>;
}

/// A [Connection] fresh from [Transport::connect], carrying the halves of its already opened
/// control stream.
pub struct ConnectedControl<C>
where
    C: Connection,
{
    /// The established connection.
    pub connection: C,

    /// The sending half of the control stream.
    pub control_tx: C::Sender,

    /// The receiving half of the control stream.
    pub control_rx: C::Receiver,
}

/// An established connection produced by a [Transport], carrying its maximum frame size: one
/// control stream plus as many data streams as the transport supports, each ordered on its own
/// and unordered against the others.
#[trait_variant::make(Send)]
pub trait Connection
where
    Self: Send + Sync + 'static,
{
    /// The sending half of a stream.
    type Sender: FrameSender;

    /// The receiving half of a stream.
    type Receiver: FrameReceiver;

    /// Await the bidirectional control stream, which the dialing side has opened via
    /// [Transport::connect]. Only called on an accepted connection, exactly once and before any
    /// data stream.
    async fn accept_control(&self) -> Result<(Self::Sender, Self::Receiver), TransportError>;

    /// Open one sending data stream. Only called if [Transport::data_streams] is `Some`.
    async fn open_data(&self) -> Result<Self::Sender, TransportError>;

    /// Accept the next data stream the peer opens; `None` once the connection is gone.
    async fn accept_data(&self) -> Result<Option<Self::Receiver>, TransportError>;

    /// The identity the peer's certificate proves, `None` where there is none, e.g. for a dialer
    /// not asked for a certificate. Where an identity exists, the endpoint admits the peer only
    /// if the address it advertises is among the identity's IP addresses; see docs/cluster.md
    /// on trust.
    fn peer_identity(&self) -> Option<PeerIdentity> {
        None
    }
}

/// The names a peer's certificate is issued for, parsed from its subject alternative names: what
/// binds an authenticated peer to the address it advertises.
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    /// The DNS names of the certificate.
    pub dns_names: Vec<String>,

    /// The IP addresses of the certificate.
    pub ip_addresses: Vec<IpAddr>,
}

/// The sending half of a [Connection]'s stream.
#[trait_variant::make(Send)]
pub trait FrameSender
where
    Self: Send + 'static,
{
    /// Send one frame; the bytes are only read, never retained. Frames sent on one sender must be
    /// received by the peer in send order or the connection must die; a transport must never
    /// reorder them. Frames sent on different senders are unordered against each other.
    async fn send(&mut self, frame: &[u8]) -> Result<(), TransportError>;
}

/// The receiving half of a [Connection]'s stream.
#[trait_variant::make(Send)]
pub trait FrameReceiver
where
    Self: Send + 'static,
{
    /// Receive the next frame; `None` once the peer has closed the stream. The bytes borrow the
    /// receiver, valid until the next call, so a frame can be decoded without a copy per frame.
    async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError>;
}

/// A connection or frame which a [Transport] cannot carry.
#[derive(Debug, ThisError)]
pub enum TransportError {
    /// A frame announcing more bytes than the maximum frame size: a protocol violation of the
    /// peer rather than a transient failure, hence not worth a reconnect.
    #[error("frame of {len} bytes exceeds the maximum frame size of {max} bytes")]
    FrameTooLarge {
        /// The length the frame announced.
        len: usize,

        /// The maximum frame size of the connection.
        max: usize,
    },

    /// The driver's failure whatever its kind: I/O, TLS, protocol or otherwise. Reach the cause
    /// via [TransportError::downcast_ref].
    #[error(transparent)]
    Other(Box<dyn Error + Send + Sync>),
}

impl TransportError {
    /// Wrap any error as a transport failure.
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        Self::Other(error.into())
    }

    /// The wrapped driver failure, if there is one and it is of the given type: the transparent
    /// `source` of [TransportError::Other] skips the wrapped error itself, so it cannot be
    /// reached through the error chain.
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: Error + 'static,
    {
        match self {
            TransportError::Other(error) => error.downcast_ref(),
            TransportError::FrameTooLarge { .. } => None,
        }
    }
}

impl From<io::Error> for TransportError {
    fn from(error: io::Error) -> Self {
        Self::other(error)
    }
}
