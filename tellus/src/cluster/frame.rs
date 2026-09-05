use crate::{
    ActorId,
    cluster::{
        discovery::{LookupResult, Nonce, WireKey},
        membership::WireMember,
        node::NodeId,
        reachability::WireReachability,
        reply::ReplyTag,
    },
};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use thiserror::Error;

const PROTOCOL_MAGIC: u32 = 0x574C_545A;
const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Frame<'a> {
    Handshake(Handshake),

    Message {
        target: ActorId,
        reply_tags: Vec<ReplyTag>,
        #[serde(borrow, with = "payload_bytes")]
        payload: Cow<'a, [u8]>,
    },

    Watch {
        target: ActorId,
        watcher: ActorId,
    },

    Unwatch {
        target: ActorId,
        watcher: ActorId,
    },

    Terminated {
        target: ActorId,
        watcher: ActorId,
    },

    Lookup {
        nonce: Nonce,
        key: WireKey,
    },

    LookupReply {
        nonce: Nonce,
        result: LookupResult,
    },

    Reply {
        nonce: Nonce,
        recipient: Option<ActorId>,
        #[serde(borrow, with = "payload_bytes")]
        payload: Cow<'a, [u8]>,
    },

    ReplyDropped {
        nonce: Nonce,
        recipient: Option<ActorId>,
    },

    Gossip {
        members: Vec<WireMember>,
        more: bool,
    },

    Refused {
        reason: RefusalReason,
    },

    Reachability {
        observations: Vec<WireReachability>,
    },
}

mod payload_bytes {
    use serde::{
        Deserializer, Serializer,
        de::{Error, Visitor},
    };
    use std::{borrow::Cow, fmt::Formatter};

    pub(super) fn serialize<S>(payload: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(payload)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Cow<'de, [u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(BytesVisitor)
    }

    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Cow<'de, [u8]>;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a byte payload")
        }

        fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Cow::Borrowed(value))
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Cow::Owned(value.to_vec()))
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Cow::Owned(value))
        }
    }
}

impl<'a> Frame<'a> {
    pub(crate) fn encode_into(&self, mut buffer: Vec<u8>) -> Result<Vec<u8>, postcard::Error> {
        buffer.clear();
        postcard::to_extend(self, buffer)
    }

    pub(crate) fn encoded_len(&self) -> Result<usize, postcard::Error> {
        postcard::experimental::serialized_size(self)
    }

    pub(crate) fn from_bytes(bytes: &'a [u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    pub(crate) fn is_counted(&self) -> bool {
        match self {
            Frame::Message { .. } | Frame::Reply { .. } => true,

            Frame::Handshake(_)
            | Frame::Watch { .. }
            | Frame::Unwatch { .. }
            | Frame::Terminated { .. }
            | Frame::Lookup { .. }
            | Frame::LookupReply { .. }
            | Frame::ReplyDropped { .. }
            | Frame::Gossip { .. }
            | Frame::Refused { .. }
            | Frame::Reachability { .. } => false,
        }
    }

    pub(crate) fn stream_key(&self) -> Option<StreamKey> {
        match self {
            Frame::Message { target, .. } => Some(StreamKey::Actor(*target)),

            Frame::Terminated { watcher, .. } => Some(StreamKey::Actor(*watcher)),

            Frame::Reply {
                nonce, recipient, ..
            }
            | Frame::ReplyDropped { nonce, recipient } => {
                Some(recipient.map_or(StreamKey::Nonce(*nonce), StreamKey::Actor))
            }

            Frame::Handshake(_)
            | Frame::Watch { .. }
            | Frame::Unwatch { .. }
            | Frame::Lookup { .. }
            | Frame::LookupReply { .. }
            | Frame::Gossip { .. }
            | Frame::Refused { .. }
            | Frame::Reachability { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StreamKey {
    Actor(ActorId),
    Nonce(Nonce),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Handshake {
    magic: u32,
    protocol_version: u16,
    node: NodeId,
    intent: HandshakeIntent,
}

impl Handshake {
    pub(crate) fn new(node: NodeId, intent: HandshakeIntent) -> Self {
        Self {
            magic: PROTOCOL_MAGIC,
            protocol_version: PROTOCOL_VERSION,
            node,
            intent,
        }
    }

    pub(crate) fn validate(self) -> Result<(NodeId, HandshakeIntent), HandshakeError> {
        if self.magic != PROTOCOL_MAGIC {
            return Err(HandshakeError::Magic(self.magic));
        }
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(HandshakeError::ProtocolVersion(self.protocol_version));
        }
        Ok((self.node, self.intent))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum HandshakeIntent {
    Member,
    Join,
}

/// [RefusalReason::UnknownMember] and [RefusalReason::NoCluster] are worth retrying.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RefusalReason {
    #[display("not a member of this cluster")]
    UnknownMember,

    #[display("dead node incarnation")]
    Down,

    #[display("not a member of any cluster")]
    NoCluster,
}

#[derive(Debug, Error)]
pub(crate) enum HandshakeError {
    #[error("unexpected protocol magic {0:#010x}")]
    Magic(u32),

    #[error("unsupported protocol version {0}")]
    ProtocolVersion(u16),
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId,
        cluster::{
            discovery::{LookupResult, Nonce, WireKey},
            frame::{
                Frame, Handshake, HandshakeError, HandshakeIntent, PROTOCOL_MAGIC,
                PROTOCOL_VERSION, RefusalReason, StreamKey,
            },
            membership::{MemberState, WireMember},
            node::NodeId,
            reachability::WireReachability,
            reply::ReplyTag,
        },
    };
    use std::borrow::Cow;

    /// A terminated signal rides the watcher's stream: routing it by target would break its
    /// ordering behind the messages the terminated actor sent to that watcher. A reply rides the
    /// stream of the actor it is delivered to, for the same reason.
    #[test]
    fn frames_key_on_the_actor_they_are_delivered_to() {
        let target = ActorId::new();
        let watcher = ActorId::new();

        let message = Frame::Message {
            target,
            reply_tags: Vec::new(),
            payload: Cow::Borrowed(&[]),
        };
        assert_eq!(message.stream_key(), Some(StreamKey::Actor(target)));
        assert_eq!(
            Frame::Terminated { target, watcher }.stream_key(),
            Some(StreamKey::Actor(watcher))
        );
        assert_eq!(
            Frame::Reply {
                nonce: Nonce::first(),
                recipient: Some(watcher),
                payload: Cow::Borrowed(&[]),
            }
            .stream_key(),
            Some(StreamKey::Actor(watcher))
        );
        assert_eq!(
            Frame::ReplyDropped {
                nonce: Nonce::first(),
                recipient: Some(watcher),
            }
            .stream_key(),
            Some(StreamKey::Actor(watcher))
        );
        assert_eq!(Frame::Watch { target, watcher }.stream_key(), None);
        assert_eq!(
            Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .stream_key(),
            None
        );
        assert_eq!(
            Frame::Refused {
                reason: RefusalReason::UnknownMember
            }
            .stream_key(),
            None
        );
    }

    /// A reply no actor awaits keys on its nonce, not on nothing: falling back to the control
    /// stream would put a user payload in front of gossip, watch and lookup frames.
    #[test]
    fn a_reply_without_a_recipient_keys_on_its_nonce() {
        let nonce = Nonce::first();

        assert_eq!(
            Frame::Reply {
                nonce,
                recipient: None,
                payload: Cow::Borrowed(&[]),
            }
            .stream_key(),
            Some(StreamKey::Nonce(nonce))
        );
        assert_eq!(
            Frame::ReplyDropped {
                nonce,
                recipient: None,
            }
            .stream_key(),
            Some(StreamKey::Nonce(nonce))
        );
    }

    /// Protocol version 1 pins the original frame discriminants to hardcoded wire bytes: a round
    /// trip cannot catch a format break, since reordered variants change both directions at once.
    #[test]
    fn gossip_matches_its_pinned_wire_bytes() {
        let bytes = [9, 0, 0];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        assert!(matches!(frame, Frame::Gossip { members, more } if members.is_empty() && !more));

        assert_eq!(
            Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .encode_into(Vec::new())
            .expect("frame encodes"),
            bytes
        );

        assert_eq!(
            Frame::Gossip {
                members: Vec::new(),
                more: true,
            }
            .encode_into(Vec::new())
            .expect("frame encodes"),
            [9, 0, 1]
        );

        let bytes = [10, 1];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        assert!(matches!(
            frame,
            Frame::Refused {
                reason: RefusalReason::Down
            }
        ));

        assert_eq!(
            Frame::Refused {
                reason: RefusalReason::Down
            }
            .encode_into(Vec::new())
            .expect("frame encodes"),
            bytes
        );
    }

    /// The frames delivered to an actor are pinned to hardcoded wire bytes too, and these are the
    /// ones that matter. Their field layout carries the ordering guarantees across the wire, since
    /// a terminated signal names its watcher and a reply its asker to ride the right stream.
    /// Decoding the IDs back out of the bytes they were built from catches a transposed `target`
    /// and `watcher`, which a round trip alone cannot see.
    #[test]
    fn delivered_frames_match_their_pinned_wire_bytes() {
        const TARGET: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
        const RECIPIENT: &str = "10111213-1415-1617-1819-1a1b1c1d1e1f";

        let bytes = [
            1, // Frame::Message
            16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // target
            0,  // no reply tags
            3, 1, 2, 3, // payload
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Message {
            target,
            reply_tags,
            payload,
        } = &frame
        else {
            panic!("not a message frame");
        };
        assert_eq!(target.to_string(), TARGET);
        assert!(reply_tags.is_empty());
        assert_eq!(payload.as_ref(), &[1, 2, 3][..]);
        assert!(matches!(payload, Cow::Borrowed(_)));
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        let bytes = [
            4, // Frame::Terminated
            16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // target
            16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // watcher
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Terminated { target, watcher } = &frame else {
            panic!("not a terminated frame");
        };
        assert_eq!(target.to_string(), TARGET);
        assert_eq!(watcher.to_string(), RECIPIENT);
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        let bytes = [
            7, // Frame::Reply
            0, // nonce
            1, // recipient: Some
            16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // recipient
            2, 4, 5, // payload
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Reply {
            nonce,
            recipient,
            payload,
        } = &frame
        else {
            panic!("not a reply frame");
        };
        assert_eq!(*nonce, Nonce::first());
        assert_eq!(
            recipient.expect("the reply names a recipient").to_string(),
            RECIPIENT
        );
        assert_eq!(payload.as_ref(), &[4, 5][..]);
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        let bytes = [
            7, // Frame::Reply
            0, // nonce
            0, // recipient: None
            2, 4, 5, // payload
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Reply { recipient, .. } = &frame else {
            panic!("not a reply frame");
        };
        assert_eq!(*recipient, None);
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);
    }

    /// A refusal's reason is a discriminant on the wire, so a variant may be appended but never
    /// reordered: a peer of another build reads these bytes, not the names.
    #[test]
    fn refusals_match_their_pinned_wire_bytes() {
        let reasons = [
            (RefusalReason::UnknownMember, 0),
            (RefusalReason::Down, 1),
            (RefusalReason::NoCluster, 2),
        ];

        for (reason, discriminant) in reasons {
            let bytes = [
                10, // Frame::Refused
                discriminant,
            ];

            let frame = Frame::from_bytes(&bytes).expect("frame decodes");
            let Frame::Refused { reason: decoded } = &frame else {
                panic!("not a refused frame");
            };
            assert_eq!(*decoded, reason);
            assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);
        }
    }

    /// Every frame survives a round trip through the wire format; a change to the variant order
    /// or to a field breaks this, which is what makes the format explicit rather than incidental.
    #[test]
    fn frames_round_trip() {
        let node = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let other = NodeId::new("127.0.0.1:5678".parse().expect("valid address"));
        let target = ActorId::new();
        let watcher = ActorId::new();

        let frames = [
            Frame::Handshake(Handshake::new(node, HandshakeIntent::Join)),
            Frame::Message {
                target,
                reply_tags: vec![ReplyTag {
                    nonce: Nonce::first(),
                    recipient: Some(watcher),
                }],
                payload: Cow::Borrowed(&[1, 2, 3]),
            },
            Frame::Watch { target, watcher },
            Frame::Unwatch { target, watcher },
            Frame::Terminated { target, watcher },
            Frame::Lookup {
                nonce: Nonce::first(),
                key: WireKey::new::<u64>("worker-pool"),
            },
            Frame::LookupReply {
                nonce: Nonce::first(),
                result: LookupResult::Found { id: target },
            },
            Frame::Reply {
                nonce: Nonce::first(),
                recipient: Some(watcher),
                payload: Cow::Borrowed(&[4, 5, 6]),
            },
            Frame::ReplyDropped {
                nonce: Nonce::first(),
                recipient: Some(watcher),
            },
            Frame::Gossip {
                members: vec![
                    WireMember {
                        node,
                        state: MemberState::Up,
                    },
                    WireMember {
                        node: other,
                        state: MemberState::Down,
                    },
                ],
                more: true,
            },
            Frame::Refused {
                reason: RefusalReason::UnknownMember,
            },
            Frame::Reachability {
                observations: vec![WireReachability {
                    observer: node,
                    subject: other,
                    version: 1,
                    reachable: false,
                }],
            },
        ];

        for frame in frames {
            let bytes = frame.encode_into(Vec::new()).expect("frame encodes");
            let decoded = Frame::from_bytes(&bytes).expect("frame decodes");
            assert_eq!(decoded, frame);
        }
    }

    /// Only message and reply frames count against the outbound capacity; system frames bypass
    /// it, since a terminated signal, like a reply-dropped notification, must never be dropped.
    #[test]
    fn only_message_and_reply_frames_are_counted() {
        let target = ActorId::new();
        let watcher = ActorId::new();

        assert!(
            Frame::Message {
                target,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[])
            }
            .is_counted()
        );
        assert!(
            Frame::Reply {
                nonce: Nonce::first(),
                recipient: Some(watcher),
                payload: Cow::Borrowed(&[]),
            }
            .is_counted()
        );
        assert!(
            !Frame::ReplyDropped {
                nonce: Nonce::first(),
                recipient: Some(watcher),
            }
            .is_counted()
        );
        assert!(!Frame::Watch { target, watcher }.is_counted());
        assert!(!Frame::Unwatch { target, watcher }.is_counted());
        assert!(!Frame::Terminated { target, watcher }.is_counted());
        assert!(
            !Frame::Lookup {
                nonce: Nonce::first(),
                key: WireKey::new::<u64>("worker-pool"),
            }
            .is_counted()
        );
        assert!(
            !Frame::LookupReply {
                nonce: Nonce::first(),
                result: LookupResult::NotFound,
            }
            .is_counted()
        );
        assert!(
            !Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .is_counted()
        );
        assert!(
            !Frame::Refused {
                reason: RefusalReason::Down
            }
            .is_counted()
        );
    }

    #[test]
    fn handshake_accepts_this_protocol() {
        let node = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));

        let (peer, intent) = Handshake::new(node, HandshakeIntent::Join)
            .validate()
            .expect("handshake is valid");
        assert_eq!(peer, node);
        assert_eq!(intent, HandshakeIntent::Join);
    }

    #[test]
    fn handshake_rejects_alien_magic() {
        let node = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let handshake = Handshake {
            magic: !PROTOCOL_MAGIC,
            protocol_version: PROTOCOL_VERSION,
            node,
            intent: HandshakeIntent::Member,
        };

        assert!(matches!(
            handshake.validate(),
            Err(HandshakeError::Magic(_))
        ));
    }

    #[test]
    fn handshake_rejects_other_protocol_versions() {
        let node = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let handshake = Handshake {
            magic: PROTOCOL_MAGIC,
            protocol_version: PROTOCOL_VERSION + 1,
            node,
            intent: HandshakeIntent::Member,
        };

        assert!(matches!(
            handshake.validate(),
            Err(HandshakeError::ProtocolVersion(_))
        ));
    }
}
