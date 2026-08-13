use crate::{
    ActorSystem, actor_system,
    cluster::endpoint::{self, EndpointInner},
};
use std::time::Duration;
use thiserror::Error;
use tokio::time::{sleep, timeout};
use tracing::warn;

const DRAIN_POLL: Duration = Duration::from_millis(5);

/// The cluster cannot be left.
#[derive(Debug, Error)]
pub enum LeaveError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// The actor system did not terminate cleanly; only [leave_on_terminated] returns this, and
    /// only after the departure was announced anyway.
    #[error(transparent)]
    Terminated(#[from] actor_system::TerminatedError),
}

/// Wait until the given actor system's root actor and all its descendants have terminated, then
/// [leave] the cluster: what a process calls instead of [ActorSystem::terminated] where that
/// system's lifetime is the node's lifetime.
///
/// Waiting first is what makes the real terminated signals the normal case: once a root has
/// terminated, every local actor has run its destructors and every terminated signal owed to a
/// remote watcher is already queued, so the departure is queued behind them and the drain covers
/// both. See docs/cluster.md for the race which remains.
pub async fn leave_on_terminated<M>(system: ActorSystem<M>) -> Result<(), LeaveError>
where
    M: Send + 'static,
{
    let terminated = system.terminated().await;
    leave().await?;
    terminated.map_err(LeaveError::from)
}

/// Leave the cluster: gossip this node's own entry as [Down](crate::cluster::MemberState::Down),
/// wait until the departure has left the outbound queues, then sever every connection. A member
/// receiving it runs the ordinary node death sequence for this node right away, instead of
/// waiting out failure detection plus downing as it does for a node which merely fell silent.
///
/// Leaving is terminal for this process, exactly as being downed is: nothing is sent or admitted
/// afterwards, [join](crate::cluster::join) fails as [Downed](crate::cluster::JoinError::Downed),
/// and only a restarted process, with a fresh incarnation, rejoins, so the caller is expected to
/// exit. Leaving again, or leaving after this node was downed, is a no-op.
///
/// The drain proves the departure left this node's queues, not that a peer received it, and it is
/// bounded by [leave_timeout](crate::cluster::EndpointConfig::leave_timeout). A member which is
/// gone already costs that timeout and learns of the departure from another member's gossip, or,
/// with no live path at all, from its own failure detection, exactly as it would from a crash.
pub async fn leave() -> Result<(), LeaveError> {
    let endpoint = endpoint::get().ok_or(LeaveError::EndpointNotStarted)?;
    if endpoint.downed() {
        return Ok(());
    }

    // Announce before the latch, since a Down endpoint sends nothing, and drain before the
    // sever, since a sever turns whatever is still queued into dead letters.
    endpoint.membership().down(endpoint.node());
    endpoint.announce();

    if timeout(endpoint.config().leave_timeout, drain(endpoint))
        .await
        .is_err()
    {
        warn!("leaving with the outbound queues not drained");
    }

    endpoint.leave_down();
    Ok(())
}

/// Two consecutive empty observations: an empty queue only proves the writer dequeued the frame.
async fn drain(endpoint: &'static EndpointInner) {
    let mut was_drained = false;

    loop {
        let drained = endpoint.outbound_drained();
        if drained && was_drained {
            return;
        }

        was_drained = drained;
        sleep(DRAIN_POLL).await;
    }
}
