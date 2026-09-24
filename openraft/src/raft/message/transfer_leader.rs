//! An engine-issued request for a planned leadership handoff.

use crate::LogId;
use crate::NodeId;
use crate::Vote;

/// A planned handoff bound to one committed leader vote and membership.
///
/// Created by [`crate::Raft::begin_leadership_transfer`]. The application must
/// authenticate delivery as the node named by `from`, using its ordinary
/// consensus transport. This request does not change membership or authorize
/// any application write. Acceptance is not evidence that a successor won an
/// election; observe fresh successor leadership before stopping the old voter.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct TransferLeaderRequest<NID: NodeId> {
    pub(crate) from: Vote<NID>,
    pub(crate) to: NID,
    pub(crate) last_log_id: Option<LogId<NID>>,
    pub(crate) membership_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> TransferLeaderRequest<NID> {
    /// The committed vote of the leader that stopped admitting proposals.
    pub fn from(&self) -> &Vote<NID> {
        &self.from
    }

    /// The intended successor, which must be a voter in the exact membership.
    pub fn to(&self) -> &NID {
        &self.to
    }

    /// The log prefix the successor must have applied before campaigning.
    pub fn last_log_id(&self) -> Option<&LogId<NID>> {
        self.last_log_id.as_ref()
    }

    /// The stable membership under which the handoff was initiated.
    pub fn membership_log_id(&self) -> Option<&LogId<NID>> {
        self.membership_log_id.as_ref()
    }
}

/// A non-fatal refusal to initiate or accept a planned leadership handoff.
///
/// Every refusal leaves the local vote, lease, membership and log unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum TransferLeaderError {
    /// The initiator is not the current committed leader.
    #[error("leadership transfer requires the current leader")]
    NotLeader,
    /// Delivery to the issuing leader cannot release a lease before admission stops.
    #[error("leadership transfer has not stopped leader admission")]
    NotPrepared,
    /// The target is this leader, absent, or not a voter.
    #[error("leadership transfer target is not an eligible voter")]
    InvalidTarget,
    /// A different handoff has already stopped admission in this term.
    #[error("leadership transfer is already in progress")]
    TransferInProgress,
    /// The membership is joint, uncommitted, or differs from the request.
    #[error("leadership transfer membership is not stable and exact")]
    MembershipChanged,
    /// The request is not bound to the receiver's exact committed vote.
    #[error("leadership transfer vote is not current")]
    VoteChanged,
    /// The intended successor has not applied the required log prefix.
    #[error("leadership transfer target has not applied the required log")]
    LogNotApplied,
    /// The intended successor has already prepared for its own shutdown.
    #[error("leadership transfer target is retiring")]
    Retiring,
}
