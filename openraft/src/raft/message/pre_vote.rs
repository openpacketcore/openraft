use std::borrow::Borrow;
use std::fmt;

use crate::display_ext::DisplayOptionExt;
use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::Vote;

/// A read-only inquiry before starting an ordinary election.
///
/// `vote` is a proposed future vote. Receiving this request must not change the
/// receiver's vote, term, leader lease, election timer or persistent storage.
/// Transport implementations must use a distinct RPC from [`super::VoteRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct PreVoteRequest<NID: NodeId> {
    /// The uncommitted future vote the sender proposes to request.
    pub vote: Vote<NID>,
    /// The sender's last log ID, using the ordinary voting freshness rule.
    pub last_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> PreVoteRequest<NID> {
    /// Construct a speculative inquiry without casting a real vote.
    pub fn new(vote: Vote<NID>, last_log_id: Option<LogId<NID>>) -> Self {
        Self { vote, last_log_id }
    }
}

impl<NID: NodeId> fmt::Display for PreVoteRequest<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{proposed_vote:{}, last_log:{}}}",
            self.vote,
            self.last_log_id.display()
        )
    }
}

impl<NID: NodeId> MessageSummary<PreVoteRequest<NID>> for PreVoteRequest<NID> {
    fn summary(&self) -> String {
        self.to_string()
    }
}

/// A speculative grant, which never represents a persisted vote.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct PreVoteResponse<NID: NodeId> {
    /// The receiver's actual vote, never the request's proposed future vote.
    pub vote: Vote<NID>,
    /// Whether the receiver would grant the proposed vote at the time of inquiry.
    pub vote_granted: bool,
    /// The receiver's last log ID at the time of the inquiry.
    pub last_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> PreVoteResponse<NID> {
    /// Construct a reply from actual receiver state and a speculative decision.
    pub fn new(vote: impl Borrow<Vote<NID>>, last_log_id: Option<LogId<NID>>, vote_granted: bool) -> Self {
        Self {
            vote: vote.borrow().clone(),
            vote_granted,
            last_log_id,
        }
    }
}

impl<NID: NodeId> fmt::Display for PreVoteResponse<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{actual_vote:{}, granted:{}, last_log:{}}}",
            self.vote,
            self.vote_granted,
            self.last_log_id.display()
        )
    }
}

impl<NID: NodeId> MessageSummary<PreVoteResponse<NID>> for PreVoteResponse<NID> {
    fn summary(&self) -> String {
        self.to_string()
    }
}
