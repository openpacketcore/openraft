use std::borrow::Borrow;
use std::fmt;

use crate::display_ext::DisplayOptionExt;
use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::Vote;

/// An RPC sent by candidates to gather votes (§5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct VoteRequest<NID: NodeId> {
    pub vote: Vote<NID>,
    pub last_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> fmt::Display for VoteRequest<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{vote:{}, last_log:{}}}", self.vote, self.last_log_id.display(),)
    }
}

impl<NID: NodeId> MessageSummary<VoteRequest<NID>> for VoteRequest<NID> {
    fn summary(&self) -> String {
        self.to_string()
    }
}

impl<NID: NodeId> VoteRequest<NID> {
    pub fn new(vote: Vote<NID>, last_log_id: Option<LogId<NID>>) -> Self {
        Self { vote, last_log_id }
    }
}

/// The response to a `VoteRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct VoteResponse<NID: NodeId> {
    /// vote after a node handling vote-request.
    /// Thus `resp.vote >= req.vote` always holds.
    ///
    /// `vote` that equals the candidate.vote does not mean the vote is granted.
    /// The `vote` may be updated when a previous Leader sees a higher vote.
    pub vote: Vote<NID>,

    /// It is true if a node accepted and saved the VoteRequest.
    pub vote_granted: bool,

    /// The last log id stored on the remote voter.
    pub last_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> MessageSummary<VoteResponse<NID>> for VoteResponse<NID> {
    fn summary(&self) -> String {
        format!(
            "{{{}, last_log:{:?}}}",
            self.vote,
            self.last_log_id.as_ref().map(|x| x.to_string())
        )
    }
}

impl<NID> VoteResponse<NID>
where NID: NodeId
{
    pub fn new(vote: impl Borrow<Vote<NID>>, last_log_id: Option<LogId<NID>>, granted: bool) -> Self {
        Self {
            vote: vote.borrow().clone(),
            vote_granted: granted,
            last_log_id: last_log_id.map(|x| x.borrow().clone()),
        }
    }

    /// Returns `true` if the response indicates that the target node has granted a vote to the
    /// candidate.
    pub fn is_granted_to(&self, candidate_vote: &Vote<NID>) -> bool {
        &self.vote == candidate_vote
    }
}

impl<NID> fmt::Display for VoteResponse<NID>
where NID: NodeId
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{{}, last_log:{:?}}}",
            self.vote,
            self.last_log_id.as_ref().map(|x| x.to_string())
        )
    }
}

/// The outcome of a Pre-Vote RPC, as a network reports it.
///
/// See [`RaftNetwork::pre_vote()`](`crate::network::RaftNetwork::pre_vote`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum PreVoteReply<NID: NodeId> {
    /// The target answered whether it would grant the vote.
    Answered(VoteResponse<NID>),

    /// The target is not positively known to answer Pre-Vote, so nothing was sent to it.
    ///
    /// Such a target can still grant a vote, so the node runs the classic election for this
    /// campaign instead of a Pre-Vote round that could never reach a quorum without it.
    Unsupported,
}

impl<NID> fmt::Display for PreVoteReply<NID>
where NID: NodeId
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Answered(resp) => write!(f, "Answered{}", resp),
            Self::Unsupported => write!(f, "Unsupported"),
        }
    }
}
