//! The Pre-Vote rounds of a node that may still collect replies.

use std::time::Duration;

use crate::proposer::Candidate;
use crate::proposer::LeaderQuorumSet;
use crate::type_config::alias::InstantOf;
use crate::RaftTypeConfig;

/// One Pre-Vote round.
#[derive(Debug)]
pub(crate) struct PreVoteRound<C>
where C: RaftTypeConfig
{
    /// The identifier that every reply to this round names.
    pub(crate) id: u64,

    /// The quorum tracker of this round. Its vote is the hypothetical next-term vote, never
    /// persisted.
    pub(crate) candidate: Candidate<C, LeaderQuorumSet<C::NodeId>>,

    /// Whether a voter rejected this round.
    pub(crate) rejected: bool,
}

/// The Pre-Vote rounds of a node that may still collect replies, oldest first.
///
/// A round stays open until its replies are due, one Pre-Vote deadline after it started: a request
/// that is not answered by then never is, and no reply to an open round is discarded. While the
/// latest round awaits replies and no voter rejected it, no new round starts. Once a voter rejects
/// it, a new round may start after the width of the election-timeout window, and the rejected
/// round stays open beside it: a late grant still counts for the round it answers.
///
/// Rounds in one term propose the same hypothetical vote, so every reply names its round by an
/// identifier. Accepting a vote, campaigning or leading ends every open round.
#[derive(Debug)]
pub(crate) struct PreVoteRounds<C>
where C: RaftTypeConfig
{
    rounds: Vec<PreVoteRound<C>>,

    /// The identifier of the latest round started, open or not.
    latest_id: u64,
}

impl<C> Default for PreVoteRounds<C>
where C: RaftTypeConfig
{
    fn default() -> Self {
        Self {
            rounds: Vec::new(),
            latest_id: 0,
        }
    }
}

impl<C> PreVoteRounds<C>
where C: RaftTypeConfig
{
    /// Open a new round tracked by `candidate`, and return its identifier.
    pub(crate) fn open(&mut self, candidate: Candidate<C, LeaderQuorumSet<C::NodeId>>) -> u64 {
        self.latest_id = self.latest_id.wrapping_add(1);
        self.rounds.push(PreVoteRound {
            id: self.latest_id,
            candidate,
            rejected: false,
        });
        self.latest_id
    }

    /// The identifier of the latest round started.
    pub(crate) fn latest_id(&self) -> u64 {
        self.latest_id
    }

    /// The latest round, if it is still open.
    pub(crate) fn latest(&self) -> Option<&PreVoteRound<C>> {
        self.rounds.last().filter(|round| round.id == self.latest_id)
    }

    pub(crate) fn latest_mut(&mut self) -> Option<&mut PreVoteRound<C>> {
        let latest_id = self.latest_id;
        self.rounds.last_mut().filter(|round| round.id == latest_id)
    }

    /// The open round `id`.
    pub(crate) fn get_mut(&mut self, id: u64) -> Option<&mut PreVoteRound<C>> {
        self.rounds.iter_mut().find(|round| round.id == id)
    }

    pub(crate) fn is_open(&self, id: u64) -> bool {
        self.rounds.iter().any(|round| round.id == id)
    }

    /// The number of open rounds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.rounds.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.rounds.is_empty()
    }

    /// End every open round.
    pub(crate) fn clear(&mut self) {
        self.rounds.clear();
    }

    /// Close the rounds whose replies are due by `now`, `deadline` after they started.
    pub(crate) fn close_due(&mut self, now: InstantOf<C>, deadline: Duration) {
        self.rounds.retain(|round| now < round.candidate.starting_time() + deadline);
    }

    /// Whether a new round may start at `now`.
    ///
    /// No round may start while the latest one awaits replies that no voter has rejected yet, until
    /// they are due `deadline` after it started. Once a voter rejected it, a new round may start
    /// `window` after it started.
    pub(crate) fn next_round_due(&mut self, now: InstantOf<C>, window: Duration, deadline: Duration) -> bool {
        self.close_due(now, deadline);
        match self.latest() {
            None => true,
            Some(latest) => latest.rejected && now >= latest.candidate.starting_time() + window,
        }
    }
}
