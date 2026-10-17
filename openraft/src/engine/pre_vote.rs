//! Volatile quorum inquiries before changing a term (Raft dissertation §9.6).

use crate::engine::Command;
use crate::engine::Engine;
use crate::progress::Progress;
use crate::proposer::Candidate;
use crate::proposer::LeaderQuorumSet;
use crate::quorum::QuorumSet;
use crate::raft::PreVoteRequest;
use crate::raft::PreVoteResponse;
use crate::raft::VoteRequest;
use crate::raft_state::LogStateReader;
use crate::type_config::alias::InstantOf;
use crate::type_config::TypeConfigExt;
use crate::LogId;
use crate::RaftTypeConfig;
use crate::Vote;

#[derive(Debug)]
pub(crate) struct PreVote<C: RaftTypeConfig> {
    pub(crate) round: u64,
    pub(crate) started_at: InstantOf<C>,
    pub(crate) candidate: Candidate<C, LeaderQuorumSet<C::NodeId>>,
    deadline: InstantOf<C>,
    base_vote: Vote<C::NodeId>,
    vote_updated_at: Option<InstantOf<C>>,
    membership_log_id: Option<LogId<C::NodeId>>,
}

/// Evidence of an ordinary campaign, not an inferred capability or a grant.
/// A legacy voter cannot answer an inquiry, but it does still send real votes.
#[derive(Debug)]
pub(crate) struct RealCampaign<C: RaftTypeConfig> {
    vote: Vote<C::NodeId>,
    base_vote: Vote<C::NodeId>,
    vote_updated_at: Option<InstantOf<C>>,
    last_log_id: Option<LogId<C::NodeId>>,
    membership_log_id: Option<LogId<C::NodeId>>,
}

impl<C: RaftTypeConfig> Engine<C> {
    fn pre_vote_lease_active(&self, now: InstantOf<C>) -> bool {
        if !self.state.vote_ref().is_committed() || self.state.vote.lease_disabled() {
            return false;
        }
        let lease = self.config.timer_config.leader_lease;
        if self.state.vote_last_modified().is_some_and(|at| now <= at + lease) {
            return true;
        }
        // A leader does not receive its own heartbeats. Inspect recent quorum
        // acknowledgements without updating its clock progress. This only
        // suppresses disruptive inquiries; real votes retain their ordinary
        // persistence and quorum rules, independent of clock accuracy.
        self.leader.as_ref().is_some_and(|leader| {
            let progress = &leader.clock_progress;
            progress.quorum_set().is_quorum(
                progress.iter().filter_map(|(id, at)| {
                    (id == &self.config.id || at.is_some_and(|at| now <= at + lease)).then_some(id)
                }),
            )
        })
    }

    /// Inspect a speculative request without touching any local state.
    pub(crate) fn handle_pre_vote_req(&self, request: &PreVoteRequest<C::NodeId>) -> PreVoteResponse<C::NodeId> {
        let membership = self.state.membership_state.effective();
        let eligible = request.vote.leader_id().voted_for().as_ref().is_some_and(|id| membership.is_voter(id));
        let granted = membership.is_voter(&self.config.id)
            && eligible
            && !request.vote.is_committed()
            && &request.vote > self.state.vote_ref()
            && request.last_log_id.as_ref() >= self.state.last_log_id()
            && !self.pre_vote_lease_active(C::now());

        PreVoteResponse::new(self.state.vote_ref(), self.state.last_log_id().cloned(), granted)
    }

    /// Start one inquiry round. Real votes are still persisted by the ordinary election path.
    pub(crate) fn pre_elect(&mut self) {
        self.discard_invalid_pre_vote();
        if self.prepared_shutdown.is_some()
            || self.leader.is_some()
            || !self.state.membership_state.effective().is_voter(&self.config.id)
        {
            return;
        }

        self.config.resample_election_timeout::<C::AsyncRuntime>();
        // A live legacy voter with a shorter log cannot win or answer PreVote.
        // After one bounded inquiry round, join its observed real election.
        // Silence alone never takes this path, so isolated upgraded voters
        // continue to inquire without consuming terms. No speculative grant
        // is converted into an actual vote, and the ordinary persistence and
        // joint-quorum rules below are unchanged.
        if self.pre_vote.as_ref().is_some_and(|round| C::now() >= round.deadline)
            && !self.pre_vote_lease_active(C::now())
        {
            if let Some(campaign) = &self.real_campaign {
                self.elect_with_current_timeout_above(campaign.vote.leader_id().term);
                return;
            }
        }
        self.pre_vote_round = self.pre_vote_round.wrapping_add(1);
        self.candidate = None;

        let now = C::now();
        let request = PreVoteRequest::new(
            Vote::new(self.state.vote_ref().leader_id().term + 1, self.config.id.clone()),
            self.state.last_log_id().cloned(),
        );
        let membership = self.state.membership_state.effective();
        let mut candidate = Candidate::new(
            now,
            request.vote.clone(),
            request.last_log_id.clone(),
            membership.membership().to_quorum_set(),
            membership.membership().learner_ids(),
        );
        // This local grant is volatile too. A single voter still uses the same
        // persist-before-leadership path as every other real election.
        let quorum = candidate.grant_by(&self.config.id);
        self.pre_vote = Some(PreVote {
            round: self.pre_vote_round,
            started_at: now,
            deadline: now + self.config.timer_config.election_timeout,
            candidate,
            base_vote: self.state.vote_ref().clone(),
            vote_updated_at: self.state.vote_last_modified(),
            membership_log_id: membership.log_id().clone(),
        });

        if quorum {
            self.elect_with_current_timeout();
        } else {
            self.output.push_command(Command::SendPreVote {
                round: self.pre_vote_round,
                request,
            });
        }
    }

    fn pre_vote_context_matches(&self, round: &PreVote<C>) -> bool {
        self.prepared_shutdown.is_none()
            && self.leader.is_none()
            && self.state.membership_state.effective().is_voter(&self.config.id)
            && self.state.vote_ref() == &round.base_vote
            && self.state.vote_last_modified() == round.vote_updated_at
            && self.state.last_log_id() == round.candidate.last_log_id()
            && self.state.membership_state.effective().log_id() == &round.membership_log_id
    }

    pub(crate) fn discard_invalid_pre_vote(&mut self) {
        if self.pre_vote.as_ref().is_some_and(|round| !self.pre_vote_context_matches(round)) {
            self.pre_vote = None;
        }
        if self.real_campaign.as_ref().is_some_and(|campaign| {
            self.prepared_shutdown.is_some()
                || self.leader.is_some()
                || !self.state.membership_state.effective().is_voter(&self.config.id)
                || self.state.vote_ref() != &campaign.base_vote
                || self.state.vote_last_modified() != campaign.vote_updated_at
                || self.state.last_log_id() != campaign.last_log_id.as_ref()
                || self.state.membership_state.effective().log_id() != &campaign.membership_log_id
                || self.pre_vote_lease_active(C::now())
        }) {
            self.real_campaign = None;
        }
    }

    /// Remember only an eligible real request refused for its shorter log.
    /// This changes no vote, timestamp, timer, log, grant or persistent state.
    pub(super) fn remember_real_campaign(&mut self, request: &VoteRequest<C::NodeId>) {
        self.discard_invalid_pre_vote();
        let membership = self.state.membership_state.effective();
        let Some(candidate) = request.vote.leader_id().voted_for() else {
            return;
        };
        if self.prepared_shutdown.is_some()
            || self.leader.is_some()
            || !membership.is_voter(&self.config.id)
            || !membership.is_voter(&candidate)
            || candidate == self.config.id
            || request.vote.is_committed()
            || &request.vote <= self.state.vote_ref()
            || request.last_log_id.as_ref() >= self.state.last_log_id()
            || self.pre_vote_lease_active(C::now())
        {
            return;
        }
        if self
            .real_campaign
            .as_ref()
            .is_some_and(|known| known.vote.leader_id().term > request.vote.leader_id().term)
        {
            return;
        }
        self.real_campaign = Some(RealCampaign {
            vote: request.vote.clone(),
            base_vote: self.state.vote_ref().clone(),
            vote_updated_at: self.state.vote_last_modified(),
            last_log_id: self.state.last_log_id().cloned(),
            membership_log_id: membership.log_id().clone(),
        });
    }

    /// A new authenticated peer generation revokes its previous election evidence.
    pub(crate) fn revoke_pre_vote_evidence(&mut self, peer: &C::NodeId) {
        if let Some(round) = &mut self.pre_vote {
            round.candidate.revoke_grant(peer);
        }
        if self
            .real_campaign
            .as_ref()
            .is_some_and(|campaign| campaign.vote.leader_id().voted_for().as_ref() == Some(peer))
        {
            self.real_campaign = None;
        }
    }

    pub(crate) fn handle_pre_vote_resp(
        &mut self,
        round_id: u64,
        target: C::NodeId,
        response: PreVoteResponse<C::NodeId>,
    ) {
        self.discard_invalid_pre_vote();
        let Some(round) = self.pre_vote.as_ref() else {
            return;
        };
        if round.round != round_id
            || C::now() >= round.deadline
            || self.pre_vote_lease_active(C::now())
            || !self.state.membership_state.effective().is_voter(&target)
        {
            return;
        }

        // Only an actual remote vote can advance local state. As with an
        // ordinary rejected vote response, never restore leadership by adopting
        // a committed version of our own former vote after state reversion.
        let mut actual = response.vote.clone();
        actual.committed = false;
        if &actual > self.state.vote_ref() {
            self.pre_vote = None;
            let _ = self.vote_handler().update_vote(&actual);
            return;
        }

        if response.last_log_id.as_ref() > self.state.last_log_id() {
            self.set_greater_log();
            return;
        }
        if response.vote_granted {
            let round = self.pre_vote.as_mut().unwrap();
            if round.candidate.grant_by(&target) {
                // This does not establish leadership: the existing path saves
                // the real self-vote and requires durable votes from a quorum.
                self.elect_with_current_timeout();
            }
        }
    }
}
