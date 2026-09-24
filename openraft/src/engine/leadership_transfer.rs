//! Planned handoff uses the engine's normal election and persistence path.

use crate::engine::Engine;
use crate::raft::TransferLeaderError;
use crate::raft::TransferLeaderRequest;
use crate::raft_state::LogStateReader;
use crate::NodeId;
use crate::RaftTypeConfig;

/// Monotonic, process-local retirement; replication and voting remain available.
#[derive(Debug)]
pub(crate) struct PreparedShutdown<NID: NodeId> {
    handoff: Option<TransferLeaderRequest<NID>>,
}

impl<C: RaftTypeConfig> Engine<C> {
    pub(crate) fn prepare_shutdown(
        &mut self,
        to: Option<C::NodeId>,
    ) -> Result<Option<TransferLeaderRequest<C::NodeId>>, TransferLeaderError> {
        if let Some(prepared) = &self.prepared_shutdown {
            if let (Some(request), Some(to)) = (&prepared.handoff, &to) {
                if request.to() != to {
                    return Err(TransferLeaderError::TransferInProgress);
                }
            }
            return Ok(prepared.handoff.clone());
        }
        // Decide whether a handoff is needed and stop campaigning in the same
        // core turn. A role snapshot followed by disabling election ticks would
        // leave a pending candidate able to win from delayed vote responses.
        let handoff = if self.leader.is_some() {
            Some(self.begin_leadership_transfer(to.ok_or(TransferLeaderError::InvalidTarget)?)?)
        } else {
            None
        };
        self.candidate = None;
        self.prepared_shutdown = Some(PreparedShutdown {
            handoff: handoff.clone(),
        });
        Ok(handoff)
    }

    pub(crate) fn begin_leadership_transfer(
        &mut self,
        to: C::NodeId,
    ) -> Result<TransferLeaderRequest<C::NodeId>, TransferLeaderError> {
        if self.prepared_shutdown.as_ref().is_some_and(|prepared| prepared.handoff.is_none()) {
            return Err(TransferLeaderError::Retiring);
        }
        let Some(leader) = self.leader.as_mut() else {
            return Err(TransferLeaderError::NotLeader);
        };
        if !leader.vote.is_committed() || &leader.vote != self.state.vote_ref() {
            return Err(TransferLeaderError::NotLeader);
        }
        let membership = self.state.membership_state.effective();
        if membership.membership().get_joint_config().len() != 1
            || membership.log_id() != self.state.membership_state.committed().log_id()
        {
            return Err(TransferLeaderError::MembershipChanged);
        }
        if to == self.config.id || !membership.is_voter(&to) || !membership.is_voter(&self.config.id) {
            return Err(TransferLeaderError::InvalidTarget);
        }
        if leader.transfer_to.as_ref().is_some_and(|target| target != &to) {
            return Err(TransferLeaderError::TransferInProgress);
        }
        let request = TransferLeaderRequest {
            from: leader.vote.clone(),
            to: to.clone(),
            last_log_id: leader.last_log_id().cloned(),
            membership_log_id: membership.log_id().clone(),
        };
        // Stop new proposals, reads and heartbeats before releasing this
        // vote's election lease. Existing replication and accepted writes
        // continue; the target cannot campaign until this prefix is applied.
        leader.transfer_to = Some(to);
        self.state.vote.disable_lease();
        Ok(request)
    }

    pub(crate) fn handle_leadership_transfer(
        &mut self,
        request: TransferLeaderRequest<C::NodeId>,
    ) -> Result<(), TransferLeaderError> {
        if !request.from.is_committed() || &request.from != self.state.vote_ref() {
            return Err(TransferLeaderError::VoteChanged);
        }
        let membership = self.state.membership_state.effective();
        if membership.membership().get_joint_config().len() != 1
            || membership.log_id() != self.state.membership_state.committed().log_id()
            || membership.log_id() != &request.membership_log_id
        {
            return Err(TransferLeaderError::MembershipChanged);
        }
        let from = request.from.leader_id().voted_for();
        if !from.as_ref().is_some_and(|node| membership.is_voter(node)) || !membership.is_voter(&self.config.id) {
            return Err(TransferLeaderError::MembershipChanged);
        }
        if !membership.is_voter(&request.to) || from.as_ref() == Some(&request.to) {
            return Err(TransferLeaderError::InvalidTarget);
        }
        if from.as_ref() == Some(&self.config.id)
            && !self
                .leader
                .as_ref()
                .is_some_and(|leader| leader.vote == request.from && leader.transfer_to.as_ref() == Some(&request.to))
        {
            return Err(TransferLeaderError::NotPrepared);
        }
        if self.config.id == request.to {
            if self.prepared_shutdown.is_some() {
                return Err(TransferLeaderError::Retiring);
            }
            if self.state.io_applied() < request.last_log_id.as_ref() {
                return Err(TransferLeaderError::LogNotApplied);
            }
        }
        // Preserve the vote timestamp used for the ordinary election timer.
        // Only the exact retiring vote's lease is released. An in-flight old
        // heartbeat cannot re-arm it; a new vote gets a fresh ordinary lease.
        self.state.vote.disable_lease();
        if self.config.id == request.to {
            self.elect();
        }
        Ok(())
    }
}
