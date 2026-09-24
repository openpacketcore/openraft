use std::sync::Arc;

use maplit::btreeset;
use validit::Validate;

use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::EngineConfig;
use crate::engine::LogIdList;
use crate::raft::TransferLeaderError;
use crate::raft::TransferLeaderRequest;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft_state::IOState;
use crate::raft_state::LogIOId;
use crate::raft_state::MembershipState;
use crate::raft_state::RaftState;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::CommittedLeaderId;
use crate::EffectiveMembership;
use crate::Membership;
use crate::ServerState;
use crate::TokioInstant;
use crate::Vote;

// Build a complete, validated state. No engine validation is disabled here.
fn engine(id: u64) -> Engine<UTConfig> {
    let vote = Vote::new_committed(5, 0);
    let last = Some(log_id(5, 0, 2));
    let membership = Arc::new(EffectiveMembership::new(
        Some(log_id(2, 0, 1)),
        Membership::new(vec![btreeset! {0, 1, 2}], Some(btreeset! {3})),
    ));
    let state = RaftState {
        vote: UTime::new(TokioInstant::now(), vote),
        committed: last,
        log_ids: LogIdList::new([log_id(0, 0, 0), log_id(2, 0, 1), log_id(5, 0, 2)]),
        membership_state: MembershipState::new(membership.clone(), membership),
        io_state: IOState::new(vote, LogIOId::new(CommittedLeaderId::new(5, 0), last), last, None, None),
        server_state: if id == 0 {
            ServerState::Leader
        } else if id == 3 {
            ServerState::Learner
        } else {
            ServerState::Follower
        },
        ..Default::default()
    };
    state.validate().unwrap();
    let mut engine = Engine::new(state, EngineConfig {
        id,
        ..Default::default()
    });
    if id == 0 {
        engine.testing_new_leader();
    }
    engine
}

fn request() -> TransferLeaderRequest<u64> {
    engine(0).begin_leadership_transfer(1).unwrap()
}

fn assert_refused(engine: &mut Engine<UTConfig>, request: TransferLeaderRequest<u64>, error: TransferLeaderError) {
    let before = engine.state.clone();
    assert_eq!(Err(error), engine.handle_leadership_transfer(request));
    assert_eq!(
        before, engine.state,
        "a refusal must not change vote, lease, log or membership"
    );
    assert!(engine.output.take_commands().is_empty());
    engine.state.validate().unwrap();
}

#[test]
fn begin_stops_admission_and_preserves_the_exact_log_and_vote() {
    let mut engine = engine(0);
    let before = engine.state.clone();
    let request = engine.begin_leadership_transfer(1).unwrap();
    assert_eq!(&Vote::new_committed(5, 0), request.from());
    assert_eq!(&1, request.to());
    assert_eq!(Some(&log_id(5, 0, 2)), request.last_log_id());
    assert_eq!(Some(&log_id(2, 0, 1)), request.membership_log_id());
    assert_eq!(before.vote_ref(), engine.state.vote_ref());
    assert_eq!(before.vote_last_modified(), engine.state.vote_last_modified());
    assert_eq!(before.log_ids, engine.state.log_ids);
    assert_eq!(before.committed, engine.state.committed);
    assert!(engine.state.vote.lease_disabled());
    assert!(
        engine.output.take_commands().is_empty(),
        "begin does not manufacture votes or log entries"
    );
    assert_eq!(Some(1), engine.leader_handler().err().unwrap().leader_id);
    assert_eq!(request, engine.begin_leadership_transfer(1).unwrap());
    assert_eq!(
        Err(TransferLeaderError::TransferInProgress),
        engine.begin_leadership_transfer(2)
    );
    assert_eq!(Some(1), engine.leader.as_ref().unwrap().transfer_to);
    engine.handle_leadership_transfer(request).unwrap();
    assert!(engine.candidate_ref().is_none(), "the old leader does not campaign");
    engine.state.validate().unwrap();
}

#[test]
fn begin_rejects_self_learner_missing_target_and_non_leader_without_effects() {
    for target in [0, 3, 4] {
        let mut engine = engine(0);
        let before = engine.state.clone();
        assert_eq!(
            Err(TransferLeaderError::InvalidTarget),
            engine.begin_leadership_transfer(target)
        );
        assert_eq!(before, engine.state);
        assert!(engine.leader_handler().is_ok());
    }
    let mut follower = engine(1);
    let before = follower.state.clone();
    assert_eq!(
        Err(TransferLeaderError::NotLeader),
        follower.begin_leadership_transfer(2)
    );
    assert_eq!(before, follower.state);
}

#[test]
fn only_exact_applied_target_campaigns_through_normal_persisted_vote() {
    let mut target = engine(1);
    target.handle_leadership_transfer(request()).unwrap();
    assert_eq!(&Vote::new(6, 1), target.state.vote_ref());
    assert_eq!(ServerState::Candidate, target.state.server_state);
    assert_eq!(&Vote::new(6, 1), target.candidate_ref().unwrap().vote_ref());
    assert!(
        !target.state.vote.lease_disabled(),
        "the next vote has the ordinary lease policy"
    );
    assert_eq!(
        vec![Command::SaveVote { vote: Vote::new(6, 1) }, Command::SendVote {
            vote_req: VoteRequest::new(Vote::new(6, 1), Some(log_id(5, 0, 2)))
        },],
        target.output.take_commands()
    );
    assert_refused(&mut target, request(), TransferLeaderError::VoteChanged);
}

#[test]
fn other_voter_releases_only_the_old_lease_and_late_heartbeat_cannot_restore_it() {
    let mut voter = engine(2);
    let before = voter.state.clone();
    let next_vote = VoteRequest::new(Vote::new(6, 1), Some(log_id(5, 0, 2)));
    assert!(
        !voter.handle_vote_req(next_vote.clone()).vote_granted,
        "the old lease initially blocks election"
    );
    voter.handle_leadership_transfer(request()).unwrap();
    assert_eq!(before.vote_ref(), voter.state.vote_ref());
    assert_eq!(before.vote_last_modified(), voter.state.vote_last_modified());
    assert!(voter.candidate_ref().is_none());
    assert!(voter.output.take_commands().is_empty());
    voter.vote_handler().update_vote(&Vote::new_committed(5, 0)).unwrap();
    assert!(
        voter.state.vote.lease_disabled(),
        "an equal-vote heartbeat cannot restore the released lease"
    );
    assert!(voter.handle_vote_req(next_vote).vote_granted);
    assert!(!voter.state.vote.lease_disabled());
    assert_eq!(
        vec![Command::SaveVote { vote: Vote::new(6, 1) }],
        voter.output.take_commands()
    );
}

#[test]
fn unprepared_leader_cannot_release_its_lease_by_receiving_a_request() {
    let mut leader = engine(0);
    assert_refused(&mut leader, request(), TransferLeaderError::NotPrepared);
    assert!(leader.leader_handler().is_ok());
}

#[test]
fn stale_or_uncommitted_vote_cannot_release_a_lease() {
    for from in [Vote::new_committed(4, 0), Vote::new_committed(5, 2), Vote::new(5, 0)] {
        let mut request = request();
        request.from = from;
        assert_refused(&mut engine(1), request, TransferLeaderError::VoteChanged);
    }
}

#[test]
fn changed_or_uncommitted_membership_cannot_release_a_lease() {
    let mut changed = request();
    changed.membership_log_id = Some(log_id(1, 0, 1));
    assert_refused(&mut engine(1), changed, TransferLeaderError::MembershipChanged);
    for joint in [false, true] {
        for id in [0, 1] {
            let mut engine = engine(id);
            let membership = if joint {
                Membership::new(vec![btreeset! {0, 1, 2}, btreeset! {0, 1, 3}], None)
            } else {
                Membership::new(vec![btreeset! {0, 1, 2}], None)
            };
            let effective = Arc::new(EffectiveMembership::new(Some(log_id(5, 0, 2)), membership));
            let committed = if joint {
                effective.clone()
            } else {
                engine.state.membership_state.committed().clone()
            };
            engine.state.membership_state = MembershipState::new(committed, effective);
            if id == 0 {
                let before = engine.state.clone();
                assert_eq!(
                    Err(TransferLeaderError::MembershipChanged),
                    engine.begin_leadership_transfer(1)
                );
                assert_eq!(before, engine.state);
                assert!(engine.leader_handler().is_ok());
            } else {
                let mut request = request();
                request.membership_log_id = Some(log_id(5, 0, 2));
                assert_refused(&mut engine, request, TransferLeaderError::MembershipChanged);
            }
        }
    }
}

#[test]
fn learner_or_missing_target_and_learner_receiver_cannot_campaign() {
    for to in [0, 3, 4] {
        let mut request = request();
        request.to = to;
        assert_refused(&mut engine(1), request, TransferLeaderError::InvalidTarget);
    }
    assert_refused(&mut engine(3), request(), TransferLeaderError::MembershipChanged);
}

#[test]
fn target_must_apply_the_entire_accepted_prefix_before_campaigning() {
    let mut target = engine(1);
    target.state.io_state.applied = Some(log_id(2, 0, 1));
    assert_refused(&mut target, request(), TransferLeaderError::LogNotApplied);
    target.state.io_state.update_applied(Some(log_id(5, 0, 2)));
    target.handle_leadership_transfer(request()).unwrap();
    assert_eq!(&Vote::new(6, 1), target.state.vote_ref());
}

#[test]
fn retiring_candidate_ignores_delayed_vote_grants() {
    let mut voter = engine(1);
    voter.elect();
    let vote = *voter.state.vote_ref();
    voter.handle_vote_resp(1, VoteResponse::new(vote, Some(log_id(5, 0, 2)), true));
    assert!(voter.candidate_ref().is_some());
    assert!(voter.leader.is_none());
    voter.output.take_commands();

    assert_eq!(None, voter.prepare_shutdown(Some(2)).unwrap());
    voter.handle_vote_resp(2, VoteResponse::new(vote, Some(log_id(5, 0, 2)), true));
    assert!(
        voter.leader.is_none(),
        "a retiring candidate must not win from a delayed grant"
    );
    assert!(voter.candidate_ref().is_none());
    assert_eq!(&vote, voter.state.vote_ref());
    assert!(voter.output.take_commands().is_empty());
    voter.state.validate().unwrap();
}

#[test]
fn retiring_follower_does_not_start_new_campaign() {
    let mut voter = engine(1);
    let before = voter.state.clone();
    assert_eq!(None, voter.prepare_shutdown(None).unwrap());
    voter.elect();
    assert!(voter.candidate_ref().is_none(), "a retiring follower must not campaign");
    assert_eq!(before, voter.state);
    assert!(voter.output.take_commands().is_empty());
}

#[test]
fn retiring_target_cannot_release_lease_or_campaign() {
    let mut voter = engine(1);
    let before = voter.state.clone();
    assert_eq!(None, voter.prepare_shutdown(None).unwrap());
    assert!(
        voter.handle_leadership_transfer(request()).is_err(),
        "a retiring target must refuse handoff"
    );
    assert_eq!(before, voter.state);
    assert!(voter.candidate_ref().is_none());
    assert!(voter.output.take_commands().is_empty());
}

#[test]
fn retirement_preserves_voting_for_other_candidates_and_never_resumes() {
    let mut voter = engine(1);
    assert_eq!(None, voter.prepare_shutdown(None).unwrap());
    let mut transfer = engine(0).begin_leadership_transfer(2).unwrap();
    voter.handle_leadership_transfer(transfer.clone()).unwrap();
    assert!(voter.state.vote.lease_disabled());
    assert!(voter.handle_vote_req(VoteRequest::new(Vote::new(6, 2), Some(log_id(5, 0, 2)))).vote_granted);
    voter.output.take_commands();
    assert_eq!(None, voter.prepare_shutdown(Some(2)).unwrap());
    voter.elect();
    assert!(voter.candidate_ref().is_none());
    assert!(voter.leader.is_none());
    assert_eq!(&Vote::new(6, 2), voter.state.vote_ref());
    assert!(voter.output.take_commands().is_empty());

    // Even an externally delivered committed self vote cannot reopen admission.
    voter.vote_handler().update_vote(&Vote::new_committed(7, 1)).unwrap();
    assert!(voter.leader_handler().is_err());
    assert_eq!(Err(TransferLeaderError::Retiring), voter.begin_leadership_transfer(2));
    transfer.from = Vote::new_committed(7, 1);
    transfer.to = 2;
    assert_eq!(
        Err(TransferLeaderError::NotPrepared),
        voter.handle_leadership_transfer(transfer)
    );
    voter.state.validate().unwrap();
}

#[test]
fn leader_retirement_retains_exact_result_after_successor_vote() {
    let mut voter = engine(0);
    let transfer = voter.prepare_shutdown(Some(1)).unwrap().unwrap();
    assert_eq!(Some(1), voter.leader_handler().err().unwrap().leader_id);
    assert_eq!(Some(transfer.clone()), voter.prepare_shutdown(None).unwrap());
    assert_eq!(
        Err(TransferLeaderError::TransferInProgress),
        voter.prepare_shutdown(Some(2))
    );
    voter.vote_handler().update_vote(&Vote::new_committed(6, 1)).unwrap();
    voter.output.take_commands();
    assert_eq!(Some(transfer), voter.prepare_shutdown(Some(1)).unwrap());
    voter.elect();
    assert!(voter.candidate_ref().is_none());
    assert!(voter.leader.is_none());
    assert!(voter.output.take_commands().is_empty());
}

#[test]
fn rejected_leader_retirement_does_not_change_admission_or_vote() {
    for to in [None, Some(0), Some(3), Some(4)] {
        let mut voter = engine(0);
        let before = voter.state.clone();
        assert_eq!(Err(TransferLeaderError::InvalidTarget), voter.prepare_shutdown(to));
        assert_eq!(before, voter.state);
        assert!(voter.prepared_shutdown.is_none());
        assert!(voter.leader_handler().is_ok());
        assert!(voter.output.take_commands().is_empty());
    }
}

#[cfg(feature = "serde")]
#[test]
fn serialized_request_keeps_the_exact_vote_membership_target_and_prefix() {
    let request = request();
    let bytes = serde_json::to_vec(&request).unwrap();
    let decoded: TransferLeaderRequest<u64> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(request, decoded);
    let mut target = engine(1);
    target.handle_leadership_transfer(decoded).unwrap();
    assert_eq!(&Vote::new(6, 1), target.state.vote_ref());
}
