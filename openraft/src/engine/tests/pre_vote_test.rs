use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;

use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::progress::Progress;
use crate::raft::PreVoteRequest;
use crate::raft::PreVoteResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::EffectiveMembership;
use crate::Membership;
use crate::ServerState;
use crate::TokioInstant;
use crate::Vote;

fn engine(id: u64) -> Engine<UTConfig> {
    let mut engine = Engine::testing_default(id);
    engine.state.enable_validation(false);
    engine.config.election_timeout_min = 100;
    engine.config.election_timeout_max = 101;
    engine.config.timer_config.leader_lease = Duration::from_millis(200);
    engine.state.vote = UTime::new(TokioInstant::now() - Duration::from_secs(1), Vote::new_committed(5, 2));
    engine.state.server_state = ServerState::Follower;
    engine.state.log_ids = LogIdList::new([log_id(5, 2, 3)]);
    engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
        Some(log_id(4, 2, 1)),
        Membership::new(vec![btreeset! {0, 1, 2}], None),
    )));
    engine
}

fn grant() -> PreVoteResponse<u64> {
    PreVoteResponse::new(Vote::new_committed(5, 2), Some(log_id(5, 2, 3)), true)
}

fn begin(engine: &mut Engine<UTConfig>) -> u64 {
    let before = engine.state.clone();
    engine.pre_elect();
    let round = engine.pre_vote.as_ref().unwrap().round;
    assert_eq!(before, engine.state, "inquiry does not cast a self-vote");
    assert!(matches!(engine.output.take_commands().as_slice(), [
        Command::SendPreVote { .. }
    ]));
    round
}

#[tokio::test(start_paused = true)]
async fn pre_vote_grants_and_refusals_do_not_change_the_recipient() {
    let mut engine = engine(1);
    let before = engine.state.clone();
    for proposed in [6, 100, 200] {
        for candidate in [0, 2] {
            let response = engine.handle_pre_vote_req(&PreVoteRequest::new(
                Vote::new(proposed, candidate),
                Some(log_id(5, 2, 3)),
            ));
            assert_eq!(grant(), response);
            assert_eq!(before, engine.state);
            assert!(engine.output.take_commands().is_empty());
        }
    }
    for request in [
        PreVoteRequest::new(Vote::new(6, 0), Some(log_id(4, 2, 99))),
        PreVoteRequest::new(Vote::new(5, 0), Some(log_id(5, 2, 3))),
        PreVoteRequest::new(Vote::new_committed(6, 0), Some(log_id(5, 2, 3))),
        PreVoteRequest::new(Vote::new(6, 9), Some(log_id(5, 2, 3))),
    ] {
        let response = engine.handle_pre_vote_req(&request);
        assert!(!response.vote_granted);
        assert_eq!(*before.vote_ref(), response.vote);
        assert_eq!(before, engine.state);
        assert!(engine.output.take_commands().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn pre_vote_respects_the_existing_lease_boundary_without_renewing_it() {
    let mut engine = engine(1);
    let at = TokioInstant::now();
    engine.state.vote.touch(at);
    let request = PreVoteRequest::new(Vote::new(6, 0), Some(log_id(5, 2, 3)));
    assert!(!engine.handle_pre_vote_req(&request).vote_granted);
    tokio::time::advance(Duration::from_millis(200)).await;
    assert!(!engine.handle_pre_vote_req(&request).vote_granted);
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(engine.handle_pre_vote_req(&request).vote_granted);
    assert_eq!(Some(at), engine.state.vote_last_modified());
    assert_eq!(Vote::new_committed(5, 2), *engine.state.vote_ref());
    assert!(engine.output.take_commands().is_empty());
}

#[tokio::test(start_paused = true)]
async fn leader_grants_pre_vote_only_after_its_quorum_lease_expires_without_mutation() {
    for voters in [btreeset! {0, 2}, btreeset! {0, 1, 2}] {
        let mut engine = engine(2);
        engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
            Some(log_id(4, 2, 1)),
            Membership::new(vec![voters], None),
        )));
        engine.state.server_state = ServerState::Leader;
        let at = TokioInstant::now();
        let leader = engine.testing_new_leader();
        let _ = leader.clock_progress.increase_to(&0, Some(at));
        let before_leader = engine.leader.clone();
        let before_state = engine.state.clone();
        let request = PreVoteRequest::new(Vote::new(6, 0), Some(log_id(5, 2, 3)));

        assert!(!engine.handle_pre_vote_req(&request).vote_granted);
        tokio::time::advance(Duration::from_millis(200)).await;
        assert!(!engine.handle_pre_vote_req(&request).vote_granted);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(engine.handle_pre_vote_req(&request).vote_granted);
        assert_eq!(before_state, engine.state);
        assert_eq!(before_leader, engine.leader);
        assert!(engine.output.take_commands().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn rejected_or_unanswered_pre_vote_rounds_do_not_consume_terms() {
    let mut engine = engine(0);
    let before = engine.state.clone();
    for _ in 0..3 {
        let round = begin(&mut engine);
        engine.handle_pre_vote_resp(
            round,
            1,
            PreVoteResponse::new(Vote::new_committed(5, 2), Some(log_id(5, 2, 4)), false),
        );
        assert!(engine.is_there_greater_log());
        assert_eq!(before, engine.state);
        assert!(engine.candidate.is_none());
        assert!(engine.leader.is_none());
        assert!(engine.output.take_commands().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn pre_vote_quorum_starts_real_persisted_voting_without_establishing_leadership() {
    let mut engine = engine(0);
    let round = begin(&mut engine);
    engine.handle_pre_vote_resp(round, 1, grant());
    let vote = Vote::new(6, 0);
    assert_eq!(vote, *engine.state.vote_ref());
    assert!(engine.pre_vote.is_none());
    assert!(engine.leader.is_none());
    assert_eq!(0, engine.candidate.as_ref().unwrap().granters().count());
    assert_eq!(engine.output.take_commands(), vec![
        Command::SaveVote { vote },
        Command::SendVote {
            vote_req: VoteRequest::new(vote, Some(log_id(5, 2, 3))),
        }
    ]);
    // The ordinary self acknowledgement is produced only after SaveVote.
    engine.handle_vote_resp(0, VoteResponse::new(vote, None, true));
    assert!(engine.leader.is_none());
    engine.handle_vote_resp(1, VoteResponse::new(vote, Some(log_id(5, 2, 3)), true));
    assert!(engine.leader.is_some());
    assert_eq!(Vote::new_committed(6, 0), *engine.state.vote_ref());
}

#[tokio::test(start_paused = true)]
async fn pre_vote_requires_both_joint_majorities() {
    let mut engine = engine(0);
    engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
        Some(log_id(4, 2, 1)),
        Membership::new(vec![btreeset! {0, 1, 2}, btreeset! {0, 3, 4}], None),
    )));
    let round = begin(&mut engine);
    engine.handle_pre_vote_resp(round, 1, grant());
    engine.handle_pre_vote_resp(round, 2, grant());
    assert_eq!(Vote::new_committed(5, 2), *engine.state.vote_ref());
    assert!(engine.output.take_commands().is_empty());
    engine.handle_pre_vote_resp(round, 3, grant());
    assert_eq!(Vote::new(6, 0), *engine.state.vote_ref());
    assert!(matches!(engine.output.take_commands().as_slice(), [
        Command::SaveVote { .. },
        Command::SendVote { .. }
    ]));
}

#[tokio::test(start_paused = true)]
async fn stale_pre_vote_round_and_nonmember_grants_are_ignored() {
    let mut engine = engine(0);
    let old_round = begin(&mut engine);
    let current = begin(&mut engine);
    engine.handle_pre_vote_resp(old_round, 1, grant());
    engine.handle_pre_vote_resp(current, 9, grant());
    assert_eq!(Vote::new_committed(5, 2), *engine.state.vote_ref());
    assert!(engine.output.take_commands().is_empty());
    engine.handle_pre_vote_resp(current, 1, grant());
    assert_eq!(Vote::new(6, 0), *engine.state.vote_ref());
}

#[tokio::test(start_paused = true)]
async fn heartbeat_vote_log_membership_and_retirement_changes_invalidate_pre_vote() {
    for change in 0..5 {
        let mut engine = engine(0);
        let round = begin(&mut engine);
        match change {
            0 => engine.state.vote.touch(TokioInstant::now()),
            1 => engine.state.vote.update(TokioInstant::now(), Vote::new(6, 1)),
            2 => engine.state.log_ids = LogIdList::new([log_id(5, 2, 4)]),
            3 => engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
                Some(log_id(5, 2, 3)),
                Membership::new(vec![btreeset! {0, 1, 3}], None),
            ))),
            4 => {
                assert!(engine.prepare_shutdown(None).unwrap().is_none());
            }
            _ => unreachable!(),
        }
        let before = engine.state.clone();
        engine.output.take_commands();
        engine.handle_pre_vote_resp(round, 1, grant());
        assert!(engine.pre_vote.is_none(), "changed context {change}");
        assert_eq!(before, engine.state);
        assert!(engine.output.take_commands().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn equal_timestamp_heartbeat_and_expired_round_cannot_promote_a_pre_vote() {
    let mut engine = engine(0);
    let at = TokioInstant::now();
    engine.state.vote.touch(at);
    let round = begin(&mut engine);
    engine.state.vote.touch(at);
    engine.handle_pre_vote_resp(round, 1, grant());
    assert_eq!(Vote::new_committed(5, 2), *engine.state.vote_ref());
    assert!(engine.output.take_commands().is_empty());

    tokio::time::advance(Duration::from_millis(201)).await;
    let round = begin(&mut engine);
    tokio::time::advance(Duration::from_millis(100)).await;
    engine.handle_pre_vote_resp(round, 1, grant());
    assert_eq!(Vote::new_committed(5, 2), *engine.state.vote_ref());
    assert!(engine.output.take_commands().is_empty());
}

#[tokio::test(start_paused = true)]
async fn only_an_actual_higher_response_vote_can_advance_term_without_restoring_leadership() {
    let mut engine = engine(0);
    let round = begin(&mut engine);
    engine.handle_pre_vote_resp(
        round,
        1,
        PreVoteResponse::new(Vote::new_committed(100, 0), Some(log_id(5, 2, 3)), false),
    );
    assert_eq!(Vote::new(100, 0), *engine.state.vote_ref());
    assert!(engine.leader.is_none());
    assert!(engine.candidate.is_none());
    assert!(engine.pre_vote.is_none());
    assert_eq!(engine.output.take_commands(), vec![Command::SaveVote {
        vote: Vote::new(100, 0)
    }]);
}

#[tokio::test(start_paused = true)]
async fn single_voter_still_persists_its_real_vote_before_leadership() {
    let mut engine = engine(0);
    engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
        Some(log_id(4, 2, 1)),
        Membership::new(vec![btreeset! {0}], None),
    )));
    engine.pre_elect();
    assert!(engine.pre_vote.is_none());
    assert!(engine.leader.is_none());
    assert_eq!(0, engine.candidate.as_ref().unwrap().granters().count());
    assert!(matches!(engine.output.take_commands().as_slice(), [
        Command::SaveVote { .. },
        Command::SendVote { .. }
    ]));
    engine.handle_vote_resp(0, VoteResponse::new(Vote::new(6, 0), None, true));
    assert!(engine.leader.is_some());
}

fn stale_real_request(vote: Vote<u64>) -> VoteRequest<u64> {
    VoteRequest::new(vote, Some(log_id(5, 2, 2)))
}

#[tokio::test(start_paused = true)]
async fn pre_vote_fallback_deadline_survives_retries_before_a_real_campaign_arrives() {
    for retry_after_ms in [99, 100] {
        let mut engine = engine(0);
        let before = engine.state.clone();
        begin(&mut engine);
        tokio::time::advance(Duration::from_millis(retry_after_ms)).await;
        begin(&mut engine);
        tokio::time::advance(Duration::from_millis(1)).await;

        assert!(!engine.handle_vote_req(stale_real_request(Vote::new(100, 1))).vote_granted);
        assert_eq!(before, engine.state, "a stale real request grants no vote");
        assert!(engine.output.take_commands().is_empty());
        engine.pre_elect();

        let actual = Vote::new(101, 0);
        assert_eq!(actual, *engine.state.vote_ref(), "one inquiry timeout bounds fallback");
        assert!(engine.leader.is_none());
        assert_eq!(0, engine.candidate.as_ref().unwrap().granters().count());
        assert_eq!(engine.output.take_commands(), vec![
            Command::SaveVote { vote: actual },
            Command::SendVote {
                vote_req: VoteRequest::new(actual, Some(log_id(5, 2, 3))),
            }
        ]);
    }
}

#[tokio::test(start_paused = true)]
async fn a_real_campaign_allows_one_bounded_inquiry_then_durable_ordinary_voting() {
    let mut engine = engine(0);
    let before = engine.state.clone();
    let response = engine.handle_vote_req(stale_real_request(Vote::new(100, 1)));
    assert!(!response.vote_granted);
    assert_eq!(before, engine.state);
    assert!(engine.output.take_commands().is_empty());
    assert!(engine.real_campaign.is_some());
    begin(&mut engine);

    tokio::time::advance(Duration::from_millis(99)).await;
    begin(&mut engine);
    assert_eq!(before, engine.state, "an unexpired round cannot fall back");
    tokio::time::advance(Duration::from_millis(100)).await;
    engine.pre_elect();
    let actual = Vote::new(101, 0);
    assert_eq!(actual, *engine.state.vote_ref());
    assert!(engine.pre_vote.is_none());
    assert!(engine.real_campaign.is_none());
    assert!(engine.leader.is_none());
    assert_eq!(0, engine.candidate.as_ref().unwrap().granters().count());
    assert_eq!(engine.output.take_commands(), vec![
        Command::SaveVote { vote: actual },
        Command::SendVote {
            vote_req: VoteRequest::new(actual, Some(log_id(5, 2, 3))),
        }
    ]);
    engine.handle_vote_resp(0, VoteResponse::new(actual, None, true));
    assert!(engine.leader.is_none());
    engine.handle_vote_resp(1, VoteResponse::new(actual, Some(log_id(5, 2, 2)), true));
    assert!(engine.leader.is_some());
}

#[tokio::test(start_paused = true)]
async fn nonmember_self_committed_and_stale_requests_do_not_enable_fallback() {
    for vote in [
        Vote::new(100, 9),
        Vote::new(100, 0),
        Vote::new_committed(100, 1),
        Vote::new(4, 1),
    ] {
        let mut engine = engine(0);
        let before = engine.state.clone();
        assert!(!engine.handle_vote_req(stale_real_request(vote)).vote_granted);
        assert!(engine.real_campaign.is_none());
        for _ in 0..4 {
            begin(&mut engine);
            tokio::time::advance(Duration::from_millis(101)).await;
        }
        assert_eq!(before, engine.state);
    }
}

#[tokio::test(start_paused = true)]
async fn election_context_changes_and_peer_fences_revoke_real_campaign_evidence() {
    for change in 0..6 {
        let mut engine = engine(0);
        assert!(!engine.handle_vote_req(stale_real_request(Vote::new(100, 1))).vote_granted);
        begin(&mut engine);
        tokio::time::advance(Duration::from_millis(101)).await;
        match change {
            0 => engine.state.vote.touch(TokioInstant::now()),
            1 => engine.state.vote.update(TokioInstant::now(), Vote::new(6, 1)),
            2 => engine.state.log_ids = LogIdList::new([log_id(5, 2, 4)]),
            3 => engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
                Some(log_id(5, 2, 3)),
                Membership::new(vec![btreeset! {0, 1, 2}], None),
            ))),
            4 => {
                assert!(engine.prepare_shutdown(None).unwrap().is_none());
            }
            5 => {
                engine.revoke_pre_vote_evidence(&2);
                assert!(
                    engine.real_campaign.is_some(),
                    "an unrelated peer does not own this evidence"
                );
                engine.revoke_pre_vote_evidence(&1);
            }
            _ => unreachable!(),
        }
        let before = engine.state.clone();
        engine.output.take_commands();
        engine.discard_invalid_pre_vote();
        assert!(engine.real_campaign.is_none(), "changed context {change}");
        engine.pre_elect();
        assert_eq!(before, engine.state);
        assert!(engine.leader.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn compatibility_fallback_still_requires_both_real_joint_majorities() {
    let mut engine = engine(0);
    engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
        Some(log_id(4, 2, 1)),
        Membership::new(vec![btreeset! {0, 1, 2}, btreeset! {0, 3, 4}], None),
    )));
    assert!(!engine.handle_vote_req(stale_real_request(Vote::new(100, 1))).vote_granted);
    begin(&mut engine);
    tokio::time::advance(Duration::from_millis(100)).await;
    engine.pre_elect();
    let actual = Vote::new(101, 0);
    assert_eq!(actual, *engine.state.vote_ref());
    assert!(matches!(engine.output.take_commands().as_slice(), [
        Command::SaveVote { .. },
        Command::SendVote { .. }
    ]));
    for id in [0, 1, 2] {
        engine.handle_vote_resp(id, VoteResponse::new(actual, None, true));
    }
    assert!(engine.leader.is_none());
    engine.handle_vote_resp(3, VoteResponse::new(actual, None, true));
    assert!(engine.leader.is_some());
}

#[tokio::test(start_paused = true)]
async fn healthy_leases_and_nonvoters_do_not_remember_ordinary_campaigns() {
    for case in 0..4 {
        let mut engine = engine(0);
        match case {
            0 => engine.state.vote.touch(TokioInstant::now()),
            1 => {
                engine.state.server_state = ServerState::Leader;
                let leader = engine.testing_new_leader();
                let _ = leader.clock_progress.increase_to(&1, Some(TokioInstant::now()));
            }
            2 => engine.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(
                Some(log_id(4, 2, 1)),
                Membership::new(vec![btreeset! {1, 2}], None),
            ))),
            3 => {
                assert!(engine.prepare_shutdown(None).unwrap().is_none());
            }
            _ => unreachable!(),
        }
        let before = engine.state.clone();
        engine.output.take_commands();
        assert!(!engine.handle_vote_req(stale_real_request(Vote::new(100, 1))).vote_granted);
        assert!(engine.real_campaign.is_none());
        assert_eq!(before, engine.state);
        assert!(engine.output.take_commands().is_empty());
    }
}
