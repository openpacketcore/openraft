use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::core::ServerState;
use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::progress::Progress;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::EffectiveMembership;
use crate::Membership;
use crate::TokioInstant;
use crate::Vote;

fn m1() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {1}], None)
}

fn m12() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {1,2}], None)
}

fn m123() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {1,2,3}], None)
}

/// A follower of node 2 at term 1, whose leader lease has expired.
fn eng(membership: Membership<u64, ()>) -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 1;
    eng.state.vote = UTime::new(TokioInstant::now() - Duration::from_secs(1), Vote::new_committed(1, 2));
    eng.state.server_state = ServerState::Follower;
    eng.state.log_ids = LogIdList::new(vec![log_id(1, 2, 3)]);
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(1, 2, 1)), membership)));
    eng.output.take_commands();

    eng
}

#[test]
fn test_pre_elect_changes_no_state() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    let vote_before = *eng.state.vote_ref();

    eng.pre_elect();

    // No term bump, no persisted vote and no server-state change.
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert!(
        eng.candidate_ref().is_none(),
        "no real campaign during a Pre-Vote round"
    );

    // A Pre-Vote round at the hypothetical next term, granted by this node itself.
    let pre_candidate = eng.pre_candidate_ref().unwrap();
    assert_eq!(Vote::new(2, 1), *pre_candidate.vote_ref());
    assert_eq!(btreeset! {1}, pre_candidate.granters().collect::<BTreeSet<_>>());

    // Only SendPreVote is emitted: no SaveVote and no SendVote.
    assert_eq!(
        vec![Command::SendPreVote {
            vote_req: VoteRequest::new(Vote::new(2, 1), Some(log_id(1, 2, 3))),
            round: 1,
        }],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_pre_elect_single_voter_starts_real_election() -> anyhow::Result<()> {
    let mut eng = eng(m1());

    eng.pre_elect();

    // A single voter grants its own Pre-Vote and campaigns at once.
    assert_eq!(Vote::new(2, 1), *eng.state.vote_ref());
    assert!(
        eng.pre_candidate_ref().is_none(),
        "the real election consumes the round"
    );
    assert!(eng.candidate_ref().is_some());
    assert_eq!(ServerState::Candidate, eng.state.server_state);

    let commands = eng.output.take_commands();
    assert!(commands.contains(&Command::SaveVote { vote: Vote::new(2, 1) }));
    assert!(commands.contains(&Command::SendVote {
        vote_req: VoteRequest::new(Vote::new(2, 1), Some(log_id(1, 2, 3))),
    }));
    assert!(!commands.iter().any(|c| matches!(c, Command::SendPreVote { .. })));

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_rejected_by_leader_lease() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.state.vote.update(TokioInstant::now(), Vote::new_committed(1, 2));
    let vote_before = *eng.state.vote_ref();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(2, 3), Some(log_id(1, 2, 3))));

    assert_eq!(
        VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 3)), false),
        resp
    );
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_rejected_by_smaller_last_log_id() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    let vote_before = *eng.state.vote_ref();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(2, 3), Some(log_id(1, 2, 2))));

    assert_eq!(
        VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 3)), false),
        resp
    );
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_granted_without_mutation() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    let vote_before = *eng.state.vote_ref();
    let utime_before = eng.state.vote_last_modified();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(2, 3), Some(log_id(1, 2, 3))));

    assert_eq!(
        VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 3)), true),
        resp
    );
    // Granting a Pre-Vote changes nothing locally, not even the vote timestamp.
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(utime_before, eng.state.vote_last_modified());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_rejects_a_vote_not_greater_than_the_local_one() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    // This node already granted node 3 a vote in term 2.
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new(2, 3));
    let vote_before = *eng.state.vote_ref();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(1, 2), Some(log_id(1, 2, 3))));

    assert_eq!(VoteResponse::new(Vote::new(2, 3), Some(log_id(1, 2, 3)), false), resp);
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_pre_vote_resp_quorum_starts_real_election() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.handle_pre_vote_resp(
        3,
        eng.pre_vote_round,
        VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 3)), true),
    );

    assert!(
        eng.pre_candidate_ref().is_none(),
        "the real election consumes the round"
    );
    assert_eq!(
        Vote::new(2, 1),
        *eng.state.vote_ref(),
        "the real election bumps the term"
    );
    assert!(eng.candidate_ref().is_some());
    assert_eq!(ServerState::Candidate, eng.state.server_state);

    let commands = eng.output.take_commands();
    assert!(commands.contains(&Command::SaveVote { vote: Vote::new(2, 1) }));
    assert!(commands.contains(&Command::SendVote {
        vote_req: VoteRequest::new(Vote::new(2, 1), Some(log_id(1, 2, 3))),
    }));

    Ok(())
}

#[test]
fn test_handle_pre_vote_resp_rejection_keeps_waiting() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();
    let vote_before = *eng.state.vote_ref();

    eng.handle_pre_vote_resp(
        3,
        eng.pre_vote_round,
        VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 3)), false),
    );

    assert!(eng.pre_candidate_ref().is_some(), "the round is still in flight");
    assert!(eng.candidate_ref().is_none(), "no real election starts");
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert!(!eng.is_there_greater_log());
    assert_eq!(0, eng.output.take_commands().len());

    tracing::info!("--- a rejection that reports a greater log delays the next campaign");
    eng.handle_pre_vote_resp(
        2,
        eng.pre_vote_round,
        VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 5)), false),
    );
    assert!(eng.is_there_greater_log());
    assert!(eng.pre_candidate_ref().is_some());
    assert_eq!(vote_before, *eng.state.vote_ref());

    Ok(())
}

#[test]
fn test_handle_pre_vote_resp_rejection_adopts_a_higher_vote() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.handle_pre_vote_resp(
        3,
        eng.pre_vote_round,
        VoteResponse::new(Vote::new_committed(5, 3), Some(log_id(1, 2, 3)), false),
    );

    // Catch up to the strictly higher term, never as committed, and end the round.
    assert_eq!(Vote::new(5, 3), *eng.state.vote_ref());
    assert!(eng.pre_candidate_ref().is_none(), "adopting a vote ends the round");
    assert!(eng.candidate_ref().is_none());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert!(eng.output.take_commands().contains(&Command::SaveVote { vote: Vote::new(5, 3) }));

    Ok(())
}

#[test]
fn test_handle_pre_vote_resp_rejection_with_equal_vote_keeps_the_round() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();
    let vote_before = *eng.state.vote_ref();

    eng.handle_pre_vote_resp(
        3,
        eng.pre_vote_round,
        VoteResponse::new(vote_before, Some(log_id(1, 2, 3)), false),
    );

    assert!(
        eng.pre_candidate_ref().is_some(),
        "an equal vote does not end the round"
    );
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_accepted_vote_ends_the_pre_vote_round() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();

    // A heartbeat from the current leader touches the same vote and ends the round.
    let leader_vote = *eng.state.vote_ref();
    eng.vote_handler().update_vote(&leader_vote)?;

    assert!(eng.pre_candidate_ref().is_none());
    assert_eq!(leader_vote, *eng.state.vote_ref());

    // A delayed grant for the ended round starts nothing.
    eng.handle_pre_vote_resp(
        3,
        eng.pre_vote_round,
        VoteResponse::new(leader_vote, Some(log_id(1, 2, 3)), true),
    );
    assert!(eng.candidate_ref().is_none());
    assert_eq!(leader_vote, *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_server_state_refresh_keeps_the_pre_vote_round() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();

    // The periodic re-evaluation of the server state changes no vote and keeps the round.
    eng.vote_handler().update_internal_server_state();

    assert!(eng.pre_candidate_ref().is_some());

    Ok(())
}

#[test]
fn test_winning_election_ends_an_overlapping_pre_vote_round() -> anyhow::Result<()> {
    let mut eng = eng(m12());

    // Round A wins a Pre-Vote quorum and starts election A.
    eng.pre_elect();
    eng.handle_pre_vote_resp(
        2,
        eng.pre_vote_round,
        VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 3)), true),
    );
    assert_eq!(Vote::new(2, 1), *eng.candidate_ref().unwrap().vote_ref());
    // The local vote is granted once it is persisted.
    eng.candidate_mut().unwrap().grant_by(&1);

    // Election A is pending when the next election timer fires and starts round B.
    eng.pre_elect();
    assert_eq!(Vote::new(3, 1), *eng.pre_candidate_ref().unwrap().vote_ref());

    // Election A wins before round B is answered.
    eng.handle_vote_resp(2, VoteResponse::new(Vote::new(2, 1), Some(log_id(1, 2, 3)), true));
    assert!(eng.leader.is_some(), "election A established a leader");
    assert!(eng.pre_candidate_ref().is_none(), "winning election A ends round B");
    eng.output.take_commands();

    // A delayed grant for round B starts no campaign on the leader.
    eng.handle_pre_vote_resp(
        2,
        eng.pre_vote_round,
        VoteResponse::new(Vote::new(2, 1), Some(log_id(1, 2, 3)), true),
    );
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_pre_elect_refused_while_the_leader_lease_is_valid() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    // A follower that its leader served just now.
    eng.state.vote.update(TokioInstant::now(), Vote::new_committed(1, 2));
    let timeout_before = eng.config.timer_config.election_timeout;
    let vote_before = *eng.state.vote_ref();

    eng.pre_elect();

    // Refused before it changes anything, including the sampled timeout.
    assert!(eng.pre_candidate_ref().is_none());
    assert!(eng.candidate_ref().is_none());
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(timeout_before, eng.config.timer_config.election_timeout);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_rejected_by_quorum_acknowledged_lease() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    // A leader whose own vote is no longer leased, acknowledged by node 2 just now.
    eng.state.vote = UTime::new(TokioInstant::now() - Duration::from_secs(1), Vote::new_committed(2, 1));
    eng.testing_new_leader().clock_progress.increase_to(&2, Some(TokioInstant::now())).unwrap();
    eng.state.server_state = ServerState::Leader;
    let vote_before = *eng.state.vote_ref();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(3, 3), Some(log_id(1, 2, 3))));

    assert_eq!(
        VoteResponse::new(Vote::new_committed(2, 1), Some(log_id(1, 2, 3)), false),
        resp
    );
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert!(eng.leader.is_some(), "the leader keeps leading");
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_pre_vote_resp_rejection_never_adopts_a_vote_for_this_node() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();
    let vote_before = *eng.state.vote_ref();
    let proposed = *eng.pre_candidate_ref().unwrap().vote_ref();
    assert!(proposed > vote_before);

    // A network that cannot ask the voter answers with a rejection of its own. If it echoed the
    // proposed vote, adopting it would vote for this node in a term it never campaigned in.
    eng.handle_pre_vote_resp(3, eng.pre_vote_round, VoteResponse::new(proposed, None, false));

    assert_eq!(vote_before, *eng.state.vote_ref());
    assert!(
        eng.pre_candidate_ref().is_some(),
        "the round keeps waiting for real answers"
    );
    assert_eq!(0, eng.output.take_commands().len());
    Ok(())
}

/// A grant that waited in the notification queue must not reach a later round.
///
/// Rounds in one term share their hypothetical vote. A same-term AppendEntries that removes a
/// voter ends the round; if its flush outlasts the election timeout, the next tick starts a new
/// round with the same vote. The removed voter's grant for the ended round, delivered only then,
/// cannot be told apart by that vote, and it must neither count nor stop this node.
#[test]
fn test_delayed_pre_vote_grant_from_a_removed_voter_does_not_reach_a_later_round() -> anyhow::Result<()> {
    let mut eng = eng(m123());
    eng.pre_elect();
    eng.output.take_commands();
    let first_round = eng.pre_vote_round;
    let first_round_vote = *eng.pre_candidate_ref().unwrap().vote_ref();

    // Leader 2 replicates a membership without voter 3 in its unchanged term, which ends the round.
    eng.vote_handler().update_vote(&Vote::new_committed(1, 2))?;
    assert!(eng.pre_candidate_ref().is_none());
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(1, 2, 3)), m12())));

    // The flush outlasts the election timeout, and the queued tick starts a new round.
    eng.state.vote = UTime::new(TokioInstant::now() - Duration::from_secs(1), Vote::new_committed(1, 2));
    eng.pre_elect();
    eng.output.take_commands();
    assert_eq!(
        first_round_vote,
        *eng.pre_candidate_ref().unwrap().vote_ref(),
        "both rounds propose the same vote"
    );
    assert_ne!(first_round, eng.pre_vote_round, "each round has its own identifier");

    // Voter 3's grant for the first round arrives now.
    let grant = VoteResponse::new(Vote::new_committed(1, 2), Some(log_id(1, 2, 3)), true);
    eng.handle_pre_vote_resp(3, first_round, grant.clone());

    assert!(eng.pre_candidate_ref().is_some(), "the new round keeps waiting");
    assert_eq!(
        btreeset! {1},
        eng.pre_candidate_ref().unwrap().granters().collect::<BTreeSet<_>>()
    );
    assert!(eng.candidate_ref().is_none(), "no real election starts");
    assert_eq!(Vote::new_committed(1, 2), *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    // Even a grant that names the new round cannot count for a node outside its quorum set.
    eng.handle_pre_vote_resp(3, eng.pre_vote_round, grant);

    assert!(eng.pre_candidate_ref().is_some(), "the new round keeps waiting");
    assert_eq!(
        btreeset! {1},
        eng.pre_candidate_ref().unwrap().granters().collect::<BTreeSet<_>>()
    );
    assert!(eng.candidate_ref().is_none(), "no real election starts");
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}
