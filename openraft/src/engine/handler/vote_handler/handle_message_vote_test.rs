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
use crate::error::RejectVoteRequest;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft_state::LogStateReader;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::EffectiveMembership;
use crate::Membership;
use crate::TokioInstant;
use crate::Vote;

fn m01() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {0,1}], None)
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 0;
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new(2, 1));
    eng.state.server_state = ServerState::Candidate;
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(1, 1, 1)), m01())));

    eng.output.take_commands();
    eng
}

#[test]
fn test_handle_message_vote_reject_smaller_vote() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(2, 1));
    eng.testing_new_leader();

    let resp = eng.vote_handler().update_vote(&Vote::new(1, 2));

    assert_eq!(Err(RejectVoteRequest::ByVote(Vote::new_committed(2, 1))), resp);

    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert!(eng.leader.is_some());

    assert_eq!(ServerState::Candidate, eng.state.server_state);

    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_message_vote_committed_vote() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(vec![log_id(2, 1, 3)]);
    let now = TokioInstant::now();

    let resp = eng.vote_handler().update_vote(&Vote::new_committed(3, 2));

    assert_eq!(Ok(()), resp);

    assert_eq!(Vote::new_committed(3, 2), *eng.state.vote_ref());
    assert!(eng.leader.is_none());

    assert_eq!(ServerState::Follower, eng.state.server_state);

    assert!(Some(now) <= eng.state.vote_last_modified());
    assert!(eng.state.vote_last_modified() <= Some(now + Duration::from_millis(20)));
    assert_eq!(
        vec![Command::SaveVote {
            vote: Vote::new_committed(3, 2)
        },],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_handle_message_vote_granted_equal_vote() -> anyhow::Result<()> {
    // Equal vote should not emit a SaveVote command.

    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(vec![log_id(2, 1, 3)]);
    let now = TokioInstant::now();

    let resp = eng.vote_handler().update_vote(&Vote::new(2, 1));

    assert_eq!(Ok(()), resp);

    assert_eq!(Vote::new(2, 1), *eng.state.vote_ref());
    assert!(eng.leader.is_none());

    assert_eq!(ServerState::Follower, eng.state.server_state);

    assert!(Some(now) <= eng.state.vote_last_modified());
    assert!(eng.state.vote_last_modified() <= Some(now + Duration::from_millis(20)));

    assert!(eng.output.take_commands().is_empty());
    Ok(())
}

#[test]
fn test_handle_message_vote_granted_greater_vote() -> anyhow::Result<()> {
    // A greater vote should emit a SaveVote command.

    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(vec![log_id(2, 1, 3)]);

    let resp = eng.vote_handler().update_vote(&Vote::new(3, 1));

    assert_eq!(Ok(()), resp);

    assert_eq!(Vote::new(3, 1), *eng.state.vote_ref());
    assert!(eng.leader.is_none());

    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(
        vec![Command::SaveVote { vote: Vote::new(3, 1) },],
        eng.output.take_commands()
    );
    Ok(())
}

#[test]
fn test_handle_message_vote_granted_follower_learner_does_not_emit_update_server_state_cmd() -> anyhow::Result<()> {
    // A greater vote should emit a SaveVote command.

    // Learner
    {
        let st = ServerState::Learner;

        let mut eng = eng();
        eng.config.id = 100; // make it a non-voter
        eng.vote_handler().become_following();
        eng.state.server_state = st;
        eng.output.clear_commands();

        let resp = eng.vote_handler().update_vote(&Vote::new(3, 1));

        assert_eq!(Ok(()), resp);

        assert_eq!(st, eng.state.server_state);
        assert_eq!(
            vec![
                //
                Command::SaveVote { vote: Vote::new(3, 1) },
            ],
            eng.output.take_commands()
        );
    }
    // Follower
    {
        let st = ServerState::Follower;

        let mut eng = eng();
        eng.config.id = 0; // make it a voter
        eng.vote_handler().become_following();
        eng.state.server_state = st;
        eng.output.clear_commands();

        let resp = eng.vote_handler().update_vote(&Vote::new(3, 1));

        assert_eq!(Ok(()), resp);

        assert_eq!(st, eng.state.server_state);
        assert_eq!(
            vec![
                //
                Command::SaveVote { vote: Vote::new(3, 1) },
            ],
            eng.output.take_commands()
        );
    }
    Ok(())
}

#[test]
fn test_greater_self_vote_relinquishes_previous_leader() -> anyhow::Result<()> {
    let mut eng = eng();
    let expired = TokioInstant::now() - eng.config.timer_config.leader_lease - Duration::from_millis(1);
    eng.state.vote = UTime::new(expired, Vote::new_committed(2, 0));
    eng.state.server_state = ServerState::Leader;
    eng.state.log_ids = LogIdList::new(vec![log_id(2, 0, 3)]);
    eng.testing_new_leader();
    eng.output.take_commands();

    // Refused requests must leave the established leader untouched. Both the
    // full log freshness check and the vote check precede relinquishment.
    for (vote, last) in [(Vote::new(3, 0), log_id(2, 0, 2)), (Vote::new(1, 0), log_id(2, 0, 3))] {
        let reply = eng.handle_vote_req(VoteRequest::new(vote, Some(last)));
        assert!(!reply.vote_granted);
        assert_eq!(Vote::new_committed(2, 0), *eng.state.vote_ref());
        assert!(eng.leader.is_some());
        assert_eq!(ServerState::Leader, eng.state.server_state);
        assert!(eng.output.take_commands().is_empty());
    }

    // An actual accepted request changes the local vote without running elect().
    let vote = Vote::new(3, 0);
    let reply = eng.handle_vote_req(VoteRequest::new(vote, Some(log_id(2, 0, 3))));
    assert!(reply.vote_granted);
    assert_eq!(vote, reply.vote);
    assert_eq!(Some(log_id(2, 0, 3)), reply.last_log_id);
    assert_eq!(vote, *eng.state.vote_ref());
    assert!(eng.leader.is_none(), "the prior term must relinquish leadership");
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert_eq!(
        vec![Command::SaveVote { vote }, Command::QuitLeader],
        eng.output.take_commands()
    );

    // Repeating the accepted vote must not retire leadership twice.
    let reply = eng.handle_vote_req(VoteRequest::new(vote, Some(log_id(2, 0, 3))));
    assert!(reply.vote_granted);
    assert!(eng.output.take_commands().is_empty());

    // The next campaign preserves the candidate it creates, saves its real
    // vote and sends requests. It cannot become leader without quorum grants.
    eng.elect();
    assert_eq!(Vote::new(4, 0), *eng.state.vote_ref());
    assert!(eng.leader.is_none());
    assert_eq!(Vote::new(4, 0), *eng.candidate_ref().unwrap().vote_ref());
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert_eq!(
        vec![Command::SaveVote { vote: Vote::new(4, 0) }, Command::SendVote {
            vote_req: VoteRequest::new(Vote::new(4, 0), Some(log_id(2, 0, 3))),
        },],
        eng.output.take_commands()
    );
    Ok(())
}

#[test]
fn test_greater_self_vote_discards_stale_campaign_before_delayed_grants() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(vec![log_id(1, 1, 1)]);
    eng.elect();
    let campaign = *eng.state.vote_ref();
    let last = eng.state.last_log_id().copied();
    assert_eq!(Vote::new(3, 0), campaign);

    // The real response handler records one grant, short of the two-voter quorum.
    eng.handle_vote_resp(0, VoteResponse::new(campaign, last, true));
    assert_eq!(
        btreeset! {0},
        eng.candidate_ref().unwrap().granters().collect::<BTreeSet<_>>()
    );
    eng.output.take_commands();

    let accepted = Vote::new(4, 0);
    let reply = eng.handle_vote_req(VoteRequest::new(accepted, last));
    assert!(reply.vote_granted);
    assert_eq!(accepted, reply.vote);
    assert_eq!(last, reply.last_log_id);
    assert_eq!(vec![Command::SaveVote { vote: accepted }], eng.output.take_commands());

    // Deliver the old campaign's missing grant before inspecting the candidate.
    // Keeping that candidate would try to commit its older vote despite the
    // higher accepted vote, reaching establish_leader's invariant assertion.
    eng.handle_vote_resp(1, VoteResponse::new(campaign, last, true));
    eng.handle_vote_resp(0, VoteResponse::new(campaign, last, true));
    assert!(eng.candidate_ref().is_none());
    assert!(eng.leader.is_none());
    assert_eq!(accepted, *eng.state.vote_ref());
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert_eq!(last, eng.state.last_log_id().copied());
    assert!(eng.output.take_commands().is_empty());
    Ok(())
}

#[test]
fn test_greater_self_vote_retires_leader_and_stale_campaign() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(vec![log_id(1, 1, 1)]);
    eng.elect();
    let campaign = *eng.state.vote_ref();
    let campaign_last = eng.state.last_log_id().copied();
    assert_eq!(Vote::new(3, 0), campaign);
    eng.handle_vote_resp(0, VoteResponse::new(campaign, campaign_last, true));
    assert_eq!(
        btreeset! {0},
        eng.candidate_ref().unwrap().granters().collect::<BTreeSet<_>>()
    );

    // A committed self-vote can establish a newer leader while responses to
    // the older campaign remain in flight. Use the ordinary vote transition.
    let committed = Vote::new_committed(4, 0);
    eng.vote_handler().update_vote(&committed)?;
    assert_eq!(committed, eng.leader.as_ref().unwrap().vote);
    assert_eq!(ServerState::Leader, eng.state.server_state);
    let last = eng.state.last_log_id().copied();
    let expired = TokioInstant::now() - eng.config.timer_config.leader_lease - Duration::from_millis(1);
    eng.state.vote = UTime::new(expired, committed);
    eng.output.take_commands();

    let accepted = Vote::new(5, 0);
    let reply = eng.handle_vote_req(VoteRequest::new(accepted, last));
    assert!(reply.vote_granted);
    assert_eq!(accepted, reply.vote);
    assert_eq!(last, reply.last_log_id);

    // Clearing only the newer Leader would remove establish_leader's guard
    // against this old campaign. Its delayed quorum grant must stay harmless.
    eng.handle_vote_resp(1, VoteResponse::new(campaign, campaign_last, true));
    assert!(eng.leader.is_none());
    assert!(eng.candidate_ref().is_none());
    assert_eq!(accepted, *eng.state.vote_ref());
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert_eq!(last, eng.state.last_log_id().copied());
    assert_eq!(
        vec![Command::SaveVote { vote: accepted }, Command::QuitLeader],
        eng.output.take_commands()
    );
    Ok(())
}

#[test]
fn test_equal_self_vote_preserves_current_campaign_and_grants() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(vec![log_id(1, 1, 1)]);
    eng.elect();
    let campaign = *eng.state.vote_ref();
    let last = eng.state.last_log_id().copied();
    assert_eq!(Vote::new(3, 0), campaign);
    eng.handle_vote_resp(0, VoteResponse::new(campaign, last, true));
    let candidate_before = eng.candidate_ref().unwrap().clone();
    assert_eq!(btreeset! {0}, candidate_before.granters().collect::<BTreeSet<_>>());
    eng.output.take_commands();

    // An equal accepted vote must keep the same campaign, including its first
    // grant and start time. Recreating an empty candidate would lose progress.
    let reply = eng.handle_vote_req(VoteRequest::new(campaign, last));
    assert!(reply.vote_granted);
    assert_eq!(campaign, reply.vote);
    assert_eq!(last, reply.last_log_id);
    assert_eq!(Some(&candidate_before), eng.candidate_ref());
    assert!(eng.leader.is_none());
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert!(eng.output.take_commands().is_empty());

    // The remaining real response still completes this exact campaign.
    eng.handle_vote_resp(1, VoteResponse::new(campaign, last, true));
    let committed = Vote::new_committed(3, 0);
    assert_eq!(committed, *eng.state.vote_ref());
    assert_eq!(committed, eng.leader.as_ref().unwrap().vote);
    assert!(eng.candidate_ref().is_none());
    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(Some(log_id(3, 0, 2)), eng.state.last_log_id().copied());
    Ok(())
}
