//! Participation is recorded only after the core accepts a replication reply.
//! These tests control that acceptance order, independently of network scheduling.

use super::*;
use crate::error::ForwardToLeader;
use crate::ReplicationReadback;

const A: [u8; 16] = [1; 16];
const B: [u8; 16] = [2; 16];
const C: [u8; 16] = [3; 16];

async fn capture(core: &mut Core) -> Result<ReplicationReadback<TestConfig>, ForwardToLeader<u64, EmptyNode>> {
    let (tx, rx) = TestConfig::oneshot();
    core.handle_api_msg(RaftMsg::ReadReplication { tx }).await;
    rx.await.unwrap()
}

fn append(
    core: &Core,
    target: u64,
    request_id: RequestId,
    sent: tokio::time::Instant,
    runtime_id: Option<[u8; 16]>,
    result: Result<Option<LogId<u64>>, LogId<u64>>,
) -> Notify<TestConfig> {
    Notify::Network {
        generation: core.peer_admission.generation.clone(),
        response: Response::Progress {
            target,
            request_id,
            result: Ok(ReplicationResult::new(sent, result).with_runtime_id(runtime_id)),
            session_id: ReplicationSessionId::new(
                *core.engine.state.vote_ref(),
                *core.engine.state.membership_state.effective().log_id(),
            ),
        },
    }
}

fn heartbeat(core: &mut Core, runtime: [u8; 16], sent: tokio::time::Instant, matching: Option<LogId<u64>>) {
    let reply = append(core, 3, RequestId::HeartBeat, sent, Some(runtime), Ok(matching));
    core.handle_notify(reply).unwrap();
}

#[tokio::test(start_paused = true)]
async fn participation_target_is_sampled_before_the_first_ack_advances_commit() {
    let mut core = core(&[1, 2, 3], true);
    assert!(capture(&mut core).await.unwrap().observations.is_empty());
    let reply = append(
        &core,
        3,
        RequestId::new_append_entries(7),
        TestConfig::now(),
        Some(A),
        Ok(Some(log(10))),
    );
    core.handle_notify(reply).unwrap();
    let first = capture(&mut core).await.unwrap();
    assert_eq!(first.committed, Some(log(10)));
    assert_eq!(first.observations[&3].target, Some(log(0)));
    assert_eq!(first.observations[&3].matching, Some(Some(log(10))));

    heartbeat(&mut core, A, TestConfig::now(), Some(log(9)));
    let same = capture(&mut core).await.unwrap();
    assert_eq!(same.observations[&3].target, Some(log(0)));
    assert_eq!(same.observations[&3].matching, Some(Some(log(10))));
    heartbeat(&mut core, B, TestConfig::now(), Some(log(2)));
    let restarted = capture(&mut core).await.unwrap();
    assert_eq!(restarted.observations[&3].runtime_id, B);
    assert_eq!(restarted.observations[&3].target, Some(log(10)));
    assert_eq!(restarted.observations[&3].matching, Some(Some(log(2))));
    assert_eq!(
        core.engine.leader.as_ref().unwrap().progress.get(&3).matching,
        Some(log(10))
    );
}

#[tokio::test(start_paused = true)]
async fn participation_target_includes_the_uncommitted_first_own_term_entry() {
    let mut core = core(&[1, 2, 3], true);
    let own_term = LogId::new(CommittedLeaderId::new(6, 1), 11);
    core.engine.state.vote = UTime::new(TestConfig::now(), Vote::new_committed(6, 1));
    core.engine.state.log_ids = LogIdList::new([log(0), log(10), own_term]);
    core.engine.testing_new_leader();
    heartbeat(&mut core, A, TestConfig::now(), Some(log(3)));
    let read = capture(&mut core).await.unwrap();
    assert_eq!(read.vote, Vote::new_committed(6, 1));
    assert_eq!(read.committed, Some(log(0)));
    assert_eq!(read.observations[&3].target, Some(own_term));
    assert_eq!(read.observations[&3].matching, Some(Some(log(3))));
    assert!(read.observations[&3].matching.unwrap() < read.observations[&3].target);
}

#[tokio::test(start_paused = true)]
async fn participation_empty_commit_and_prefix_are_not_unobserved() {
    let mut core = core(&[1, 2, 3], true);
    core.engine.state.committed = None;
    heartbeat(&mut core, A, TestConfig::now(), None);
    let read = capture(&mut core).await.unwrap();
    assert_eq!(read.committed, None);
    assert_eq!(read.observations[&3].matching, Some(None));
    assert_eq!(read.observations[&3].target, Some(log(0)));
}

#[tokio::test(start_paused = true)]
async fn participation_unknown_and_confirmed_empty_prefixes_differ() {
    let mut core = core(&[1, 2, 3], true);
    let conflict = append(&core, 3, RequestId::HeartBeat, TestConfig::now(), Some(A), Err(log(8)));
    core.handle_notify(conflict).unwrap();
    assert_eq!(capture(&mut core).await.unwrap().observations[&3].matching, None);
    heartbeat(&mut core, A, TestConfig::now(), None);
    assert_eq!(capture(&mut core).await.unwrap().observations[&3].matching, Some(None));
    heartbeat(&mut core, A, TestConfig::now(), Some(log(5)));
    heartbeat(&mut core, A, TestConfig::now(), None);
    assert_eq!(
        capture(&mut core).await.unwrap().observations[&3].matching,
        Some(Some(log(5)))
    );
}

#[tokio::test(start_paused = true)]
async fn participation_conflicts_clear_only_at_or_below_the_confirmed_prefix() {
    for (conflict_index, expected) in [(4, None), (5, None), (6, Some(Some(log(5))))] {
        let mut core = core(&[1, 2, 3], true);
        heartbeat(&mut core, A, TestConfig::now(), Some(log(5)));
        let conflict = append(
            &core,
            3,
            RequestId::HeartBeat,
            TestConfig::now(),
            Some(A),
            Err(log(conflict_index)),
        );
        core.handle_notify(conflict).unwrap();
        let contradicted = capture(&mut core).await.unwrap();
        assert_eq!(contradicted.observations[&3].matching, expected);
        assert_eq!(contradicted.observations[&3].target, Some(log(0)));
        heartbeat(&mut core, A, TestConfig::now(), Some(log(2)));
        assert_eq!(
            capture(&mut core).await.unwrap().observations[&3].matching,
            expected.or(Some(Some(log(2))))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn participation_two_restarts_and_an_older_send_cannot_restore_the_middle_runtime() {
    let mut core = core(&[1, 2, 3], true);
    let start = TestConfig::now();
    heartbeat(&mut core, A, start, Some(log(7)));
    let held_b = append(
        &core,
        3,
        RequestId::HeartBeat,
        start + Duration::from_millis(10),
        Some(B),
        Ok(Some(log(9))),
    );
    heartbeat(&mut core, C, start + Duration::from_millis(20), Some(log(2)));
    let before = capture(&mut core).await.unwrap();
    core.handle_notify(held_b).unwrap();
    assert_eq!(capture(&mut core).await.unwrap(), before);
    assert_eq!(before.observations[&3].runtime_id, C);
    assert_eq!(before.observations[&3].matching, Some(Some(log(2))));
}

#[tokio::test(start_paused = true)]
async fn participation_ages_include_network_core_queue_and_reply_construction_time() {
    let mut core = core(&[1, 2, 3], true);
    tokio::time::advance(Duration::from_millis(10)).await;
    let sent = TestConfig::now();
    let reply = append(&core, 3, RequestId::HeartBeat, sent, Some(A), Ok(Some(log(5))));
    tokio::time::advance(Duration::from_millis(20)).await;
    core.handle_notify(reply).unwrap();
    let captured = capture(&mut core).await.unwrap();
    tokio::time::advance(Duration::from_millis(20)).await;
    assert_eq!(
        TestConfig::now() - captured.observations[&3].sent_at,
        Duration::from_millis(40)
    );
    tokio::time::advance(Duration::from_millis(20)).await;
    let read = capture(&mut core).await.unwrap();
    assert_eq!(
        TestConfig::now() - read.observations[&3].sent_at,
        Duration::from_millis(60)
    );
    tokio::time::advance(Duration::from_millis(10)).await;
    heartbeat(&mut core, A, TestConfig::now(), Some(log(6)));
    tokio::time::advance(Duration::from_millis(20)).await;
    heartbeat(&mut core, B, sent, Some(log(9)));
    let read = capture(&mut core).await.unwrap();
    assert_eq!(read.observations[&3].runtime_id, A);
    assert_eq!(
        TestConfig::now() - read.observations[&3].sent_at,
        Duration::from_millis(20)
    );
    assert_eq!(
        captured.observations[&3].matching,
        Some(Some(log(5))),
        "capture remains immutable"
    );
}

#[tokio::test(start_paused = true)]
async fn participation_fence_hides_and_clears_prefix_but_retains_the_same_runtime_target() {
    let mut core = core(&[1, 2, 3], true);
    heartbeat(&mut core, A, TestConfig::now(), Some(log(7)));
    let delayed = append(
        &core,
        3,
        RequestId::HeartBeat,
        TestConfig::now(),
        Some(B),
        Ok(Some(log(9))),
    );
    let delayed_after_release = append(
        &core,
        3,
        RequestId::HeartBeat,
        TestConfig::now(),
        Some(B),
        Ok(Some(log(9))),
    );
    let receipt = fence(&mut core, 3).await;
    assert!(capture(&mut core).await.unwrap().observations.is_empty());
    core.handle_notify(delayed).unwrap();
    heartbeat(&mut core, B, TestConfig::now(), Some(log(9)));
    assert!(capture(&mut core).await.unwrap().observations.is_empty());
    release(&mut core, receipt).await;
    assert!(capture(&mut core).await.unwrap().observations.is_empty());
    core.handle_notify(delayed_after_release).unwrap();
    assert!(
        capture(&mut core).await.unwrap().observations.is_empty(),
        "a reply queued before the fence must stay hidden after release"
    );
    core.engine.state.update_committed(&Some(log(8)));
    heartbeat(&mut core, A, TestConfig::now(), Some(log(2)));
    let read = capture(&mut core).await.unwrap();
    assert_eq!(read.observations[&3].target, Some(log(0)));
    assert_eq!(read.observations[&3].matching, Some(Some(log(2))));
    heartbeat(&mut core, B, TestConfig::now(), Some(log(3)));
    assert_eq!(capture(&mut core).await.unwrap().observations[&3].target, Some(log(8)));
}

#[tokio::test(start_paused = true)]
async fn participation_snapshot_and_metadata_free_replies_do_not_supply_evidence() {
    let mut core = core(&[1, 2, 3], true);
    let old_style = append(
        &core,
        3,
        RequestId::HeartBeat,
        TestConfig::now(),
        None,
        Ok(Some(log(9))),
    );
    core.handle_notify(old_style).unwrap();
    assert!(capture(&mut core).await.unwrap().observations.is_empty());
    core.engine.leader.as_mut().unwrap().progress.get_mut(&3).unwrap().inflight =
        Inflight::snapshot(Some(log(10))).with_id(8);
    let snapshot = append(
        &core,
        3,
        RequestId::new_snapshot(8),
        TestConfig::now(),
        Some(A),
        Ok(Some(log(10))),
    );
    core.handle_notify(snapshot).unwrap();
    let read = capture(&mut core).await.unwrap();
    assert_eq!(read.committed, Some(log(10)));
    assert!(
        read.observations.is_empty(),
        "even synthetic snapshot metadata is not append evidence"
    );
    heartbeat(&mut core, A, TestConfig::now(), Some(log(10)));
    assert_eq!(
        capture(&mut core).await.unwrap().observations[&3].matching,
        Some(Some(log(10)))
    );
}

#[tokio::test(start_paused = true)]
async fn participation_existing_evidence_is_not_refreshed_by_a_snapshot_or_metadata_free_reply() {
    let mut core = core(&[1, 2, 3], true);
    heartbeat(&mut core, A, TestConfig::now(), Some(log(5)));
    let before = capture(&mut core).await.unwrap().observations;
    tokio::time::advance(Duration::from_millis(20)).await;
    let plain = append(
        &core,
        3,
        RequestId::HeartBeat,
        TestConfig::now(),
        None,
        Ok(Some(log(9))),
    );
    core.handle_notify(plain).unwrap();
    assert_eq!(capture(&mut core).await.unwrap().observations, before);

    core.engine.leader.as_mut().unwrap().progress.get_mut(&3).unwrap().inflight =
        Inflight::snapshot(Some(log(10))).with_id(8);
    let snapshot = append(
        &core,
        3,
        RequestId::new_snapshot(8),
        TestConfig::now(),
        Some(B),
        Ok(Some(log(10))),
    );
    core.handle_notify(snapshot).unwrap();
    let after = capture(&mut core).await.unwrap();
    assert_eq!(after.committed, Some(log(10)));
    assert_eq!(after.observations, before);
    heartbeat(&mut core, B, TestConfig::now(), Some(log(10)));
    let confirmed = capture(&mut core).await.unwrap();
    assert_eq!(confirmed.observations[&3].runtime_id, B);
    assert_eq!(confirmed.observations[&3].matching, Some(Some(log(10))));
    assert_eq!(confirmed.observations[&3].target, Some(log(10)));
}

#[tokio::test(start_paused = true)]
async fn participation_stale_membership_and_transport_errors_do_not_refresh_a_row() {
    let mut core = core(&[1, 2, 3], true);
    core.engine.state.server_state = crate::ServerState::Leader;
    heartbeat(&mut core, A, TestConfig::now(), Some(log(5)));
    let before = capture(&mut core).await.unwrap().observations;
    assert_eq!(before[&3].runtime_id, A);
    tokio::time::advance(Duration::from_millis(20)).await;
    let stale = append(
        &core,
        3,
        RequestId::HeartBeat,
        TestConfig::now(),
        Some(B),
        Ok(Some(log(9))),
    );
    let membership = Membership::new(vec![BTreeSet::from([1, 2, 3])], BTreeSet::from([1, 2, 3, 4]));
    core.engine.replication_handler().append_membership(&log(11), &membership);
    core.handle_notify(stale).unwrap();
    assert_eq!(capture(&mut core).await.unwrap().observations, before);
    core.handle_notify(Notify::Network {
        generation: core.peer_admission.generation.clone(),
        response: Response::Progress {
            target: 3,
            request_id: RequestId::HeartBeat,
            result: Err("synthetic transport refusal".to_string()),
            session_id: ReplicationSessionId::new(*core.engine.state.vote_ref(), Some(log(11))),
        },
    })
    .unwrap();
    assert_eq!(capture(&mut core).await.unwrap().observations, before);
}

#[tokio::test(start_paused = true)]
async fn participation_membership_changes_retain_peers_and_remove_replaced_incarnations() {
    let mut core = core(&[1, 2, 3], true);
    core.engine.state.server_state = crate::ServerState::Leader;
    heartbeat(&mut core, A, TestConfig::now(), Some(log(7)));
    let learners = Membership::new(vec![BTreeSet::from([1, 2, 3])], BTreeSet::from([1, 2, 3, 4]));
    core.engine.replication_handler().append_membership(&log(11), &learners);
    assert!(!capture(&mut core).await.unwrap().observations.contains_key(&4));
    let reply = append(
        &core,
        4,
        RequestId::HeartBeat,
        TestConfig::now(),
        Some(B),
        Ok(Some(log(3))),
    );
    core.handle_notify(reply).unwrap();
    let learner = capture(&mut core).await.unwrap();
    assert!(!learner.membership.is_voter(&4));
    let target = learner.observations[&4].target;
    let joint = Membership::new(vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])], None);
    core.engine.replication_handler().append_membership(&log(12), &joint);
    let read = capture(&mut core).await.unwrap();
    assert_eq!(read.membership.membership(), &joint);
    assert_eq!(read.observations[&3].runtime_id, A);
    assert_eq!(read.observations[&4].target, target);
    let replacement = Membership::new(vec![BTreeSet::from([1, 2, 4])], None);
    core.engine.replication_handler().append_membership(&log(13), &replacement);
    let read = capture(&mut core).await.unwrap();
    assert!(!read.observations.contains_key(&3));
    assert_eq!(read.observations[&4].target, target);
    core.engine.replication_handler().append_membership(&log(14), &learners);
    assert!(!capture(&mut core).await.unwrap().observations.contains_key(&3));
}

#[tokio::test(start_paused = true)]
async fn participation_is_lost_with_leadership_and_is_not_inherited_on_reelection() {
    let mut core = core(&[1, 2, 3], true);
    heartbeat(&mut core, A, TestConfig::now(), Some(log(7)));
    assert_eq!(capture(&mut core).await.unwrap().observations[&3].runtime_id, A);
    let stale = append(
        &core,
        3,
        RequestId::HeartBeat,
        TestConfig::now(),
        Some(A),
        Ok(Some(log(9))),
    );
    core.engine.vote_handler().update_vote(&Vote::new_committed(6, 2)).unwrap();
    let forward = capture(&mut core).await.unwrap_err();
    assert_eq!(forward.leader_id, Some(2));
    core.engine.vote_handler().update_vote(&Vote::new_committed(7, 1)).unwrap();
    core.handle_notify(stale).unwrap();
    let read = capture(&mut core).await.unwrap();
    assert!(read.observations.is_empty());
    assert_eq!(read.vote, Vote::new_committed(7, 1));
}

#[tokio::test(start_paused = true)]
async fn participation_does_not_change_raft_matching_commit_or_commands() {
    let mut plain = core(&[1, 2, 3], true);
    let mut observed = core(&[1, 2, 3], true);
    let now = TestConfig::now();
    for (core, runtime_id) in [(&mut plain, None), (&mut observed, Some(A))] {
        let reply = append(
            core,
            3,
            RequestId::new_append_entries(7),
            now,
            runtime_id,
            Ok(Some(log(10))),
        );
        core.handle_notify(reply).unwrap();
    }
    assert_eq!(plain.engine.state, observed.engine.state);
    assert_eq!(
        plain.engine.leader.as_ref().unwrap().progress,
        observed.engine.leader.as_ref().unwrap().progress
    );
    assert_eq!(
        plain.engine.leader.as_ref().unwrap().clock_progress,
        observed.engine.leader.as_ref().unwrap().clock_progress
    );
    assert_eq!(
        plain.engine.output.take_commands(),
        observed.engine.output.take_commands()
    );
    assert!(capture(&mut plain).await.unwrap().observations.is_empty());
    assert_eq!(capture(&mut observed).await.unwrap().observations.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn participation_ignored_observation_still_allows_ordinary_raft_commit_progress() {
    let mut plain = core(&[1, 2, 3], true);
    let mut observed = core(&[1, 2, 3], true);
    let held_send = TestConfig::now();
    tokio::time::advance(Duration::from_millis(20)).await;
    for (core, runtime_id) in [(&mut plain, None), (&mut observed, Some(A))] {
        let reply = append(
            core,
            3,
            RequestId::HeartBeat,
            TestConfig::now(),
            runtime_id,
            Ok(Some(log(5))),
        );
        core.handle_notify(reply).unwrap();
    }
    let before = capture(&mut observed).await.unwrap().observations;
    assert_eq!(before[&3].runtime_id, A);
    for (core, runtime_id) in [(&mut plain, None), (&mut observed, Some(B))] {
        let reply = append(
            core,
            3,
            RequestId::new_append_entries(7),
            held_send,
            runtime_id,
            Ok(Some(log(10))),
        );
        core.handle_notify(reply).unwrap();
    }
    assert_eq!(plain.engine.state, observed.engine.state);
    assert_eq!(
        plain.engine.leader.as_ref().unwrap().progress,
        observed.engine.leader.as_ref().unwrap().progress
    );
    assert_eq!(
        plain.engine.leader.as_ref().unwrap().clock_progress,
        observed.engine.leader.as_ref().unwrap().clock_progress
    );
    assert_eq!(
        plain.engine.output.take_commands(),
        observed.engine.output.take_commands()
    );
    let read = capture(&mut observed).await.unwrap();
    assert_eq!(
        read.committed,
        Some(log(10)),
        "ignoring evidence must not ignore the Raft acknowledgement"
    );
    assert_eq!(read.observations, before);
}

fn hold_next_append_reply_after_target(core: &Core) -> Notify<TestConfig> {
    let id = core.engine.leader.as_ref().unwrap().progress.get(&3).inflight.get_id().unwrap();
    append(
        core,
        3,
        RequestId::new_append_entries(id),
        TestConfig::now(),
        Some(A),
        Ok(Some(log(10))),
    )
}

#[tokio::test(start_paused = true)]
async fn participation_reaches_a_fixed_target_while_a_held_reply_keeps_it_behind_live_commit() {
    let mut core = core(&[1, 2, 3], true);
    core.engine.state.update_committed(&Some(log(5)));
    core.engine.leader.as_mut().unwrap().progress.get_mut(&3).unwrap().inflight =
        Inflight::logs(None, Some(log(10))).with_id(7);
    let reply = append(
        &core,
        3,
        RequestId::new_append_entries(7),
        TestConfig::now(),
        Some(A),
        Ok(Some(log(5))),
    );
    core.handle_notify(reply).unwrap();
    let at_target = capture(&mut core).await.unwrap();
    assert_eq!(at_target.observations[&3].target, Some(log(5)));
    assert_eq!(at_target.observations[&3].matching, Some(Some(log(5))));
    let held = hold_next_append_reply_after_target(&core);
    core.engine.leader.as_mut().unwrap().progress.get_mut(&2).unwrap().inflight =
        Inflight::logs(None, Some(log(10))).with_id(8);
    let fast = append(
        &core,
        2,
        RequestId::new_append_entries(8),
        TestConfig::now(),
        Some(B),
        Ok(Some(log(10))),
    );
    core.handle_notify(fast).unwrap();
    let read = capture(&mut core).await.unwrap();
    assert_eq!(read.observations[&3].target, Some(log(5)));
    assert!(read.observations[&3].target <= read.observations[&3].matching.unwrap());
    assert!(read.observations[&3].matching.unwrap() < read.committed);
    core.handle_notify(held).unwrap();
    assert_eq!(capture(&mut core).await.unwrap().observations[&3].target, Some(log(5)));
}
