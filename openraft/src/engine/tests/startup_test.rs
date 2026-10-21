use std::sync::Arc;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::entry::RaftEntry;
use crate::progress::entry::ProgressEntry;
use crate::progress::Inflight;
use crate::raft_state::LogStateReader;
use crate::replication::request_id::RequestId;
use crate::replication::response::ReplicationResult;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::EffectiveMembership;
use crate::Entry;
use crate::Membership;
use crate::MembershipState;
use crate::ServerState;
use crate::SnapshotMeta;
use crate::StoredMembership;
use crate::TokioInstant;
use crate::Vote;

fn m_empty() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {}], None)
}

fn m23() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {2,3}], None)
}

fn m34() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {3,4}], None)
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::testing_default(0);
    eng.state.enable_validation(false);
    eng.config.id = 2;
    // This will be overridden
    eng.state.server_state = ServerState::default();
    eng
}

/// It is a Leader but not yet append any logs.
#[test]
fn test_startup_as_leader_without_logs() -> anyhow::Result<()> {
    let mut eng = eng();
    // self.id==2 is a voter:
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(2, 1, 3)), m23())));
    // Committed vote makes it a leader at startup.
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(1, 2));

    eng.startup();

    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(
        vec![
            //
            Command::BecomeLeader,
            Command::RebuildReplicationStreams {
                targets: vec![(3, ProgressEntry {
                    matching: None,
                    curr_inflight_id: 0,
                    inflight: Inflight::None,
                    searching_end: 0
                })]
            },
            Command::AppendInputEntries {
                vote: Vote::new_committed(1, 2),
                entries: vec![Entry::<UTConfig>::new_blank(log_id(1, 2, 0))],
            },
            Command::Replicate {
                target: 3,
                req: Inflight::logs(None, Some(log_id(1, 2, 0))).with_id(1)
            }
        ],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_startup_as_leader_with_proposed_logs() -> anyhow::Result<()> {
    tracing::info!("--- a leader proposed logs and restarted, reuse noop_log_id");
    let mut eng = eng();
    // self.id==2 is a voter:
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(2, 1, 3)), m23())));
    // Fake existing log ids
    eng.state.log_ids = LogIdList::new([log_id(1, 1, 2), log_id(1, 2, 4), log_id(1, 2, 6)]);
    // Committed vote makes it a leader at startup.
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(1, 2));

    eng.startup();

    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(eng.leader.as_ref().unwrap().noop_log_id(), Some(&log_id(1, 2, 4)));
    assert_eq!(eng.leader.as_ref().unwrap().last_log_id(), Some(&log_id(1, 2, 6)));
    assert_eq!(
        vec![
            //
            Command::BecomeLeader,
            Command::RebuildReplicationStreams {
                targets: vec![(3, ProgressEntry {
                    matching: None,
                    curr_inflight_id: 0,
                    inflight: Inflight::None,
                    searching_end: 7
                })]
            },
            Command::Replicate {
                target: 3,
                req: Inflight::logs(None, Some(log_id(1, 2, 6))).with_id(1)
            }
        ],
        eng.output.take_commands()
    );

    Ok(())
}

/// When starting up, a leader that is not a voter should not panic.
#[test]
fn test_startup_as_leader_not_voter_issue_920() -> anyhow::Result<()> {
    let mut eng = eng();
    // self.id==2 is a voter:
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(2, 1, 3)), m_empty())));
    // Committed vote makes it a leader at startup.
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(1, 2));

    eng.startup();

    assert_eq!(ServerState::Learner, eng.state.server_state);
    assert_eq!(eng.output.take_commands(), vec![]);

    Ok(())
}

#[test]
fn test_startup_candidate_becomes_follower() -> anyhow::Result<()> {
    let mut eng = eng();
    // self.id==2 is a voter:
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(2, 1, 3)), m23())));
    // Non-committed vote makes it a candidate at startup.
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new(1, 2));

    eng.startup();

    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}
#[test]
fn test_startup_as_follower() -> anyhow::Result<()> {
    let mut eng = eng();
    // self.id==2 is a voter:
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(2, 1, 3)), m23())));

    eng.startup();

    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_startup_as_learner() -> anyhow::Result<()> {
    let mut eng = eng();
    // self.id==2 is not a voter:
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(2, 1, 3)), m34())));

    eng.startup();

    assert_eq!(ServerState::Learner, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

/// Recover a durable snapshot whose completion did not reach the previous engine.
fn recovered_snapshot_engine(membership: Membership<u64, ()>, vote: Vote<u64>) -> Engine<UTConfig> {
    let mut eng = eng();
    eng.config.max_in_snapshot_log_to_keep = 2;
    eng.config.purge_batch_size = 1;
    eng.config.max_payload_entries = 2;
    let stored = StoredMembership::new(Some(log_id(1, 2, 0)), membership.clone());
    let effective = EffectiveMembership::new_arc(Some(log_id(1, 2, 0)), membership);
    eng.state.membership_state = MembershipState::new(effective.clone(), effective);
    eng.state.vote = UTime::new(TokioInstant::now(), vote);
    eng.state.log_ids = LogIdList::new([log_id(1, 2, 2), log_id(1, 2, 10)]);
    eng.state.purged_next = 3;
    eng.state.purge_upto = Some(log_id(1, 2, 2));
    eng.state.io_state.purged = Some(log_id(1, 2, 2));
    eng.state.committed = Some(log_id(1, 2, 8));
    eng.state.io_state.update_applied(Some(log_id(1, 2, 8)));
    eng.state.io_state.update_snapshot(Some(log_id(1, 2, 8)));
    eng.state.snapshot_meta = SnapshotMeta {
        last_log_id: Some(log_id(1, 2, 8)),
        last_membership: stored,
        snapshot_id: "published-before-shutdown".to_string(),
    };
    eng
}

#[test]
fn test_startup_follower_purges_recovered_snapshot() {
    let mut eng = recovered_snapshot_engine(m23(), Vote::new_committed(2, 3));

    eng.startup();

    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(Some(log_id(1, 2, 6)), eng.state.purge_upto);
    assert_eq!(Some(&log_id(1, 2, 6)), eng.state.last_purged_log_id());
    assert_eq!(
        vec![Command::PurgeLog { upto: log_id(1, 2, 6) }],
        eng.output.take_commands()
    );
}

#[test]
fn test_startup_learner_purges_recovered_snapshot() {
    let membership = Membership::new(vec![btreeset! {3, 4}], Some(btreeset! {2, 3, 4}));
    let mut eng = recovered_snapshot_engine(membership, Vote::new_committed(2, 3));

    eng.startup();

    assert_eq!(ServerState::Learner, eng.state.server_state);
    assert_eq!(Some(log_id(1, 2, 6)), eng.state.purge_upto);
    assert_eq!(Some(&log_id(1, 2, 6)), eng.state.last_purged_log_id());
    assert_eq!(
        vec![Command::PurgeLog { upto: log_id(1, 2, 6) }],
        eng.output.take_commands()
    );
}

#[test]
fn test_startup_leader_purges_recovered_snapshot_without_inflight_logs() {
    let membership = Membership::new(vec![btreeset! {2}], None);
    let mut eng = recovered_snapshot_engine(membership, Vote::new_committed(1, 2));

    eng.startup();

    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(Some(log_id(1, 2, 6)), eng.state.purge_upto);
    assert_eq!(Some(&log_id(1, 2, 6)), eng.state.last_purged_log_id());
    assert!(eng.output.take_commands().contains(&Command::PurgeLog { upto: log_id(1, 2, 6) }));
}

#[test]
fn test_startup_leader_defers_recovered_snapshot_purge_until_replication_releases_logs() {
    let mut eng = recovered_snapshot_engine(m23(), Vote::new_committed(1, 2));

    eng.startup();

    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(Some(log_id(1, 2, 6)), eng.state.purge_upto);
    assert_eq!(Some(&log_id(1, 2, 2)), eng.state.last_purged_log_id());
    let commands = eng.output.take_commands();
    assert!(commands.contains(&Command::Replicate {
        target: 3,
        req: Inflight::logs(Some(log_id(1, 2, 2)), Some(log_id(1, 2, 4))).with_id(1),
    }));
    assert!(!commands.iter().any(|command| matches!(command, Command::PurgeLog { .. })));

    eng.replication_handler().update_progress(
        3,
        RequestId::new_append_entries(1),
        Ok(ReplicationResult::new(TokioInstant::now(), Ok(Some(log_id(1, 2, 4))))),
    );
    assert_eq!(Some(&log_id(1, 2, 2)), eng.state.last_purged_log_id());
    let commands = eng.output.take_commands();
    assert!(commands.contains(&Command::Replicate {
        target: 3,
        req: Inflight::logs(Some(log_id(1, 2, 4)), Some(log_id(1, 2, 6))).with_id(2),
    }));
    assert!(!commands.iter().any(|command| matches!(command, Command::PurgeLog { .. })));

    eng.replication_handler().update_progress(
        3,
        RequestId::new_append_entries(2),
        Ok(ReplicationResult::new(TokioInstant::now(), Ok(Some(log_id(1, 2, 6))))),
    );
    assert_eq!(Some(&log_id(1, 2, 6)), eng.state.last_purged_log_id());
    assert!(eng.output.take_commands().contains(&Command::PurgeLog { upto: log_id(1, 2, 6) }));
}

#[test]
fn test_startup_without_snapshot_and_zero_purge_batch() {
    let mut eng = eng();
    eng.config.purge_batch_size = 0;
    eng.config.max_in_snapshot_log_to_keep = 0;

    eng.startup();

    assert_eq!(None, eng.state.purge_upto);
    assert!(!eng.output.take_commands().iter().any(|command| matches!(command, Command::PurgeLog { .. })));
}

#[test]
fn test_startup_leader_waits_for_voter_and_learner_after_failed_replication() {
    for learner_conflict in [false, true] {
        let membership = Membership::new(vec![btreeset! {2, 3}], Some(btreeset! {2, 3, 4}));
        let mut eng = recovered_snapshot_engine(membership, Vote::new_committed(1, 2));

        eng.startup();

        assert_eq!(Some(log_id(1, 2, 6)), eng.state.purge_upto);
        assert_eq!(Some(&log_id(1, 2, 2)), eng.state.last_purged_log_id());
        let commands = eng.output.take_commands();
        for target in [3, 4] {
            assert!(commands.contains(&Command::Replicate {
                target,
                req: Inflight::logs(Some(log_id(1, 2, 2)), Some(log_id(1, 2, 4))).with_id(1),
            }));
        }
        assert!(!commands.iter().any(|command| matches!(command, Command::PurgeLog { .. })));

        eng.replication_handler().update_progress(
            3,
            RequestId::new_append_entries(1),
            Err("synthetic voter replication failure".to_string()),
        );
        assert_eq!(Some(&log_id(1, 2, 2)), eng.state.last_purged_log_id());
        let commands = eng.output.take_commands();
        assert!(commands.contains(&Command::Replicate {
            target: 3,
            req: Inflight::logs(Some(log_id(1, 2, 6)), Some(log_id(1, 2, 8))).with_id(2),
        }));
        assert!(!commands.iter().any(|command| matches!(command, Command::PurgeLog { .. })));

        let response = if learner_conflict {
            Ok(ReplicationResult::new(TokioInstant::now(), Err(log_id(1, 2, 2))))
        } else {
            Err("synthetic learner replication failure".to_string())
        };
        eng.replication_handler().update_progress(4, RequestId::new_append_entries(1), response);
        assert_eq!(Some(&log_id(1, 2, 6)), eng.state.last_purged_log_id());
        assert_eq!(Some(log_id(1, 2, 6)), eng.state.purge_upto);
        let commands = eng.output.take_commands();
        assert_eq!(
            1,
            commands.iter().filter(|command| **command == Command::PurgeLog { upto: log_id(1, 2, 6) }).count()
        );
        let retry = if learner_conflict {
            Inflight::snapshot(Some(log_id(1, 2, 8))).with_id(2)
        } else {
            Inflight::logs(Some(log_id(1, 2, 6)), Some(log_id(1, 2, 8))).with_id(2)
        };
        assert!(commands.contains(&Command::Replicate { target: 4, req: retry }));
    }
}
