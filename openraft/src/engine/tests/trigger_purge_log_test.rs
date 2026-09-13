use std::ops::Deref;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::progress::Inflight;
use crate::progress::Progress;
use crate::raft_state::LogStateReader;
use crate::replication::request_id::RequestId;
use crate::replication::response::ReplicationResult;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::CommittedLeaderId;
use crate::EffectiveMembership;
use crate::LogId;
use crate::Membership;
use crate::MembershipState;
use crate::SnapshotMeta;
use crate::StoredMembership;
use crate::TokioInstant;
use crate::Vote;

fn m12() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {1,2}], None)
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.enable_validation(false); // Disable validation for incomplete state
    eng.state.membership_state = MembershipState::new(
        EffectiveMembership::new_arc(Some(log_id(1, 0, 1)), m12()),
        EffectiveMembership::new_arc(Some(log_id(1, 0, 1)), m12()),
    );

    eng.state.log_ids = LogIdList::new([LogId::new(CommittedLeaderId::new(0, 0), 0)]);
    eng
}

#[test]
fn test_trigger_purge_log_no_snapshot() -> anyhow::Result<()> {
    let mut eng = eng();

    eng.trigger_purge_log(1);

    assert_eq!(None, eng.state.purge_upto, "no snapshot, can not purge");

    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_trigger_purge_log_already_scheduled() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.snapshot_meta = SnapshotMeta {
        last_log_id: Some(log_id(1, 0, 3)),
        last_membership: StoredMembership::new(Some(log_id(1, 0, 1)), m12()),
        snapshot_id: "1".to_string(),
    };
    eng.state.purge_upto = Some(log_id(1, 0, 2));
    eng.state.io_state.purged = Some(log_id(1, 0, 2));

    eng.trigger_purge_log(2);

    assert_eq!(Some(log_id(1, 0, 2)), eng.state.purge_upto, "already purged, no update");

    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_trigger_purge_log_delete_only_in_snapshot_logs() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.snapshot_meta = SnapshotMeta {
        last_log_id: Some(log_id(1, 0, 3)),
        last_membership: StoredMembership::new(Some(log_id(1, 0, 1)), m12()),
        snapshot_id: "1".to_string(),
    };
    eng.state.purge_upto = Some(log_id(1, 0, 2));
    eng.state.io_state.purged = Some(log_id(1, 0, 2));
    eng.state.log_ids = LogIdList::new([log_id(1, 0, 2), log_id(1, 0, 10)]);

    eng.trigger_purge_log(5);

    assert_eq!(
        Some(log_id(1, 0, 3)),
        eng.state.purge_upto,
        "delete only in snapshot logs"
    );

    assert_eq!(
        vec![Command::PurgeLog { upto: log_id(1, 0, 3) },],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_trigger_purge_log_in_used_wont_be_delete() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.snapshot_meta = SnapshotMeta {
        last_log_id: Some(log_id(1, 0, 3)),
        last_membership: StoredMembership::new(Some(log_id(1, 0, 1)), m12()),
        snapshot_id: "1".to_string(),
    };
    eng.state.purge_upto = Some(log_id(1, 0, 2));
    eng.state.io_state.purged = Some(log_id(1, 0, 2));
    eng.state.log_ids = LogIdList::new([log_id(1, 0, 2), log_id(1, 0, 10)]);
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(2, 1));

    // Make it a leader and mark the logs are in flight.
    eng.testing_new_leader();
    let l = eng.leader.as_mut().unwrap();
    let _ = l.progress.get_mut(&2).unwrap().next_send(eng.state.deref(), 10).unwrap();

    eng.trigger_purge_log(5);

    assert_eq!(
        Some(log_id(1, 0, 3)),
        eng.state.purge_upto,
        "delete only in snapshot logs"
    );

    assert_eq!(0, eng.output.take_commands().len(), "in used log wont be deleted");

    Ok(())
}

fn snapshot_tail_engine() -> Engine<UTConfig> {
    let mut eng = eng();
    eng.config.id = 1;
    eng.config.max_payload_entries = 3;
    eng.state.snapshot_meta = SnapshotMeta {
        last_log_id: Some(log_id(1, 0, 12)),
        last_membership: StoredMembership::new(Some(log_id(1, 0, 1)), m12()),
        snapshot_id: "newer-than-inflight".to_string(),
    };
    eng.state.log_ids = LogIdList::new([log_id(1, 0, 2), log_id(1, 0, 15)]);
    eng.state.purged_next = 3;
    eng.state.io_state.purged = Some(log_id(1, 0, 2));
    eng.state.purge_upto = Some(log_id(1, 0, 10));
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(2, 1));
    eng.testing_new_leader();
    let target = eng.leader.as_mut().unwrap().progress.get_mut(&2).unwrap();
    target.searching_end = 6;
    target.curr_inflight_id = 7;
    target.inflight = Inflight::snapshot(Some(log_id(1, 0, 5))).with_id(7);
    eng.output.take_commands();
    eng
}

#[test]
fn test_snapshot_tail_survives_scheduled_purge_while_transfer_is_owned() {
    let mut eng = snapshot_tail_engine();
    eng.replication_handler().try_purge_log();
    assert_eq!(Some(&log_id(1, 0, 2)), eng.state.last_purged_log_id());
    assert!(eng.output.take_commands().is_empty());
}

#[test]
fn test_snapshot_tail_handoff_sends_retained_logs_before_pending_purge() {
    let mut eng = snapshot_tail_engine();
    for (request, matched, last) in [
        (RequestId::new_snapshot(7), 5, 8),
        (RequestId::new_append_entries(8), 8, 11),
    ] {
        eng.replication_handler().update_progress(
            2,
            request,
            Ok(ReplicationResult::new(
                TokioInstant::now(),
                Ok(Some(log_id(1, 0, matched))),
            )),
        );
        assert_eq!(Some(&log_id(1, 0, 2)), eng.state.last_purged_log_id());
        assert_eq!(
            vec![Command::Replicate {
                target: 2,
                req: Inflight::logs(Some(log_id(1, 0, matched)), Some(log_id(1, 0, last)))
                    .with_id(request.request_id().unwrap() + 1),
            }],
            eng.output.take_commands(),
        );
        assert_eq!(None, eng.state.committed(), "one follower cannot grant a quorum");
    }
    eng.replication_handler().update_progress(
        2,
        RequestId::new_append_entries(9),
        Ok(ReplicationResult::new(TokioInstant::now(), Ok(Some(log_id(1, 0, 11))))),
    );
    assert_eq!(Some(&log_id(1, 0, 10)), eng.state.last_purged_log_id());
    let commands = eng.output.take_commands();
    assert!(commands.contains(&Command::PurgeLog { upto: log_id(1, 0, 10) }));
    assert!(commands.contains(&Command::Replicate {
        target: 2,
        req: Inflight::logs(Some(log_id(1, 0, 11)), Some(log_id(1, 0, 14))).with_id(10),
    }));
    assert_eq!(None, eng.state.committed());
}

#[test]
fn test_snapshot_tail_failed_transfer_releases_retention_before_retry() {
    let mut eng = snapshot_tail_engine();
    eng.replication_handler()
        .update_progress(2, RequestId::new_snapshot(7), Err("transfer failed".to_string()));
    assert_eq!(Some(&log_id(1, 0, 10)), eng.state.last_purged_log_id());
    assert_eq!(
        vec![Command::PurgeLog { upto: log_id(1, 0, 10) }, Command::Replicate {
            target: 2,
            req: Inflight::snapshot(Some(log_id(1, 0, 12))).with_id(8),
        },],
        eng.output.take_commands()
    );
    assert_eq!(None, eng.state.committed());
}

#[test]
fn test_snapshot_tail_heartbeat_does_not_release_live_transfer() {
    for result in [
        Ok(ReplicationResult::new(TokioInstant::now(), Ok(None))),
        Err("heartbeat unavailable".to_string()),
    ] {
        let mut eng = snapshot_tail_engine();
        eng.replication_handler().update_progress(2, RequestId::HeartBeat, result);
        assert_eq!(Some(&log_id(1, 0, 2)), eng.state.last_purged_log_id());
        assert_eq!(
            Inflight::snapshot(Some(log_id(1, 0, 5))).with_id(7),
            eng.leader.as_ref().unwrap().progress.get(&2).inflight,
        );
        assert!(eng.output.take_commands().is_empty());
        assert_eq!(None, eng.state.committed());
    }
}

#[test]
fn test_snapshot_tail_newer_snapshot_ack_skips_only_covered_logs() {
    let mut eng = snapshot_tail_engine();
    eng.replication_handler().update_progress(
        2,
        RequestId::new_snapshot(7),
        Ok(ReplicationResult::new(TokioInstant::now(), Ok(Some(log_id(1, 0, 12))))),
    );
    assert_eq!(Some(&log_id(1, 0, 10)), eng.state.last_purged_log_id());
    assert_eq!(
        vec![
            Command::Replicate {
                target: 2,
                req: Inflight::logs(Some(log_id(1, 0, 12)), Some(log_id(1, 0, 15))).with_id(8),
            },
            Command::PurgeLog { upto: log_id(1, 0, 10) },
        ],
        eng.output.take_commands()
    );
    assert_eq!(None, eng.state.committed());
}

#[test]
fn test_snapshot_tail_failed_log_handoff_releases_retention_before_retry() {
    let mut eng = snapshot_tail_engine();
    eng.replication_handler().update_progress(
        2,
        RequestId::new_snapshot(7),
        Ok(ReplicationResult::new(TokioInstant::now(), Ok(Some(log_id(1, 0, 5))))),
    );
    eng.output.take_commands();
    eng.replication_handler().update_progress(
        2,
        RequestId::new_append_entries(8),
        Err("append unavailable".to_string()),
    );
    assert_eq!(Some(&log_id(1, 0, 10)), eng.state.last_purged_log_id());
    assert_eq!(
        vec![Command::PurgeLog { upto: log_id(1, 0, 10) }, Command::Replicate {
            target: 2,
            req: Inflight::snapshot(Some(log_id(1, 0, 12))).with_id(9),
        },],
        eng.output.take_commands()
    );
    assert_eq!(None, eng.state.committed());
}
