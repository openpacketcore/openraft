use super::*;
use crate::replication::response::ReplicationResult;

const RUNTIME: [u8; 16] = [9; 16];

fn observation(run: &mut Run) -> (RequestId, ReplicationResult<C>) {
    match run.rx.try_recv().expect("accepted replication notification") {
        Notify::Network {
            response: Response::Progress { request_id, result, .. },
            ..
        } => (request_id, result.unwrap()),
        other => panic!("expected progress, got {}", other.summary()),
    }
}

#[tokio::test(start_paused = true)]
async fn participation_stream_appends_are_serialized_but_quorum_probes_may_overlap() {
    let mut run = Run::with_settings(
        Some(log_id(0)),
        vec![Reply::First, Reply::Success, Reply::Success, Reply::Success],
        true,
        RPC_TIMEOUT.as_millis() as u64,
        Some(RUNTIME),
    )
    .await;
    run.start_logs();
    run.pending();
    assert_eq!(run.wire.lock().unwrap().replication_calls, 2);
    assert_eq!(run.wire.lock().unwrap().replication_inflight, 1);
    let (_, first) = observation(&mut run);
    assert_eq!(first.runtime_id, Some(RUNTIME));
    assert_eq!(first.result, Ok(Some(log_id(1))));

    run.leader
        .blocking_append([Entry {
            log_id: log_id(4),
            payload: EntryPayload::Normal(()),
        }])
        .await
        .unwrap();
    run.send(Replicate::logs(
        RequestId::new_append_entries(42),
        LogIdRange::new(Some(log_id(3)), Some(log_id(4))),
    ));
    run.send(Replicate::Heartbeat);
    run.send(Replicate::Committed(Some(log_id(3))));
    run.pending();
    assert_eq!(
        run.wire.lock().unwrap().replication_calls,
        2,
        "queued work cannot start a second stream RPC"
    );

    // A separate client to the same peer models the core's quorum round. It
    // must be allowed while the stream RPC is held, even though both are
    // AppendEntries and both may have empty payloads.
    let mut probe = ReadyNetwork {
        wire: run.wire.clone(),
        follower: run.follower.clone(),
        replies: VecDeque::from([Reply::Success]),
        second_gate: None,
        runtime_id: Some(RUNTIME),
    };
    let response = probe
        .append_entries(
            AppendEntriesRequest {
                vote: vote(),
                prev_log_id: None,
                leader_commit: None,
                entries: vec![],
            },
            RPCOption::new(RPC_TIMEOUT),
        )
        .await
        .unwrap();
    assert_eq!(response, AppendEntriesResponse::Success);
    assert_eq!(run.wire.lock().unwrap().replication_inflight, 1);
    assert_eq!(
        run.wire.lock().unwrap().replication_calls,
        2,
        "quorum append does not count as a stream append"
    );

    run.release();
    run.pending();
    let mut data = Vec::new();
    while !run.rx.is_empty() {
        let (request, response) = observation(&mut run);
        assert_eq!(response.runtime_id, Some(RUNTIME));
        if request != RequestId::HeartBeat {
            data.push((request, response.result));
        }
    }
    assert_eq!(data, vec![
        (RequestId::new_append_entries(41), Ok(Some(log_id(3)))),
        (RequestId::new_append_entries(42), Ok(Some(log_id(4)))),
    ]);
    assert_eq!(run.wire.lock().unwrap().replication_inflight, 0);
    assert_eq!(
        run.follower.try_get_log_entries(4..5).await.unwrap()[0].log_id,
        log_id(4)
    );
    run.close();
}

#[tokio::test(start_paused = true)]
async fn participation_metadata_is_paired_with_partial_empty_conflict_and_heartbeat_results() {
    for (previous, reply, expected) in [
        (Some(log_id(0)), Reply::First, Ok(Some(log_id(1)))),
        (None, Reply::Previous, Ok(None)),
        (Some(log_id(0)), Reply::Conflict, Err(log_id(0))),
    ] {
        let mut run = Run::with_settings(
            previous,
            vec![reply, Reply::Success],
            true,
            RPC_TIMEOUT.as_millis() as u64,
            Some(RUNTIME),
        )
        .await;
        let sent = Instant::now();
        run.start_logs();
        run.pending();
        let (_, response) = observation(&mut run);
        assert_eq!(response.runtime_id, Some(RUNTIME));
        assert_eq!(response.result, expected);
        assert_eq!(response.sending_time, sent);
        // Drop owns and cancels any held next RPC. No producer is detached.
    }

    let mut run = Run::with_settings(
        Some(log_id(0)),
        vec![Reply::Success],
        false,
        RPC_TIMEOUT.as_millis() as u64,
        Some(RUNTIME),
    )
    .await;
    let sent = Instant::now();
    run.send(Replicate::Heartbeat);
    run.pending();
    let (request, response) = observation(&mut run);
    assert_eq!(request, RequestId::HeartBeat);
    assert_eq!(response.runtime_id, Some(RUNTIME));
    assert_eq!(response.result, Ok(Some(log_id(0))));
    assert_eq!(response.sending_time, sent);
    run.close();
}
