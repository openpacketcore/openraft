use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::error::Fatal;
use openraft::error::RaftError;
use openraft::raft::AppendEntriesRequest;
use openraft::storage::RaftLogStorage;
use openraft::Config;
use openraft::RPCTypes;
use openraft::Vote;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// A higher vote must join the previous leader's retained partial-append
/// retries. Otherwise they keep reading a suffix that can now be truncated.
#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn stepdown_joins_partial_append_retries() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 300,
            enable_tick: false,
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config);
    let committed = router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;
    router.set_append_entries_quota(Some(0));
    let count = || router.get_rpc_count().get(&RPCTypes::AppendEntries).copied().unwrap_or(0);
    let initial_count = count();
    let leader = router.get_raft_handle(&0)?;
    let writer = leader.clone();
    let write = tokio::spawn(async move { writer.client_write(ClientRequest::make_request("stepdown", 1)).await });
    router
        .wait(&0, Some(Duration::from_secs(2)))
        .metrics(
            |m| m.last_log_index == Some(committed + 1),
            "uncommitted suffix appended",
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while count() < initial_count + 4 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let (mut storage, _) = router.get_storage_handle(&0)?;
    let suffix = storage.get_log_state().await?.last_log_id;
    assert_eq!(suffix.map(|id| id.index), Some(committed + 1));
    let term = leader.metrics().borrow().current_term;
    let response = leader
        .append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(term + 1, 1),
            prev_log_id: suffix,
            entries: vec![],
            leader_commit: None,
        })
        .await?;
    assert!(response.is_success());
    // Keep the suffix so a stale reader cannot hide by crashing on its
    // removal. With ticks and automatic heartbeats disabled, only the old
    // partial-append retries could generate more AppendEntries here.
    let stopped_count = count();
    let guard = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        assert_eq!(count(), stopped_count, "former leader retained live replication tasks");
        if tokio::time::Instant::now() >= guard {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(storage.get_log_state().await?.last_log_id, suffix);
    assert_eq!(
        leader.metrics().borrow().last_applied.map(|id| id.index),
        Some(committed)
    );
    assert_eq!(leader.metrics().borrow().running_state, Ok(()));
    leader.shutdown().await?;
    assert!(matches!(write.await?, Err(RaftError::Fatal(Fatal::Stopped))));
    for id in [1, 2] {
        router.get_raft_handle(&id)?.shutdown().await?;
    }
    Ok(())
}
