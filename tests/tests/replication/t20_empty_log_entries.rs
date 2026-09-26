use std::panic;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// Counts panics raised inside the replication task.
///
/// The panic happens in a spawned task, so it never fails the test thread by itself: RaftCore
/// rebuilds the dead replication stream and the cluster recovers, which is exactly why the
/// cluster-level assertions below cannot see the defect. The panic is therefore observed directly
/// through the panic hook.
static REPLICATION_PANICS: AtomicUsize = AtomicUsize::new(0);

/// Replication must survive a store that returns empty from `limited_get_log_entries()`.
///
/// Returning nothing for a non-empty range violates the API contract, but it must be handled as a
/// heartbeat rather than panicking the replication task. Before the fix the leader unwrapped
/// `logs.first()` on the empty vec and the replication task died; RaftCore then rebuilt the
/// stream, so the cluster recovered and only a spurious panic and a replication stall remained.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn empty_limited_get_log_entries() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    // Count replication panics without hiding them: the previous hook still runs, so a panic is
    // still reported. This binary runs its tests in parallel, but only this test drives an empty
    // `limited_get_log_entries`, and a replication panic from any other test would be a defect
    // too.
    REPLICATION_PANICS.store(0, Ordering::Relaxed);
    let prev_hook = Arc::new(panic::take_hook());
    {
        let prev_hook = prev_hook.clone();
        panic::set_hook(Box::new(move |info| {
            if let Some(loc) = info.location() {
                if loc.file().ends_with("replication/mod.rs") {
                    REPLICATION_PANICS.fetch_add(1, Ordering::Relaxed);
                }
            }
            prev_hook(info);
        }));
    }

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing 3-node cluster");
    let mut log_index = router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;

    tracing::info!(log_index, "--- write some logs to establish replication");
    log_index += router.client_request_many(0, "foo", 5).await?;
    router.wait(&1, timeout()).applied_index(Some(log_index), "node 1 replicated").await?;
    router.wait(&2, timeout()).applied_index(Some(log_index), "node 2 replicated").await?;

    tracing::info!(log_index, "--- make the leader's log reader return empty");
    {
        let (log_store, _sm) = router.get_storage_handle(&0)?;
        log_store.storage_mut().await.set_return_empty_limited_get(true);
    }

    tracing::info!(log_index, "--- the write cannot commit, but must not panic replication");
    {
        // No follower can receive the entry while the reader returns empty, so this never
        // commits. Submit it fire-and-forget and let replication attempt the send, which is
        // where the panic used to happen.
        let raft = router.get_raft_handle(&0)?;
        let _rx = raft.client_write_ff(ClientRequest::make_request("bar", 1)).await?;

        tokio::time::sleep(Duration::from_millis(800)).await;

        // RaftCore must still be reachable.
        raft.with_raft_state(|_| ()).await?;
    }
    log_index += 1;

    tracing::info!(log_index, "--- clear the fault; replication must resume on its own");
    {
        let (log_store, _sm) = router.get_storage_handle(&0)?;
        log_store.storage_mut().await.set_return_empty_limited_get(false);
    }

    router.wait(&1, timeout()).applied_index(Some(log_index), "node 1 recovered").await?;
    router.wait(&2, timeout()).applied_index(Some(log_index), "node 2 recovered").await?;

    let _ = panic::take_hook();

    assert_eq!(
        0,
        REPLICATION_PANICS.load(Ordering::Relaxed),
        "the empty result must be handled as a heartbeat, not panic the replication task"
    );

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3_000))
}

/// An empty first range has no matched log ID. Its heartbeat acknowledgement must
/// keep the leader alive without reporting a successful data match of `None`.
#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn empty_first_range_on_fresh_learner_recovers() -> Result<()> {
    use anyhow::ensure;
    use openraft::RPCTypes;

    use crate::fixtures::RPCRequest;

    let config = Arc::new(
        Config {
            enable_tick: false,
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config);
    let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;
    log_index += router.client_request_many(0, "empty-first-range", 5).await?;
    router.new_raft_node(1).await;
    let leader = router.get_raft_handle(&0)?;
    let learner = router.get_raft_handle(&1)?;
    let (log_store, _) = router.get_storage_handle(&0)?;
    let store = log_store.storage_mut().await.clone();
    let empty_first_probes = Arc::new(AtomicUsize::new(0));
    let probes = empty_first_probes.clone();
    router.set_rpc_observer(RPCTypes::AppendEntries, move |rpc, from, to| {
        if let RPCRequest::AppendEntries(request) = rpc {
            if from == 0 && to == 1 && request.prev_log_id.is_none() && request.entries.is_empty() {
                probes.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    store.set_return_empty_limited_get(true);

    let result: Result<()> = async {
        leader.add_learner(1, (), false).await?;
        log_index += 1;
        router
            .wait(&0, timeout())
            .metrics(
                |metrics| metrics.running_state.is_err() || empty_first_probes.load(Ordering::SeqCst) >= 3,
                "fresh learner attempted empty first-range heartbeats",
            )
            .await?;
        ensure!(
            leader.metrics().borrow().running_state.is_ok(),
            "EMPTY_FIRST_RANGE_LEADER: an empty first-range heartbeat must not kill the leader"
        );
        ensure!(
            empty_first_probes.load(Ordering::SeqCst) >= 3,
            "empty first-range retries must actually execute"
        );
        leader.with_raft_state(|_| ()).await?;
        store.set_return_empty_limited_get(false);
        router.wait(&1, timeout()).applied_index(Some(log_index), "fresh learner recovered").await?;
        ensure!(
            leader.metrics().borrow().running_state.is_ok(),
            "leader must remain healthy after recovery"
        );
        Ok(())
    }
    .await;

    // A failing detector must also release the fault and join its real tasks.
    store.set_return_empty_limited_get(false);
    leader.shutdown().await?;
    learner.shutdown().await?;
    result
}
