use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use tokio::time::sleep;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// AppendEntries is bounded by its own deadline, not by the heartbeat interval.
///
/// Log replication, heartbeats and the heartbeats that confirm leadership for a linearizable read
/// all carry `Config::append_entries_timeout` as their hard deadline. A follower that holds one
/// AppendEntries for several heartbeat intervals is waited for instead of being abandoned and sent
/// the request again.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn append_entries_deadline_is_independent_of_the_heartbeat_interval() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            append_entries_timeout: Some(1_000),
            election_timeout_min: 1_500,
            election_timeout_max: 1_600,
            ..Default::default()
        }
        .validate()?,
    );
    let deadline = Duration::from_millis(1_000);

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    tracing::info!("--- replicate logs and confirm leadership for a linearizable read");
    router.client_request_many(0, "foo", 3).await?;
    log_index += 3;
    for id in [0, 1, 2] {
        router.wait(&id, timeout()).applied_index(Some(log_index), "logs are replicated").await?;
    }
    router.ensure_linearizable(0).await?;

    let sent = router.append_entries_deadlines();
    assert!(!sent.is_empty(), "AppendEntries RPCs were sent");
    for (target, ttl) in &sent {
        assert_eq!(
            deadline, *ttl,
            "an AppendEntries RPC to node {} is bounded by the AppendEntries deadline",
            target
        );
    }

    tracing::info!("--- node 2 holds AppendEntries for six heartbeat intervals");
    let before_hold = router.append_entries_deadlines().len();
    router.set_rpc_blocked(2, true);
    sleep(Duration::from_millis(300)).await;
    router.set_rpc_blocked(2, false);
    let during_hold =
        router.append_entries_deadlines()[before_hold..].iter().filter(|(target, _)| *target == 2).count();
    assert_eq!(
        1, during_hold,
        "the held AppendEntries is waited for within its deadline, not abandoned and resent"
    );

    tracing::info!("--- node 2 keeps up after the hold");
    router.client_request_many(0, "foo", 1).await?;
    log_index += 1;
    router.wait(&2, timeout()).applied_index(Some(log_index), "node 2 keeps replicating").await?;

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2_000))
}
