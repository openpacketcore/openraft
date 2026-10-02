use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::ServerState;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// A node with higher term takes leadership from the current leader.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn elect_seize_leadership() -> Result<()> {
    // Only the triggered election runs. Without heartbeats every follower would otherwise campaign
    // as soon as its lease and election timeout expire, and compete with the triggered one.
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2");
    let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 becomes leader").await?;

    tracing::info!(log_index, "--- let every leader lease expire without heartbeats");
    tokio::time::sleep(Duration::from_millis(config.election_timeout_max)).await;

    tracing::info!(log_index, "--- trigger election on node 1");
    {
        let n1 = router.get_raft_handle(&1)?;
        n1.trigger().elect().await?;

        n1.wait(timeout()).state(ServerState::Leader, "node 1 becomes leader").await?;
    }

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2000))
}
