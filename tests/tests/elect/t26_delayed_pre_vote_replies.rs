use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::ServerState;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::Direction;
use crate::fixtures::RPCErrorType;
use crate::fixtures::RaftRouter;

/// The tightest valid election-timeout window: its width is 1 ms, and the heartbeat tick is 30 ms.
fn tight_config() -> Result<Arc<Config>> {
    Ok(Arc::new(
        Config {
            heartbeat_interval: 20,
            election_timeout_min: 1_000,
            election_timeout_max: 1_001,
            enable_pre_vote: Some(true),
            ..Default::default()
        }
        .validate()?,
    ))
}

/// The timing of a production deployment: a 1,500 ms window and a 300 ms tick.
fn production_config() -> Result<Arc<Config>> {
    Ok(Arc::new(
        Config {
            heartbeat_interval: 200,
            append_entries_timeout: Some(2_000),
            election_timeout_min: 5_000,
            election_timeout_max: 6_500,
            enable_pre_vote: Some(true),
            ..Default::default()
        }
        .validate()?,
    ))
}

/// The surviving voters of a lost leader answer every Pre-Vote after `delay`, longer than the
/// election-timeout window and the tick but within the Pre-Vote deadline. A round that awaits
/// their answers must stay open to count them, or no round ever reaches a quorum and no leader is
/// elected.
async fn delayed_grants_elect_a_successor(config: Arc<Config>, delay: Duration) -> Result<()> {
    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    router
        .get_raft_handle(&0)?
        .wait(timeout())
        .state(ServerState::Leader, "node 0 is the initial leader")
        .await?;

    tracing::info!("--- nodes 1 and 2 answer every Pre-Vote after {:?}", delay);
    for id in [1, 2] {
        router.set_pre_vote_reply_delay(id, Some(delay));
    }

    tracing::info!("--- node 0 is lost");
    router.set_network_error(0, true);

    let election = Some(Duration::from_millis(config.election_timeout_max * 6) + delay * 4);
    router
        .wait(&1, election)
        .metrics(
            |m| m.current_leader.is_some_and(|leader| leader != 0),
            "nodes 1 and 2 elect a successor",
        )
        .await?;

    Ok(())
}

/// A voter that cannot answer Pre-Vote is reported only after `delay`, longer than the
/// election-timeout window and the tick but within the Pre-Vote deadline. Its log is behind, so the
/// other survivor can only win with its classic vote. The round that awaits the report must stay
/// open to receive it, or that survivor never runs the classic election and no leader is elected.
async fn delayed_unsupported_runs_the_classic_election(config: Arc<Config>, delay: Duration) -> Result<()> {
    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    router
        .get_raft_handle(&0)?
        .wait(timeout())
        .state(ServerState::Leader, "node 0 is the initial leader")
        .await?;

    tracing::info!(
        "--- node 2 cannot answer Pre-Vote, and that is reported after {:?}",
        delay
    );
    router.get_raft_handle(&2)?.runtime_config().pre_vote(false);
    router.set_pre_vote_unsupported(2, true);
    router.set_pre_vote_reply_delay(2, Some(delay));

    tracing::info!("--- node 2 misses the last entries");
    router.set_rpc_failure(2, Direction::NetRecv, Some(RPCErrorType::NetworkError));
    router.client_request_many(0, "missed", 5).await?;
    log_index += 5;
    router.wait(&1, timeout()).applied_index(Some(log_index), "node 1 has the entries").await?;

    tracing::info!("--- node 0 is lost; node 2 is reachable again");
    router.set_network_error(0, true);
    router.set_rpc_failure(2, Direction::NetRecv, None);

    let election = Some(Duration::from_millis(config.election_timeout_max * 6) + delay * 4);
    router
        .wait(&1, election)
        .state(ServerState::Leader, "node 1 is elected with node 2's classic vote")
        .await?;

    Ok(())
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn tight_window_delayed_pre_vote_grants_elect_a_successor() -> Result<()> {
    delayed_grants_elect_a_successor(tight_config()?, Duration::from_millis(65)).await
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn production_timing_delayed_pre_vote_grants_elect_a_successor() -> Result<()> {
    delayed_grants_elect_a_successor(production_config()?, Duration::from_millis(1_900)).await
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn tight_window_delayed_unsupported_runs_the_classic_election() -> Result<()> {
    delayed_unsupported_runs_the_classic_election(tight_config()?, Duration::from_millis(65)).await
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn production_timing_delayed_unsupported_runs_the_classic_election() -> Result<()> {
    delayed_unsupported_runs_the_classic_election(production_config()?, Duration::from_millis(1_900)).await
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(10_000))
}
