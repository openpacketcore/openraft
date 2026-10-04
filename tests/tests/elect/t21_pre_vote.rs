use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::ServerState;
use tokio::time::sleep;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// A fault window several election timeouts long, as in a fault campaign that cuts one voter off
/// from consensus traffic while the others keep committing.
const ISOLATION_ELECTION_TIMEOUTS: u64 = 4;

fn pre_vote_config() -> Result<Arc<Config>> {
    Ok(Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 500,
            election_timeout_max: 600,
            enable_pre_vote: Some(true),
            ..Default::default()
        }
        .validate()?,
    ))
}

/// How the isolated voter returns to the cluster.
#[derive(Clone, Copy, Debug)]
enum Rejoin {
    /// Its consensus traffic is admitted again.
    Reconnect,
    /// It is restarted from its own durable state while still cut off, then admitted again.
    Restart,
}

/// With Pre-Vote, a voter cut off from consensus traffic for several election timeouts does not
/// raise its term, so it rejoins as a follower without deposing the healthy leader.
///
/// Without Pre-Vote the isolated voter campaigns on every election timeout and persists each new
/// term. Once it is reachable again, the leader sees that higher term in an AppendEntries response
/// and steps down, although a quorum was serving it throughout.
async fn isolated_voter_rejoins_without_deposing_the_leader(voter_count: u64, rejoin: Rejoin) -> Result<()> {
    let config = pre_vote_config()?;
    let isolation = Duration::from_millis(config.election_timeout_max * ISOLATION_ELECTION_TIMEOUTS);

    let mut router = RaftRouter::new(config.clone());

    let voters = (0..voter_count).collect::<std::collections::BTreeSet<u64>>();
    tracing::info!("--- create cluster of {:?}; node 0 becomes leader", voters);
    let mut log_index = router.new_cluster(voters.clone(), btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;
    for id in voters.iter() {
        router
            .wait(id, timeout())
            .metrics(|m| m.current_leader == Some(0), "every voter follows node 0")
            .await?;
    }
    let leader_term = n0.metrics().borrow().current_term;

    tracing::info!("--- cut node 1 off from consensus traffic for {:?}", isolation);
    router.set_network_error(1, true);
    let isolated_at = Instant::now();
    let mut highest_isolated_term = router.get_raft_handle(&1)?.metrics().borrow().current_term;
    while isolated_at.elapsed() < isolation {
        // The other voters keep committing while node 1 is cut off.
        router.client_request_many(0, "isolated", 1).await?;
        log_index += 1;
        let term = router.get_raft_handle(&1)?.metrics().borrow().current_term;
        highest_isolated_term = highest_isolated_term.max(term);
        sleep(Duration::from_millis(50)).await;
    }

    if let Rejoin::Restart = rejoin {
        tracing::info!("--- restart node 1 from its durable state while it is still cut off");
        let (n1, log_store, sm) = router.remove_node(1).unwrap();
        n1.shutdown().await?;
        router.new_raft_node_with_sto(1, log_store, sm).await;
        router
            .wait(&1, timeout())
            .metrics(
                |m| m.current_term >= leader_term,
                "node 1 restarted with its durable vote",
            )
            .await?;
        let term = router.get_raft_handle(&1)?.metrics().borrow().current_term;
        highest_isolated_term = highest_isolated_term.max(term);
    }

    tracing::info!("--- admit node 1 again and watch the cluster for two more election timeouts");
    router.set_network_error(1, false);
    let watched_at = Instant::now();
    while watched_at.elapsed() < Duration::from_millis(config.election_timeout_max * 2) {
        for id in voters.iter() {
            let m = router.get_raft_handle(id)?.metrics().borrow().clone();
            assert_eq!(
                leader_term, m.current_term,
                "node {} stays in the healthy leader's term after node 1 rejoins ({:?})",
                id, rejoin
            );
        }
        sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        leader_term, highest_isolated_term,
        "the cut-off voter never raised its term ({:?})",
        rejoin
    );
    router.wait(&1, timeout()).applied_index(Some(log_index), "node 1 catches up").await?;
    let m = n0.metrics().borrow().clone();
    assert_eq!(ServerState::Leader, m.state, "node 0 is still the leader");
    assert_eq!(Some(0), m.current_leader);
    for id in voters.iter() {
        router
            .wait(id, timeout())
            .metrics(|m| m.current_leader == Some(0), "every voter still follows node 0")
            .await?;
    }

    tracing::info!("--- the leader keeps serving writes with node 1");
    router.client_request_many(0, "rejoined", 1).await?;
    log_index += 1;
    router.wait(&1, timeout()).applied_index(Some(log_index), "node 1 replicates again").await?;

    Ok(())
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn three_voters_isolated_voter_reconnects_without_deposing_the_leader() -> Result<()> {
    isolated_voter_rejoins_without_deposing_the_leader(3, Rejoin::Reconnect).await
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn five_voters_isolated_voter_reconnects_without_deposing_the_leader() -> Result<()> {
    isolated_voter_rejoins_without_deposing_the_leader(5, Rejoin::Reconnect).await
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn three_voters_isolated_voter_restarts_without_deposing_the_leader() -> Result<()> {
    isolated_voter_rejoins_without_deposing_the_leader(3, Rejoin::Restart).await
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn five_voters_isolated_voter_restarts_without_deposing_the_leader() -> Result<()> {
    isolated_voter_rejoins_without_deposing_the_leader(5, Rejoin::Restart).await
}

/// Pre-Vote keeps elections possible: after the leader is lost, the survivors grant each other's
/// Pre-Vote once their leases expire, and one of them is elected.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn pre_vote_elects_a_survivor_after_leader_loss() -> Result<()> {
    let config = pre_vote_config()?;

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;

    tracing::info!("--- lose the leader");
    router.set_network_error(0, true);

    for id in [1, 2] {
        router
            .wait(&id, Some(Duration::from_secs(5)))
            .metrics(
                |m| m.current_leader == Some(1) || m.current_leader == Some(2),
                "a survivor is elected through Pre-Vote",
            )
            .await?;
    }

    Ok(())
}

/// An unreachable peer is not a Pre-Vote grant: a voter whose every peer is unreachable keeps its
/// term.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn unreachable_peers_do_not_grant_pre_vote() -> Result<()> {
    let config = pre_vote_config()?;

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n1 = router.get_raft_handle(&1)?;
    n1.wait(timeout()).state(ServerState::Follower, "node 1 is a follower").await?;
    let term = n1.metrics().borrow().current_term;

    tracing::info!("--- make node 1 unreachable for several election timeouts");
    router.set_unreachable(1, true);
    sleep(Duration::from_millis(
        config.election_timeout_max * ISOLATION_ELECTION_TIMEOUTS,
    ))
    .await;

    assert_eq!(
        term,
        n1.metrics().borrow().current_term,
        "an unreachable peer never counts toward a Pre-Vote quorum"
    );

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3_000))
}
