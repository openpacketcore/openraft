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

/// A voter that cannot answer Pre-Vote and still believes it leads does not block an election.
///
/// Node 0 cannot answer Pre-Vote, as a voter of a release without it cannot, and leads. It is
/// cut off, nodes 1 and 2 elect a successor in a later term, and the successor is lost too.
/// Node 0 is reachable again, but nothing it sends arrives, so it never learns of the later term
/// and keeps believing it leads: it never campaigns. The remaining voter can still win a classic
/// vote with node 0's grant. If it ran Pre-Vote, node 0 could never answer, and no leader would
/// ever be elected.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn classic_leader_that_cannot_send_does_not_block_an_election() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 500,
            election_timeout_max: 600,
            enable_pre_vote: Some(true),
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;

    tracing::info!("--- node 0 cannot answer Pre-Vote and campaigns without it");
    n0.runtime_config().pre_vote(false);
    router.set_pre_vote_unsupported(0, true);

    tracing::info!("--- node 0 is cut off; nodes 1 and 2 elect a successor");
    router.set_network_error(0, true);
    let election = Some(Duration::from_millis(config.election_timeout_max * 10));
    let m1 = router
        .wait(&1, election)
        .metrics(
            |m| m.current_leader.is_some_and(|l| l != 0),
            "nodes 1 and 2 elect a successor",
        )
        .await?;
    let successor = m1.current_leader.unwrap();
    let remaining = if successor == 1 { 2 } else { 1 };
    router.client_request_many(successor, "successor", 1).await?;
    log_index += 2;
    router
        .wait(&remaining, timeout())
        .applied_index(Some(log_index), "the remaining voter has the successor's log")
        .await?;
    assert_eq!(
        ServerState::Leader,
        n0.metrics().borrow().state,
        "the cut-off node 0 still believes it leads"
    );

    tracing::info!(
        "--- the successor {} is lost; node 0 receives again, but nothing it sends arrives",
        successor
    );
    router.set_network_error(successor, true);
    router.set_rpc_failure(0, Direction::NetRecv, None);
    router.set_rpc_failure(0, Direction::NetSend, Some(RPCErrorType::NetworkError));

    router
        .wait(&remaining, election)
        .state(ServerState::Leader, "the remaining voter is elected with node 0's vote")
        .await?;
    n0.wait(timeout())
        .metrics(
            |m| m.state == ServerState::Follower && m.current_leader == Some(remaining),
            "node 0 follows the remaining voter",
        )
        .await?;

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3_000))
}
