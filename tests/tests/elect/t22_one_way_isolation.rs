use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::ServerState;
use tokio::time::sleep;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::Direction;
use crate::fixtures::RPCErrorType;
use crate::fixtures::RaftRouter;

/// A voter that can still send but no longer receives consensus traffic does not depose the
/// healthy leader through Pre-Vote.
///
/// The cut-off voter's lease expires, so it runs Pre-Vote rounds that reach the other voters. The
/// other follower rejects them under its own lease. The leader never renews the lease on its own
/// vote, so it must reject them under its quorum-acknowledged lease instead: a quorum keeps
/// acknowledging its heartbeats. Otherwise the leader's grant completes a quorum, the cut-off
/// voter campaigns, and the leader grants that vote too and steps down.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn voter_that_cannot_receive_does_not_depose_the_leader() -> Result<()> {
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
    let watch = Duration::from_millis(config.election_timeout_max * 4);

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;
    let n1 = router.get_raft_handle(&1)?;
    let n2 = router.get_raft_handle(&2)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;
    for id in [1, 2] {
        router
            .wait(&id, timeout())
            .metrics(|m| m.current_leader == Some(0), "the follower follows node 0")
            .await?;
    }
    let leader_term = n0.metrics().borrow().current_term;

    tracing::info!("--- node 1 can still send, but receives nothing for {:?}", watch);
    router.set_rpc_failure(1, Direction::NetRecv, Some(RPCErrorType::NetworkError));

    let started = Instant::now();
    let mut highest_cut_off_term = leader_term;
    while started.elapsed() < watch {
        let m0 = n0.metrics().borrow().clone();
        assert_eq!(ServerState::Leader, m0.state, "node 0 keeps leading");
        assert_eq!(leader_term, m0.current_term, "node 0 stays in its term");
        assert_eq!(
            leader_term,
            n2.metrics().borrow().current_term,
            "node 2 stays in the leader's term"
        );
        highest_cut_off_term = highest_cut_off_term.max(n1.metrics().borrow().current_term);
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        leader_term, highest_cut_off_term,
        "the cut-off voter never raised its term"
    );

    tracing::info!("--- node 1 receives again and follows node 0");
    router.set_rpc_failure(1, Direction::NetRecv, None);
    router
        .wait(&1, timeout())
        .metrics(|m| m.current_leader == Some(0), "node 1 follows node 0 again")
        .await?;
    router.client_request_many(0, "after", 1).await?;

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3_000))
}
