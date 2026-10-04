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

/// How much earlier node 1 stops hearing from the leader than node 2. More than the width of the
/// election-timeout window and a tick, so node 1's first Pre-Vote round always meets node 2's
/// running lease; less than twice the minimum election timeout less the maximum and a tick, so a
/// retry one sampled election timeout later always lands beyond the bound.
const CONTACT_SKEW: Duration = Duration::from_millis(400);

/// Scheduling and round-trip allowance on top of the bound.
const SLACK: Duration = Duration::from_millis(120);

/// A Pre-Vote round rejected by a lease that is still running is retried in time.
///
/// After an unplanned leader loss every survivor's lease runs out within the minimum election
/// timeout of the loss, and the survivor with the most up-to-date log starts its first Pre-Vote
/// round within the maximum election timeout and a tick of its last leader contact. Node 1 holds
/// the most up-to-date log but heard from the leader last before node 2, whose log is behind, so
/// node 2's lease rejects node 1's first round. Node 1 must retry within the width of the
/// election-timeout window, so it is still elected within the maximum election timeout and a tick
/// of the loss. Holding the rejected round for another sampled election timeout instead would push
/// the election beyond that bound.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn pre_vote_round_rejected_by_a_running_lease_is_retried_within_the_bound() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 20,
            election_timeout_min: 1_000,
            election_timeout_max: 1_150,
            enable_pre_vote: Some(true),
            ..Default::default()
        }
        .validate()?,
    );
    let tick = Duration::from_millis(config.heartbeat_interval * 3 / 2);
    let bound = Duration::from_millis(config.election_timeout_max) + tick;
    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    let n1 = router.get_raft_handle(&1)?;
    let n2 = router.get_raft_handle(&2)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;

    tracing::info!("--- node 2 misses the last entries");
    router.set_network_error(2, true);
    router.client_request_many(0, "missed", 10).await?;
    log_index += 10;
    n1.wait(timeout()).applied_index(Some(log_index), "node 1 has the entries").await?;

    tracing::info!("--- node 2 hears from the leader again, but receives no entries");
    router.set_append_entries_quota(Some(0));
    router.set_network_error(2, false);
    sleep(Duration::from_millis(200)).await;
    let node2_last_log = n2.metrics().borrow().last_log_index;
    assert!(
        node2_last_log < Some(log_index),
        "node 2's log stays behind node 1's: {:?} < {}",
        node2_last_log,
        log_index
    );

    tracing::info!(
        "--- node 1 stops hearing from the leader {:?} before node 2",
        CONTACT_SKEW
    );
    router.set_rpc_failure(1, Direction::NetRecv, Some(RPCErrorType::NetworkError));
    sleep(CONTACT_SKEW).await;

    tracing::info!("--- the leader is lost");
    let lost = Instant::now();
    router.set_network_error(0, true);
    router.set_rpc_failure(1, Direction::NetRecv, None);

    // Keep the quota at zero through the election and timing assertions. An AppendEntries RPC
    // admitted before isolating node 0 can still be waiting on the router's send delay.
    n1.wait(Some(bound * 4))
        .state(ServerState::Leader, "node 1, with the most up-to-date log, is elected")
        .await?;
    let elected = lost.elapsed();
    tracing::info!("--- node 1 was elected {:?} after the loss; bound {:?}", elected, bound);
    assert!(
        elected <= bound + SLACK,
        "node 1 was elected {:?} after the loss, beyond the maximum election timeout and a tick, {:?}",
        elected,
        bound
    );
    assert_eq!(
        n0.metrics().borrow().current_term + 1,
        n1.metrics().borrow().current_term
    );
    router.set_append_entries_quota(None);

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3_000))
}
