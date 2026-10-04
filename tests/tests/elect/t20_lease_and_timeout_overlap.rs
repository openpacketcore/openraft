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

/// After an unplanned leader loss, a follower campaigns once both its leader lease and its sampled
/// election timeout have expired. The two run in parallel from the last leader contact; they are
/// not waited for one after the other.
///
/// Every sampled election timeout covers the follower lease, so the first campaign starts within
/// `election_timeout_max` plus one tick of the last leader contact, never before
/// `election_timeout_min`. Waiting for the sum would hold every survivor back for at least
/// `election_timeout_max + election_timeout_min`.
///
/// The first campaign is observed as a survivor's term exceeding the lost leader's term, whether
/// or not that campaign wins: a survivor whose last leader contact was a little later can still
/// reject it under its own lease, and then a later campaign elects the new leader.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn leader_loss_campaign_overlaps_lease_and_election_timeout() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 1_500,
            election_timeout_max: 1_600,
            ..Default::default()
        }
        .validate()?,
    );
    let tick = Duration::from_millis(config.heartbeat_interval * 3 / 2);
    let election_timeout_min = Duration::from_millis(config.election_timeout_min);
    let election_timeout_max = Duration::from_millis(config.election_timeout_max);

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;
    for id in [1, 2] {
        router
            .wait(&id, timeout())
            .metrics(|m| m.current_leader == Some(0), "the follower follows node 0")
            .await?;
    }

    let leader_term = n0.metrics().borrow().current_term;

    tracing::info!("--- lose the leader: node 0 can neither send nor receive");
    let lost_at = Instant::now();
    router.set_network_error(0, true);

    tracing::info!("--- a survivor campaigns: its term exceeds the lost leader's term");
    let survivors = [router.get_raft_handle(&1)?, router.get_raft_handle(&2)?];
    let campaigned = loop {
        if survivors.iter().any(|n| n.metrics().borrow().current_term > leader_term) {
            break lost_at.elapsed();
        }
        assert!(
            lost_at.elapsed() < Duration::from_secs(10),
            "a survivor campaigns after the leader loss"
        );
        sleep(Duration::from_millis(5)).await;
    };
    tracing::info!("--- a survivor campaigned {:?} after the leader loss", campaigned);

    // The last leader contact is at most one tick before the loss. The campaign then starts on
    // the first tick after the longer of the lease and the sampled timeout; the remaining slack
    // covers polling and scheduling.
    let slack = Duration::from_millis(600);
    assert!(
        campaigned < election_timeout_max + tick + slack,
        "the first campaign waits for the longer of the lease and the timeout, not their sum: \
         campaigned after {:?}, expected below {:?}",
        campaigned,
        election_timeout_max + tick + slack
    );
    assert!(
        campaigned + tick >= election_timeout_min,
        "the lease still holds back every campaign: campaigned after {:?}, lease {:?}",
        campaigned,
        election_timeout_min
    );

    tracing::info!("--- a survivor is elected");
    for id in [1, 2] {
        router
            .wait(&id, Some(Duration::from_secs(10)))
            .metrics(
                |m| m.current_leader == Some(1) || m.current_leader == Some(2),
                "a survivor leads after the leader loss",
            )
            .await?;
    }

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2_000))
}
