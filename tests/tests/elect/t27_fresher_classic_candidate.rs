use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::ServerState;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// A voter whose log is behind defers to a classic candidate with a more up-to-date log.
///
/// Node 1 cannot answer Pre-Vote and campaigns with the classic vote on slower timers, as a voter
/// of a previous release does; only it holds the most up-to-date log after node 0 is lost. Node 2
/// is behind, so it can never win, but it runs the classic election because node 1 cannot answer
/// Pre-Vote. Node 2 campaigns first and votes for itself; node 1's first campaign then falls in
/// the same term, and node 2 rejects it: node 1's vote is not greater than its own. If node 2
/// campaigned again before node 1's next campaign, it would again hold that term, and each of
/// node 1's campaigns could meet another self-vote of node 2. Once node 2 rejects a candidate
/// whose log is more up to date only because of its own vote, it defers its own next campaign, so
/// node 1 wins its second campaign.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn voter_behind_defers_to_a_fresher_classic_candidate() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 1_000,
            election_timeout_max: 1_100,
            enable_pre_vote: Some(true),
            ..Default::default()
        }
        .validate()?,
    );
    let classic = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 2_500,
            election_timeout_max: 2_600,
            ..Default::default()
        }
        .validate()?,
    );
    let classic_tick = Duration::from_millis(classic.heartbeat_interval * 3 / 2);
    // Node 1's second campaign, measured from the loss: its lease and longest timeout, then its
    // longest timeout again, each on its next tick.
    let second_classic_campaign = (Duration::from_millis(classic.election_timeout_max) + classic_tick) * 2;
    let slack = Duration::from_millis(300);

    let mut router = RaftRouter::new(config.clone());
    router.set_node_config(1, classic.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    router
        .get_raft_handle(&0)?
        .wait(timeout())
        .state(ServerState::Leader, "node 0 is the initial leader")
        .await?;

    tracing::info!("--- node 1 cannot answer Pre-Vote and campaigns without it");
    let n1 = router.get_raft_handle(&1)?;
    n1.runtime_config().pre_vote(false);
    router.set_pre_vote_unsupported(1, true);

    tracing::info!("--- node 2 misses the last entries");
    router.set_network_error(2, true);
    router.client_request_many(0, "missed", 5).await?;
    log_index += 5;
    n1.wait(timeout()).applied_index(Some(log_index), "node 1 has the entries").await?;

    tracing::info!("--- node 0 is lost; node 2 is reachable again");
    let lost = Instant::now();
    router.set_network_error(0, true);
    router.set_network_error(2, false);

    n1.wait(Some(second_classic_campaign * 4))
        .state(ServerState::Leader, "node 1, with the most up-to-date log, is elected")
        .await?;
    let elected = lost.elapsed();
    tracing::info!(
        "--- node 1 was elected {:?} after the loss; its second campaign is due within {:?}",
        elected,
        second_classic_campaign
    );
    assert!(
        elected <= second_classic_campaign + slack,
        "node 1 was elected {:?} after the loss, after its second campaign was due within {:?}",
        elected,
        second_classic_campaign
    );

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(5_000))
}
