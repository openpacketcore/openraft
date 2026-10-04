use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::raft::VoteRequest;
use openraft::Config;
use openraft::ServerState;
use openraft::Vote;
use tokio::time::sleep;
use tokio::time::Instant;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// A leader answers the vote that deposes it without waiting for an AppendEntries to a hung
/// learner.
///
/// Granting the vote makes the leader stop leading, and it closes every replication stream before
/// it answers. A stream whose AppendEntries is held by a learner that never answers must give up
/// that request when it is closed. Otherwise the answer waits for the AppendEntries deadline,
/// which may be far longer than the candidate waits for a vote.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn deposing_vote_is_answered_without_waiting_for_a_hung_learner() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            append_entries_timeout: Some(10_000),
            election_timeout_min: 500,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()?,
    );
    let vote_deadline = Duration::from_millis(config.election_timeout_min);

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of voters 0,1,2 and learner 3; node 0 becomes leader");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {3}).await?;
    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;
    for id in [1, 2] {
        router.get_raft_handle(&id)?.runtime_config().elect(false);
    }

    tracing::info!("--- learner 3 holds every AppendEntries without answering");
    router.set_rpc_blocked(3, true);
    sleep(Duration::from_millis(200)).await;

    tracing::info!("--- node 0 stops sending heartbeats, so its lease runs out");
    n0.runtime_config().heartbeat(false);
    sleep(Duration::from_millis(config.election_timeout_max + 200)).await;

    let m0 = n0.metrics().borrow().clone();
    assert_eq!(ServerState::Leader, m0.state);
    let req = VoteRequest::new(Vote::new(m0.current_term + 1, 1), m0.last_applied);

    tracing::info!("--- node 1 asks node 0 for its vote");
    let started = Instant::now();
    let resp = n0.vote(req).await?;
    let answered = started.elapsed();
    router.set_rpc_blocked(3, false);

    assert!(resp.vote_granted, "node 0 grants the vote: {:?}", resp);
    assert!(
        answered < vote_deadline,
        "node 0 answered the vote after {:?}, beyond the candidate's {:?} vote deadline",
        answered,
        vote_deadline
    );

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2_000))
}
