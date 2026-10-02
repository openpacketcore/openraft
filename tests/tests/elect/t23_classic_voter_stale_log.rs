use std::collections::BTreeSet;
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

/// Voters that cannot answer Pre-Vote, such as voters of a release without it, and that miss the
/// last entries do not block an election.
///
/// The survivors with the up-to-date log run Pre-Vote, and the network answers their Pre-Vote to a
/// classic voter as a rejection, so they cannot reach a Pre-Vote quorum without those voters. The
/// classic voters campaign with the classic vote, but their logs are behind, so they cannot win
/// either. A survivor that rejects such a candidate for its log must campaign itself, without
/// Pre-Vote and in a term above that candidate's, or no leader is ever elected.
async fn classic_voters_with_stale_logs_do_not_block_an_election(voter_count: u64) -> Result<()> {
    let config = pre_vote_config()?;
    let mut router = RaftRouter::new(config.clone());

    let voters = (0..voter_count).collect::<BTreeSet<u64>>();
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

    // A minority that the up-to-date survivors still need for a quorum once node 0 is lost.
    let classic = (voter_count / 2 + 1..voter_count).collect::<Vec<u64>>();
    let current = (1..voter_count).filter(|id| !classic.contains(id)).collect::<Vec<u64>>();
    tracing::info!(
        "--- voters {:?} cannot answer Pre-Vote and campaign without it",
        classic
    );
    for id in classic.iter() {
        router.get_raft_handle(id)?.runtime_config().pre_vote(false);
        router.set_pre_vote_unsupported(*id, true);
    }

    tracing::info!("--- voters {:?} miss the last entries", classic);
    for id in classic.iter() {
        router.set_rpc_failure(*id, Direction::NetRecv, Some(RPCErrorType::NetworkError));
    }
    router.client_request_many(0, "missed", 5).await?;
    log_index += 5;
    for id in current.iter() {
        router
            .wait(id, timeout())
            .applied_index(Some(log_index), "the up-to-date survivors have the entries")
            .await?;
    }

    tracing::info!("--- node 0 is lost; voters {:?} are reachable again", classic);
    let (n0, _, _) = router.remove_node(0).unwrap();
    n0.shutdown().await?;
    for id in classic.iter() {
        router.set_rpc_failure(*id, Direction::NetRecv, None);
    }

    let election = Some(Duration::from_millis(config.election_timeout_max * 10));
    let survivors = (1..voter_count).collect::<Vec<u64>>();
    let mut leader = None;
    for id in survivors.iter() {
        let m = router
            .wait(id, election)
            .metrics(
                |m| m.current_leader.is_some_and(|l| current.contains(&l)),
                "an up-to-date survivor is elected",
            )
            .await?;
        let elected = m.current_leader;
        assert!(
            leader.is_none() || leader == elected,
            "the survivors agree on one leader"
        );
        leader = elected;
    }
    let leader = leader.unwrap();

    tracing::info!("--- node {} commits, and every survivor applies the log", leader);
    router.client_request_many(leader, "elected", 1).await?;
    log_index += 2;
    for id in survivors.iter() {
        router.wait(id, timeout()).applied_index(Some(log_index), "every survivor applies the log").await?;
    }

    Ok(())
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn three_voters_classic_voter_with_a_stale_log_does_not_block_an_election() -> Result<()> {
    classic_voters_with_stale_logs_do_not_block_an_election(3).await
}

#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn five_voters_classic_voters_with_stale_logs_do_not_block_an_election() -> Result<()> {
    classic_voters_with_stale_logs_do_not_block_an_election(5).await
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3_000))
}
