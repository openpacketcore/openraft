use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::raft::VoteRequest;
use openraft::storage::RaftLogStorage;
use openraft::Config;
use openraft::ServerState;
use openraft::Vote;
use openraft_memstore::ClientRequest;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// Triggering an election on the current Leader is ignored.
///
/// A Leader can not win a campaign it starts: its own heartbeats keep refreshing the voters' leader
/// lease, and a vote request is rejected while that lease has not expired. Entering Candidate would
/// therefore only strip the node of leadership and inflate the term for as long as it keeps
/// heartbeating, leaving the group without a Leader.
///
/// Instead the trigger is a no-op: the established leadership is left alone.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn elect_on_leader_is_ignored() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: true,
            enable_elect: true,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 becomes leader");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is the initial leader").await?;

    let term_before = n0.metrics().borrow().current_term;

    tracing::info!("--- trigger an election on the current leader");
    n0.trigger().elect().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!("--- node 0 is still the leader of the same term");
    {
        let m = n0.metrics().borrow().clone();
        assert_eq!(ServerState::Leader, m.state, "node 0 remains a Leader");
        assert_eq!(term_before, m.current_term, "the term is not inflated");
        assert_eq!(Some(0), m.current_leader, "node 0 remains the leader");
    }

    tracing::info!("--- the followers still see node 0 as the leader");
    for id in [1, 2] {
        router
            .get_raft_handle(&id)?
            .wait(timeout())
            .metrics(|m| m.current_leader == Some(0), "follower still follows node 0")
            .await?;
    }

    tracing::info!("--- and the leader still serves writes");
    router.client_request_many(0, "foo", 1).await?;

    Ok(())
}

/// A greater accepted self-vote relinquishes the old leader before a public
/// election trigger. All votes, quorum grants and writes pass through RaftCore.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn elect_after_greater_self_vote_relinquishes_previous_leader() -> Result<()> {
    // Drive campaigns explicitly so an automatic election cannot hide a lost
    // trigger. The configured lease, RPC and election durations are unchanged.
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config);
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let leader = router.get_raft_handle(&0)?;
    leader.wait(timeout()).state(ServerState::Leader, "initial leader").await?;
    let before = leader.with_raft_state(|state| *state.vote_ref()).await?;
    assert!(before.is_committed());

    // An external request follows the trigger in the core API queue. Observing
    // the unchanged vote here proves that the valid-leader no-op was processed.
    leader.trigger().elect().await?;
    let unchanged = leader.with_raft_state(|state| (state.server_state, *state.vote_ref())).await?;
    assert_eq!((ServerState::Leader, before), unchanged);

    // Prepare every voter through the public RequestVote path, with its actual
    // retained last log. Refusals are retried only while the real leader lease
    // remains in force; neither a vote response nor a clock value is fabricated.
    for id in [0, 1, 2] {
        let node = router.get_raft_handle(&id)?;
        let (mut log_store, _) = router.get_storage_handle(&id)?;
        let last = log_store.get_log_state().await?.last_log_id;
        // Keep the selected node's vote above every other preparation. A late
        // old-term replication response must not incidentally step it down and
        // conceal the self-vote bug in the multi-leader-per-term profile.
        let prepared_term = before.leader_id.term + if id == 0 { 2 } else { 1 };
        let prepared = Vote::new(prepared_term, id);
        tokio::time::timeout(timeout().unwrap(), async {
            loop {
                let reply = node.vote(VoteRequest::new(prepared, last)).await?;
                assert_eq!(last, reply.last_log_id);
                if reply.vote_granted {
                    assert_eq!(prepared, reply.vote);
                    break;
                }
                assert_eq!(before, reply.vote);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    }

    let elected = Vote::new_committed(before.leader_id.term + 3, 0);
    leader.trigger().elect().await?;
    leader.wait(timeout()).vote(elected, "public trigger must complete the next real election").await?;
    leader.wait(timeout()).state(ServerState::Leader, "higher-term quorum leader").await?;

    let client = "self-vote-recovery";
    let status = "committed-after-new-election";
    let response = tokio::time::timeout(
        timeout().unwrap(),
        leader.client_write(ClientRequest {
            client: client.to_string(),
            serial: 1,
            status: status.to_string(),
        }),
    )
    .await??;
    assert_eq!(elected.leader_id.term, response.log_id.leader_id.term);
    assert_eq!(None, response.data.0);
    tokio::time::timeout(timeout().unwrap(), leader.ensure_linearizable()).await??;
    router
        .wait_for_log(
            &btreeset! {0,1,2},
            Some(response.log_id.index),
            timeout(),
            "new write applied on all voters",
        )
        .await?;
    for id in [0, 1, 2] {
        let (_, state_machine) = router.get_storage_handle(&id)?;
        let state = state_machine.storage().await.get_state_machine().await;
        assert_eq!(Some(status), state.client_status.get(client).map(String::as_str));
        assert_eq!(Some(&(1, None)), state.client_serial_responses.get(client));
        assert_eq!(Some(response.log_id), state.last_applied_log);
        router.get_raft_handle(&id)?.shutdown().await?;
    }
    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2_000))
}
