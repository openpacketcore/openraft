use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use maplit::btreeset;
use openraft::error::ClientWriteError;
use openraft::error::RaftError;
use openraft::raft::TransferLeaderError;
use openraft::Config;
use openraft::ServerState;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

const HANDOFF_BUDGET: Duration = Duration::from_secs(5);

fn config() -> Arc<Config> {
    // Keep the deployed engine timing profile. The handoff must work before
    // normal failure detection, not by reducing the election/lease timers.
    Arc::new(
        Config {
            heartbeat_interval: 2_000,
            election_timeout_min: 5_000,
            election_timeout_max: 8_000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

async fn stop_all(router: &mut RaftRouter) -> anyhow::Result<()> {
    for id in [0, 1, 2] {
        if let Some((node, _, _)) = router.remove_node(id) {
            node.shutdown().await?;
        }
    }
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn planned_handoff_preserves_acknowledged_writes_and_surviving_quorum() -> anyhow::Result<()> {
    let mut router = RaftRouter::new(config());
    let log_index = router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;
    router.client_request(0, "handoff", 1).await?;
    router
        .wait_for_log(
            &btreeset! {0, 1, 2},
            Some(log_index + 1),
            Some(HANDOFF_BUDGET),
            "seed replicated",
        )
        .await?;
    let old = router.get_raft_handle(&0)?;
    let next = router.get_raft_handle(&1)?;
    let other = router.get_raft_handle(&2)?;
    let old_vote = old.metrics().borrow().vote;
    let started = Instant::now();

    let result = tokio::time::timeout(HANDOFF_BUDGET, async {
        let request = old.prepare_shutdown(Some(1)).await?.unwrap();
        assert_eq!(&old_vote, request.from());
        assert_eq!(Some(request.clone()), old.prepare_shutdown(Some(1)).await?);
        let refusal = old.client_write(ClientRequest::make_request("must-not-append", 1)).await.unwrap_err();
        assert!(matches!(
            refusal,
            RaftError::APIError(ClientWriteError::ForwardToLeader(ref leader)) if leader.leader_id == Some(1)
        ));
        assert!(
            old.ensure_linearizable().await.is_err(),
            "a retiring leader cannot grant a new read lease"
        );

        // Deliver the engine-issued request as the authenticated old leader.
        // No log write, vote, membership update or election algorithm is supplied
        // by this transport harness.
        assert!(other.prepare_shutdown(None).await?.is_none());
        other.trigger().elect().await?;
        other.handle_leadership_transfer(request.clone()).await?;
        next.handle_leadership_transfer(request.clone()).await?;
        next.wait(Some(HANDOFF_BUDGET))
            .metrics(
                |m| m.state == ServerState::Leader && m.current_term > old_vote.leader_id().term,
                "successor elected in a fresh term",
            )
            .await?;
        next.ensure_linearizable().await?;
        other.wait(Some(HANDOFF_BUDGET)).current_leader(1, "surviving majority observes successor").await?;

        let (retiring, _, _) = router.remove_node(0).unwrap();
        retiring.shutdown().await?;
        let reply = next.client_write(ClientRequest::make_request("handoff", 2)).await?;
        assert_eq!(
            Some("request-1"),
            reply.data.0.as_deref(),
            "the acknowledged value survives retirement"
        );
        other
            .wait(Some(HANDOFF_BUDGET))
            .applied_index(Some(reply.log_id.index), "new write replicated")
            .await?;
        let stale = next.handle_leadership_transfer(request).await.unwrap_err();
        assert_eq!(Some(TransferLeaderError::VoteChanged), stale.into_api_error());
        Ok::<(), anyhow::Error>(())
    })
    .await;
    let elapsed = started.elapsed();
    stop_all(&mut router).await?;
    result??;
    assert!(elapsed < HANDOFF_BUDGET);
    eprintln!(
        "planned_handoff elapsed_ms={} acknowledged_value_preserved=true surviving_quorum_write=true",
        elapsed.as_millis()
    );
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn handoff_retains_pending_write_and_refuses_unapplied_target() -> anyhow::Result<()> {
    let mut router = RaftRouter::new(config());
    let log_index = router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;
    let old = router.get_raft_handle(&0)?;
    let next = router.get_raft_handle(&1)?;
    let other = router.get_raft_handle(&2)?;
    router.set_network_error(0, true);

    // Submit once, preserve the original response receiver across the handoff,
    // and pause the old leader's network. It is admitted but cannot yet be committed.
    let pending = old.client_write_ff(ClientRequest::make_request("pending", 1)).await?;
    old.wait(Some(HANDOFF_BUDGET)).log_index(Some(log_index + 1), "accepted before handoff").await?;
    let request = old.prepare_shutdown(Some(1)).await?.unwrap();
    assert_eq!(Some(log_index + 1), request.last_log_id().map(|id| id.index));
    let vote_before = next.metrics().borrow().vote;
    let error = next.handle_leadership_transfer(request.clone()).await.unwrap_err();
    assert_eq!(Some(TransferLeaderError::LogNotApplied), error.into_api_error());
    assert_eq!(vote_before, next.metrics().borrow().vote);
    assert!(old.ensure_linearizable().await.is_err());

    router.set_network_error(0, false);
    let result = tokio::time::timeout(HANDOFF_BUDGET, async {
        let reply = pending.await??;
        assert_eq!(log_index + 1, reply.log_id.index, "the original mutation settles once");
        next.wait(Some(HANDOFF_BUDGET)).applied_index(Some(reply.log_id.index), "target caught up").await?;
        other.handle_leadership_transfer(request.clone()).await?;
        next.handle_leadership_transfer(request).await?;
        next.wait(Some(HANDOFF_BUDGET)).state(ServerState::Leader, "fresh leader").await?;
        next.ensure_linearizable().await?;
        let reply = next.client_write(ClientRequest::make_request("pending", 2)).await?;
        assert_eq!(Some("request-1"), reply.data.0.as_deref());
        Ok::<(), anyhow::Error>(())
    })
    .await;
    stop_all(&mut router).await?;
    result?
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn handoff_does_not_make_an_isolated_target_a_leader() -> anyhow::Result<()> {
    let mut router = RaftRouter::new(config());
    let log_index = router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;
    let old = router.get_raft_handle(&0)?;
    let next = router.get_raft_handle(&1)?;
    let other = router.get_raft_handle(&2)?;
    let request = old.begin_leadership_transfer(1).await?;
    other.handle_leadership_transfer(request.clone()).await?;
    router.set_network_error(1, true);
    next.handle_leadership_transfer(request).await?;
    next.wait(Some(HANDOFF_BUDGET))
        .state(ServerState::Candidate, "target still requires a quorum")
        .await?;
    assert!(next.ensure_linearizable().await.is_err());
    assert!(next.client_write(ClientRequest::make_request("isolated", 1)).await.is_err());
    assert!(old.ensure_linearizable().await.is_err());
    assert_eq!(Some(log_index), next.metrics().borrow().last_applied.map(|id| id.index));
    stop_all(&mut router).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_follower_refuses_handoff_without_changing_its_vote() -> anyhow::Result<()> {
    let mut router = RaftRouter::new(config());
    router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;
    let old = router.get_raft_handle(&0)?;
    let target = router.get_raft_handle(&1)?;
    let before = target.metrics().borrow().vote;
    assert!(target.prepare_shutdown(None).await?.is_none());
    target.trigger().elect().await?;
    let request = old.prepare_shutdown(Some(1)).await?.unwrap();
    let refusal = target.handle_leadership_transfer(request).await.unwrap_err();
    assert_eq!(Some(TransferLeaderError::Retiring), refusal.into_api_error());
    assert_eq!(before, target.metrics().borrow().vote);
    assert!(target.ensure_linearizable().await.is_err());
    assert!(old.ensure_linearizable().await.is_err());
    stop_all(&mut router).await
}
