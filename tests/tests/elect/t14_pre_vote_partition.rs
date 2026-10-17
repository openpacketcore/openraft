use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use openraft::network::RPCTypes;
use openraft::Config;
use openraft::ServerState;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;

use crate::fixtures::RaftRouter;

#[tokio::test(start_paused = true)]
async fn partitioned_voter_does_not_inflate_term_or_depose_a_healthy_leader_on_rejoin() -> anyhow::Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 2_000,
            election_timeout_min: 5_000,
            election_timeout_max: 8_000,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config);
    router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;
    let leader = router.get_raft_handle(&0)?;
    let isolated = router.get_raft_handle(&2)?;
    let original = leader.with_raft_state(|state| *state.vote_ref()).await?;
    assert!(original.is_committed());

    let inquiries = Arc::new(AtomicUsize::new(0));
    let observed = inquiries.clone();
    router.set_rpc_observer(RPCTypes::PreVote, move |_, from, _| {
        if from == 2 {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    });
    router.set_network_error(2, true);
    let before = isolated.with_raft_state(|state| (*state.vote_ref(), state.vote_last_modified())).await?;
    // Several complete lease/election intervals run on the real engine. Only
    // network reachability is changed; no campaign or response is fabricated.
    tokio::time::sleep(Duration::from_secs(45)).await;
    assert!(
        inquiries.load(Ordering::SeqCst) >= 4,
        "the isolated voter actually retried pre-voting"
    );
    assert_eq!(
        before,
        isolated.with_raft_state(|state| (*state.vote_ref(), state.vote_last_modified())).await?
    );
    assert_eq!(original, leader.with_raft_state(|state| *state.vote_ref()).await?);
    assert_eq!(ServerState::Leader, leader.metrics().borrow().state);

    router.set_network_error(2, false);
    tokio::time::sleep(Duration::from_secs(18)).await;
    let written = tokio::time::timeout(
        Duration::from_secs(5),
        leader.client_write(ClientRequest::make_request("pre-vote-rejoin", 1)),
    )
    .await??;
    router
        .wait_for_log(
            &btreeset! {0, 1, 2},
            Some(written.log_id.index),
            Some(Duration::from_secs(5)),
            "rejoined follower catches the healthy leader",
        )
        .await?;
    for id in [0, 1, 2] {
        let node = router.get_raft_handle(&id)?;
        assert_eq!(original, node.with_raft_state(|state| *state.vote_ref()).await?);
    }
    assert_eq!(ServerState::Leader, leader.metrics().borrow().state);
    for id in [0, 1, 2] {
        let (node, _, _) = router.remove_node(id).unwrap();
        node.shutdown().await?;
    }
    Ok(())
}
