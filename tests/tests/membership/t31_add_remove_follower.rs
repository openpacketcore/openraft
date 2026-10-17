use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::error::NetworkError;
use openraft::network::RPCTypes;
use openraft::Config;
use openraft::LogIdOptionExt;
use openraft::ServerState;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RPCRequest;
use crate::fixtures::RaftRouter;

/// When a node is removed from cluster, replication to it should be stopped.
///
/// - brings 5 nodes online: one leader and 4 follower.
/// - asserts that the leader was able to successfully commit logs and that the followers has
///   successfully replicated the payload.
/// - remove one follower: node-4
/// - observes the joint configuration on node-4 while its uniform configuration is lost.
/// - asserts replication stops and node-4 cannot disrupt the retained leader after its timers
///   expire.
// The router hook requires its existing unboxed RPCError return type.
#[allow(clippy::result_large_err)]
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn add_remove_voter() -> Result<()> {
    let c01234 = btreeset![0, 1, 2, 3, 4];
    let c0123 = btreeset![0, 1, 2, 3];

    let config = Arc::new(Config::default().validate()?);
    let mut router = RaftRouter::new(config.clone());

    let mut log_index = router.new_cluster(c01234.clone(), btreeset! {}).await?;

    tracing::info!(log_index, "--- write 100 logs");
    {
        router.client_request_many(0, "client", 100).await?;
        log_index += 100;

        router.wait_for_log(&c01234, Some(log_index), timeout(), "write 100 logs").await?;
    }

    let leader = router.get_raft_handle(&0)?;
    let leader_vote = leader.with_raft_state(|state| *state.vote_ref()).await?;
    assert!(leader_vote.is_committed());
    let joint_index = log_index + 1;

    // Let node 4 receive the joint entry before the other followers can commit
    // it. Lose only its subsequent uniform entry, so this exercises the case
    // where the removed node still considers itself a voter.
    router.set_rpc_pre_hook(RPCTypes::AppendEntries, move |router, request, from, to| {
        let RPCRequest::AppendEntries(request) = request else {
            unreachable!()
        };
        let has_joint = router.get_metrics(&4).unwrap().membership_config.log_id().index() >= Some(joint_index);
        if from == 0
            && ((to != 4 && !has_joint && request.entries.iter().any(|entry| entry.log_id.index >= joint_index))
                || (to == 4 && request.entries.iter().any(|entry| entry.log_id.index > joint_index)))
        {
            return Err(NetworkError::new(&std::io::Error::other("defer membership delivery")).into());
        }
        Ok(())
    });

    tracing::info!(log_index, "--- remove n{}", 4);
    {
        let node = router.get_raft_handle(&0)?;
        node.change_membership(c0123.clone(), false).await?;
        log_index += 2; // two member-change logs

        router.wait_for_log(&c0123, Some(log_index), timeout(), "removed node-4 from membership").await?;
    }

    router
        .wait(&4, timeout())
        .metrics(
            |metrics| metrics.membership_config.log_id().index() >= Some(joint_index),
            "removed node-4 has received the joint membership configuration",
        )
        .await?;
    // Subsequent traffic is unrestricted: the original replication assertion
    // must detect a leader that mistakenly continues replicating to node 4.
    router.rpc_pre_hook(RPCTypes::AppendEntries, None);

    tracing::info!(log_index, "--- write another 100 logs");
    {
        router.client_request_many(0, "client", 100).await?;
        log_index += 100;
    }

    router.wait_for_log(&c0123, Some(log_index), timeout(), "4 nodes recv logs 100~200").await?;

    tracing::info!(log_index, "--- log will not be sync to removed node");
    {
        let x = router.latest_metrics();
        assert!(x[4].last_log_index < Some(log_index - 50));
    }

    let removed = router.get_raft_handle(&4)?;
    assert_eq!(
        Some(joint_index),
        removed.metrics().borrow().membership_config.log_id().index(),
        "the removed voter missed the uniform configuration"
    );
    let inquiries = Arc::new(AtomicUsize::new(0));
    let real_votes = Arc::new(AtomicUsize::new(0));
    for (kind, count) in [
        (RPCTypes::PreVote, inquiries.clone()),
        (RPCTypes::Vote, real_votes.clone()),
    ] {
        router.set_rpc_observer(kind, move |_, from, _| {
            if from == 4 {
                count.fetch_add(1, Ordering::SeqCst);
            }
        });
    }

    // Keep the existing two-second bound. It exceeds the maximum lease,
    // election wait, inquiry deadline and newer-log backoff, plus tick margins.
    // Heartbeats keep the retained quorum healthy but no longer reach node 4.
    let observation = timeout().unwrap();
    assert!(observation > Duration::from_millis(5 * config.election_timeout_max + 3 * config.heartbeat_interval));
    let until = tokio::time::Instant::now() + observation;
    loop {
        let removed_metrics = removed.metrics().borrow().clone();
        assert!(
            removed_metrics.vote.leader_id().term <= leader_vote.leader_id().term,
            "removed node-4 started a real election: {removed_metrics:?}"
        );
        assert!(matches!(
            removed_metrics.state,
            ServerState::Learner | ServerState::Follower
        ));
        assert_eq!(
            0,
            real_votes.load(Ordering::SeqCst),
            "removed node-4 sent a real vote request"
        );
        let leader_metrics = leader.metrics().borrow().clone();
        assert_eq!(ServerState::Leader, leader_metrics.state);
        assert_eq!(Some(0), leader_metrics.current_leader);
        assert_eq!(
            leader_vote, leader_metrics.vote,
            "node 0 must retain leadership in the same term"
        );

        let now = tokio::time::Instant::now();
        if now >= until {
            break;
        }
        tokio::time::sleep_until((now + Duration::from_millis(10)).min(until)).await;
    }
    assert!(
        inquiries.load(Ordering::SeqCst) > 0,
        "the removed voter actually attempted pre-voting"
    );

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2000))
}
