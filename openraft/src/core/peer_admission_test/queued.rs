//! Deliver actual queued notifications after an acknowledged API command.
//! Keeping the core loop under test control makes each ordering deterministic.

use super::*;
use crate::config::RuntimeConfig;
use crate::core::command_state::CommandState;
use crate::core::notify::Notify;
use crate::core::raft_msg::RaftMsg;
use crate::core::sm::worker;
use crate::core::RaftCore;
use crate::engine::Engine;
use crate::engine::EngineConfig;
use crate::engine::LogIdList;
use crate::error::CheckIsLeaderError;
use crate::error::Fatal;
use crate::metrics::RaftDataMetrics;
use crate::metrics::RaftMetrics;
use crate::metrics::RaftServerMetrics;
use crate::peer_admission::ResponseGeneration;
use crate::progress::entry::ProgressEntry;
use crate::progress::Inflight;
use crate::progress::Progress;
use crate::raft_state::LogStateReader;
use crate::replication::request_id::RequestId;
use crate::replication::response::ReplicationResult;
use crate::replication::response::Response;
use crate::replication::ReplicationSessionId;
use crate::type_config::TypeConfigExt;
use crate::utime::UTime;
use crate::CommittedLeaderId;
use crate::EffectiveMembership;
use crate::Membership;
use crate::MembershipState;
use crate::PeerResponseFence;
use crate::StorageIOError;

type Core = RaftCore<TestConfig, Network, MemoryStore, MemoryStore>;

#[tokio::test(start_paused = true)]
async fn a_fence_revokes_the_ordinary_campaign_that_could_enable_pre_vote_fallback() {
    let mut core = core(&[1, 2, 3], false);
    core.engine.handle_vote_req(VoteRequest::new(Vote::new(100, 2), Some(log(9))));
    assert!(core.engine.real_campaign.is_some());
    core.engine.pre_elect();
    let before = core.engine.state.clone();
    core.engine.output.take_commands();
    let receipt = fence(&mut core, 2).await;
    assert!(core.engine.real_campaign.is_none());
    release(&mut core, receipt).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    core.engine.pre_elect();
    assert_eq!(before, core.engine.state);
    assert!(core.engine.real_campaign.is_none());
    assert!(matches!(core.engine.output.take_commands().as_slice(), [
        crate::engine::Command::SendPreVote { .. }
    ]));
}

#[tokio::test(start_paused = true)]
async fn a_fence_rejects_queued_pre_vote_grants_and_higher_actual_terms() {
    for higher in [false, true] {
        let mut core = core(&[1, 2, 3], false);
        core.engine.pre_elect();
        let round = core.engine.pre_vote.as_ref().unwrap().round;
        core.engine.output.take_commands();
        core.tx_notify
            .send(Notify::PreVoteResponse {
                target: 3,
                generation: core.peer_admission.generation.clone(),
                round,
                resp: raft::raft::PreVoteResponse::new(
                    if higher { Vote::new(100, 3) } else { Vote::new(5, 1) },
                    Some(log(10)),
                    !higher,
                ),
            })
            .unwrap();
        let receipt = fence(&mut core, 3).await;
        let before = core.engine.state.clone();
        consume(&mut core).unwrap();
        assert_eq!(before, core.engine.state);
        assert!(core.engine.output.take_commands().is_empty());
        assert_eq!(
            vec![1],
            core.engine.pre_vote.as_ref().unwrap().candidate.granters().collect::<Vec<_>>()
        );

        release(&mut core, receipt).await;
        core.handle_notify(Notify::PreVoteResponse {
            target: 3,
            generation: core.peer_admission.generation.clone(),
            round,
            resp: raft::raft::PreVoteResponse::new(Vote::new(5, 1), Some(log(10)), true),
        })
        .unwrap();
        assert_eq!(Vote::new(6, 1), *core.engine.state.vote_ref());
        assert!(core.engine.leader.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn a_fence_revokes_pre_vote_grants_already_counted() {
    let mut core = core(&[1, 2, 3, 4, 5], false);
    core.engine.pre_elect();
    let round = core.engine.pre_vote.as_ref().unwrap().round;
    core.engine.output.take_commands();
    let original_generation = core.peer_admission.generation.clone();
    core.handle_notify(Notify::PreVoteResponse {
        target: 2,
        generation: original_generation.clone(),
        round,
        resp: raft::raft::PreVoteResponse::new(Vote::new(5, 1), Some(log(10)), true),
    })
    .unwrap();
    let receipt = fence(&mut core, 2).await;
    core.handle_notify(Notify::PreVoteResponse {
        target: 3,
        generation: core.peer_admission.generation.clone(),
        round,
        resp: raft::raft::PreVoteResponse::new(Vote::new(5, 1), Some(log(10)), true),
    })
    .unwrap();
    assert_eq!(Vote::new(5, 1), *core.engine.state.vote_ref());
    assert!(core.engine.output.take_commands().is_empty());
    release(&mut core, receipt).await;
    core.handle_notify(Notify::PreVoteResponse {
        target: 2,
        generation: original_generation,
        round,
        resp: raft::raft::PreVoteResponse::new(Vote::new(5, 1), Some(log(10)), true),
    })
    .unwrap();
    assert_eq!(Vote::new(5, 1), *core.engine.state.vote_ref());
    assert!(core.engine.output.take_commands().is_empty());
    core.handle_notify(Notify::PreVoteResponse {
        target: 2,
        generation: core.peer_admission.generation.clone(),
        round,
        resp: raft::raft::PreVoteResponse::new(Vote::new(5, 1), Some(log(10)), true),
    })
    .unwrap();
    assert_eq!(Vote::new(6, 1), *core.engine.state.vote_ref());
    assert!(matches!(core.engine.output.take_commands().as_slice(), [
        crate::engine::Command::SaveVote { .. },
        crate::engine::Command::SendVote { .. }
    ]));
}

#[tokio::test(start_paused = true)]
async fn unanswered_pre_vote_retries_use_their_own_deadline_without_inflating_term() {
    let mut core = core(&[1, 2, 3], false);
    core.runtime_config.enable_elect.store(true, Ordering::Relaxed);
    core.engine.config.election_timeout_min = 100;
    core.engine.config.election_timeout_max = 101;
    core.engine.config.timer_config.election_timeout = Duration::from_millis(100);
    let before = core.engine.state.clone();

    tokio::time::advance(Duration::from_millis(99)).await;
    core.handle_notify(Notify::Tick { i: 1 }).unwrap();
    assert!(core.engine.output.take_commands().is_empty());
    tokio::time::advance(Duration::from_millis(1)).await;
    core.handle_notify(Notify::Tick { i: 2 }).unwrap();
    let first = core.engine.pre_vote.as_ref().unwrap().round;
    assert!(matches!(core.engine.output.take_commands().as_slice(), [
        crate::engine::Command::SendPreVote { .. }
    ]));
    assert_eq!(before, core.engine.state);

    tokio::time::advance(Duration::from_millis(99)).await;
    core.handle_notify(Notify::Tick { i: 3 }).unwrap();
    assert_eq!(first, core.engine.pre_vote.as_ref().unwrap().round);
    assert!(core.engine.output.take_commands().is_empty());
    tokio::time::advance(Duration::from_millis(1)).await;
    core.handle_notify(Notify::Tick { i: 4 }).unwrap();
    assert_ne!(first, core.engine.pre_vote.as_ref().unwrap().round);
    assert!(matches!(core.engine.output.take_commands().as_slice(), [
        crate::engine::Command::SendPreVote { .. }
    ]));
    assert_eq!(before, core.engine.state);
}

#[tokio::test(start_paused = true)]
async fn pre_vote_fallback_deadline_allows_the_next_tick_after_a_late_real_campaign() {
    let mut core = core(&[1, 2, 3], false);
    core.runtime_config.enable_elect.store(true, Ordering::Relaxed);
    core.engine.config.election_timeout_min = 100;
    core.engine.config.election_timeout_max = 101;
    core.engine.config.timer_config.election_timeout = Duration::from_millis(100);
    let before = core.engine.state.clone();

    for i in 1..=2 {
        tokio::time::advance(Duration::from_millis(100)).await;
        core.handle_notify(Notify::Tick { i }).unwrap();
        assert!(matches!(core.engine.output.take_commands().as_slice(), [
            crate::engine::Command::SendPreVote { .. }
        ]));
        assert_eq!(before, core.engine.state, "silence does not start a real election");
    }

    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(!core.engine.handle_vote_req(VoteRequest::new(Vote::new(100, 2), Some(log(9)))).vote_granted);
    assert_eq!(before, core.engine.state, "a stale real request grants no vote");
    assert!(core.engine.output.take_commands().is_empty());
    core.handle_notify(Notify::Tick { i: 3 }).unwrap();

    let actual = Vote::new(101, 1);
    assert_eq!(actual, *core.engine.state.vote_ref());
    assert!(core.engine.leader.is_none());
    assert_eq!(0, core.engine.candidate.as_ref().unwrap().granters().count());
    assert_eq!(core.engine.output.take_commands(), vec![
        crate::engine::Command::SaveVote { vote: actual },
        crate::engine::Command::SendVote {
            vote_req: VoteRequest::new(actual, Some(log(10))),
        }
    ]);
}

#[tokio::test]
async fn resign_commands_recheck_the_current_sender_response_fence() {
    let mut core = core(&[1, 2, 3], false);
    let vote = Vote::new_committed(6, 3);
    core.engine.state.vote = UTime::new(TestConfig::now(), vote);
    core.engine.candidate = None;
    core.engine.state.server_state = crate::ServerState::Follower;
    let receipt = fence(&mut core, 3).await;
    core.engine.output.take_commands();
    let before = core.engine.state.clone();
    let (tx, rx) = TestConfig::oneshot();
    core.handle_api_msg(RaftMsg::ReleaseLeaderLease {
        sender: 3,
        vote,
        membership_log_id: Some(log(0)),
        tx,
    })
    .await;
    assert!(!rx.await.unwrap().unwrap());
    assert_eq!(before, core.engine.state);
    assert!(core.engine.output.take_commands().is_empty());

    release(&mut core, receipt).await;
    let (tx, rx) = TestConfig::oneshot();
    core.handle_api_msg(RaftMsg::ReleaseLeaderLease {
        sender: 3,
        vote,
        membership_log_id: Some(log(0)),
        tx,
    })
    .await;
    assert!(rx.await.unwrap().unwrap());
    assert!(core.engine.state.vote.lease_disabled());

    let receipt = fence(&mut core, 3).await;
    core.engine.output.take_commands();
    let before = core.engine.state.clone();
    let (tx, rx) = TestConfig::oneshot();
    core.handle_api_msg(RaftMsg::ElectIfLeaderResigned {
        sender: 3,
        vote,
        membership_log_id: Some(log(0)),
        tx,
    })
    .await;
    assert!(
        !rx.await.unwrap().unwrap(),
        "a fence installed during the application delay must win"
    );
    assert_eq!(before, core.engine.state);
    assert!(core.engine.output.take_commands().is_empty());

    release(&mut core, receipt).await;
    core.engine.output.take_commands();
    let (tx, rx) = TestConfig::oneshot();
    core.handle_api_msg(RaftMsg::ElectIfLeaderResigned {
        sender: 3,
        vote,
        membership_log_id: Some(log(0)),
        tx,
    })
    .await;
    assert!(rx.await.unwrap().unwrap());
    assert_eq!(core.engine.state.vote_ref(), &Vote::new(7, 1));
    assert!(matches!(core.engine.output.take_commands().as_slice(), [
        crate::engine::Command::SaveVote { .. },
        crate::engine::Command::SendVote { .. }
    ]));
}

fn log(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(5, 1), index)
}

fn core(voters: &[u64], leader: bool) -> Core {
    let config = Arc::new(
        raft::Config {
            enable_tick: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let (tx_api, rx_api) = tokio::sync::mpsc::unbounded_channel();
    let (tx_notify, rx_notify) = tokio::sync::mpsc::unbounded_channel();
    let (tx_metrics, _) = tokio::sync::watch::channel(RaftMetrics::new_initial(1));
    let (tx_data_metrics, _) = tokio::sync::watch::channel(RaftDataMetrics::default());
    let (tx_server_metrics, _) = tokio::sync::watch::channel(RaftServerMetrics::default());
    let store = MemoryStore::default();
    let sm_handle = worker::Worker::spawn(store.clone(), tx_notify.clone());
    let mut engine = Engine::<TestConfig>::default();
    // These consumption tests drive engine state without executing its storage
    // commands, just as the existing Engine unit fixtures do.
    engine.state.enable_validation(false);
    engine.config = EngineConfig::new::<HandoffRuntime>(1, &config);
    let membership = Arc::new(EffectiveMembership::new(
        Some(log(0)),
        Membership::new(vec![voters.iter().copied().collect()], None),
    ));
    engine.state.membership_state = MembershipState::new(membership.clone(), membership);
    engine.state.log_ids = LogIdList::new([log(0), log(10)]);
    engine.state.vote = UTime::new(
        TestConfig::now(),
        if leader {
            Vote::new_committed(5, 1)
        } else {
            Vote::new(5, 1)
        },
    );
    engine.state.update_committed(&Some(log(0)));
    if leader {
        engine.testing_new_leader();
        let state = engine.leader.as_mut().unwrap();
        let _ = state.progress.update(&1, ProgressEntry::new(Some(log(10))));
        state.progress.get_mut(&3).unwrap().inflight = Inflight::logs(Some(log(9)), Some(log(10))).with_id(7);
    } else {
        let candidate = engine.new_candidate(Vote::new(5, 1));
        candidate.grant_by(&1);
    }
    engine.output.take_commands();
    Core {
        id: 1,
        config: config.clone(),
        runtime_config: Arc::new(RuntimeConfig::new(&config)),
        network: Network(Arc::default()),
        log_store: store,
        sm_handle,
        engine,
        client_resp_channels: Default::default(),
        replications: Default::default(),
        retired_replications: Default::default(),
        leader_data: None,
        peer_admission: Default::default(),
        tx_api,
        rx_api,
        tx_notify,
        rx_notify,
        tx_metrics,
        tx_data_metrics,
        tx_server_metrics,
        command_state: CommandState::default(),
        span: tracing::Span::none(),
        _p: Default::default(),
    }
}

async fn fence(core: &mut Core, peer: u64) -> PeerResponseFence<u64> {
    let (tx, rx) = TestConfig::oneshot();
    core.tx_api.send(RaftMsg::FencePeerResponses { peer, tx }).unwrap();
    let command = core.rx_api.recv().await.unwrap();
    core.handle_api_msg(command).await;
    rx.await.unwrap().unwrap()
}

async fn release(core: &mut Core, fence: PeerResponseFence<u64>) {
    let (tx, rx) = TestConfig::oneshot();
    core.tx_api.send(RaftMsg::ReleasePeerResponseFence { fence, tx }).unwrap();
    let command = core.rx_api.recv().await.unwrap();
    core.handle_api_msg(command).await;
    assert!(rx.await.unwrap().unwrap());
}

fn consume(core: &mut Core) -> Result<(), Fatal<u64>> {
    let notification = core.rx_notify.try_recv().expect("result was queued before consumption");
    core.handle_notify(notification)
}

#[derive(Clone, Copy, Debug)]
enum Reply {
    VoteGrant,
    VoteHigher,
    AppendMatch,
    AppendConflict,
    AppendError,
    AppendHigher,
    Heartbeat,
    SnapshotMatch,
    ReadHigher,
}

impl Reply {
    fn is_vote(self) -> bool {
        matches!(self, Self::VoteGrant | Self::VoteHigher)
    }

    fn notification(self, core: &Core, generation: ResponseGeneration) -> Notify<TestConfig> {
        let vote = *core.engine.state.vote_ref();
        match self {
            Self::VoteGrant | Self::VoteHigher => Notify::VoteResponse {
                generation,
                target: 3,
                sender_vote: vote,
                resp: VoteResponse::new(
                    if matches!(self, Self::VoteHigher) {
                        Vote::new(100, 3)
                    } else {
                        vote
                    },
                    Some(log(10)),
                    matches!(self, Self::VoteGrant),
                ),
            },
            Self::AppendHigher => Notify::Network {
                generation,
                response: Response::HigherVote {
                    target: 3,
                    higher: Vote::new(100, 3),
                    sender_vote: vote,
                },
            },
            Self::ReadHigher => Notify::HigherVote {
                generation,
                target: 3,
                higher: Vote::new(100, 3),
                sender_vote: vote,
            },
            _ => Notify::Network {
                generation,
                response: Response::Progress {
                    target: 3,
                    request_id: match self {
                        Self::Heartbeat => RequestId::HeartBeat,
                        Self::SnapshotMatch => RequestId::new_snapshot(7),
                        _ => RequestId::new_append_entries(7),
                    },
                    result: match self {
                        Self::AppendError => Err("old network failure".to_owned()),
                        Self::AppendConflict => Ok(ReplicationResult::new(TestConfig::now(), Err(log(9)))),
                        _ => Ok(ReplicationResult::new(TestConfig::now(), Ok(Some(log(10))))),
                    },
                    session_id: ReplicationSessionId::new(vote, Some(log(0))),
                },
            },
        }
    }
}

#[tokio::test]
async fn queued_remote_results_cannot_mutate_any_state_after_close_or_release() {
    for reply in [
        Reply::VoteGrant,
        Reply::VoteHigher,
        Reply::AppendMatch,
        Reply::AppendConflict,
        Reply::AppendError,
        Reply::AppendHigher,
        Reply::Heartbeat,
        Reply::SnapshotMatch,
        Reply::ReadHigher,
    ] {
        for reopen in [false, true] {
            let mut core = core(&[1, 2, 3], !reply.is_vote());
            let generation = core.peer_admission.generation.clone();
            core.tx_notify.send(reply.notification(&core, generation)).unwrap();
            let receipt = fence(&mut core, 3).await;
            if reopen {
                release(&mut core, receipt).await;
            }
            let vote = *core.engine.state.vote_ref();
            let leader = core.engine.leader.clone();
            let candidate = core.engine.candidate.clone();
            let committed = core.engine.state.committed().cloned();
            let pending = core.engine.output.take_commands();
            consume(&mut core).unwrap();
            assert_eq!(*core.engine.state.vote_ref(), vote, "{reply:?} reopen={reopen}");
            assert_eq!(core.engine.leader, leader, "{reply:?} reopen={reopen}");
            assert_eq!(core.engine.candidate, candidate, "{reply:?} reopen={reopen}");
            assert_eq!(core.engine.state.committed(), committed.as_ref());
            assert!(
                core.engine.output.take_commands().is_empty(),
                "no effects from {reply:?}; prior commands={pending:?}"
            );
        }
    }
}

#[tokio::test]
async fn same_vote_current_results_work_without_a_fence_and_after_release() {
    for reopen in [false, true] {
        for reply in [
            Reply::VoteGrant,
            Reply::VoteHigher,
            Reply::AppendHigher,
            Reply::ReadHigher,
            Reply::Heartbeat,
            Reply::AppendMatch,
            Reply::SnapshotMatch,
        ] {
            let mut core = core(&[1, 2, 3], !reply.is_vote());
            if reopen {
                let receipt = fence(&mut core, 3).await;
                release(&mut core, receipt).await;
            }
            if matches!(reply, Reply::AppendMatch | Reply::SnapshotMatch) {
                let progress = core.engine.leader.as_mut().unwrap().progress.get_mut(&3).unwrap();
                progress.inflight = if matches!(reply, Reply::SnapshotMatch) {
                    Inflight::snapshot(Some(log(10))).with_id(7)
                } else {
                    Inflight::logs(None, Some(log(10))).with_id(7)
                };
            }
            let generation = core.peer_admission.generation.clone();
            core.tx_notify.send(reply.notification(&core, generation)).unwrap();
            consume(&mut core).unwrap();
            match reply {
                Reply::VoteGrant => assert!(core.engine.leader.is_some(), "fresh vote can win"),
                Reply::VoteHigher | Reply::AppendHigher | Reply::ReadHigher => {
                    assert_eq!(core.engine.state.vote_ref().leader_id.term, 100)
                }
                Reply::Heartbeat => assert!(core.engine.leader.as_ref().unwrap().clock_progress.get(&3).is_some()),
                Reply::AppendMatch | Reply::SnapshotMatch => assert_eq!(
                    core.engine.state.committed(),
                    Some(&log(10)),
                    "original majority commits"
                ),
                _ => unreachable!(),
            }
        }
    }
}

#[tokio::test]
async fn queued_read_proofs_are_published_only_in_their_original_generation() {
    for change in [0, 1, 2] {
        let mut core = core(&[1, 2, 3], true);
        let (tx, mut rx) = TestConfig::oneshot();
        core.tx_notify
            .send(Notify::ReadResult {
                generation: core.peer_admission.generation.clone(),
                result: Ok((Some(log(10)), Some(log(0)))),
                tx,
            })
            .unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        if change > 0 {
            let receipt = fence(&mut core, 3).await;
            if change == 2 {
                release(&mut core, receipt).await;
            }
        }
        consume(&mut core).unwrap();
        let result = rx.await.unwrap();
        if change == 0 {
            assert_eq!(result.unwrap(), (Some(log(10)), Some(log(0))));
        } else {
            assert!(matches!(result, Err(CheckIsLeaderError::QuorumNotEnough(_))));
        }
    }
}

#[tokio::test]
async fn fencing_forgets_cached_grants_matching_and_clocks_but_keeps_commits() {
    let mut candidate = core(&[1, 2, 3, 4, 5], false);
    candidate.engine.candidate.as_mut().unwrap().grant_by(&3);
    fence(&mut candidate, 3).await;
    let vote = *candidate.engine.state.vote_ref();
    for target in [2, 4] {
        candidate
            .handle_notify(Notify::VoteResponse {
                generation: candidate.peer_admission.generation.clone(),
                target,
                resp: VoteResponse::new(vote, Some(log(10)), true),
                sender_vote: vote,
            })
            .unwrap();
        assert_eq!(
            candidate.engine.leader.is_some(),
            target == 4,
            "three out of original five are required"
        );
    }

    let mut leader = core(&[1, 2, 3], true);
    let progress = leader.engine.leader.as_mut().unwrap();
    let _ = progress.progress.update(&3, ProgressEntry::new(Some(log(10))));
    let _ = progress.clock_progress.update(&1, Some(TestConfig::now()));
    let _ = progress.clock_progress.update(&3, Some(TestConfig::now()));
    assert!(progress.clock_progress.granted().is_some());
    assert_eq!(progress.progress.granted(), &Some(log(10)));
    fence(&mut leader, 3).await;
    let progress = leader.engine.leader.as_ref().unwrap();
    assert_eq!(progress.progress.granted(), &None);
    assert_eq!(progress.progress.get(&3).matching, None);
    assert_eq!(progress.clock_progress.granted(), &None);
    assert_eq!(progress.progress.get(&1).matching, Some(log(10)));
    assert_eq!(
        leader.engine.state.committed(),
        Some(&log(0)),
        "no committed frontier is undone"
    );
}

#[tokio::test]
async fn a_fence_never_invalidates_local_durable_vote_acknowledgements_or_errors() {
    let mut core = core(&[1, 2, 3], false);
    let old_generation = core.peer_admission.generation.clone();
    let vote = *core.engine.state.vote_ref();
    core.engine.new_candidate(vote); // no grant yet; self's save is pending
    core.tx_notify
        .send(Notify::VoteResponse {
            generation: old_generation.clone(),
            target: 1,
            resp: VoteResponse::new(vote, None, true),
            sender_vote: vote,
        })
        .unwrap();
    fence(&mut core, 3).await;
    consume(&mut core).unwrap();
    assert_eq!(
        core.engine.candidate.as_ref().unwrap().granters().collect::<Vec<_>>(),
        vec![1]
    );
    core.handle_notify(Notify::VoteResponse {
        generation: core.peer_admission.generation.clone(),
        target: 2,
        resp: VoteResponse::new(vote, Some(log(10)), true),
        sender_vote: vote,
    })
    .unwrap();
    assert!(core.engine.leader.is_some());
    let error = StorageError::from(StorageIOError::read_logs(raft::AnyError::error(
        "retired reader failed",
    )));
    core.tx_notify
        .send(Notify::Network {
            generation: old_generation,
            response: Response::StorageError { error: error.clone() },
        })
        .unwrap();
    assert_eq!(consume(&mut core), Err(Fatal::StorageError(error)));
    assert_eq!(
        core.handle_notify(Notify::ReplicationFatal { error: Fatal::Panicked }),
        Err(Fatal::Panicked)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_vote_workers_keep_the_generation_captured_before_rpc_return() {
    tokio::time::timeout(Duration::from_secs(8), async {
        for fenced in [false, true] {
            for success in [false, true] {
                let mut core = core(&[1, 2, 3], false);
                let handoff = core.network.0.clone();
                let _release = ReleaseOnDrop(handoff.clone());
                handoff.unavailable.lock().unwrap().insert(2);
                handoff.rpc_kind.store(1, Ordering::SeqCst);
                handoff.reply_success.store(success, Ordering::SeqCst);
                handoff.armed.store(true, Ordering::SeqCst);
                let original = core.peer_admission.generation.clone();
                core.engine.output.push_command(crate::engine::Command::SendVote {
                    vote_req: VoteRequest {
                        vote: *core.engine.state.vote_ref(),
                        last_log_id: Some(log(10)),
                    },
                });
                core.run_engine_commands().await.unwrap();
                handoff.entered.notified().await;
                if fenced {
                    fence(&mut core, 3).await;
                }
                handoff.resume();
                let result = core.rx_notify.recv().await.unwrap();
                match &result {
                    Notify::VoteResponse { generation, target, .. } => {
                        assert_eq!(*target, 3);
                        assert!(
                            generation.same_as(&original),
                            "worker must never borrow a later generation"
                        );
                    }
                    other => panic!("unexpected worker result: {}", crate::MessageSummary::summary(other)),
                }
                core.handle_notify(result).unwrap();
                if fenced {
                    assert_eq!(core.engine.state.vote_ref().leader_id.term, 5);
                    assert!(core.engine.leader.is_none());
                } else if success {
                    assert!(core.engine.leader.is_some());
                } else {
                    assert_eq!(core.engine.state.vote_ref().leader_id.term, 100);
                }
            }
        }
    })
    .await
    .expect("vote-worker hang guard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_read_probes_publish_through_core_and_keep_their_generation() {
    tokio::time::timeout(Duration::from_secs(8), async {
        for fenced in [false, true] {
            for success in [false, true] {
                let mut core = core(&[1, 2, 3], true);
                let handoff = core.network.0.clone();
                let _release = ReleaseOnDrop(handoff.clone());
                handoff.unavailable.lock().unwrap().insert(2);
                handoff.reply_success.store(success, Ordering::SeqCst);
                handoff.armed.store(true, Ordering::SeqCst);
                let original = core.peer_admission.generation.clone();
                let (tx, mut rx) = TestConfig::oneshot();
                core.handle_api_msg(RaftMsg::CheckIsLeaderRequest { tx }).await;
                handoff.entered.notified().await;
                if fenced {
                    fence(&mut core, 3).await;
                }
                handoff.resume();
                for _ in 0..if success { 1 } else { 2 } {
                    let result = core.rx_notify.recv().await.unwrap();
                    assert!(
                        matches!(rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
                        "a completed network round cannot publish directly to the caller"
                    );
                    match &result {
                        Notify::ReadResult { generation, .. } | Notify::HigherVote { generation, .. } => {
                            assert!(generation.same_as(&original));
                        }
                        other => panic!("unexpected read result: {}", crate::MessageSummary::summary(other)),
                    }
                    core.handle_notify(result).unwrap();
                }
                let published = rx.await.unwrap();
                if fenced {
                    assert!(matches!(published, Err(CheckIsLeaderError::QuorumNotEnough(_))));
                    assert_eq!(core.engine.state.vote_ref().leader_id.term, 5);
                } else if success {
                    assert!(published.is_ok());
                } else {
                    assert!(matches!(published, Err(CheckIsLeaderError::ForwardToLeader(_))));
                    assert_eq!(core.engine.state.vote_ref().leader_id.term, 100);
                }
            }
        }
    })
    .await
    .expect("read-probe hang guard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_snapshot_callbacks_queued_before_fence_are_stale_at_consumption() {
    use futures::StreamExt;

    use crate::replication::request::Replicate;

    tokio::time::timeout(Duration::from_secs(8), async {
        for fenced in [false, true] {
            for success in [false, true] {
                let mut core = core(&[1, 2, 3], true);
                let handoff = core.network.0.clone();
                let _release = ReleaseOnDrop(handoff.clone());
                handoff.rpc_kind.store(2, Ordering::SeqCst);
                handoff.reply_success.store(success, Ordering::SeqCst);
                handoff.armed.store(true, Ordering::SeqCst);
                let original = core.peer_admission.generation.clone();
                {
                    let mut store = core.log_store.0.lock().unwrap();
                    store.applied = Some(log(10));
                    store.membership = StoredMembership::new(
                        Some(log(0)),
                        core.engine.state.membership_state.effective().membership().clone(),
                    );
                }
                core.log_store.build_snapshot().await.unwrap();
                let progress = core.engine.leader.as_mut().unwrap().progress.get_mut(&3).unwrap();
                progress.inflight = Inflight::snapshot(Some(log(10))).with_id(7);
                let progress = *progress;
                let stream = core.spawn_replication_stream(3, progress).await;
                stream.tx_repl.send(Replicate::snapshot(RequestId::new_snapshot(7), Some(log(10)))).unwrap();
                core.replications.insert(3, stream);
                handoff.entered.notified().await;
                handoff.resume();
                // Receiving this notification proves the actual snapshot child
                // finished and its callback passed through ReplicationCore.
                let result = core.rx_notify.recv().await.unwrap();
                match &result {
                    Notify::Network { generation, response } => {
                        assert!(generation.same_as(&original));
                        if success {
                            assert!(matches!(response, Response::Progress {
                                request_id: RequestId::Snapshot { id: 7 },
                                ..
                            }));
                        } else {
                            assert!(matches!(response, Response::HigherVote { .. }));
                        }
                    }
                    other => panic!(
                        "unexpected snapshot callback: {}",
                        crate::MessageSummary::summary(other)
                    ),
                }
                if fenced {
                    fence(&mut core, 3).await;
                }
                core.handle_notify(result).unwrap();
                if fenced {
                    assert_eq!(core.engine.state.vote_ref().leader_id.term, 5);
                    assert_eq!(core.engine.state.committed(), Some(&log(0)));
                    assert_eq!(core.engine.leader.as_ref().unwrap().progress.get(&3).matching, None);
                } else if success {
                    assert_eq!(core.engine.state.committed(), Some(&log(10)));
                } else {
                    assert_eq!(core.engine.state.vote_ref().leader_id.term, 100);
                }
                core.remove_all_replication();
                while let Some(joined) = core.retired_replications.next().await {
                    joined.unwrap().unwrap();
                }
            }
        }
    })
    .await
    .expect("snapshot-callback hang guard");
}

#[tokio::test]
async fn a_closed_peer_is_refused_even_with_a_current_generation() {
    let mut core = core(&[1, 2, 3], false);
    fence(&mut core, 3).await;
    let generation = core.peer_admission.generation.clone();
    core.tx_notify.send(Reply::VoteGrant.notification(&core, generation)).unwrap();
    consume(&mut core).unwrap();
    assert!(core.engine.leader.is_none());
}

#[tokio::test]
async fn reads_still_require_the_original_majority_when_remote_peers_are_fenced() {
    let mut core = core(&[1, 2, 3], true);
    fence(&mut core, 2).await;
    fence(&mut core, 3).await;
    let (tx, rx) = TestConfig::oneshot();
    core.handle_api_msg(RaftMsg::CheckIsLeaderRequest { tx }).await;
    let result = core.rx_notify.recv().await.unwrap();
    core.handle_notify(result).unwrap();
    assert!(matches!(rx.await.unwrap(), Err(CheckIsLeaderError::QuorumNotEnough(_))));
    assert_eq!(
        core.engine.state.membership_state.effective().voter_ids().collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn admission_changes_rebuild_streams_during_transfer_and_shutdown_without_heartbeats() {
    use futures::StreamExt;

    use crate::engine::Command;

    let mut observed = Vec::new();
    for mode in ["normal", "transfer", "shutdown"] {
        for reopen in [false, true] {
            let mut core = core(&[1, 2, 3], true);
            core.log_store.0.lock().unwrap().log.extend((0..=10).map(|index| {
                (index, Entry {
                    log_id: log(index),
                    payload: EntryPayload::Blank,
                })
            }));
            // The admitted successor is already caught up: only ordinary
            // leadership may send it an empty heartbeat when streams restart.
            *core.engine.leader.as_mut().unwrap().progress.get_mut(&2).unwrap() = ProgressEntry::new(Some(log(10)));
            core.engine.replication_handler().rebuild_replication_streams();
            core.run_engine_commands().await.unwrap();
            let receipt = if reopen { Some(fence(&mut core, 3).await) } else { None };
            if reopen {
                core.run_engine_commands().await.unwrap();
            }
            match mode {
                "transfer" => {
                    core.engine.begin_leadership_transfer(2).unwrap();
                }
                "shutdown" => {
                    core.engine.prepare_shutdown(Some(2)).unwrap();
                }
                _ => {}
            }
            core.engine.output.take_commands();
            let vote = core.engine.state.vote.clone();
            if let Some(receipt) = receipt {
                release(&mut core, receipt).await;
            } else {
                fence(&mut core, 3).await;
            }
            let heartbeat = core.engine.output.commands.iter().any(|command| {
                matches!(command, Command::Replicate {
                    target: 2,
                    req: Inflight::None
                })
            });
            core.run_engine_commands().await.unwrap();
            let streams = core.replications.keys().copied().collect::<Vec<_>>();
            assert_eq!(core.engine.state.vote, vote, "admission cannot re-arm the lease");
            observed.push((mode, reopen, heartbeat, streams));
            core.remove_all_replication();
            while let Some(joined) = core.retired_replications.next().await {
                joined.unwrap().unwrap();
            }
        }
    }
    for (mode, reopen, heartbeat, streams) in &observed {
        let expected = if *reopen { vec![2, 3] } else { vec![2] };
        assert_eq!(*streams, expected, "admitted peers must retain streams: {observed:?}");
        assert_eq!(
            *heartbeat,
            *mode == "normal",
            "handoff must not renew leases: {observed:?}"
        );
    }
}

#[tokio::test]
async fn a_fenced_handoff_target_is_refused_before_stopping_leader_admission() {
    use crate::raft::TransferLeaderError;

    for shutdown in [false, true] {
        let mut core = core(&[1, 2, 3], true);
        fence(&mut core, 3).await;
        let vote = core.engine.state.vote.clone();
        if shutdown {
            let (tx, rx) = TestConfig::oneshot();
            core.handle_api_msg(RaftMsg::PrepareShutdown { to: Some(3), tx }).await;
            assert_eq!(rx.await.unwrap(), Err(TransferLeaderError::InvalidTarget));
        } else {
            let (tx, rx) = TestConfig::oneshot();
            core.handle_api_msg(RaftMsg::BeginLeadershipTransfer { to: 3, tx }).await;
            assert_eq!(rx.await.unwrap(), Err(TransferLeaderError::InvalidTarget));
        }
        assert_eq!(core.engine.state.vote, vote);
        assert!(core.engine.prepared_shutdown.is_none());
        assert!(core.engine.leader.as_ref().unwrap().transfer_to.is_none());
        assert!(
            core.engine.leader_handler().is_ok(),
            "the refused target must not stop admission"
        );
    }

    // A nonleader needs no successor to retire, even if its caller supplies
    // a target that is currently fenced on this node.
    let mut core = core(&[1, 2, 3], false);
    fence(&mut core, 3).await;
    let (tx, rx) = TestConfig::oneshot();
    core.handle_api_msg(RaftMsg::PrepareShutdown { to: Some(3), tx }).await;
    assert_eq!(rx.await.unwrap(), Ok(None));
    assert!(core.engine.prepared_shutdown.is_some());
}
