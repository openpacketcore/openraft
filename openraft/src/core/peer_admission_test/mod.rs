#![cfg(not(feature = "singlethreaded"))]

//! Characterize the engine handoff that a retirement barrier must cover.
//!
//! The fault runtime suspends a replication task after its network future has
//! returned, before Openraft consumes the returned value. This models ordinary
//! scheduler preemption at that boundary without editing the pinned engine.
//! The in-memory storage and synthetic peers isolate response accounting; this
//! is not a replacement, durability, transport-authentication, or process proof.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

#[cfg(not(feature = "generic-snapshot-data"))]
use raft::error::InstallSnapshotError;
use raft::error::RPCError;
use raft::error::RaftError;
use raft::error::Unreachable;
use raft::network::RPCOption;
use raft::network::RaftNetwork;
use raft::network::RaftNetworkFactory;
use raft::raft::AppendEntriesRequest;
use raft::raft::AppendEntriesResponse;
#[cfg(not(feature = "generic-snapshot-data"))]
use raft::raft::InstallSnapshotRequest;
#[cfg(not(feature = "generic-snapshot-data"))]
use raft::raft::InstallSnapshotResponse;
use raft::raft::VoteRequest;
use raft::raft::VoteResponse;
use raft::storage::LogFlushed;
use raft::storage::RaftLogStorage;
use raft::storage::RaftStateMachine;
use raft::AsyncRuntime;
use raft::EmptyNode;
use raft::Entry;
use raft::EntryPayload;
use raft::LogId;
use raft::LogState;
use raft::RaftLogReader;
use raft::RaftSnapshotBuilder;
use raft::Snapshot;
use raft::SnapshotMeta;
use raft::StorageError;
use raft::StoredMembership;
use raft::TokioRuntime;
use raft::Vote;

use crate as raft;

raft::declare_raft_types!(
    TestConfig:
        D = u64,
        R = (),
        NodeId = u64,
        Node = EmptyNode,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = HandoffRuntime,
);

#[derive(Debug, Default)]
struct Handoff {
    armed: AtomicBool,
    closed: AtomicBool,
    active_calls: AtomicUsize,
    // 0: AppendEntries/read probe, 1: Vote, 2: snapshot transport, 3: PreVote.
    rpc_kind: AtomicUsize,
    reply_success: AtomicBool,
    unavailable: Mutex<BTreeSet<u64>>,
    last_seen: Mutex<Option<tokio::time::Instant>>,
    entered: tokio::sync::Notify,
    producer_completed: tokio::sync::Notify,
    released: Mutex<bool>,
    release: Condvar,
}

impl Handoff {
    fn resume(&self) {
        *self.released.lock().expect("handoff release") = true;
        self.release.notify_all();
    }

    fn hold_returned_response(&self) {
        self.entered.notify_one();
        let (released, _) = self
            .release
            .wait_timeout_while(
                self.released.lock().expect("handoff release"),
                Duration::from_secs(10),
                |released| !*released,
            )
            .expect("bounded response handoff");
        assert!(*released, "response handoff exceeded its hang guard");
    }
}

struct ReleaseOnDrop(Arc<Handoff>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.resume();
    }
}

// The network sets this only in the poll that returns Ready. The wrapping
// timeout consumes it in that same poll, before returning to Openraft. No
// thread-local state is carried over an await or between unrelated calls.
thread_local! {
    static RETURNED_RESPONSE: RefCell<Option<Arc<Handoff>>> = const { RefCell::new(None) };
}

// For a replication HigherVote, task completion follows enqueueing the core
// notification. The regression waits for this signal before issuing a fresh
// majority write, whose acknowledgement must follow that notification in FIFO.
tokio::task_local! {
    static PRODUCER_HANDOFF: Arc<Mutex<Option<Arc<Handoff>>>>;
}

struct HandoffTimeout<F: Future> {
    inner: Pin<Box<tokio::time::Timeout<F>>>,
}

impl<F: Future> Future for HandoffTimeout<F> {
    type Output = Result<F::Output, tokio::time::error::Elapsed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                if let Some(handoff) = RETURNED_RESPONSE.with(|returned| returned.take()) {
                    handoff.hold_returned_response();
                }
                Poll::Ready(result)
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct HandoffRuntime;

impl AsyncRuntime for HandoffRuntime {
    type JoinError = <TokioRuntime as AsyncRuntime>::JoinError;
    type JoinHandle<T: Send + 'static> = <TokioRuntime as AsyncRuntime>::JoinHandle<T>;
    type Sleep = <TokioRuntime as AsyncRuntime>::Sleep;
    type Instant = <TokioRuntime as AsyncRuntime>::Instant;
    type TimeoutError = <TokioRuntime as AsyncRuntime>::TimeoutError;
    type Timeout<R, F: Future<Output = R> + Send> = HandoffTimeout<F>;
    type ThreadLocalRng = <TokioRuntime as AsyncRuntime>::ThreadLocalRng;
    type OneshotSender<T: Send> = <TokioRuntime as AsyncRuntime>::OneshotSender<T>;
    type OneshotReceiverError = <TokioRuntime as AsyncRuntime>::OneshotReceiverError;
    type OneshotReceiver<T: Send> = <TokioRuntime as AsyncRuntime>::OneshotReceiver<T>;

    fn spawn<F>(future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handoff = Arc::new(Mutex::new(None));
        TokioRuntime::spawn(PRODUCER_HANDOFF.scope(handoff.clone(), async move {
            let result = future.await;
            if let Some(handoff) = handoff.lock().unwrap().take() {
                handoff.producer_completed.notify_one();
            }
            result
        }))
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        TokioRuntime::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        TokioRuntime::sleep_until(deadline)
    }

    fn timeout<R, F: Future<Output = R> + Send>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        HandoffTimeout {
            inner: Box::pin(tokio::time::timeout(duration, future)),
        }
    }

    fn timeout_at<R, F: Future<Output = R> + Send>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        HandoffTimeout {
            inner: Box::pin(tokio::time::timeout_at(deadline, future)),
        }
    }

    fn is_panic(error: &Self::JoinError) -> bool {
        TokioRuntime::is_panic(error)
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        TokioRuntime::thread_rng()
    }

    fn oneshot<T: Send>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>) {
        TokioRuntime::oneshot()
    }
}

#[derive(Default)]
struct MemoryState {
    log: BTreeMap<u64, Entry<TestConfig>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    purged: Option<LogId<u64>>,
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, EmptyNode>,
    control_applied: bool,
    snapshot: Option<SnapshotMeta<u64, EmptyNode>>,
}

#[derive(Clone, Default)]
struct MemoryStore(Arc<Mutex<MemoryState>>);

impl RaftLogReader<TestConfig> for MemoryStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry<TestConfig>>, StorageError<u64>>
    where RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send {
        Ok(self.0.lock().expect("memory log").log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

#[cfg(not(feature = "storage-v2"))]
impl crate::storage::TestStorageSealed for MemoryStore {}

impl RaftLogStorage<TestConfig> for MemoryStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TestConfig>, StorageError<u64>> {
        let state = self.0.lock().expect("memory log");
        Ok(LogState {
            last_purged_log_id: state.purged,
            last_log_id: state.log.last_key_value().map(|(_, e)| e.log_id).or(state.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.0.lock().expect("memory log").vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.0.lock().expect("memory log").vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.0.lock().expect("memory log").committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.0.lock().expect("memory log").committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TestConfig>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TestConfig>> + Send,
        I::IntoIter: Send,
    {
        self.0.lock().expect("memory log").log.extend(entries.into_iter().map(|e| (e.log_id.index, e)));
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.0.lock().expect("memory log").log.retain(|index, _| *index < log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.0.lock().expect("memory log");
        state.log.retain(|index, _| *index > log_id.index);
        state.purged = Some(log_id);
        Ok(())
    }
}

impl RaftSnapshotBuilder<TestConfig> for MemoryStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TestConfig>, StorageError<u64>> {
        let mut state = self.0.lock().unwrap();
        let meta = SnapshotMeta {
            last_log_id: state.applied,
            last_membership: state.membership.clone(),
            snapshot_id: "peer-response-test".to_owned(),
        };
        state.snapshot = Some(meta.clone());
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(vec![1, 2, 3])),
        })
    }
}

impl RaftStateMachine<TestConfig> for MemoryStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, EmptyNode>), StorageError<u64>> {
        let state = self.0.lock().expect("memory state machine");
        Ok((state.applied, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TestConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut state = self.0.lock().expect("memory state machine");
        let mut responses = Vec::new();
        for entry in entries {
            state.applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Membership(membership) => {
                    state.membership = StoredMembership::new(Some(entry.log_id), membership)
                }
                EntryPayload::Normal(1) => state.control_applied = true,
                _ => {}
            }
            responses.push(());
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        panic!("no snapshot sender in the response-boundary test")
    }

    async fn install_snapshot(
        &mut self,
        _: &SnapshotMeta<u64, EmptyNode>,
        _: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        panic!("no snapshot sender in the response-boundary test")
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TestConfig>>, StorageError<u64>> {
        let state = self.0.lock().unwrap();
        Ok(state.snapshot.clone().map(|meta| Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(vec![1, 2, 3])),
        }))
    }
}

struct Network(Arc<Handoff>);
struct Peer {
    node_id: u64,
    handoff: Arc<Handoff>,
}

impl RaftNetworkFactory<TestConfig> for Network {
    type Network = Peer;
    async fn new_client(&mut self, node_id: u64, _: &EmptyNode) -> Peer {
        Peer {
            node_id,
            handoff: Arc::clone(&self.0),
        }
    }
}

type RpcFailure<E = raft::error::Infallible> = RPCError<u64, EmptyNode, RaftError<u64, E>>;

impl Peer {
    fn unavailable(&self) -> bool {
        self.handoff.unavailable.lock().unwrap().contains(&self.node_id)
            || (self.node_id == 3 && self.handoff.closed.load(Ordering::SeqCst))
    }

    fn hold_reply(&self, kind: usize) -> bool {
        if self.node_id != 3
            || self.handoff.rpc_kind.load(Ordering::SeqCst) != kind
            || !self.handoff.armed.swap(false, Ordering::SeqCst)
        {
            return false;
        }
        *self.handoff.last_seen.lock().unwrap() = Some(tokio::time::Instant::now());
        let _ = PRODUCER_HANDOFF.try_with(|slot| *slot.lock().unwrap() = Some(self.handoff.clone()));
        RETURNED_RESPONSE.with(|returned| *returned.borrow_mut() = Some(self.handoff.clone()));
        true
    }
}

impl RaftNetwork<TestConfig> for Peer {
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<TestConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RpcFailure> {
        self.handoff.active_calls.fetch_add(1, Ordering::SeqCst);
        let response = if self.unavailable() {
            Err(RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
                "peer unavailable",
            ))))
        } else if self.hold_reply(0) && !self.handoff.reply_success.load(Ordering::SeqCst) {
            Ok(AppendEntriesResponse::HigherVote(Vote::new(100, 3)))
        } else {
            Ok(AppendEntriesResponse::Success)
        };
        self.handoff.active_calls.fetch_sub(1, Ordering::SeqCst);
        response
    }

    async fn pre_vote(
        &mut self,
        request: raft::raft::PreVoteRequest<u64>,
        _: RPCOption,
    ) -> Result<raft::raft::PreVoteResponse<u64>, RpcFailure> {
        if self.unavailable() {
            return Err(RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
                "peer unavailable",
            ))));
        }
        let higher = self.hold_reply(3) && !self.handoff.reply_success.load(Ordering::SeqCst);
        let actual = if higher { Vote::new(100, 3) } else { Vote::default() };
        Ok(raft::raft::PreVoteResponse::new(actual, request.last_log_id, !higher))
    }

    async fn vote(&mut self, request: VoteRequest<u64>, _: RPCOption) -> Result<VoteResponse<u64>, RpcFailure> {
        if self.unavailable() {
            return Err(RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
                "peer unavailable",
            ))));
        }
        let vote = if self.hold_reply(1) && !self.handoff.reply_success.load(Ordering::SeqCst) {
            Vote::new(100, 3)
        } else {
            request.vote
        };
        Ok(VoteResponse::new(vote, request.last_log_id, vote == request.vote))
    }

    #[cfg(not(feature = "generic-snapshot-data"))]
    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<TestConfig>,
        _: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RpcFailure<InstallSnapshotError>> {
        let vote = if self.hold_reply(2) && !self.handoff.reply_success.load(Ordering::SeqCst) {
            Vote::new(100, 3)
        } else {
            request.vote
        };
        Ok(InstallSnapshotResponse { vote })
    }

    #[cfg(feature = "generic-snapshot-data")]
    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        _: Snapshot<TestConfig>,
        _: impl Future<Output = raft::error::ReplicationClosed> + Send + 'static,
        _: RPCOption,
    ) -> Result<raft::raft::SnapshotResponse<u64>, raft::error::StreamingError<TestConfig, raft::error::Fatal<u64>>>
    {
        // A custom full-snapshot transport with the same ready-return pause as
        // the default chunked transport. Its callback still uses the real
        // ReplicationCore child task and result channel.
        HandoffRuntime::timeout(Duration::from_secs(2), async {
            let vote = if self.hold_reply(2) && !self.handoff.reply_success.load(Ordering::SeqCst) {
                Vote::new(100, 3)
            } else {
                vote
            };
            Ok(raft::raft::SnapshotResponse { vote })
        })
        .await
        .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_fence_rejects_returned_response_after_transport_drain() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let handoff = Arc::new(Handoff::default());
        let _release = ReleaseOnDrop(Arc::clone(&handoff));
        let store = MemoryStore::default();
        let mut config = raft::Config::default().validate().expect("valid engine profile");
        config.enable_elect = false;
        config.snapshot_policy = raft::SnapshotPolicy::Never;
        let engine = raft::Raft::<TestConfig>::new(
            1,
            Arc::new(config),
            Network(Arc::clone(&handoff)),
            store.clone(),
            store.clone(),
        )
        .await
        .expect("real pinned engine");
        engine.initialize(BTreeSet::from([1, 2, 3])).await.expect("fixed configuration");
        engine.trigger().elect().await.expect("ordinary engine election");
        engine
            .wait(Some(Duration::from_secs(2)))
            .current_leader(1, "test leader")
            .await
            .expect("leader elected");
        engine.client_write(0).await.expect("initial applied barrier");
        let initial_term = engine.metrics().borrow().current_term;

        handoff.armed.store(true, Ordering::SeqCst);
        engine.trigger().heartbeat().await.expect("start held peer RPC");
        handoff.entered.notified().await;
        // A recent-traffic refusal cannot make the response handoff safe:
        // scheduling delay can exceed any finite observation window. This is
        // a synthetic policy window, not a new production setting.
        let absence_window = Duration::from_millis(25);
        let last_seen = handoff
            .last_seen
            .lock()
            .expect("last proven target traffic")
            .expect("the held reply proved its peer");
        tokio::time::sleep_until(last_seen + absence_window).await;
        assert!(last_seen.elapsed() >= absence_window);
        handoff.closed.store(true, Ordering::SeqCst);
        assert_eq!(
            handoff.active_calls.load(Ordering::SeqCst),
            0,
            "the network adapter drained completely"
        );
        let observed = engine
            .with_raft_state(|state| state.vote_ref().leader_id.term)
            .await
            .expect("serialized core observation");
        assert_eq!(observed, initial_term);
        let _fence = engine.fence_peer_responses(3).await.expect("serialized engine fence");
        let committed = engine.client_write(1).await.expect("unchanged majority commits the control entry");
        assert!(store.0.lock().expect("applied control").control_applied);
        assert_eq!(engine.metrics().borrow().current_term, initial_term);

        handoff.resume();
        handoff.producer_completed.notified().await;
        // The old notification is now queued. A new majority acknowledgement
        // from peer 2 is enqueued after it on the same core notification channel,
        // proving the old result was consumed before this write can complete.
        engine.client_write(2).await.expect("fresh majority write after old callback consumption");
        engine.shutdown().await.expect("join test engine and held response producer");
        let final_term = engine.metrics().borrow().current_term;
        eprintln!(
            "adapter_active=0 gate_closed=true committed_control_index={} term_before={} term_after={}",
            committed.log_id.index, initial_term, final_term
        );
        assert_eq!(
            final_term, initial_term,
            "an acknowledged engine fence must reject a late response"
        );
    })
    .await
    .expect("response-boundary hang guard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fence_receipts_are_scoped_and_do_not_shrink_the_quorum() {
    let config = Arc::new(
        raft::Config {
            enable_tick: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let store = MemoryStore::default();
    let engine = raft::Raft::<TestConfig>::new(1, config.clone(), Network(Arc::default()), store.clone(), store)
        .await
        .unwrap();
    let other_store = MemoryStore::default();
    let other = raft::Raft::<TestConfig>::new(1, config, Network(Arc::default()), other_store.clone(), other_store)
        .await
        .unwrap();
    assert!(
        engine.fence_peer_responses(1).await.is_err(),
        "the local voter cannot be fenced"
    );
    let older = engine.fence_peer_responses(3).await.unwrap();
    let newer = engine.fence_peer_responses(3).await.unwrap();
    assert_eq!(*newer.peer_id(), 3);
    let foreign = other.fence_peer_responses(3).await.unwrap();
    assert!(
        !other.release_peer_response_fence(newer.clone()).await.unwrap(),
        "receipt is instance-local even for the same fenced peer"
    );
    assert!(other.release_peer_response_fence(foreign).await.unwrap());
    assert!(
        !engine.release_peer_response_fence(older).await.unwrap(),
        "old operation cannot reopen a newer gate"
    );
    let peer2 = engine.fence_peer_responses(2).await.unwrap();
    engine.initialize(BTreeSet::from([1, 2, 3])).await.unwrap();
    engine.trigger().elect().await.unwrap();
    // A same-core observation follows the election command. The engine has no
    // remote RPCs to wait for because both remote peers are fenced.
    assert!(
        !engine.with_raft_state(|s| s.vote_ref().is_committed()).await.unwrap(),
        "one out of three is not a majority"
    );
    assert!(
        engine.release_peer_response_fence(peer2).await.unwrap(),
        "distinct peer's receipt stays valid"
    );
    engine.trigger().elect().await.unwrap();
    engine
        .wait(Some(Duration::from_secs(2)))
        .current_leader(1, "original majority elects")
        .await
        .unwrap();
    engine.client_write(0).await.unwrap();
    engine.ensure_linearizable().await.unwrap();
    assert!(engine.release_peer_response_fence(newer.clone()).await.unwrap());
    assert!(
        !engine.release_peer_response_fence(newer).await.unwrap(),
        "release does not recreate authority"
    );
    engine.client_write(0).await.unwrap();
    engine.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

mod queued;
