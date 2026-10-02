//! Poll the production replication future against ready log storage and replies.
//! A gate on only the second RPC bounds the unfixed loop. The first storage read
//! and legal partial reply have no gate, yield or timer that could hide a spin.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;

use super::ReplicationCore;
use super::ReplicationSessionId;
use crate::core::notify::Notify;
use crate::core::sm::handle::SnapshotReader;
use crate::engine::testing::UTConfig as C;
use crate::error::Fatal;
use crate::error::NetworkError;
use crate::error::PayloadTooLarge;
use crate::error::RPCError;
use crate::error::RaftError;
use crate::error::RemoteError;
use crate::error::ReplicationClosed;
use crate::error::StreamingError;
use crate::error::Unreachable;
use crate::log_id_range::LogIdRange;
use crate::network::Backoff;
use crate::network::RPCOption;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendEntriesResponse;
use crate::raft::SnapshotResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::replication::request::Replicate;
use crate::replication::request_id::RequestId;
use crate::replication::response::Response;
use crate::storage::LogFlushed;
use crate::storage::RaftLogStorage;
use crate::storage::RaftLogStorageExt;
use crate::Config;
use crate::Entry;
use crate::EntryPayload;
use crate::LogId;
use crate::LogState;
use crate::MessageSummary;
use crate::OptionalSend;
use crate::RaftLogReader;
use crate::RaftNetwork;
use crate::RaftNetworkFactory;
use crate::Snapshot;
use crate::StorageError;
use crate::StorageIOError;
use crate::Vote;

const RETRY: Duration = Duration::from_millis(10);
const UNREACHABLE_RETRY: Duration = Duration::from_millis(37);
const RPC_TIMEOUT: Duration = Duration::from_millis(100);

type AppendResult = Result<AppendEntriesResponse<u64>, RPCError<u64, (), RaftError<u64>>>;
type Core = ReplicationCore<C, NetworkFactory, ReadyStore>;
type Main = Pin<Box<dyn Future<Output = Result<(), Fatal<u64>>>>>;

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(crate::CommittedLeaderId::new(7, 1), index)
}

fn vote() -> Vote<u64> {
    Vote::new_committed(7, 1)
}

fn entries() -> Vec<Entry<C>> {
    (0..=3)
        .map(|index| Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(()),
        })
        .collect()
}

#[derive(Default)]
struct LogData {
    entries: BTreeMap<u64, Entry<C>>,
    reads: Vec<(u64, u64)>,
    vote: Option<Vote<u64>>,
    purged: Option<LogId<u64>>,
    fail_read: bool,
}

/// A real in-memory log store: setup and successful RPCs append through the
/// storage trait, complete the flush callback, and are read back independently.
/// Its read futures are ready; no executor yield stands in for the retry timer.
#[derive(Clone, Default)]
struct ReadyStore(Arc<Mutex<LogData>>);

impl RaftLogReader<C> for ReadyStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<u64>> {
        let data = self.0.lock().unwrap();
        if data.fail_read {
            return Err(StorageIOError::read_logs(anyerror::AnyError::error("ready log read failure")).into());
        }
        Ok(data.entries.range(range).map(|(_, entry)| entry.clone()).collect())
    }

    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> Result<Vec<Entry<C>>, StorageError<u64>> {
        self.0.lock().unwrap().reads.push((start, end));
        self.try_get_log_entries(start..end).await
    }
}

#[cfg(not(feature = "storage-v2"))]
impl crate::storage::TestStorageSealed for ReadyStore {}

impl RaftLogStorage<C> for ReadyStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u64>> {
        let data = self.0.lock().unwrap();
        Ok(LogState {
            last_purged_log_id: data.purged,
            last_log_id: data.entries.last_key_value().map(|(_, entry)| entry.log_id).or(data.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.0.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.0.lock().unwrap().vote)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut data = self.0.lock().unwrap();
            for entry in entries {
                data.entries.insert(entry.log_id.index, entry);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.0.lock().unwrap().entries.retain(|index, _| *index < log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut data = self.0.lock().unwrap();
        data.entries.retain(|index, _| *index > log_id.index);
        data.purged = Some(log_id);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Reply {
    Previous,
    PreviousAfter(Duration),
    First,
    Success,
    HigherVote,
    Conflict,
    Network,
    RemoteFatal,
    Unreachable,
    TooLarge,
    Timeout,
}

#[derive(Default)]
struct Wire {
    requests: Vec<AppendEntriesRequest<C>>,
    completed: usize,
    backoffs: usize,
}

struct ReadyNetwork {
    wire: Arc<Mutex<Wire>>,
    follower: ReadyStore,
    replies: VecDeque<Reply>,
    second_gate: Option<oneshot::Receiver<()>>,
}

impl RaftNetwork<C> for ReadyNetwork {
    async fn append_entries(&mut self, rpc: AppendEntriesRequest<C>, _option: RPCOption) -> AppendResult {
        let arrival = {
            let mut wire = self.wire.lock().unwrap();
            wire.requests.push(rpc.clone());
            wire.requests.len()
        };
        if arrival == 2 {
            if let Some(gate) = self.second_gate.take() {
                let _ = gate.await;
            }
        }
        if let Some(prev) = rpc.prev_log_id {
            assert_eq!(
                self.follower.0.lock().unwrap().entries.get(&prev.index).map(|entry| entry.log_id),
                Some(prev),
                "RPC predecessor must exist in the actual follower store"
            );
        }
        let error = std::io::Error::other("synthetic replication error");
        let result = match self.replies.pop_front().expect("unexpected replication retry") {
            Reply::Previous => Ok(AppendEntriesResponse::PartialSuccess(rpc.prev_log_id)),
            Reply::PreviousAfter(delay) => {
                tokio::time::sleep(delay).await;
                Ok(AppendEntriesResponse::PartialSuccess(rpc.prev_log_id))
            }
            Reply::First => {
                let first = rpc.entries.first().expect("positive partial requires an entry").clone();
                let matching = first.log_id;
                self.follower.blocking_append([first]).await.unwrap();
                Ok(AppendEntriesResponse::PartialSuccess(Some(matching)))
            }
            Reply::Success => {
                self.follower.blocking_append(rpc.entries).await.unwrap();
                Ok(AppendEntriesResponse::Success)
            }
            Reply::HigherVote => Ok(AppendEntriesResponse::HigherVote(Vote::new_committed(8, 2))),
            Reply::Conflict => Ok(AppendEntriesResponse::Conflict),
            Reply::Network => Err(NetworkError::new(&error).into()),
            Reply::RemoteFatal => Err(RemoteError::new(2, RaftError::Fatal(Fatal::Stopped)).into()),
            Reply::Unreachable => Err(Unreachable::new(&error).into()),
            Reply::TooLarge => Err(PayloadTooLarge::new_entries_hint(1).into()),
            Reply::Timeout => std::future::pending().await,
        };
        self.wire.lock().unwrap().completed += 1;
        result
    }

    #[cfg(not(feature = "generic-snapshot-data"))]
    async fn install_snapshot(
        &mut self,
        _rpc: crate::raft::InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        crate::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, (), RaftError<u64, crate::error::InstallSnapshotError>>,
    > {
        Err(NetworkError::new(&std::io::Error::other("unexpected snapshot")).into())
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote<u64>,
        _snapshot: Snapshot<C>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<C, Fatal<u64>>> {
        Err(StreamingError::Closed(ReplicationClosed::new("unexpected snapshot")))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, (), RaftError<u64>>> {
        Err(NetworkError::new(&std::io::Error::other("unexpected vote RPC")).into())
    }

    fn backoff(&self) -> Backoff {
        self.wire.lock().unwrap().backoffs += 1;
        Backoff::new(std::iter::repeat(UNREACHABLE_RETRY))
    }
}

struct NetworkFactory;

impl RaftNetworkFactory<C> for NetworkFactory {
    type Network = ReadyNetwork;

    async fn new_client(&mut self, _target: u64, _node: &()) -> Self::Network {
        panic!("fixture passes its real network clients directly to ReplicationCore")
    }
}

struct Run {
    main: Main,
    tx: Option<mpsc::UnboundedSender<Replicate<C>>>,
    tx_close: Option<watch::Sender<()>>,
    rx: mpsc::UnboundedReceiver<Notify<C>>,
    leader: ReadyStore,
    follower: ReadyStore,
    wire: Arc<Mutex<Wire>>,
    release_second: Option<oneshot::Sender<()>>,
    range: LogIdRange<u64>,
    request_id: RequestId,
}

impl Run {
    async fn new(prev: Option<LogId<u64>>, replies: Vec<Reply>, gate_second: bool) -> Self {
        Self::with_heartbeat_interval(prev, replies, gate_second, RPC_TIMEOUT.as_millis() as u64).await
    }

    async fn with_heartbeat_interval(
        prev: Option<LogId<u64>>,
        replies: Vec<Reply>,
        gate_second: bool,
        heartbeat_interval: u64,
    ) -> Self {
        let mut leader = ReadyStore::default();
        leader.blocking_append(entries()).await.unwrap();
        let mut follower = ReadyStore::default();
        if let Some(prev) = prev {
            follower
                .blocking_append(entries().into_iter().filter(|entry| entry.log_id.index <= prev.index))
                .await
                .unwrap();
        }
        let wire = Arc::new(Mutex::new(Wire::default()));
        let (release, gate) = oneshot::channel();
        let network = ReadyNetwork {
            wire: wire.clone(),
            follower: follower.clone(),
            replies: replies.into(),
            second_gate: gate_second.then_some(gate),
        };
        let snapshot_network = ReadyNetwork {
            wire: wire.clone(),
            follower: follower.clone(),
            replies: VecDeque::new(),
            second_gate: None,
        };
        let (tx, rx_event) = mpsc::unbounded_channel();
        let (tx_close, rx_close) = watch::channel(());
        let (tx_raft_core, rx) = mpsc::unbounded_channel();
        let core = Core {
            target: 2,
            session_id: ReplicationSessionId::new(vote(), Some(log_id(0))),
            tx_raft_core,
            rx_event,
            rx_close,
            weak_tx_event: tx.downgrade(),
            network,
            snapshot_network: Arc::new(tokio::sync::Mutex::new(snapshot_network)),
            snapshot_state: None,
            backoff: None,
            log_reader: leader.get_log_reader().await,
            snapshot_reader: SnapshotReader::closed_for_test(),
            config: Arc::new(
                Config {
                    heartbeat_interval,
                    // No election runs here; keep the timeouts valid for every heartbeat interval.
                    election_timeout_min: 10_000,
                    election_timeout_max: 10_001,
                    ..Config::default()
                }
                .validate()
                .unwrap(),
            ),
            committed: prev,
            matching: prev,
            next_action: None,
            entries_hint: Default::default(),
        };
        // The production future is owned and directly polled, never detached.
        // Remove incidental Tokio cooperative yields as a possible false positive;
        // the second-arrival gate still bounds the unfixed, otherwise-ready loop.
        let main = Box::pin(tokio::task::unconstrained(core.main()));
        Self {
            main,
            tx: Some(tx),
            tx_close: Some(tx_close),
            rx,
            leader,
            follower,
            wire,
            release_second: gate_second.then_some(release),
            range: LogIdRange::new(prev, Some(log_id(3))),
            request_id: RequestId::new_append_entries(41),
        }
    }

    fn send(&self, event: Replicate<C>) {
        self.tx.as_ref().unwrap().send(event).unwrap();
    }

    fn start_logs(&self) {
        self.send(Replicate::logs(self.request_id, self.range));
    }

    fn poll(&mut self) -> Poll<Result<(), Fatal<u64>>> {
        let waker = futures::task::noop_waker();
        self.main.as_mut().poll(&mut Context::from_waker(&waker))
    }

    fn pending(&mut self) {
        assert!(self.poll().is_pending(), "replication should remain alive");
    }

    fn counts(&self) -> (usize, usize, usize) {
        let reads = self.leader.0.lock().unwrap().reads.len();
        let wire = self.wire.lock().unwrap();
        (reads, wire.requests.len(), wire.completed)
    }

    fn first_no_progress(&mut self) {
        self.start_logs();
        self.pending();
        assert_eq!(
            self.wire.lock().unwrap().completed,
            1,
            "first ready partial response completed"
        );
        assert_eq!(
            self.counts(),
            (1, 1, 1),
            "zero_progress_retry_waits_before_second_log_read"
        );
        let progress_id = if self.range.prev.is_some() {
            self.request_id
        } else {
            RequestId::new_heartbeat()
        };
        self.progress(progress_id, self.range.prev);
    }

    fn progress(&mut self, expected_id: RequestId, matching: Option<LogId<u64>>) -> Instant {
        match self.rx.try_recv().expect("production replication notification") {
            Notify::Network {
                response:
                    Response::Progress {
                        target,
                        request_id,
                        result,
                        session_id,
                    },
            } => {
                assert_eq!(target, 2);
                assert_eq!(request_id, expected_id, "original replication request identity");
                assert_eq!(session_id.vote, vote());
                assert_eq!(session_id.membership_log_id, Some(log_id(0)));
                let result = result.unwrap();
                assert_eq!(result.result, Ok(matching));
                result.sending_time
            }
            other => panic!("expected progress, got {}", other.summary()),
        }
    }

    fn error(&mut self, expected_id: RequestId, fragment: &str) {
        match self.rx.try_recv().expect("production error notification") {
            Notify::Network {
                response: Response::Progress { request_id, result, .. },
            } => {
                assert_eq!(request_id, expected_id);
                assert!(result.unwrap_err().contains(fragment));
            }
            other => panic!("expected replication error, got {}", other.summary()),
        }
    }

    fn release(&mut self) {
        self.release_second.take().expect("second RPC gate").send(()).unwrap();
    }

    fn close(&mut self) {
        let before = Instant::now();
        self.tx.take();
        self.tx_close.take();
        assert!(
            matches!(self.poll(), Poll::Ready(Ok(()))),
            "zero_progress_close_does_not_wait_for_retry_timer"
        );
        assert_eq!(Instant::now(), before, "close must not advance the clock");
    }

    async fn assert_complete_suffix(&mut self) {
        assert_eq!(self.leader.try_get_log_entries(0..4).await.unwrap(), entries());
        assert_eq!(self.follower.try_get_log_entries(0..4).await.unwrap(), entries());
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        // A failing assertion cannot strand a gate or a detached producer. The
        // production future and any active RPC are owned fields dropped here.
        if let Some(release) = self.release_second.take() {
            let _ = release.send(());
        }
        self.tx.take();
        self.tx_close.take();
    }
}

async fn retry_waits(prev: Option<LogId<u64>>) {
    let mut run = Run::new(prev, vec![Reply::Previous, Reply::Success], true).await;
    let start = Instant::now();
    run.first_no_progress();
    assert_eq!(Instant::now(), start, "the first read and reply must be ready");
    run.pending();
    assert_eq!(run.counts(), (1, 1, 1));
    tokio::time::advance(RETRY - Duration::from_millis(1)).await;
    run.pending();
    assert_eq!(run.counts(), (1, 1, 1), "no retry before the fixed deadline");
    tokio::time::advance(Duration::from_millis(1)).await;
    run.pending();
    assert_eq!(
        run.counts(),
        (2, 2, 1),
        "expiry reaches the second actual read and RPC gate"
    );
    assert_eq!(Instant::now(), start + RETRY);
    {
        let reads = run.leader.0.lock().unwrap().reads.clone();
        assert_eq!(reads[0], reads[1], "retry the exact original storage range");
        let wire = run.wire.lock().unwrap();
        let first = &wire.requests[0];
        let second = &wire.requests[1];
        assert_eq!(first.vote, second.vote);
        assert_eq!(first.prev_log_id, prev);
        assert_eq!(second.prev_log_id, prev);
        assert_eq!(first.entries, second.entries, "retry the stored original entries");
        assert!(!second.entries.is_empty());
    }
    run.release();
    run.pending();
    run.assert_complete_suffix().await;
    assert_eq!(run.progress(run.request_id, Some(log_id(3))), start + RETRY);
    assert_eq!(run.counts(), (2, 2, 2));
    run.close();
}

#[tokio::test(start_paused = true)]
async fn zero_progress_retry_waits_before_second_log_read() {
    retry_waits(Some(log_id(0))).await;
}

#[tokio::test(start_paused = true)]
async fn zero_progress_retry_waits_before_second_log_read_from_none() {
    retry_waits(None).await;
}

#[tokio::test(start_paused = true)]
async fn zero_progress_close_does_not_wait_for_retry_timer() {
    for prev in [Some(log_id(0)), None] {
        let mut run = Run::new(prev, vec![Reply::Previous, Reply::Success], true).await;
        run.first_no_progress();
        run.close();
        assert_eq!(
            run.counts(),
            (1, 1, 1),
            "closed replication must not start another read or RPC"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn zero_progress_events_coalesce_without_resetting_retry_deadline() {
    for prev in [Some(log_id(0)), None] {
        let mut run = Run::new(prev, vec![Reply::Previous, Reply::Success], true).await;
        let start = Instant::now();
        run.first_no_progress();
        tokio::time::advance(Duration::from_millis(4)).await;
        run.send(Replicate::Committed(Some(log_id(1))));
        run.send(Replicate::Heartbeat);
        run.pending();
        assert_eq!(run.counts(), (1, 1, 1));
        tokio::time::advance(Duration::from_millis(5)).await;
        run.send(Replicate::Committed(Some(log_id(3))));
        run.send(Replicate::Heartbeat);
        run.send(Replicate::Heartbeat);
        run.pending();
        assert_eq!(run.counts(), (1, 1, 1), "events must not send an early retry");
        tokio::time::advance(Duration::from_millis(1)).await;
        run.pending();
        assert_eq!(run.counts(), (2, 2, 1), "events must not extend the original deadline");
        assert_eq!(Instant::now(), start + RETRY);
        {
            let wire = run.wire.lock().unwrap();
            assert_eq!(wire.requests[1].leader_commit, Some(log_id(3)));
            assert_eq!(wire.requests[1].prev_log_id, prev);
            assert_eq!(wire.requests[1].entries, wire.requests[0].entries);
            assert!(
                !wire.requests[1].entries.is_empty(),
                "heartbeat must not replace the pending range"
            );
        }
        run.release();
        run.pending();
        run.progress(run.request_id, Some(log_id(3)));
        run.assert_complete_suffix().await;
        assert_eq!(run.counts(), (2, 2, 2), "events coalesce into one data retry");
        run.close();
    }
}

#[tokio::test(start_paused = true)]
async fn positive_partial_progress_retries_without_cooldown() {
    for prev in [Some(log_id(0)), None] {
        let mut run = Run::new(prev, vec![Reply::First, Reply::Success], true).await;
        let start = Instant::now();
        run.start_logs();
        run.pending();
        assert_eq!(
            run.counts(),
            (2, 2, 1),
            "positive progress must immediately read the remaining suffix"
        );
        let first_index = prev.map_or(0, |id| id.index + 1);
        run.progress(run.request_id, Some(log_id(first_index)));
        assert_eq!(Instant::now(), start);
        {
            let reads = &run.leader.0.lock().unwrap().reads;
            assert_eq!(reads.as_slice(), &[(first_index, 4), (first_index + 1, 4)]);
            let wire = run.wire.lock().unwrap();
            assert_eq!(wire.requests[1].prev_log_id, Some(log_id(first_index)));
            assert_eq!(
                wire.requests[1].entries.first().unwrap().log_id,
                log_id(first_index + 1)
            );
        }
        run.release();
        run.pending();
        run.progress(run.request_id, Some(log_id(3)));
        run.assert_complete_suffix().await;
        run.close();
    }
}

#[tokio::test(start_paused = true)]
async fn heartbeat_partial_and_full_success_have_no_cooldown() {
    for prev in [Some(log_id(0)), None] {
        let mut run = Run::new(prev, vec![Reply::Previous, Reply::Success], false).await;
        let start = Instant::now();
        run.send(Replicate::Heartbeat);
        run.pending();
        assert_eq!(run.counts(), (0, 1, 1));
        run.progress(RequestId::new_heartbeat(), prev);
        run.send(Replicate::Heartbeat);
        run.pending();
        assert_eq!(run.counts(), (0, 2, 2));
        run.progress(RequestId::new_heartbeat(), prev);
        assert!(run.wire.lock().unwrap().requests.iter().all(|rpc| rpc.entries.is_empty()));
        assert_eq!(Instant::now(), start);
        run.close();
    }
    let mut run = Run::new(None, vec![Reply::Success, Reply::Success], false).await;
    let start = Instant::now();
    run.start_logs();
    run.pending();
    run.progress(run.request_id, Some(log_id(3)));
    run.send(Replicate::Heartbeat);
    run.pending();
    assert_eq!(
        run.counts(),
        (1, 2, 2),
        "completed append may send a new heartbeat immediately"
    );
    run.progress(RequestId::new_heartbeat(), Some(log_id(3)));
    run.assert_complete_suffix().await;
    assert_eq!(Instant::now(), start);
    run.close();
}

#[tokio::test(start_paused = true)]
async fn higher_vote_and_conflict_keep_their_original_classification() {
    let mut run = Run::new(Some(log_id(0)), vec![Reply::HigherVote], false).await;
    let start = Instant::now();
    run.start_logs();
    assert!(matches!(run.poll(), Poll::Ready(Ok(()))));
    assert_eq!(run.counts(), (1, 1, 1));
    match run.rx.try_recv().unwrap() {
        Notify::Network {
            response:
                Response::HigherVote {
                    target,
                    higher,
                    sender_vote,
                },
        } => {
            assert_eq!(target, 2);
            assert_eq!(higher, Vote::new_committed(8, 2));
            assert_eq!(sender_vote, vote());
        }
        other => panic!("expected higher vote, got {}", other.summary()),
    }
    assert_eq!(Instant::now(), start);

    let mut run = Run::new(Some(log_id(0)), vec![Reply::Conflict, Reply::Success], false).await;
    run.start_logs();
    run.pending();
    match run.rx.try_recv().unwrap() {
        Notify::Network {
            response: Response::Progress { request_id, result, .. },
        } => {
            assert_eq!(request_id, run.request_id);
            assert_eq!(result.unwrap().result, Err(log_id(0)));
        }
        other => panic!("expected conflict, got {}", other.summary()),
    }
    run.start_logs();
    run.pending();
    assert_eq!(run.counts(), (2, 2, 2));
    run.progress(run.request_id, Some(log_id(3)));
    run.assert_complete_suffix().await;
    assert_eq!(Instant::now(), start);
    run.close();
}

#[tokio::test(start_paused = true)]
async fn rpc_errors_keep_their_existing_retry_policy() {
    for reply in [Reply::Network, Reply::Unreachable, Reply::RemoteFatal, Reply::Timeout] {
        let mut run = Run::new(Some(log_id(0)), vec![reply, Reply::Success], false).await;
        run.start_logs();
        run.pending();
        if matches!(reply, Reply::Timeout) {
            assert_eq!(run.counts(), (1, 1, 0));
            tokio::time::advance(RPC_TIMEOUT).await;
            run.pending();
        }
        let delayed = matches!(reply, Reply::Unreachable | Reply::RemoteFatal);
        let fragment = match reply {
            Reply::Network => "NetworkError",
            Reply::Timeout => "timeout",
            _ => "Unreachable",
        };
        run.error(run.request_id, fragment);
        let start = Instant::now();
        let retry_id = RequestId::new_append_entries(42);
        run.send(Replicate::logs(retry_id, run.range));
        run.pending();
        if delayed {
            assert_eq!(run.counts(), (1, 1, 1));
            assert_eq!(run.wire.lock().unwrap().backoffs, 1);
            tokio::time::advance(UNREACHABLE_RETRY - Duration::from_millis(1)).await;
            run.pending();
            assert_eq!(run.counts(), (1, 1, 1));
            tokio::time::advance(Duration::from_millis(1)).await;
            run.pending();
            assert_eq!(Instant::now(), start + UNREACHABLE_RETRY);
        } else {
            assert_eq!(
                Instant::now(),
                start,
                "network/timeout retry must not inherit a cooldown"
            );
            assert_eq!(run.wire.lock().unwrap().backoffs, 0);
        }
        run.progress(retry_id, Some(log_id(3)));
        let completed = if matches!(reply, Reply::Timeout) { 1 } else { 2 };
        assert_eq!(run.counts(), (2, 2, completed));
        run.assert_complete_suffix().await;
        run.close();
    }
}

#[tokio::test(start_paused = true)]
async fn payload_too_large_retries_immediately_with_the_same_request_id() {
    let mut run = Run::new(
        Some(log_id(0)),
        vec![Reply::TooLarge, Reply::Success, Reply::Success, Reply::Success],
        true,
    )
    .await;
    let start = Instant::now();
    run.start_logs();
    run.pending();
    assert_eq!(run.counts(), (2, 2, 1));
    assert!(
        run.rx.try_recv().is_err(),
        "payload hint does not finish the data request"
    );
    assert_eq!(run.leader.0.lock().unwrap().reads, vec![(1, 4), (1, 2)]);
    run.release();
    run.pending();
    for index in 1..=3 {
        run.progress(run.request_id, Some(log_id(index)));
    }
    assert_eq!(run.counts(), (4, 4, 4));
    run.assert_complete_suffix().await;
    assert_eq!(Instant::now(), start);
    run.close();
}

#[tokio::test(start_paused = true)]
async fn storage_error_is_fatal_without_a_retry_timer() {
    let mut run = Run::new(None, vec![Reply::Success], false).await;
    run.leader.0.lock().unwrap().fail_read = true;
    let start = Instant::now();
    run.start_logs();
    assert!(matches!(run.poll(), Poll::Ready(Err(Fatal::StorageError(_)))));
    assert_eq!(run.counts(), (1, 0, 0));
    assert!(matches!(run.rx.try_recv().unwrap(), Notify::Network {
        response: Response::StorageError { .. }
    }));
    assert_eq!(Instant::now(), start);
}

#[tokio::test(start_paused = true)]
async fn short_and_zero_heartbeat_intervals_bound_no_progress_retry() {
    for (heartbeat, expected) in [(2, Duration::from_millis(2)), (0, Duration::from_millis(1))] {
        let mut run =
            Run::with_heartbeat_interval(Some(log_id(0)), vec![Reply::Previous, Reply::Success], false, heartbeat)
                .await;
        let start = Instant::now();
        run.first_no_progress();
        if heartbeat == 2 {
            tokio::time::advance(Duration::from_millis(1)).await;
            run.pending();
            assert_eq!(run.counts(), (1, 1, 1));
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        run.pending();
        assert_eq!(
            run.counts(),
            (2, 2, 2),
            "zero_progress_retry_respects_heartbeat_interval"
        );
        assert_eq!(Instant::now(), start + expected);
        run.progress(run.request_id, Some(log_id(3)));
        run.assert_complete_suffix().await;
        run.close();
    }
}

#[tokio::test(start_paused = true)]
async fn elapsed_rpc_time_counts_toward_the_no_progress_interval() {
    for (heartbeat, delay, interval) in [(100, 9, 10), (2, 1, 2), (100, 15, 10)] {
        let mut run = Run::with_heartbeat_interval(
            Some(log_id(0)),
            vec![Reply::PreviousAfter(Duration::from_millis(delay)), Reply::Success],
            false,
            heartbeat,
        )
        .await;
        let start = Instant::now();
        run.start_logs();
        run.pending();
        assert_eq!(run.counts(), (1, 1, 0), "only this control delays its first response");
        tokio::time::advance(Duration::from_millis(delay)).await;
        run.pending();
        if delay < interval {
            assert_eq!(run.counts(), (1, 1, 1));
            tokio::time::advance(Duration::from_millis(interval - delay)).await;
            run.pending();
        }
        assert_eq!(
            run.counts(),
            (2, 2, 2),
            "no_progress_interval_includes_elapsed_rpc_time"
        );
        assert_eq!(run.progress(run.request_id, Some(log_id(0))), start);
        assert_eq!(Instant::now(), start + Duration::from_millis(interval.max(delay)));
        run.progress(run.request_id, Some(log_id(3)));
        run.assert_complete_suffix().await;
        run.close();
    }
}

/// Closing a stream gives up an AppendEntries that never answers at once, not at its deadline.
///
/// The RaftCore joins a closed stream before it answers the vote that made it stop leading, so a
/// stream held by an unresponsive follower or learner would otherwise delay that answer by the
/// whole AppendEntries deadline. The entries were read before the request was sent, so no storage
/// operation is abandoned.
#[tokio::test(start_paused = true)]
async fn close_gives_up_an_append_entries_that_never_answers() {
    let mut run = Run::new(Some(log_id(0)), vec![Reply::Timeout], false).await;
    run.start_logs();
    run.pending();
    assert_eq!(
        run.counts(),
        (1, 1, 0),
        "the entries were read and the request is in flight"
    );
    run.close();
    assert_eq!(run.counts(), (1, 1, 0), "the request never completed");
}
