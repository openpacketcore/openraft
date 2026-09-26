//! Replication ownership across membership rebuilds and storage/lifecycle barriers.
//!
//! The gates are inside individual readers, after construction and without holding a
//! store lock. A replacement generation can therefore replicate the same log while
//! its predecessor remains alive. All data and faults in this fixture are synthetic.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use maplit::btreeset;
use openraft::error::Fatal;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::Adaptor;
use openraft::AsyncRuntime;
use openraft::CommittedLeaderId;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::LogIdOptionExt;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::RaftTypeConfig;
use openraft::ServerState;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::SnapshotPolicy;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::TokioRuntime;
use openraft::Vote;
use openraft_memstore::ClientRequest;
use openraft_memstore::ClientResponse;
use openraft_memstore::IntoMemClientRequest;
use openraft_memstore::MemStore;
use openraft_memstore::TypeConfig;
use tokio::sync::watch;
use tokio::sync::Semaphore;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;
use crate::fixtures::RaftRouterNetwork;

// Deliberately shorter than the RPC timeout, as in the existing rebuild detector.
const PROGRESS: Duration = Duration::from_millis(1_500);
const HELD: Duration = Duration::from_millis(100);
const RPC_TIMEOUT: u64 = 5_000;
const FAULT: &str = "synthetic retirement append failure";
const VOTE_FAULT: &str = "synthetic retirement vote persistence failure";
const SNAPSHOT_FAULT: &str = "synthetic retirement snapshot read failure";
const STREAMED_SNAPSHOT_FAULT: &str = "synthetic retiring snapshot data read failure";

trait FixtureConfig:
    RaftTypeConfig<
    NodeId = u64,
    Node = (),
    D = ClientRequest,
    R = ClientResponse,
    Entry = Entry<TypeConfig>,
    SnapshotData = std::io::Cursor<Vec<u8>>,
>
{
}

impl FixtureConfig for TypeConfig {}

openraft::declare_raft_types!(
    CallbackConfig:
        D = ClientRequest,
        R = ClientResponse,
        Node = (),
        Entry = Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = CallbackRuntime,
);

impl FixtureConfig for CallbackConfig {}

tokio::task_local! {
    // Each spawned task has its own slot. The transport marks its actual current
    // task, avoiding type-name matching, spawn-order assumptions or global gates.
    static TASK_TAIL: Arc<Mutex<Option<Arc<WorkGate>>>>;
}

#[derive(Debug, Default, PartialEq, Eq)]
struct CallbackRuntime;

impl AsyncRuntime for CallbackRuntime {
    type JoinError = <TokioRuntime as AsyncRuntime>::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = <TokioRuntime as AsyncRuntime>::JoinHandle<T>;
    type Sleep = <TokioRuntime as AsyncRuntime>::Sleep;
    type Instant = <TokioRuntime as AsyncRuntime>::Instant;
    type TimeoutError = <TokioRuntime as AsyncRuntime>::TimeoutError;
    type Timeout<R, F: Future<Output = R> + OptionalSend> = <TokioRuntime as AsyncRuntime>::Timeout<R, F>;
    type ThreadLocalRng = <TokioRuntime as AsyncRuntime>::ThreadLocalRng;
    type OneshotSender<T: OptionalSend> = <TokioRuntime as AsyncRuntime>::OneshotSender<T>;
    type OneshotReceiverError = <TokioRuntime as AsyncRuntime>::OneshotReceiverError;
    type OneshotReceiver<T: OptionalSend> = <TokioRuntime as AsyncRuntime>::OneshotReceiver<T>;

    fn spawn<F>(future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + OptionalSend + 'static,
        F::Output: OptionalSend + 'static,
    {
        let slot = Arc::new(Mutex::new(None::<Arc<WorkGate>>));
        TokioRuntime::spawn(TASK_TAIL.scope(slot.clone(), async move {
            let result = future.await;
            let gate = slot.lock().unwrap().take();
            if let Some(gate) = gate {
                // Production sent its callback before returning the future. The
                // task and its JoinHandle are still incomplete at this gate.
                gate.enter().await;
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

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        TokioRuntime::timeout(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        TokioRuntime::timeout_at(deadline, future)
    }

    fn is_panic(error: &Self::JoinError) -> bool {
        TokioRuntime::is_panic(error)
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        TokioRuntime::thread_rng()
    }

    fn oneshot<T: OptionalSend>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>) {
        TokioRuntime::oneshot()
    }
}

async fn within<F: Future>(future: F, contract: &str) -> Result<F::Output> {
    tokio::time::timeout(PROGRESS, future).await.with_context(|| contract.to_owned())
}

async fn observed(sender: &watch::Sender<bool>, contract: &str) -> Result<()> {
    let mut rx = sender.subscribe();
    within(rx.wait_for(|value| *value), contract).await??;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct ReadObservation {
    generation: usize,
    target: u64,
    start: u64,
    end: u64,
}

struct ReadGate {
    entered: watch::Sender<Option<ReadObservation>>,
    dropped: watch::Sender<bool>,
    release: Semaphore,
    panic_on_release: AtomicBool,
}

impl ReadGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: watch::channel(None).0,
            dropped: watch::channel(false).0,
            release: Semaphore::new(0),
            panic_on_release: AtomicBool::new(false),
        })
    }

    async fn entered(&self) -> Result<ReadObservation> {
        let mut rx = self.entered.subscribe();
        let observation = within(
            rx.wait_for(Option::is_some),
            "replication reader did not enter its gate",
        )
        .await??;
        Ok(observation.expect("observed a reader"))
    }

    fn release(&self) {
        // Closing is idempotent and also releases a waiter that has not been polled yet.
        self.release.close();
    }
}

struct SnapshotGate {
    target: u64,
    entered: watch::Sender<bool>,
    cancelled: watch::Sender<bool>,
    dropped: watch::Sender<bool>,
    release: Semaphore,
    panic_on_release: AtomicBool,
    storage_error_on_release: AtomicBool,
    returned_storage_error: watch::Sender<Option<StorageError<u64>>>,
    nonfatal_on_release: Mutex<Option<NonfatalSnapshotError>>,
    tail: Mutex<Option<Arc<WorkGate>>>,
}

#[derive(Clone, Copy, Debug)]
enum NonfatalSnapshotError {
    Network,
    RemoteStorage,
}

struct SnapshotReadGate {
    entered: watch::Sender<bool>,
    finished: watch::Sender<bool>,
    release: Semaphore,
}

impl SnapshotReadGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: watch::channel(false).0,
            finished: watch::channel(false).0,
            release: Semaphore::new(0),
        })
    }
}

// This records the end of the actual storage future, including cancellation.
// The future also retains its real snapshot result across the gate below.
struct SnapshotReadOwner(Arc<SnapshotReadGate>);

impl Drop for SnapshotReadOwner {
    fn drop(&mut self) {
        self.0.finished.send_replace(true);
    }
}

struct WorkGate {
    entered: watch::Sender<bool>,
    release: Semaphore,
}

impl WorkGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: watch::channel(false).0,
            release: Semaphore::new(0),
        })
    }

    async fn enter(&self) {
        self.entered.send_replace(true);
        let _ = self.release.acquire().await;
    }
}

impl SnapshotGate {
    fn new(target: u64) -> Arc<Self> {
        Arc::new(Self {
            target,
            entered: watch::channel(false).0,
            cancelled: watch::channel(false).0,
            dropped: watch::channel(false).0,
            release: Semaphore::new(0),
            panic_on_release: AtomicBool::new(false),
            storage_error_on_release: AtomicBool::new(false),
            returned_storage_error: watch::channel(None).0,
            nonfatal_on_release: Mutex::new(None),
            tail: Mutex::new(None),
        })
    }
}

struct OwnedSnapshot {
    snapshot: Option<Snapshot<TypeConfig>>,
    gate: Arc<SnapshotGate>,
}

impl Drop for OwnedSnapshot {
    fn drop(&mut self) {
        // The observation is published after the owned snapshot data is actually dropped.
        drop(self.snapshot.take());
        self.gate.dropped.send_replace(true);
    }
}

#[derive(Clone, Debug)]
struct Maintenance {
    operation: &'static str,
    log_id: Option<LogId<u64>>,
    readers: Vec<usize>,
    snapshot_owned: bool,
}

struct Probe {
    next_generation: AtomicUsize,
    // Network construction and log-reader construction are serialized by the core.
    // Record their public factory calls to identify the target of each reader.
    next_target: Mutex<Option<u64>>,
    armed: Mutex<BTreeMap<u64, Arc<ReadGate>>>,
    gates: Mutex<Vec<Arc<ReadGate>>>,
    live_readers: Mutex<BTreeMap<usize, u64>>,
    maintenance: Mutex<Vec<Maintenance>>,
    armed_snapshot: Mutex<Option<Arc<SnapshotGate>>>,
    snapshots: Mutex<Vec<Arc<SnapshotGate>>>,
    io_calls: AtomicUsize,
    fail_append: AtomicBool,
    failed_append: watch::Sender<bool>,
    fail_snapshot: AtomicBool,
    armed_snapshot_read: Mutex<Option<Arc<SnapshotReadGate>>>,
    snapshot_reads: Mutex<Vec<Arc<SnapshotReadGate>>>,
    armed_apply: Mutex<Option<Arc<WorkGate>>>,
    armed_append: Mutex<Option<Arc<WorkGate>>>,
    armed_vote_failure: Mutex<Option<Arc<WorkGate>>>,
    failed_vote: watch::Sender<bool>,
    work_gates: Mutex<Vec<Arc<WorkGate>>>,
    panic_rpc: Mutex<Option<u64>>,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_generation: AtomicUsize::new(0),
            next_target: Mutex::new(None),
            armed: Mutex::new(BTreeMap::new()),
            gates: Mutex::new(Vec::new()),
            live_readers: Mutex::new(BTreeMap::new()),
            maintenance: Mutex::new(Vec::new()),
            armed_snapshot: Mutex::new(None),
            snapshots: Mutex::new(Vec::new()),
            io_calls: AtomicUsize::new(0),
            fail_append: AtomicBool::new(false),
            failed_append: watch::channel(false).0,
            fail_snapshot: AtomicBool::new(false),
            armed_snapshot_read: Mutex::new(None),
            snapshot_reads: Mutex::new(Vec::new()),
            armed_apply: Mutex::new(None),
            armed_append: Mutex::new(None),
            armed_vote_failure: Mutex::new(None),
            failed_vote: watch::channel(false).0,
            work_gates: Mutex::new(Vec::new()),
            panic_rpc: Mutex::new(None),
        })
    }

    fn arm_reader(&self, target: u64) -> Arc<ReadGate> {
        let gate = ReadGate::new();
        assert!(self.armed.lock().unwrap().insert(target, gate.clone()).is_none());
        self.gates.lock().unwrap().push(gate.clone());
        gate
    }

    fn arm_snapshot(&self, target: u64) -> Arc<SnapshotGate> {
        let gate = SnapshotGate::new(target);
        assert!(self.armed_snapshot.lock().unwrap().replace(gate.clone()).is_none());
        self.snapshots.lock().unwrap().push(gate.clone());
        gate
    }

    fn arm_apply(&self) -> Arc<WorkGate> {
        let gate = WorkGate::new();
        assert!(self.armed_apply.lock().unwrap().replace(gate.clone()).is_none());
        self.work_gates.lock().unwrap().push(gate.clone());
        gate
    }

    fn arm_snapshot_read(&self) -> Arc<SnapshotReadGate> {
        let gate = SnapshotReadGate::new();
        assert!(self.armed_snapshot_read.lock().unwrap().replace(gate.clone()).is_none());
        self.snapshot_reads.lock().unwrap().push(gate.clone());
        gate
    }

    fn arm_append(&self) -> Arc<WorkGate> {
        let gate = WorkGate::new();
        assert!(self.armed_append.lock().unwrap().replace(gate.clone()).is_none());
        self.work_gates.lock().unwrap().push(gate.clone());
        gate
    }

    fn record(&self, operation: &'static str, log_id: Option<LogId<u64>>) {
        let readers = self
            .gates
            .lock()
            .unwrap()
            .iter()
            .filter_map(|gate| {
                let entry = *gate.entered.borrow();
                entry.filter(|_| !*gate.dropped.borrow()).map(|entry| entry.generation)
            })
            .collect();
        let snapshot_owned =
            self.snapshots.lock().unwrap().iter().any(|gate| *gate.entered.borrow() && !*gate.dropped.borrow());
        self.maintenance.lock().unwrap().push(Maintenance {
            operation,
            log_id,
            readers,
            snapshot_owned,
        });
    }

    fn operations(&self) -> Vec<Maintenance> {
        self.maintenance.lock().unwrap().clone()
    }

    fn assert_no_overlap(&self) -> Result<()> {
        let operations = self.operations();
        anyhow::ensure!(
            operations.iter().all(|operation| operation.readers.is_empty() && !operation.snapshot_owned),
            "RETIRED_STORAGE_OWNERSHIP: destructive operation overlapped a retained owner: {operations:?}"
        );
        Ok(())
    }

    fn release_all(&self) {
        for gate in self.gates.lock().unwrap().iter() {
            gate.release();
        }
        for gate in self.snapshots.lock().unwrap().iter() {
            gate.release.close();
        }
        for gate in self.work_gates.lock().unwrap().iter() {
            gate.release.close();
        }
        for gate in self.snapshot_reads.lock().unwrap().iter() {
            gate.release.close();
        }
    }
}

struct GenerationReader {
    inner: Arc<MemStore>,
    probe: Arc<Probe>,
    generation: usize,
    target: Option<u64>,
    gate: Option<Arc<ReadGate>>,
    entered: bool,
}

impl Drop for GenerationReader {
    fn drop(&mut self) {
        if self.target.is_some() {
            self.probe.live_readers.lock().unwrap().remove(&self.generation);
        }
        if let Some(gate) = &self.gate {
            gate.dropped.send_replace(true);
        }
    }
}

impl<C: FixtureConfig> RaftLogReader<C> for GenerationReader {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        self.inner.try_get_log_entries(range).await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        self.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            if !self.entered {
                self.entered = true;
                gate.entered.send_replace(Some(ReadObservation {
                    generation: self.generation,
                    target: self.target.expect("only replication readers are gated"),
                    start,
                    end,
                }));
                let _ = gate.release.acquire().await;
                assert!(
                    !gate.panic_on_release.load(Ordering::SeqCst),
                    "synthetic replication reader panic"
                );
            }
        }
        self.inner.limited_get_log_entries(start, end).await
    }
}

#[derive(Clone)]
struct ObservedStore {
    inner: Arc<MemStore>,
    probe: Arc<Probe>,
}

impl<C: FixtureConfig> RaftLogReader<C> for ObservedStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        self.inner.try_get_log_entries(range).await
    }
}

struct ObservedBuilder(Arc<MemStore>);

impl<C: FixtureConfig> RaftSnapshotBuilder<C> for ObservedBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<u64>> {
        let snapshot = self.0.build_snapshot().await?;
        Ok(Snapshot {
            meta: snapshot.meta,
            snapshot: snapshot.snapshot,
        })
    }
}

impl<C: FixtureConfig> RaftStorage<C> for ObservedStore {
    type LogReader = GenerationReader;
    type SnapshotBuilder = ObservedBuilder;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let gate = self.probe.armed_vote_failure.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.enter().await;
            self.probe.failed_vote.send_replace(true);
            return Err(StorageError::from_io_error(
                ErrorSubject::Vote,
                ErrorVerb::Write,
                std::io::Error::other(VOTE_FAULT),
            ));
        }
        self.inner.save_vote(vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.inner.read_vote().await
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.inner.read_committed().await
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u64>> {
        let state = self.inner.get_log_state().await?;
        Ok(LogState {
            last_purged_log_id: state.last_purged_log_id,
            last_log_id: state.last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        let generation = self.probe.next_generation.fetch_add(1, Ordering::SeqCst);
        let target = self.probe.next_target.lock().unwrap().take();
        let gate = target.and_then(|target| self.probe.armed.lock().unwrap().remove(&target));
        if let Some(target) = target {
            self.probe.live_readers.lock().unwrap().insert(generation, target);
        }
        GenerationReader {
            inner: self.inner.clone(),
            probe: self.probe.clone(),
            generation,
            target,
            gate,
            entered: false,
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend {
        let gate = self.probe.armed_append.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.enter().await;
        }
        if self.probe.fail_append.swap(false, Ordering::SeqCst) {
            self.probe.failed_append.send_replace(true);
            return Err(StorageError::from_io_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                std::io::Error::other(FAULT),
            ));
        }
        self.inner.append_to_log(entries).await
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.probe.record("truncate", Some(log_id));
        self.inner.delete_conflict_logs_since(log_id).await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.probe.record("purge", Some(log_id));
        self.inner.purge_logs_upto(log_id).await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, ()>), StorageError<u64>> {
        self.inner.last_applied_state().await
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ClientResponse>, StorageError<u64>> {
        let gate = self.probe.armed_apply.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.enter().await;
        }
        self.inner.apply_to_state_machine(entries).await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ObservedBuilder(self.inner.get_snapshot_builder().await)
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        self.inner.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, ()>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        self.probe.record("install", meta.last_log_id);
        self.inner.install_snapshot(meta, snapshot).await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<u64>> {
        let gate = self.probe.armed_snapshot_read.lock().unwrap().take();
        if let Some(gate) = gate {
            let snapshot = self.inner.get_current_snapshot().await?;
            assert!(
                snapshot.is_some(),
                "HELD_SM_GET_SNAPSHOT_SETUP: no actual snapshot to retain"
            );
            let _owner = SnapshotReadOwner(gate.clone());
            gate.entered.send_replace(true);
            let _ = gate.release.acquire().await;
            return Ok(snapshot.map(|snapshot| Snapshot {
                meta: snapshot.meta,
                snapshot: snapshot.snapshot,
            }));
        }
        if self.probe.fail_snapshot.swap(false, Ordering::SeqCst) {
            return Err(StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::other(SNAPSHOT_FAULT),
            ));
        }
        Ok(self.inner.get_current_snapshot().await?.map(|snapshot| Snapshot {
            meta: snapshot.meta,
            snapshot: snapshot.snapshot,
        }))
    }
}

#[derive(Clone)]
struct ObservedNetwork {
    router: RaftRouter,
    probe: Arc<Probe>,
}

struct ObservedConnection {
    inner: RaftRouterNetwork,
    target: u64,
    probe: Arc<Probe>,
}

impl<C: FixtureConfig> RaftNetworkFactory<C> for ObservedNetwork {
    type Network = ObservedConnection;

    async fn new_client(&mut self, target: u64, node: &()) -> Self::Network {
        *self.probe.next_target.lock().unwrap() = Some(target);
        ObservedConnection {
            inner: self.router.new_client(target, node).await,
            target,
            probe: self.probe.clone(),
        }
    }
}

impl<C: FixtureConfig> RaftNetwork<C> for ObservedConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, (), RaftError<u64>>> {
        self.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        let panic = {
            let mut armed = self.probe.panic_rpc.lock().unwrap();
            if *armed == Some(self.target) {
                armed.take();
                true
            } else {
                false
            }
        };
        assert!(!panic, "synthetic replication heartbeat panic");
        self.inner
            .append_entries(
                AppendEntriesRequest {
                    vote: rpc.vote,
                    prev_log_id: rpc.prev_log_id,
                    entries: rpc.entries,
                    leader_commit: rpc.leader_commit,
                },
                option,
            )
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, (), RaftError<u64, InstallSnapshotError>>> {
        self.inner
            .install_snapshot(
                InstallSnapshotRequest {
                    vote: rpc.vote,
                    meta: rpc.meta,
                    offset: rpc.offset,
                    data: rpc.data,
                    done: rpc.done,
                },
                option,
            )
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, (), RaftError<u64>>> {
        self.inner.vote(rpc, option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<C>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<C, Fatal<u64>>> {
        let snapshot = Snapshot::<TypeConfig> {
            meta: snapshot.meta,
            snapshot: snapshot.snapshot,
        };
        let gate = {
            let mut armed = self.probe.armed_snapshot.lock().unwrap();
            if armed.as_ref().map(|gate| gate.target) == Some(self.target) {
                armed.take()
            } else {
                None
            }
        };
        let Some(gate) = gate else {
            return self.inner.full_snapshot(vote, snapshot, cancel, option).await.map_err(map_streaming_error);
        };

        let tail = gate.tail.lock().unwrap().clone();
        if let Some(tail) = tail {
            TASK_TAIL.with(|slot| {
                assert!(slot.lock().unwrap().replace(tail).is_none());
            });
            gate.entered.send_replace(true);
            let result = self.inner.full_snapshot(vote, snapshot, cancel, option).await;
            gate.dropped.send_replace(true);
            return result.map_err(map_streaming_error);
        }

        let owned = OwnedSnapshot {
            snapshot: Some(snapshot),
            gate: gate.clone(),
        };
        gate.entered.send_replace(true);
        if gate.panic_on_release.load(Ordering::SeqCst) {
            tokio::select! {
                _ = gate.release.acquire() => {}
                _ = cancel => {
                    gate.cancelled.send_replace(true);
                    let _ = gate.release.acquire().await;
                }
            }
            panic!("synthetic snapshot child panic");
        }
        let cancelled = cancel.await;
        gate.cancelled.send_replace(true);
        let _ = gate.release.acquire().await;
        // Retain and use the actual supplied data after observing cancellation.
        assert!(!owned.snapshot.as_ref().unwrap().snapshot.get_ref().is_empty());
        drop(owned);
        if gate.storage_error_on_release.load(Ordering::SeqCst) {
            // Model a real local snapshot-data seek/read that was pending when
            // cancellation was requested, then completed with an I/O failure.
            let error = StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::other(STREAMED_SNAPSHOT_FAULT),
            );
            gate.returned_storage_error.send_replace(Some(error.clone()));
            return Err(StreamingError::StorageError(error));
        }
        match *gate.nonfatal_on_release.lock().unwrap() {
            Some(NonfatalSnapshotError::Network) => {
                return Err(StreamingError::Network(NetworkError::new(&std::io::Error::other(
                    "synthetic retired snapshot network failure",
                ))));
            }
            Some(NonfatalSnapshotError::RemoteStorage) => {
                return Err(StreamingError::RemoteError(RemoteError::new(
                    self.target,
                    Fatal::StorageError(StorageError::from_io_error(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Read,
                        std::io::Error::other("synthetic remote snapshot storage failure"),
                    )),
                )));
            }
            None => {}
        }
        Err(StreamingError::Closed(cancelled))
    }
}

fn map_streaming_error<C: FixtureConfig>(
    error: StreamingError<TypeConfig, Fatal<u64>>,
) -> StreamingError<C, Fatal<u64>> {
    match error {
        StreamingError::Closed(error) => StreamingError::Closed(error),
        StreamingError::StorageError(error) => StreamingError::StorageError(error),
        StreamingError::Timeout(error) => StreamingError::Timeout(error),
        StreamingError::Unreachable(error) => StreamingError::Unreachable(error),
        StreamingError::Network(error) => StreamingError::Network(error),
        StreamingError::RemoteError(error) => StreamingError::RemoteError(error),
    }
}

struct Fixture {
    raft: Raft<TypeConfig>,
    router: RaftRouter,
    store: Arc<MemStore>,
    probe: Arc<Probe>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Also runs on assertions and `?`, so negative controls never strand a gate.
        self.probe.release_all();
    }
}

impl Fixture {
    async fn new() -> Result<Self> {
        Self::with_apply_bound(None).await
    }

    async fn with_apply_bound(max_apply_entries: Option<NonZeroU64>) -> Result<Self> {
        let config = Arc::new(
            Config {
                enable_tick: false,
                enable_heartbeat: false,
                heartbeat_interval: RPC_TIMEOUT,
                election_timeout_min: RPC_TIMEOUT * 2,
                election_timeout_max: RPC_TIMEOUT * 2 + 1_000,
                snapshot_policy: SnapshotPolicy::Never,
                max_apply_entries,
                ..Default::default()
            }
            .validate()?,
        );
        let mut router = RaftRouter::new(config.clone());
        for id in 1..=3 {
            router.new_raft_node(id).await;
        }
        let store = Arc::new(MemStore::new());
        store.enable_saving_committed.store(true, Ordering::Release);
        let probe = Probe::new();
        let observed = ObservedStore {
            inner: store.clone(),
            probe: probe.clone(),
        };
        // No adapter lock is held by a GenerationReader. Separate state/log adapters
        // also ensure storage instrumentation cannot serialize unrelated work.
        let (log, _) = Adaptor::new(observed.clone());
        let (_, state) = Adaptor::new(observed);
        let network = ObservedNetwork {
            router: router.clone(),
            probe: probe.clone(),
        };
        let raft = Raft::new(0, config, network, log, state).await?;
        let fixture = Self {
            raft,
            router,
            store,
            probe,
        };
        within(fixture.raft.initialize(btreeset! {0}), "initialize single voter").await??;
        within(
            fixture.raft.wait(None).current_leader(0, "single voter elected"),
            "elect single voter",
        )
        .await??;
        fixture.write(0).await?;
        Ok(fixture)
    }

    async fn write(&self, serial: u64) -> Result<LogId<u64>> {
        let response = within(
            self.raft.client_write(ClientRequest::make_request("retirement", serial)),
            "RETIRED_REBUILD_PROGRESS: committed write blocked behind a retired generation",
        )
        .await??;
        Ok(response.log_id)
    }

    async fn add(&self, id: u64) -> Result<LogId<u64>> {
        let response = within(
            self.raft.add_learner(id, (), false),
            "RETIRED_REBUILD_PROGRESS: membership rebuild joined a gated old generation inline",
        )
        .await??;
        Ok(response.log_id)
    }

    async fn gated_add(&self, id: u64) -> Result<Arc<ReadGate>> {
        let gate = self.probe.arm_reader(1);
        self.add(id).await?;
        let observation = gate.entered().await?;
        anyhow::ensure!(observation.target == 1 && observation.start < observation.end);
        Ok(gate)
    }

    async fn retired_reader(&self) -> Result<Arc<ReadGate>> {
        let old = self.gated_add(1).await?;
        let latest = self.add(2).await?;
        self.caught_up(&[1, 2], latest).await?;
        anyhow::ensure!(!*old.dropped.borrow(), "old reader was not held across rebuild");
        Ok(old)
    }

    async fn caught_up(&self, ids: &[u64], upto: LogId<u64>) -> Result<()> {
        within(
            async {
                for id in ids {
                    self.router
                        .wait(id, None)
                        .applied_index(Some(upto.index), "replacement generation caught up")
                        .await?;
                }
                self.raft
                    .wait(None)
                    .metrics(
                        |metrics| {
                            ids.iter().all(|id| {
                                metrics
                                    .replication
                                    .as_ref()
                                    .and_then(|replication| replication.get(id))
                                    .copied()
                                    .flatten()
                                    >= Some(upto)
                            })
                        },
                        "leader observed replacement progress",
                    )
                    .await?;
                Ok::<(), anyhow::Error>(())
            },
            "replacement generations failed to replicate while old reader was held",
        )
        .await??;
        Ok(())
    }

    async fn snapshot(&self) -> Result<Snapshot<TypeConfig>> {
        let upto = self.store.get_state_machine().await.last_applied_log.unwrap();
        within(self.raft.trigger().snapshot(), "request snapshot").await??;
        within(
            self.raft.wait(None).snapshot(upto, "snapshot built"),
            "snapshot building waited for retired reader",
        )
        .await??;
        Ok(within(self.raft.get_snapshot(), "get built snapshot").await??.unwrap())
    }

    async fn future_snapshot(&self) -> Result<(Vote<u64>, Snapshot<TypeConfig>)> {
        let current = self.snapshot().await?;
        let mut source = Arc::new(MemStore::new());
        source.install_snapshot(&current.meta, current.snapshot).await?;
        let vote = Vote::new_committed(self.raft.metrics().borrow().current_term + 1, 9);
        let last = LogId::new(
            CommittedLeaderId::new(vote.leader_id().term, 9),
            current.meta.last_log_id.next_index(),
        );
        source
            .apply_to_state_machine(&[Entry {
                log_id: last,
                payload: EntryPayload::Normal(ClientRequest::make_request("incoming-snapshot", 1)),
            }])
            .await?;
        let snapshot = source.get_snapshot_builder().await.build_snapshot().await?;
        Ok((vote, snapshot))
    }

    async fn stop(&self) -> Result<()> {
        self.probe.release_all();
        let leader = within(self.raft.shutdown(), "cleanup leader").await;
        let mut followers = Ok(());
        for id in 1..=3 {
            let result = within(self.router.get_raft_handle(&id)?.shutdown(), "cleanup follower")
                .await
                .and_then(|result| result.map_err(Into::into));
            if let Err(error) = result {
                followers = Err(error);
            }
        }
        leader??;
        followers
    }

    async fn assert_quiet(&self) -> Result<()> {
        anyhow::ensure!(
            self.probe.live_readers.lock().unwrap().is_empty(),
            "RETIRED_SHUTDOWN_OWNERSHIP: replication reader survived core shutdown"
        );
        let calls = self.probe.io_calls.load(Ordering::SeqCst);
        tokio::time::sleep(HELD).await;
        anyhow::ensure!(
            self.probe.io_calls.load(Ordering::SeqCst) == calls,
            "RETIRED_SHUTDOWN_OWNERSHIP: replication I/O resumed after shutdown"
        );
        Ok(())
    }
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_reader_defers_purge_without_blocking_progress_and_completes_when_idle() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        let upto = fixture.write(1).await?;
        fixture.caught_up(&[1, 2], upto).await?;
        let snapshot = fixture.snapshot().await?;
        let purge = snapshot.meta.last_log_id.unwrap();
        let held = old.entered().await?;
        anyhow::ensure!(held.start <= purge.index && held.end <= purge.index + 1);
        within(fixture.raft.trigger().purge_log(purge.index), "request purge").await??;
        within(
            fixture.raft.with_raft_state(|_| ()),
            "RETIRED_PURGE_PROGRESS: pending purge blocked the core",
        )
        .await??;
        anyhow::ensure!(
            fixture.probe.operations().is_empty(),
            "RETIRED_PURGE_OWNERSHIP: purge crossed live old reader"
        );
        anyhow::ensure!(fixture.store.clone().get_log_state().await?.last_purged_log_id.is_none());

        within(
            async {
                for serial in 2..=9 {
                    fixture.write(serial).await?;
                }
                let last = fixture.add(3).await?;
                fixture.caught_up(&[1, 2, 3], last).await?;
                fixture.raft.with_raft_state(|_| ()).await?;
                Ok::<(), anyhow::Error>(())
            },
            "RETIRED_PURGE_PROGRESS: independent writes or new learner stalled behind purge",
        )
        .await??;
        anyhow::ensure!(
            fixture.probe.operations().is_empty(),
            "RETIRED_PURGE_OWNERSHIP: purge crossed live old reader"
        );
        anyhow::ensure!(fixture.raft.metrics().borrow().purged.is_none());

        old.release();
        // Ticks are disabled. From this point only observe storage/metrics; send no
        // unrelated API request to make the deferred purge run.
        observed(&old.dropped, "old reader did not finish after release").await?;
        within(
            fixture.raft.wait(None).purged(Some(purge), "idle retirement released purge"),
            "RETIRED_PURGE_IDLE: completion needed another message to run deferred purge",
        )
        .await??;
        let log = fixture.store.clone().get_log_state().await?;
        anyhow::ensure!(log.last_purged_log_id == Some(purge));
        let operations = fixture.probe.operations();
        anyhow::ensure!(
            operations.len() == 1 && operations[0].operation == "purge" && operations[0].log_id == Some(purge)
        );
        fixture.probe.assert_no_overlap()?;
        let suffix = fixture.store.clone().try_get_log_entries(purge.index + 1..).await?;
        anyhow::ensure!(suffix.first().map(|entry| entry.log_id.index) == Some(purge.index + 1));
        anyhow::ensure!(suffix.last().map(|entry| entry.log_id) == log.last_log_id);
        Ok(())
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

async fn purge_with_apply(release_reader_first: bool) -> Result<()> {
    let fixture = Fixture::with_apply_bound(NonZeroU64::new(64)).await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        let snapshot = fixture.snapshot().await?;
        let purge = snapshot.meta.last_log_id.unwrap();
        within(
            fixture.raft.trigger().purge_log(purge.index),
            "request purge before bounded apply",
        )
        .await??;
        within(
            fixture.raft.with_raft_state(|_| ()),
            "defer retired purge before bounded apply",
        )
        .await??;
        let apply_gate = fixture.probe.arm_apply();
        let mut applying =
            std::pin::pin!(fixture.raft.client_write(ClientRequest::make_request("bounded-retirement", 1)));
        anyhow::ensure!(futures::poll!(&mut applying).is_pending());
        observed(
            &apply_gate.entered,
            "bounded apply page did not start behind retired purge",
        )
        .await?;
        let first = fixture.store.clone().get_log_state().await?.last_log_id.unwrap();

        if release_reader_first {
            old.release();
            observed(&old.dropped, "old reader did not release before the apply page").await?;
            within(
                fixture.raft.with_raft_state(|_| ()),
                "core after retired reader completion",
            )
            .await??;
        }

        let append_gate = fixture.probe.arm_append();
        let mut successor =
            std::pin::pin!(fixture.raft.client_write(ClientRequest::make_request("bounded-retirement", 2)));
        anyhow::ensure!(futures::poll!(&mut successor).is_pending());
        observed(
            &append_gate.entered,
            "RETIRED_PURGE_APPLY_PROGRESS: pending apply made retired purge block independent durable append",
        )
        .await?;
        let vote = fixture.raft.metrics().borrow().vote;
        // A stale heartbeat is independent of the leader's gated apply page and
        // cannot force stepdown (which intentionally joins retired generations).
        let mut heartbeat = std::pin::pin!(fixture.raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(0, 9),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        }));
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut heartbeat).await.is_err(),
            "RETIRED_PURGE_DURABILITY: response bypassed an unperformed append"
        );
        anyhow::ensure!(fixture.store.clone().get_log_state().await?.last_log_id == Some(first));
        append_gate.release.close();
        let response = within(
            &mut heartbeat,
            "RETIRED_PURGE_APPLY_PROGRESS: pending apply made retired purge block independent heartbeat",
        )
        .await??;
        anyhow::ensure!(matches!(response, AppendEntriesResponse::HigherVote(actual) if actual == vote));
        let last = fixture.store.clone().get_log_state().await?.last_log_id.unwrap();
        anyhow::ensure!(last.index == first.index + 1);
        anyhow::ensure!(fixture.store.get_state_machine().await.last_applied_log == Some(purge));
        anyhow::ensure!(
            fixture.probe.operations().is_empty(),
            "RETIRED_PURGE_OWNERSHIP: physical purge crossed held work"
        );
        anyhow::ensure!(futures::poll!(&mut applying).is_pending() && futures::poll!(&mut successor).is_pending());
        within(
            fixture.raft.with_raft_state(|_| ()),
            "core responsiveness while apply and old reader are held",
        )
        .await??;

        apply_gate.release.close();
        anyhow::ensure!(within(&mut applying, "first bounded client completion").await??.log_id == first);
        anyhow::ensure!(within(&mut successor, "successor bounded client completion").await??.log_id == last);
        if !release_reader_first {
            anyhow::ensure!(
                fixture.probe.operations().is_empty(),
                "RETIRED_PURGE_OWNERSHIP: apply completion released old reader protection"
            );
        }
        old.release();
        within(
            fixture.raft.wait(None).purged(Some(purge), "retired purge finished"),
            "retired purge after both owners released",
        )
        .await??;
        fixture.probe.assert_no_overlap()
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_purge_does_not_block_durable_append_or_heartbeat_when_bounded_apply_starts() -> Result<()> {
    purge_with_apply(false).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_reader_finishing_before_apply_keeps_persistence_and_heartbeat_live() -> Result<()> {
    purge_with_apply(true).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn increasing_retired_purges_finish_at_latest_target_and_preserve_exact_suffix() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        let mut targets = Vec::new();
        for serial in 1..=3 {
            let last = fixture.write(serial).await?;
            fixture.caught_up(&[1, 2], last).await?;
            let target = fixture.snapshot().await?.meta.last_log_id.unwrap();
            within(fixture.raft.trigger().purge_log(target.index), "request increasing purge").await??;
            within(fixture.raft.with_raft_state(|_| ()), "schedule increasing purge").await??;
            anyhow::ensure!(fixture.probe.operations().is_empty(), "RETIRED_PURGE_TARGETS: a pending target crossed old reader");
            targets.push(target);
        }
        anyhow::ensure!(targets.windows(2).all(|pair| pair[0] < pair[1]));
        let latest = *targets.last().unwrap();
        let mut suffix_ids = Vec::new();
        for serial in 4..=6 {
            suffix_ids.push(fixture.write(serial).await?);
        }
        fixture.caught_up(&[1, 2], *suffix_ids.last().unwrap()).await?;
        anyhow::ensure!(fixture.store.clone().get_log_state().await?.last_purged_log_id.is_none());
        old.release();
        within(fixture.raft.wait(None).purged(Some(latest), "latest queued target purged"), "RETIRED_PURGE_TARGETS: latest pending target was lost").await??;
        let operations = fixture.probe.operations();
        anyhow::ensure!(!operations.is_empty() && operations.last().unwrap().log_id == Some(latest));
        anyhow::ensure!(operations.iter().all(|operation| operation.operation == "purge" && targets.contains(&operation.log_id.unwrap())));
        anyhow::ensure!(operations.windows(2).all(|pair| pair[0].log_id < pair[1].log_id));
        fixture.probe.assert_no_overlap()?;
        let suffix = fixture.store.clone().try_get_log_entries(0..).await?;
        anyhow::ensure!(suffix.len() == suffix_ids.len());
        for (offset, (entry, id)) in suffix.iter().zip(&suffix_ids).enumerate() {
            let serial = offset as u64 + 4;
            anyhow::ensure!(entry.log_id == *id && entry.log_id.index == latest.index + offset as u64 + 1);
            anyhow::ensure!(matches!(&entry.payload, EntryPayload::Normal(request)
                if request.client == "retirement" && request.serial == serial && request.status == format!("request-{serial}")));
        }
        anyhow::ensure!(fixture.store.clone().get_log_state().await?.last_purged_log_id == Some(latest));
        Ok(())
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_reader_is_joined_before_higher_vote_truncates_uncommitted_suffix() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        let response = within(
            fixture.raft.change_membership(btreeset! {0, 1, 2}, false),
            "promote caught-up learners",
        )
        .await??;
        fixture.caught_up(&[1, 2], response.log_id).await?;
        let committed = fixture.write(1).await?;
        fixture.caught_up(&[1, 2], committed).await?;
        fixture.router.set_append_entries_quota(Some(0));
        let _pending = fixture.raft.client_write_ff(ClientRequest::make_request("uncommitted", 1)).await?;
        within(
            fixture.raft.wait(None).metrics(
                |metrics| metrics.last_log_index == Some(committed.index + 1),
                "suffix persisted",
            ),
            "create uncommitted suffix",
        )
        .await??;
        anyhow::ensure!(fixture.store.clone().read_committed().await? == Some(committed));
        let original = fixture.store.clone().try_get_log_entries(committed.index + 1..).await?;
        anyhow::ensure!(original.len() == 1);
        let term = fixture.raft.metrics().borrow().current_term + 1;
        let replacement = LogId::new(CommittedLeaderId::new(term, 1), committed.index + 1);
        let mut append = std::pin::pin!(fixture.raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(term, 1),
            prev_log_id: Some(committed),
            entries: vec![Entry {
                log_id: replacement,
                payload: EntryPayload::Blank,
            }],
            leader_commit: Some(replacement),
        }));
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut append).await.is_err(),
            "RETIRED_TRUNCATE_RESPONSE: stepdown returned before old generation joined"
        );
        anyhow::ensure!(
            fixture.probe.operations().is_empty(),
            "RETIRED_TRUNCATE_OWNERSHIP: truncation crossed old reader"
        );
        let retained = fixture.store.clone().try_get_log_entries(committed.index + 1..).await?;
        anyhow::ensure!(retained.len() == 1 && retained[0].log_id == original[0].log_id);
        anyhow::ensure!(matches!(&retained[0].payload, EntryPayload::Normal(request)
            if request.client == "uncommitted" && request.serial == 1 && request.status == "request-1"));
        old.release();
        let response = within(&mut append, "conflicting append after old reader release").await??;
        anyhow::ensure!(response.is_success());
        observed(&old.dropped, "retired reader was not dropped before conflict response").await?;
        within(
            fixture.raft.wait(None).applied_index(Some(replacement.index), "replacement applied"),
            "apply replacement",
        )
        .await??;
        let suffix = fixture.store.clone().try_get_log_entries(committed.index + 1..).await?;
        anyhow::ensure!(
            suffix.len() == 1 && suffix[0].log_id == replacement && matches!(suffix[0].payload, EntryPayload::Blank)
        );
        anyhow::ensure!(fixture.store.get_state_machine().await.last_applied_log == Some(replacement));
        anyhow::ensure!(fixture.raft.metrics().borrow().running_state.is_ok());
        let operations = fixture.probe.operations();
        anyhow::ensure!(
            operations.len() == 1
                && operations[0].operation == "truncate"
                && operations[0].log_id == Some(original[0].log_id)
        );
        fixture.probe.assert_no_overlap()
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

async fn install_after_release(fixture: &Fixture, release: impl FnOnce()) -> Result<()> {
    let (vote, snapshot) = fixture.future_snapshot().await?;
    let expected_meta = snapshot.meta.clone();
    let expected_data = snapshot.snapshot.get_ref().clone();
    let before = fixture.probe.operations().len();
    let mut install = std::pin::pin!(fixture.raft.install_full_snapshot(vote, snapshot));
    anyhow::ensure!(
        tokio::time::timeout(HELD, &mut install).await.is_err(),
        "RETIRED_INSTALL_RESPONSE: install returned before retained owner released"
    );
    anyhow::ensure!(
        fixture.probe.operations().len() == before,
        "RETIRED_INSTALL_OWNERSHIP: destructive install crossed retained owner"
    );
    release();
    let response = within(&mut install, "install after retained owner release").await??;
    anyhow::ensure!(response.vote == vote);
    let installed = fixture.store.clone().get_current_snapshot().await?.unwrap();
    anyhow::ensure!(installed.meta == expected_meta && *installed.snapshot.get_ref() == expected_data);
    anyhow::ensure!(fixture.store.get_state_machine().await.last_applied_log == expected_meta.last_log_id);
    anyhow::ensure!(fixture
        .probe
        .operations()
        .iter()
        .any(|operation| operation.operation == "install" && operation.log_id == expected_meta.last_log_id));
    fixture.probe.assert_no_overlap()
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_reader_is_joined_before_full_snapshot_installation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        install_after_release(&fixture, || old.release()).await?;
        observed(&old.dropped, "install did not join retired reader").await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

async fn shutdown_generations(fatal: bool) -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let first = fixture.gated_add(1).await?;
        let second = fixture.gated_add(2).await?;
        let current = fixture.gated_add(3).await?;
        let ids = [first.entered().await?, second.entered().await?, current.entered().await?];
        anyhow::ensure!(ids[0].generation < ids[1].generation && ids[1].generation < ids[2].generation);
        anyhow::ensure!(ids.iter().all(|entry| entry.target == 1));
        let mut write = std::pin::pin!(fixture.raft.client_write(ClientRequest::make_request("fatal", 1)));
        if fatal {
            fixture.probe.fail_append.store(true, Ordering::SeqCst);
            anyhow::ensure!(futures::poll!(&mut write).is_pending());
            observed(&fixture.probe.failed_append, "deterministic append failure was not reached").await?;
        }
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(tokio::time::timeout(HELD, &mut shutdown).await.is_err(), "RETIRED_SHUTDOWN_OWNERSHIP: shutdown completed with all three generations held");
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown);
        current.release();
        observed(&current.dropped, "shutdown did not close current generation").await?;
        anyhow::ensure!(tokio::time::timeout(HELD, &mut shutdown).await.is_err(), "RETIRED_SHUTDOWN_OWNERSHIP: only current generation was drained");
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown);
        second.release();
        observed(&second.dropped, "second retired generation did not finish").await?;
        anyhow::ensure!(tokio::time::timeout(HELD, &mut shutdown).await.is_err(), "RETIRED_SHUTDOWN_OWNERSHIP: earlier retired generation was discarded");
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown);
        first.release();
        within(&mut shutdown, "shutdown after every generation released").await??;
        anyhow::ensure!(*first.dropped.borrow() && *second.dropped.borrow() && *current.dropped.borrow());
        let metrics = fixture.raft.metrics().borrow().clone();
        anyhow::ensure!(metrics.state == ServerState::Shutdown);
        if fatal {
            let error = within(&mut write, "fatal client response").await?.expect_err("failed append must not succeed");
            anyhow::ensure!(matches!(error.fatal(), Some(Fatal::StorageError(_))) && error.to_string().contains(FAULT));
            anyhow::ensure!(matches!(metrics.running_state, Err(Fatal::StorageError(ref error)) if error.to_string().contains(FAULT)), "original fatal storage result was lost during cleanup");
        } else {
            anyhow::ensure!(matches!(metrics.running_state, Err(Fatal::Stopped)));
        }
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn shutdown_joins_current_and_every_retired_reader_generation() -> Result<()> {
    shutdown_generations(false).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn fatal_exit_joins_current_and_every_retired_reader_generation() -> Result<()> {
    shutdown_generations(true).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn snapshot_api_preserves_fatal_storage_error_while_retired_reader_cleanup_is_pending() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        fixture.probe.fail_snapshot.store(true, Ordering::SeqCst);
        // The dead-SM API's existing one-second error lookup must see the actual
        // fatal cause even when joining a retired task takes longer than that.
        let response = within(
            fixture.raft.get_snapshot(),
            "RETIRED_FATAL_IDENTITY: snapshot API waited for retirement instead of reporting its fatal cause",
        )
        .await?;
        let error = match response {
            Err(RaftError::Fatal(Fatal::StorageError(error))) => error,
            other => anyhow::bail!("RETIRED_FATAL_IDENTITY: snapshot API lost its storage error: {other:?}"),
        };
        anyhow::ensure!(error.to_string().contains(SNAPSHOT_FAULT));
        let metrics = fixture.raft.metrics().borrow().clone();
        anyhow::ensure!(metrics.running_state == Err(Fatal::StorageError(error.clone())));
        anyhow::ensure!(
            metrics.state != ServerState::Shutdown && !*old.dropped.borrow(),
            "RETIRED_FATAL_OWNERSHIP: fatal publication claimed completed shutdown before cleanup"
        );
        // A new API call must observe the same known failure while the actual
        // retained reader is still owned and shutdown has not completed.
        let next = within(
            fixture.raft.get_snapshot(),
            "POST_FATAL_API_COMPLETION: new snapshot API waited for retained cleanup after failure was published",
        )
        .await?;
        anyhow::ensure!(
            matches!(next, Err(RaftError::Fatal(Fatal::StorageError(ref next_error))) if next_error == &error),
            "POST_FATAL_API_COMPLETION: new snapshot API did not preserve the original failure: {next:?}"
        );

        let inspected = Arc::new(AtomicBool::new(false));
        let called = inspected.clone();
        let state = within(
            fixture.raft.with_raft_state(move |_| called.store(true, Ordering::SeqCst)),
            "POST_FATAL_STATE_COMPLETION: state inspection waited for retained cleanup",
        )
        .await?;
        anyhow::ensure!(
            state == Err(Fatal::StorageError(error.clone())) && !inspected.load(Ordering::SeqCst),
            "POST_FATAL_STATE_COMPLETION: state inspection lost the original failure or executed after failure: {state:?}"
        );
        let write = within(
            fixture.raft.client_write(ClientRequest::make_request("post-fatal", 1)),
            "POST_FATAL_WRITE_COMPLETION: client write waited for retained cleanup",
        )
        .await?;
        anyhow::ensure!(
            matches!(write, Err(RaftError::Fatal(Fatal::StorageError(ref write_error))) if write_error == &error),
            "POST_FATAL_WRITE_COMPLETION: client write lost the original failure: {write:?}"
        );
        let submit = within(
            fixture.raft.client_write_ff(ClientRequest::make_request("post-fatal", 2)),
            "POST_FATAL_FF_COMPLETION: fire-and-forget submission waited for retained cleanup",
        )
        .await?;
        // After a known fatal, submission itself must refuse. A previously
        // accepted custom responder still retains its application-defined contract.
        anyhow::ensure!(
            matches!(submit, Err(Fatal::StorageError(ref submit_error)) if submit_error == &error),
            "POST_FATAL_FF_COMPLETION: fire-and-forget submission accepted work or lost the original failure"
        );
        let trigger = within(
            fixture.raft.trigger().heartbeat(),
            "POST_FATAL_TRIGGER_COMPLETION: external command waited for retained cleanup",
        )
        .await?;
        anyhow::ensure!(
            trigger == Err(Fatal::StorageError(error.clone())),
            "POST_FATAL_TRIGGER_COMPLETION: external command accepted work or lost the original failure: {trigger:?}"
        );
        anyhow::ensure!(!*old.dropped.borrow());
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_FATAL_OWNERSHIP: error reporting detached the retained reader"
        );
        old.release();
        within(&mut shutdown, "fatal cleanup after reader release").await??;
        anyhow::ensure!(*old.dropped.borrow());
        let metrics = fixture.raft.metrics().borrow().clone();
        anyhow::ensure!(
            metrics.state == ServerState::Shutdown && metrics.running_state == Err(Fatal::StorageError(error))
        );
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn accepted_client_failure_completes_before_retired_reader_cleanup() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        fixture.probe.fail_append.store(true, Ordering::SeqCst);
        let mut write = std::pin::pin!(fixture.raft.client_write(ClientRequest::make_request("fatal-response", 1)));
        anyhow::ensure!(futures::poll!(&mut write).is_pending());
        observed(&fixture.probe.failed_append, "synthetic append failure was not reached").await?;
        // client_write_ff has already moved this responder out of rx_api and into
        // the core's indexed client responders before the append fails.
        let response = within(
            &mut write,
            "ACCEPTED_FATAL_API_COMPLETION: accepted client response waited for retained cleanup",
        )
        .await?;
        let error = match response {
            Err(RaftError::Fatal(Fatal::StorageError(error))) => error,
            other => anyhow::bail!("ACCEPTED_FATAL_API_COMPLETION: client lost the original failure: {other:?}"),
        };
        anyhow::ensure!(error.to_string().contains(FAULT));
        let metrics = fixture.raft.metrics().borrow().clone();
        anyhow::ensure!(metrics.running_state == Err(Fatal::StorageError(error.clone())));
        anyhow::ensure!(metrics.state != ServerState::Shutdown && !*old.dropped.borrow());
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "ACCEPTED_FATAL_OWNERSHIP: early API completion detached the retained reader"
        );
        old.release();
        within(&mut shutdown, "accepted-client fatal cleanup after reader release").await??;
        anyhow::ensure!(*old.dropped.borrow());
        let metrics = fixture.raft.metrics().borrow().clone();
        anyhow::ensure!(
            metrics.state == ServerState::Shutdown && metrics.running_state == Err(Fatal::StorageError(error))
        );
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

async fn active_snapshot(fixture: &Fixture, panic_child: bool) -> Result<Arc<SnapshotGate>> {
    let snapshot = fixture.snapshot().await?;
    let purge = snapshot.meta.last_log_id.unwrap();
    within(
        fixture.raft.trigger().purge_log(purge.index),
        "purge before snapshot replication",
    )
    .await??;
    within(
        fixture.raft.wait(None).purged(Some(purge), "empty-pool purge completed"),
        "purge with no retired tasks",
    )
    .await??;
    let gate = fixture.probe.arm_snapshot(1);
    gate.panic_on_release.store(panic_child, Ordering::SeqCst);
    fixture.add(1).await?;
    observed(&gate.entered, "outgoing snapshot child did not acquire snapshot data").await?;
    Ok(gate)
}

async fn retired_snapshot(fixture: &Fixture) -> Result<Arc<SnapshotGate>> {
    let gate = active_snapshot(fixture, false).await?;
    let last = fixture.add(2).await?;
    observed(&gate.cancelled, "retired snapshot parent did not cancel its child").await?;
    anyhow::ensure!(!*gate.dropped.borrow(), "snapshot data was not held after cancellation");
    fixture.caught_up(&[1, 2], last).await?;
    Ok(gate)
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn shutdown_waits_for_retired_snapshot_child_after_cancellation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let gate = retired_snapshot(&fixture).await?;
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_SNAPSHOT_JOIN: cancellation was mistaken for child completion at shutdown"
        );
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown && !*gate.dropped.borrow());
        gate.release.close();
        within(&mut shutdown, "shutdown after outgoing snapshot data released").await??;
        anyhow::ensure!(
            *gate.dropped.borrow(),
            "RETIRED_SNAPSHOT_JOIN: shutdown returned before snapshot data dropped"
        );
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn install_waits_for_retired_snapshot_child_after_cancellation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let gate = retired_snapshot(&fixture).await?;
        install_after_release(&fixture, || gate.release.close()).await?;
        anyhow::ensure!(
            *gate.dropped.borrow(),
            "RETIRED_SNAPSHOT_JOIN: install returned before snapshot data dropped"
        );
        Ok(())
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_snapshot_local_storage_error_survives_closed_callback_channel() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let child = retired_snapshot(&fixture).await?;
        // Parent retirement has closed the core-to-parent channel and the child
        // has observed cancellation, but it still owns the actual snapshot data.
        child.storage_error_on_release.store(true, Ordering::SeqCst);
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_SNAPSHOT_STORAGE_OWNERSHIP: shutdown detached the snapshot child"
        );
        anyhow::ensure!(!*child.dropped.borrow());
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown);
        child.release.close();
        within(&mut shutdown, "snapshot storage-failure cleanup").await??;
        anyhow::ensure!(*child.dropped.borrow());
        let metrics = fixture.raft.metrics().borrow().clone();
        anyhow::ensure!(metrics.state == ServerState::Shutdown);
        anyhow::ensure!(
            matches!(metrics.running_state, Err(Fatal::StorageError(ref error)) if error.to_string().contains(STREAMED_SNAPSHOT_FAULT)),
            "RETIRED_SNAPSHOT_STORAGE_CAUSE: closed callback channel discarded local storage failure: {:?}",
            metrics.running_state
        );
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

async fn fatal_seen<C: FixtureConfig>(raft: &Raft<C>, expected: &Fatal<u64>) -> Result<()> {
    let mut metrics = raft.metrics();
    within(
        metrics.wait_for(|value| value.running_state.as_ref().err() == Some(expected)),
        "RETIRED_PANIC_IDENTITY: expected fatal cause was not published",
    )
    .await??;
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn parent_panic_cancels_and_joins_its_live_snapshot_child() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let child = active_snapshot(&fixture, false).await?;
        *fixture.probe.panic_rpc.lock().unwrap() = Some(1);
        // A manual heartbeat is suppressed while a snapshot is in flight. A new
        // commit still informs this parent, which sends its committed-index heartbeat.
        let _response = within(
            fixture.raft.client_write_ff(ClientRequest::make_request("parent-panic", 1)),
            "commit while snapshot child is held",
        )
        .await??;
        fatal_seen(&fixture.raft, &Fatal::Panicked).await?;
        anyhow::ensure!(fixture.probe.panic_rpc.lock().unwrap().is_none());
        observed(&child.cancelled, "panicking parent failed to cancel snapshot child").await?;
        anyhow::ensure!(!*child.dropped.borrow());
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown);
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_PARENT_PANIC: cleanup detached live snapshot child"
        );
        child.release.close();
        within(&mut shutdown, "parent panic cleanup after child release").await??;
        anyhow::ensure!(*child.dropped.borrow());
        anyhow::ensure!(fixture.raft.metrics().borrow().running_state == Err(Fatal::Panicked));
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn active_snapshot_child_panic_notifies_core_without_membership_change() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let child = active_snapshot(&fixture, true).await?;
        // Keep this parent active. A retired join result could otherwise report
        // its child's panic even if the child's own notification were missing.
        child.release.close();
        let mut metrics = fixture.raft.metrics();
        within(
            metrics.wait_for(|value| value.running_state == Err(Fatal::Panicked)),
            "ACTIVE_CHILD_FATAL: a panicked active child left its parent and core running",
        )
        .await??;
        observed(&child.dropped, "panicked active child retained snapshot data").await?;
        within(fixture.raft.shutdown(), "active child panic cleanup").await??;
        anyhow::ensure!(fixture.raft.metrics().borrow().running_state == Err(Fatal::Panicked));
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn snapshot_child_panic_drains_current_and_retired_reader_generations() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let child = active_snapshot(&fixture, true).await?;
        let retired = fixture.gated_add(2).await?;
        observed(
            &child.cancelled,
            "retired parent did not cancel its soon-to-panic child",
        )
        .await?;
        let current = fixture.gated_add(3).await?;
        child.release.close();
        fatal_seen(&fixture.raft, &Fatal::Panicked).await?;
        observed(&child.dropped, "panicking snapshot child retained its data").await?;
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_CHILD_PANIC: cleanup returned with readers held"
        );
        current.release();
        observed(&current.dropped, "child panic cleanup did not join current generation").await?;
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_CHILD_PANIC: cleanup discarded retired reader"
        );
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown);
        retired.release();
        within(&mut shutdown, "child panic cleanup after all readers release").await??;
        anyhow::ensure!(*retired.dropped.borrow());
        anyhow::ensure!(fixture.raft.metrics().borrow().running_state == Err(Fatal::Panicked));
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn cleanup_reader_panic_does_not_replace_an_earlier_storage_error() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let retired = fixture.gated_add(1).await?;
        retired.panic_on_release.store(true, Ordering::SeqCst);
        let current = fixture.gated_add(2).await?;
        fixture.probe.fail_snapshot.store(true, Ordering::SeqCst);
        let error = match within(fixture.raft.get_snapshot(), "original storage failure").await? {
            Err(RaftError::Fatal(Fatal::StorageError(error))) => error,
            other => anyhow::bail!("RETIRED_PANIC_PRECEDENCE: expected actual storage failure: {other:?}"),
        };
        anyhow::ensure!(error.to_string().contains(SNAPSHOT_FAULT));
        retired.release();
        observed(&retired.dropped, "panicking retired reader was not released").await?;
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_PANIC_PRECEDENCE: cleanup panic discarded current reader"
        );
        anyhow::ensure!(fixture.raft.metrics().borrow().state != ServerState::Shutdown);
        current.release();
        within(&mut shutdown, "storage failure cleanup after secondary panic").await??;
        anyhow::ensure!(*current.dropped.borrow());
        anyhow::ensure!(
            fixture.raft.metrics().borrow().running_state == Err(Fatal::StorageError(error)),
            "RETIRED_PANIC_PRECEDENCE: cleanup panic overwrote the original storage error"
        );
        fixture.assert_quiet().await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

struct ReleaseProbe(Arc<Probe>);

impl Drop for ReleaseProbe {
    fn drop(&mut self) {
        self.0.release_all();
    }
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn snapshot_callback_does_not_release_child_ownership_before_join_completion() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            enable_heartbeat: false,
            snapshot_policy: SnapshotPolicy::Never,
            heartbeat_interval: RPC_TIMEOUT,
            election_timeout_min: RPC_TIMEOUT * 2,
            election_timeout_max: RPC_TIMEOUT * 2 + 1_000,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    router.new_raft_node(1).await;
    let store = Arc::new(MemStore::new());
    let probe = Probe::new();
    let _release = ReleaseProbe(probe.clone());
    let observed_store = ObservedStore {
        inner: store.clone(),
        probe: probe.clone(),
    };
    let (log, _) = Adaptor::new(observed_store.clone());
    let (_, state) = Adaptor::new(observed_store);
    let raft = Raft::<CallbackConfig>::new(
        0,
        config,
        ObservedNetwork {
            router: router.clone(),
            probe: probe.clone(),
        },
        log,
        state,
    )
    .await?;
    let result = async {
        within(raft.initialize(btreeset! {0}), "initialize custom-runtime leader").await??;
        within(
            raft.wait(None).current_leader(0, "custom-runtime leader elected"),
            "elect custom-runtime leader",
        )
        .await??;
        let last = within(
            raft.client_write(ClientRequest::make_request("callback-tail", 1)),
            "initial callback fixture write",
        )
        .await??
        .log_id;
        within(raft.trigger().snapshot(), "build outgoing callback snapshot").await??;
        within(
            raft.wait(None).snapshot(last, "callback snapshot built"),
            "wait callback snapshot",
        )
        .await??;
        within(raft.trigger().purge_log(last.index), "purge callback fixture prefix").await??;
        within(
            raft.wait(None).purged(Some(last), "callback prefix purged"),
            "wait callback purge",
        )
        .await??;

        let gate = probe.arm_snapshot(1);
        let tail = WorkGate::new();
        probe.work_gates.lock().unwrap().push(tail.clone());
        *gate.tail.lock().unwrap() = Some(tail.clone());
        within(raft.add_learner(1, (), false), "add snapshot recipient").await??;
        observed(
            &tail.entered,
            "production snapshot task did not return after sending its callback",
        )
        .await?;
        // The remote really installed the snapshot, and the production child sent
        // its callback. Only the runtime tail remains before JoinHandle completion.
        router.wait(&1, Some(PROGRESS)).snapshot(last, "recipient installed callback snapshot").await?;
        anyhow::ensure!(*gate.dropped.borrow());
        anyhow::ensure!(
            tokio::time::timeout(
                HELD,
                raft.wait(None).metrics(
                    |metrics| metrics
                        .replication
                        .as_ref()
                        .and_then(|replication| replication.get(&1))
                        .copied()
                        .flatten()
                        >= Some(last),
                    "callback progress must await child join",
                )
            )
            .await
            .is_err(),
            "RETIRED_CALLBACK_JOIN: callback advanced progress before the child task completed"
        );
        let mut shutdown = std::pin::pin!(raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "RETIRED_CALLBACK_JOIN: callback consumption discarded the unjoined child handle"
        );
        anyhow::ensure!(raft.metrics().borrow().state != ServerState::Shutdown);
        tail.release.close();
        within(&mut shutdown, "shutdown after callback tail joins").await??;
        anyhow::ensure!(probe.live_readers.lock().unwrap().is_empty());
        anyhow::ensure!(raft.metrics().borrow().running_state == Err(Fatal::Stopped));
        Ok(())
    }
    .await;
    probe.release_all();
    let leader = within(raft.shutdown(), "cleanup callback leader").await;
    let follower = within(router.get_raft_handle(&1)?.shutdown(), "cleanup callback follower").await;
    result?;
    leader??;
    follower??;
    Ok(())
}

async fn finish_reader_fatal(fixture: &Fixture, old: &ReadGate, error: &StorageError<u64>, marker: &str) -> Result<()> {
    let metrics = fixture.raft.metrics().borrow().clone();
    anyhow::ensure!(
        metrics.state != ServerState::Shutdown
            && metrics.running_state == Err(Fatal::StorageError(error.clone()))
            && !*old.dropped.borrow(),
        "{marker}: API completion lost the original fatal or claimed joined cleanup"
    );
    let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
    anyhow::ensure!(
        tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
        "{marker}: shutdown detached the retained reader"
    );
    old.release();
    within(&mut shutdown, marker).await??;
    anyhow::ensure!(*old.dropped.borrow() && fixture.probe.live_readers.lock().unwrap().is_empty());
    let metrics = fixture.raft.metrics().borrow().clone();
    anyhow::ensure!(
        metrics.state == ServerState::Shutdown && metrics.running_state == Err(Fatal::StorageError(error.clone())),
        "{marker}: joined cleanup replaced the original fatal"
    );
    Ok(())
}

async fn reported_storage_failure(fixture: &Fixture, marker: &str) -> Result<StorageError<u64>> {
    let mut metrics = fixture.raft.metrics();
    let failure = within(metrics.wait_for(|value| value.running_state.is_err()), marker).await??.running_state.clone();
    match failure {
        Err(Fatal::StorageError(error)) => Ok(error),
        other => anyhow::bail!("{marker}: expected typed storage failure, received {other:?}"),
    }
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn queued_calls_complete_when_a_held_append_fails_before_retirement_finishes() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        let persisted = fixture.store.clone().get_log_state().await?.last_log_id;
        let append_gate = fixture.probe.arm_append();
        fixture.probe.fail_append.store(true, Ordering::SeqCst);
        let mut write = std::pin::pin!(fixture.raft.client_write(ClientRequest::make_request("queued-fatal", 1)));
        anyhow::ensure!(futures::poll!(&mut write).is_pending());
        observed(&append_gate.entered, "QUEUED_FATAL_SETUP: actual append was not held").await?;
        anyhow::ensure!(fixture.raft.metrics().borrow().running_state.is_ok());

        // Poll through each public send while the core is still inside append.
        // The pending receivers are already enqueued, before any fatal is visible.
        let mut snapshot = std::pin::pin!(fixture.raft.get_snapshot());
        anyhow::ensure!(futures::poll!(&mut snapshot).is_pending());
        let inspected = Arc::new(AtomicBool::new(false));
        let called = inspected.clone();
        let mut state = std::pin::pin!(fixture.raft.with_raft_state(move |_| called.store(true, Ordering::SeqCst)));
        anyhow::ensure!(futures::poll!(&mut state).is_pending());
        anyhow::ensure!(
            fixture.raft.metrics().borrow().running_state.is_ok()
                && !inspected.load(Ordering::SeqCst)
                && !*fixture.probe.failed_append.borrow(),
            "QUEUED_FATAL_SETUP: the second requests were not queued before failure"
        );
        append_gate.release.close();
        observed(
            &fixture.probe.failed_append,
            "QUEUED_FATAL_SETUP: held append did not actually fail",
        )
        .await?;
        let error = reported_storage_failure(&fixture, "QUEUED_FATAL_SETUP: append cause was not published").await?;
        anyhow::ensure!(error.to_string().contains(FAULT));
        let response = within(
            &mut snapshot,
            "QUEUED_FATAL_SNAPSHOT_COMPLETION: queued caller waited for retirement",
        )
        .await?;
        anyhow::ensure!(
            matches!(response, Err(RaftError::Fatal(Fatal::StorageError(ref actual))) if actual == &error),
            "QUEUED_FATAL_SNAPSHOT_COMPLETION: queued caller lost original append failure: {response:?}"
        );
        let response = within(
            &mut state,
            "QUEUED_FATAL_STATE_COMPLETION: queued state inspection waited for retirement",
        )
        .await?;
        anyhow::ensure!(
            response == Err(Fatal::StorageError(error.clone())) && !inspected.load(Ordering::SeqCst),
            "QUEUED_FATAL_STATE_COMPLETION: queued state inspection ran after failure or lost its cause: {response:?}"
        );
        let response = within(
            &mut write,
            "QUEUED_FATAL_WRITE_COMPLETION: indexed caller waited for retirement",
        )
        .await?;
        anyhow::ensure!(
            matches!(response, Err(RaftError::Fatal(Fatal::StorageError(ref actual))) if actual == &error),
            "QUEUED_FATAL_WRITE_COMPLETION: indexed caller lost original append failure: {response:?}"
        );
        anyhow::ensure!(fixture.store.clone().get_log_state().await?.last_log_id == persisted);
        finish_reader_fatal(&fixture, &old, &error, "QUEUED_FATAL_READER_OWNERSHIP").await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn deferred_append_response_reports_failed_persistence_before_retirement_finishes() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        let previous = fixture.store.clone().get_log_state().await?.last_log_id.unwrap();
        let persisted_vote = fixture.store.clone().read_vote().await?;
        let vote = Vote::new_committed(fixture.raft.metrics().borrow().current_term + 1, 1);
        let vote_gate = WorkGate::new();
        fixture.probe.work_gates.lock().unwrap().push(vote_gate.clone());
        *fixture.probe.armed_vote_failure.lock().unwrap() = Some(vote_gate.clone());
        // This valid higher-term heartbeat queues SaveVote before QuitLeader
        // and Command::Respond. Failing actual vote persistence therefore leaves
        // the engine-owned response pending while fatal cleanup retains readers.
        let mut response = std::pin::pin!(fixture.raft.append_entries(AppendEntriesRequest {
            vote,
            prev_log_id: Some(previous),
            entries: vec![],
            leader_commit: Some(previous),
        }));
        anyhow::ensure!(futures::poll!(&mut response).is_pending());
        observed(
            &vote_gate.entered,
            "DEFERRED_RESPOND_SETUP: higher-term append did not reach vote persistence",
        )
        .await?;
        let persisted = fixture.store.clone().get_log_state().await?.last_log_id;
        anyhow::ensure!(
            futures::poll!(&mut response).is_pending()
                && !*old.dropped.borrow()
                && fixture.store.clone().read_vote().await? == persisted_vote
                && persisted == Some(previous),
            "DEFERRED_RESPOND_DURABILITY: success was exposed before persistence"
        );
        vote_gate.release.close();
        observed(
            &fixture.probe.failed_vote,
            "DEFERRED_RESPOND_SETUP: persistence did not actually fail",
        )
        .await?;
        let error = match within(
            &mut response,
            "DEFERRED_RESPOND_FATAL_COMPLETION: engine response waited for retirement",
        )
        .await?
        {
            Err(RaftError::Fatal(Fatal::StorageError(error))) => error,
            other => anyhow::bail!(
                "DEFERRED_RESPOND_FATAL_COMPLETION: engine returned success or lost original failure: {other:?}"
            ),
        };
        anyhow::ensure!(error.to_string().contains(VOTE_FAULT));
        anyhow::ensure!(fixture.store.clone().read_vote().await? == persisted_vote);
        anyhow::ensure!(fixture.store.clone().get_log_state().await?.last_log_id == Some(previous));
        anyhow::ensure!(fixture.store.get_state_machine().await.last_applied_log == Some(previous));
        anyhow::ensure!(fixture.probe.operations().is_empty());
        finish_reader_fatal(&fixture, &old, &error, "DEFERRED_RESPOND_READER_OWNERSHIP").await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn sm_owned_snapshot_call_observes_independent_fatal_without_cancelling_storage() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let old = fixture.retired_reader().await?;
        fixture.snapshot().await?;
        let read = fixture.probe.arm_snapshot_read();
        let mut snapshot = std::pin::pin!(fixture.raft.get_snapshot());
        anyhow::ensure!(futures::poll!(&mut snapshot).is_pending());
        observed(
            &read.entered,
            "SM_OWNED_FATAL_SETUP: actual snapshot read did not retain its result",
        )
        .await?;
        anyhow::ensure!(!*read.finished.borrow() && fixture.raft.metrics().borrow().running_state.is_ok());

        // The state and log adapters are independent. The SM worker retains this
        // snapshot responder and storage future while an actual log append fails.
        let append_gate = fixture.probe.arm_append();
        fixture.probe.fail_append.store(true, Ordering::SeqCst);
        let mut write = std::pin::pin!(fixture.raft.client_write(ClientRequest::make_request("sm-owned-fatal", 1)));
        anyhow::ensure!(futures::poll!(&mut write).is_pending());
        observed(
            &append_gate.entered,
            "SM_OWNED_FATAL_SETUP: independent append did not reach storage",
        )
        .await?;
        append_gate.release.close();
        observed(
            &fixture.probe.failed_append,
            "SM_OWNED_FATAL_SETUP: independent append did not fail",
        )
        .await?;
        let error =
            reported_storage_failure(&fixture, "SM_OWNED_FATAL_SETUP: independent fatal was not published").await?;
        anyhow::ensure!(*fixture.probe.failed_append.borrow() && error.to_string().contains(FAULT));
        let response = within(
            &mut snapshot,
            "SM_OWNED_FATAL_API_COMPLETION: SM-owned responder hid original fatal",
        )
        .await?;
        anyhow::ensure!(
            matches!(response, Err(RaftError::Fatal(Fatal::StorageError(ref actual))) if actual == &error),
            "SM_OWNED_FATAL_API_COMPLETION: SM-owned API lost the independent fatal: {response:?}"
        );
        anyhow::ensure!(
            !*read.finished.borrow() && !read.release.is_closed() && !*old.dropped.borrow(),
            "SM_OWNED_FATAL_STORAGE_OWNERSHIP: returning the fatal cancelled actual storage or retired reader"
        );
        let response = within(
            &mut write,
            "SM_OWNED_FATAL_WRITE_COMPLETION: append caller waited for retained storage",
        )
        .await?;
        anyhow::ensure!(
            matches!(response, Err(RaftError::Fatal(Fatal::StorageError(ref actual))) if actual == &error),
            "SM_OWNED_FATAL_WRITE_COMPLETION: append caller lost original failure: {response:?}"
        );
        read.release.close();
        observed(
            &read.finished,
            "SM_OWNED_FATAL_STORAGE_RELEASE: actual storage did not finish after release",
        )
        .await?;
        finish_reader_fatal(&fixture, &old, &error, "SM_OWNED_FATAL_READER_OWNERSHIP").await
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

async fn returned_snapshot_storage_error(child: &SnapshotGate, marker: &str) -> Result<StorageError<u64>> {
    let mut observed = child.returned_storage_error.subscribe();
    let value = within(observed.wait_for(Option::is_some), marker).await??;
    Ok(value.as_ref().unwrap().clone())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn idle_core_stops_after_retired_snapshot_child_returns_local_storage_error() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let child = retired_snapshot(&fixture).await?;
        child.storage_error_on_release.store(true, Ordering::SeqCst);
        anyhow::ensure!(
            *child.cancelled.borrow()
                && !*child.dropped.borrow()
                && fixture.raft.metrics().borrow().running_state.is_ok()
        );
        child.release.close();
        let error =
            returned_snapshot_storage_error(&child, "IDLE_SNAPSHOT_STORAGE_SETUP: child did not return local error")
                .await?;
        anyhow::ensure!(error.to_string().contains(STREAMED_SNAPSHOT_FAULT));
        // Observe only: no shutdown, write, trigger or core callback is sent to
        // make the idle retirement/error path run.
        let mut metrics = fixture.raft.metrics();
        let stopped = within(
            metrics.wait_for(|value| value.state == ServerState::Shutdown),
            "IDLE_SNAPSHOT_STORAGE_FATAL: local error did not stop the otherwise live core",
        )
        .await??
        .clone();
        anyhow::ensure!(
            stopped.running_state == Err(Fatal::StorageError(error)),
            "IDLE_SNAPSHOT_STORAGE_CAUSE: autonomous termination lost the exact child storage error: {:?}",
            stopped.running_state
        );
        anyhow::ensure!(*child.dropped.borrow() && fixture.probe.live_readers.lock().unwrap().is_empty());
        Ok(())
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn earlier_append_fatal_survives_retired_snapshot_local_storage_error() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let child = retired_snapshot(&fixture).await?;
        child.storage_error_on_release.store(true, Ordering::SeqCst);
        fixture.probe.fail_append.store(true, Ordering::SeqCst);
        let error = match within(
            fixture.raft.client_write(ClientRequest::make_request("primary-fatal", 1)),
            "PRIMARY_FATAL_SNAPSHOT_COMPLETION: original append failure waited for snapshot cleanup",
        )
        .await?
        {
            Err(RaftError::Fatal(Fatal::StorageError(error))) => error,
            other => anyhow::bail!("PRIMARY_FATAL_SNAPSHOT_COMPLETION: original append failure was lost: {other:?}"),
        };
        anyhow::ensure!(*fixture.probe.failed_append.borrow() && error.to_string().contains(FAULT));
        anyhow::ensure!(
            fixture.raft.metrics().borrow().running_state == Err(Fatal::StorageError(error.clone()))
                && !*child.dropped.borrow()
        );
        let mut shutdown = std::pin::pin!(fixture.raft.shutdown());
        anyhow::ensure!(
            tokio::time::timeout(HELD, &mut shutdown).await.is_err(),
            "PRIMARY_FATAL_SNAPSHOT_OWNERSHIP: original fatal detached the child"
        );
        child.release.close();
        let secondary =
            returned_snapshot_storage_error(&child, "PRIMARY_FATAL_SNAPSHOT_SETUP: child error was not returned")
                .await?;
        anyhow::ensure!(secondary != error && secondary.to_string().contains(STREAMED_SNAPSHOT_FAULT));
        within(
            &mut shutdown,
            "PRIMARY_FATAL_SNAPSHOT_JOIN: cleanup failed after child error",
        )
        .await??;
        let metrics = fixture.raft.metrics().borrow().clone();
        anyhow::ensure!(
            metrics.state == ServerState::Shutdown && metrics.running_state == Err(Fatal::StorageError(error)),
            "PRIMARY_FATAL_SNAPSHOT_PRECEDENCE: secondary child storage failure replaced original append failure"
        );
        anyhow::ensure!(*child.dropped.borrow() && fixture.probe.live_readers.lock().unwrap().is_empty());
        Ok(())
    }
    .await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn retired_snapshot_cancellation_network_and_remote_errors_remain_nonfatal() -> Result<()> {
    for outcome in [
        None,
        Some(NonfatalSnapshotError::Network),
        Some(NonfatalSnapshotError::RemoteStorage),
    ] {
        let fixture = Fixture::new().await?;
        let result = async {
            let child = retired_snapshot(&fixture).await?;
            *child.nonfatal_on_release.lock().unwrap() = outcome;
            // A physical purge cannot cross the retired child. Its completion is
            // a public storage barrier proving the retirement pool joined this outcome.
            let target = fixture.snapshot().await?.meta.last_log_id.unwrap();
            within(
                fixture.raft.trigger().purge_log(target.index),
                "NONFATAL_SNAPSHOT_SETUP: schedule join barrier",
            )
            .await??;
            within(
                fixture.raft.with_raft_state(|_| ()),
                "NONFATAL_SNAPSHOT_SETUP: accept join barrier",
            )
            .await??;
            anyhow::ensure!(fixture.store.clone().get_log_state().await?.last_purged_log_id < Some(target));
            anyhow::ensure!(*child.cancelled.borrow() && !*child.dropped.borrow());
            child.release.close();
            within(
                fixture.raft.wait(None).purged(Some(target), "nonfatal retired child joined"),
                "RETIRED_SNAPSHOT_NONFATAL_JOIN: nonlocal outcome incorrectly prevented live-core purge",
            )
            .await?
            .context("RETIRED_SNAPSHOT_NONFATAL_JOIN: core failed before the retirement barrier completed")?;
            anyhow::ensure!(*child.dropped.borrow() && child.returned_storage_error.borrow().is_none());
            fixture.probe.assert_no_overlap()?;
            let response = within(
                fixture.raft.client_write(ClientRequest::make_request("nonfatal-retirement", 1)),
                "RETIRED_SNAPSHOT_NONFATAL_PROGRESS: joined nonlocal outcome stopped writes",
            )
            .await??;
            fixture.caught_up(&[1, 2], response.log_id).await?;
            anyhow::ensure!(
                fixture.raft.metrics().borrow().running_state.is_ok(),
                "RETIRED_SNAPSHOT_NONFATAL_OUTCOME: cancellation/network/remote error became a local fatal"
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;
        let cleanup = fixture.stop().await;
        result.with_context(|| format!("retired snapshot outcome {outcome:?}"))?;
        cleanup?;
        anyhow::ensure!(
            fixture.raft.metrics().borrow().running_state == Err(Fatal::Stopped),
            "RETIRED_SNAPSHOT_NONFATAL_OUTCOME: normal shutdown retained a false fatal for {outcome:?}"
        );
    }
    Ok(())
}
