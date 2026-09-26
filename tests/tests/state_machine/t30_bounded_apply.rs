//! Engine scheduling qualification with synthetic entries and an observed test
//! store. This does not qualify production storage or allocated-byte capacity.

use std::num::NonZeroU64;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::error::Fatal;
use openraft::raft::AppendEntriesRequest;
use openraft::storage::Adaptor;
use openraft::CommittedLeaderId;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::LogIdOptionExt;
use openraft::LogState;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::SnapshotPolicy;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::Vote;
use openraft_memstore::ClientRequest;
use openraft_memstore::ClientResponse;
use openraft_memstore::IntoMemClientRequest;
use openraft_memstore::MemStore;
use openraft_memstore::TypeConfig;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Semaphore;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

#[derive(Clone, Copy, Debug)]
enum ReadMode {
    Prefix(u64),
    Empty,
    Gap,
    Duplicate,
    Oversized,
}

#[derive(Clone, Copy, Debug)]
enum ResponseMode {
    Short,
    Extra,
}

#[derive(Clone)]
struct ReadProbe {
    mode: ReadMode,
    requests: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl ReadProbe {
    fn new(mode: ReadMode) -> Self {
        Self {
            mode,
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[derive(Clone)]
struct PersistenceProbe {
    enabled: Arc<AtomicBool>,
    vote_entered: watch::Sender<usize>,
    append_entered: watch::Sender<usize>,
    vote_gate: Arc<Semaphore>,
    append_gate: Arc<Semaphore>,
}

impl PersistenceProbe {
    fn new() -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(false)),
            vote_entered: watch::channel(0).0,
            append_entered: watch::channel(0).0,
            vote_gate: Arc::new(Semaphore::new(0)),
            append_gate: Arc::new(Semaphore::new(0)),
        }
    }
}

type MaintenanceFrontiers = Arc<Mutex<Vec<(&'static str, Option<LogId<u64>>)>>>;

struct ObservedStore {
    inner: Arc<MemStore>,
    gate: Arc<Semaphore>,
    entered: watch::Sender<usize>,
    maximum: Arc<AtomicUsize>,
    applied: Arc<Mutex<Vec<u64>>>,
    builder_frontiers: Arc<Mutex<Vec<Option<LogId<u64>>>>>,
    released: Option<oneshot::Sender<()>>,
    read_probe: Option<ReadProbe>,
    response_mode: Arc<Mutex<Option<ResponseMode>>>,
    persistence: Option<PersistenceProbe>,
    maintenance_frontiers: MaintenanceFrontiers,
    install_gate: Arc<Semaphore>,
    install_entered: watch::Sender<usize>,
}

impl ObservedStore {
    fn new(inner: Arc<MemStore>) -> Self {
        Self {
            inner,
            gate: Arc::new(Semaphore::new(0)),
            entered: watch::channel(0).0,
            maximum: Arc::new(AtomicUsize::new(0)),
            applied: Arc::new(Mutex::new(Vec::new())),
            builder_frontiers: Arc::new(Mutex::new(Vec::new())),
            released: None,
            read_probe: None,
            response_mode: Arc::new(Mutex::new(None)),
            persistence: None,
            maintenance_frontiers: Arc::default(),
            install_gate: Arc::new(Semaphore::new(1)),
            install_entered: watch::channel(0).0,
        }
    }
}

impl Drop for ObservedStore {
    fn drop(&mut self) {
        if let Some(released) = self.released.take() {
            let _ = released.send(());
        }
    }
}

impl RaftLogReader<TypeConfig> for ObservedStore {
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
        let Some(probe) = &self.read_probe else {
            return self.inner.limited_get_log_entries(start, end).await;
        };
        probe.requests.lock().expect("test observation mutex").push((start, end));
        match probe.mode {
            ReadMode::Prefix(limit) => self.inner.try_get_log_entries(start..end.min(start + limit)).await,
            ReadMode::Empty => Ok(Vec::new()),
            ReadMode::Gap => {
                let mut entries = self.inner.try_get_log_entries(start..end).await?;
                entries.remove(1);
                Ok(entries)
            }
            ReadMode::Duplicate => {
                let mut entries = self.inner.try_get_log_entries(start..end).await?;
                entries[1].log_id = entries[0].log_id;
                Ok(entries)
            }
            ReadMode::Oversized => self.inner.try_get_log_entries(start..end + 1).await,
        }
    }
}

impl RaftStorage<TypeConfig> for ObservedStore {
    type LogReader = Arc<MemStore>;
    type SnapshotBuilder = Arc<MemStore>;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        if let Some(probe) = &self.persistence {
            if probe.enabled.load(Ordering::Acquire) {
                probe.vote_entered.send_modify(|n| *n += 1);
                probe.vote_gate.acquire().await.expect("test vote gate remains open").forget();
            }
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

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        self.inner.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.inner.get_log_reader().await
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend {
        if let Some(probe) = &self.persistence {
            if probe.enabled.load(Ordering::Acquire) {
                probe.append_entered.send_modify(|n| *n += 1);
                probe.append_gate.acquire().await.expect("test append gate remains open").forget();
            }
        }
        self.inner.append_to_log(entries).await
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let (frontier, _) = self.inner.last_applied_state().await?;
        self.maintenance_frontiers.lock().expect("test observation mutex").push(("truncate", frontier));
        self.inner.delete_conflict_logs_since(log_id).await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let (frontier, _) = self.inner.last_applied_state().await?;
        self.maintenance_frontiers.lock().expect("test observation mutex").push(("purge", frontier));
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
        self.maximum.fetch_max(entries.len(), Ordering::AcqRel);
        let indices: Vec<_> = entries.iter().map(|entry| entry.log_id.index).collect();
        self.entered.send_modify(|n| *n += 1);
        self.gate.acquire().await.expect("test apply gate remains open").forget();
        let mut results = self.inner.apply_to_state_machine(entries).await?;
        self.applied.lock().expect("test observation mutex").extend(indices);
        match *self.response_mode.lock().expect("test observation mutex") {
            None => {}
            Some(ResponseMode::Short) => {
                results.pop();
            }
            Some(ResponseMode::Extra) => results.push(ClientResponse(None)),
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let (frontier, _) = self.inner.last_applied_state().await.expect("test applied observation");
        self.builder_frontiers.lock().expect("test observation mutex").push(frontier);
        self.inner.get_snapshot_builder().await
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        let (frontier, _) = self.inner.last_applied_state().await?;
        self.maintenance_frontiers.lock().expect("test observation mutex").push(("receive", frontier));
        self.inner.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, ()>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let (frontier, _) = self.inner.last_applied_state().await?;
        self.maintenance_frontiers.lock().expect("test observation mutex").push(("install", frontier));
        self.install_entered.send_modify(|n| *n += 1);
        let _permit = self.install_gate.acquire().await.expect("test install gate remains open");
        self.inner.install_snapshot(meta, snapshot).await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        self.inner.get_current_snapshot().await
    }
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_preserves_replication_ack_and_complete_ordered_prefix() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            max_apply_entries: NonZeroU64::new(64),
            ..Default::default()
        }
        .validate()?,
    );
    let network = RaftRouter::new(config.clone());
    let storage = Arc::new(MemStore::new());
    storage.enable_saving_committed.store(true, Ordering::Release);
    // Separate adapters keep the apply gate from locking the log-side adapter.
    let (log, _) = Adaptor::new(storage.clone());
    let gate = Arc::new(Semaphore::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let applied = Arc::new(Mutex::new(Vec::new()));
    let (entered, mut observed) = watch::channel(0);
    let state = ObservedStore {
        inner: storage.clone(),
        gate: gate.clone(),
        entered,
        maximum: maximum.clone(),
        applied: applied.clone(),
        builder_frontiers: Arc::new(Mutex::new(Vec::new())),
        released: None,
        read_probe: None,
        response_mode: Arc::new(Mutex::new(None)),
        persistence: None,
        maintenance_frontiers: Arc::default(),
        install_gate: Arc::new(Semaphore::new(1)),
        install_entered: watch::channel(0).0,
    };
    let (_, state) = Adaptor::new(state);
    let raft = Raft::<TypeConfig>::new(0, config, network, log, state).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let id = |index| LogId::new(CommittedLeaderId::new(1, 2), index);
    let first = (0..129)
        .map(|index| Entry {
            log_id: id(index),
            payload: if index == 0 {
                EntryPayload::Membership(Membership::new(vec![btreeset! {0, 1, 2}], ()))
            } else {
                EntryPayload::Blank
            },
        })
        .collect();
    // The durable append acknowledgement must arrive while application is gated.
    let first_ack = tokio::time::timeout_at(
        deadline,
        raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(1, 2),
            prev_log_id: None,
            entries: first,
            leader_commit: Some(id(128)),
        }),
    )
    .await??;
    tokio::time::timeout_at(deadline, observed.wait_for(|n| *n >= 1)).await??;
    let applied_before_release = storage.get_state_machine().await.last_applied_log.index();
    let second_ack = tokio::time::timeout_at(
        deadline,
        raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(1, 2),
            prev_log_id: Some(id(128)),
            entries: (129..193)
                .map(|index| Entry {
                    log_id: id(index),
                    payload: EntryPayload::Blank,
                })
                .collect(),
            leader_commit: Some(id(192)),
        }),
    )
    .await??;
    gate.add_permits(1);
    tokio::time::timeout_at(deadline, observed.wait_for(|n| *n >= 2)).await??;
    let first_page_applied = storage.get_state_machine().await.last_applied_log.index();
    // Drain both fixed and omission variants before asserting the page bound.
    gate.add_permits(4);
    tokio::time::timeout_at(
        deadline,
        raft.wait(None).applied_index(Some(192), "complete committed prefix"),
    )
    .await??;
    let retained = storage.get_state_machine().await.last_applied_log;
    tokio::time::timeout_at(deadline, raft.shutdown()).await??;
    assert!(first_ack.is_success() && second_ack.is_success());
    assert_eq!(
        applied_before_release, None,
        "append acknowledgement must not await state-machine application"
    );
    assert_eq!(retained, Some(id(192)), "complete committed frontier");
    assert_eq!(
        *applied.lock().expect("test observation mutex"),
        (0..193).collect::<Vec<_>>(),
        "BOUNDED_APPLY_ORDER: every committed entry applies once and in order"
    );
    assert!(
        maximum.load(Ordering::Acquire) <= 64,
        "BOUNDED_APPLY_PAGE: native engine must own at most one bounded apply page"
    );
    assert_eq!(
        first_page_applied,
        Some(63),
        "a consumed page advances exactly its durable prefix"
    );
    Ok(())
}

/// A gated follower using the existing test store. Dropping the state-machine
/// owner is observed before reusing its store; `Raft::shutdown()` alone need
/// not mean the independent state-machine task has drained.
struct ObservedNode {
    raft: Raft<TypeConfig>,
    store: Arc<MemStore>,
    gate: Arc<Semaphore>,
    maximum: Arc<AtomicUsize>,
    applied: Arc<Mutex<Vec<u64>>>,
    builder_frontiers: Arc<Mutex<Vec<Option<LogId<u64>>>>>,
    entered: watch::Receiver<usize>,
    released: oneshot::Receiver<()>,
    response_mode: Arc<Mutex<Option<ResponseMode>>>,
    persistence: PersistenceProbe,
    maintenance_frontiers: MaintenanceFrontiers,
    install_gate: Arc<Semaphore>,
    install_entered: watch::Receiver<usize>,
}

impl ObservedNode {
    async fn new(config: Arc<Config>) -> Result<Self> {
        Self::new_with_reader(config, None).await
    }

    async fn new_with_reader(config: Arc<Config>, read_probe: Option<ReadProbe>) -> Result<Self> {
        let store = Arc::new(MemStore::new());
        store.enable_saving_committed.store(true, Ordering::Release);
        let mut log_store = ObservedStore::new(store.clone());
        log_store.read_probe = read_probe;
        let persistence = PersistenceProbe::new();
        log_store.persistence = Some(persistence.clone());
        let maintenance_frontiers = log_store.maintenance_frontiers.clone();
        let (log, _) = Adaptor::new(log_store);
        let gate = Arc::new(Semaphore::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let applied = Arc::new(Mutex::new(Vec::new()));
        let builder_frontiers = Arc::new(Mutex::new(Vec::new()));
        let (entered, observed) = watch::channel(0);
        let (released, release_observation) = oneshot::channel();
        let response_mode = Arc::new(Mutex::new(None));
        let install_gate = Arc::new(Semaphore::new(1));
        let (install_entered, install_observation) = watch::channel(0);
        let state = ObservedStore {
            inner: store.clone(),
            gate: gate.clone(),
            entered,
            maximum: maximum.clone(),
            applied: applied.clone(),
            builder_frontiers: builder_frontiers.clone(),
            released: Some(released),
            read_probe: None,
            response_mode: response_mode.clone(),
            persistence: None,
            maintenance_frontiers: maintenance_frontiers.clone(),
            install_gate: install_gate.clone(),
            install_entered,
        };
        let (_, state) = Adaptor::new(state);
        let network = RaftRouter::new(config.clone());
        let raft = Raft::<TypeConfig>::new(0, config, network, log, state).await?;
        Ok(Self {
            raft,
            store,
            gate,
            maximum,
            applied,
            builder_frontiers,
            entered: observed,
            released: release_observation,
            response_mode,
            persistence,
            maintenance_frontiers,
            install_gate,
            install_entered: install_observation,
        })
    }
}

fn page_config() -> Result<Arc<Config>> {
    Ok(Arc::new(
        Config {
            enable_tick: false,
            snapshot_policy: SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: u64::MAX,
            max_apply_entries: NonZeroU64::new(64),
            ..Default::default()
        }
        .validate()?,
    ))
}

fn page_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 2), index)
}

fn committed_page_prefix() -> AppendEntriesRequest<TypeConfig> {
    AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: None,
        entries: (0..129)
            .map(|index| Entry {
                log_id: page_id(index),
                payload: if index == 0 {
                    EntryPayload::Membership(Membership::new(vec![btreeset! {0, 1, 2}], ()))
                } else {
                    EntryPayload::Blank
                },
            })
            .collect(),
        leader_commit: Some(page_id(128)),
    }
}

async fn deferred_snapshot_keeps_heartbeat_live(begin_receiving: bool) -> Result<()> {
    let mut config = (*page_config()?).clone();
    if !begin_receiving {
        config.snapshot_policy = SnapshotPolicy::LogsSinceLast(128);
    }
    let mut node = ObservedNode::new(Arc::new(config)).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let first_ack = tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix())).await??;
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 1)).await??;
    let mut receiving = std::pin::pin!(node.raft.begin_receiving_snapshot());
    if begin_receiving {
        // Polling sends the actual API request before yielding on its response.
        assert!(futures::poll!(&mut receiving).is_pending());
    }
    // This callback proves the earlier snapshot command reached the core.
    tokio::time::timeout_at(deadline, node.raft.with_raft_state(|_| ())).await??;
    let heartbeat = tokio::time::timeout_at(
        deadline,
        node.raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(1, 2),
            prev_log_id: Some(page_id(128)),
            entries: vec![],
            leader_commit: Some(page_id(128)),
        }),
    )
    .await;
    let applied_before_release = node.store.get_state_machine().await.last_applied_log;
    let builder_before_release = node.builder_frontiers.lock().expect("test observation mutex").clone();

    // A failed acknowledgement still drains and releases the real worker.
    // Cleanup has its own bound; it never converts a missed deadline to success.
    let cleanup = tokio::time::Instant::now() + Duration::from_secs(10);
    node.gate.add_permits(3);
    if begin_receiving {
        tokio::time::timeout_at(cleanup, &mut receiving).await??;
    } else {
        tokio::time::timeout_at(
            cleanup,
            node.raft.wait(None).snapshot(page_id(128), "complete deferred snapshot"),
        )
        .await??;
    }
    tokio::time::timeout_at(
        cleanup,
        node.raft.wait(None).applied_index(Some(128), "complete deferred prefix"),
    )
    .await??;
    tokio::time::timeout_at(cleanup, node.raft.shutdown()).await??;
    tokio::time::timeout_at(cleanup, node.released).await??;
    assert!(first_ack.is_success());
    assert_eq!(applied_before_release, None);
    assert!(builder_before_release.is_empty());
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..129).collect::<Vec<_>>()
    );
    assert!(node.maximum.load(Ordering::Acquire) <= 64);
    if !begin_receiving {
        assert_eq!(
            *node.builder_frontiers.lock().expect("test observation mutex"),
            vec![Some(page_id(128))],
            "BOUNDED_APPLY_SNAPSHOT: retain the complete preceding range"
        );
    } else {
        assert_eq!(
            *node.maintenance_frontiers.lock().expect("test observation mutex"),
            vec![("receive", Some(page_id(128)))],
            "BOUNDED_APPLY_RECEIVE: retain the complete preceding range"
        );
    }
    assert!(
        matches!(heartbeat, Ok(Ok(ref response)) if response.is_success()),
        "BOUNDED_APPLY_SNAPSHOT_ACK: deferred snapshot must not block an unrelated heartbeat: {heartbeat:?}"
    );
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_deferred_build_snapshot_keeps_heartbeat_live() -> Result<()> {
    deferred_snapshot_keeps_heartbeat_live(false).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_deferred_begin_receiving_keeps_heartbeat_live() -> Result<()> {
    deferred_snapshot_keeps_heartbeat_live(true).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_deferred_snapshot_accepts_sustained_successors() -> Result<()> {
    let mut node = ObservedNode::new(page_config()?).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    assert!(
        tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix()))
            .await??
            .is_success()
    );
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 1)).await??;
    tokio::time::timeout_at(deadline, node.raft.trigger().snapshot()).await??;
    let successors: Result<()> = async {
        for index in 129..385 {
            let response = tokio::time::timeout_at(
                deadline,
                node.raft.append_entries(AppendEntriesRequest {
                    vote: Vote::new_committed(1, 2),
                    prev_log_id: Some(page_id(index - 1)),
                    entries: vec![Entry {
                        log_id: page_id(index),
                        payload: EntryPayload::Blank,
                    }],
                    leader_commit: Some(page_id(index)),
                }),
            )
            .await??;
            anyhow::ensure!(response.is_success(), "successor append was rejected");
        }
        Ok(())
    }
    .await;
    let cleanup = tokio::time::Instant::now() + Duration::from_secs(10);
    if successors.is_err() {
        node.gate.add_permits(8);
        tokio::time::timeout_at(cleanup, node.raft.shutdown()).await??;
        tokio::time::timeout_at(cleanup, node.released).await??;
        panic!("BOUNDED_APPLY_SUCCESSORS: acknowledgements stalled: {successors:?}");
    }
    tokio::time::timeout_at(cleanup, node.raft.with_raft_state(|_| ())).await??;
    assert_eq!(node.store.get_state_machine().await.last_applied_log, None);
    node.gate.add_permits(3);
    tokio::time::timeout_at(cleanup, node.entered.wait_for(|n| *n == 4)).await??;
    assert_eq!(
        *node.builder_frontiers.lock().expect("test observation mutex"),
        vec![Some(page_id(128))],
        "BOUNDED_APPLY_SNAPSHOT: successor commits cannot cross the earlier snapshot"
    );
    assert_eq!(
        node.store.get_state_machine().await.last_applied_log,
        Some(page_id(128))
    );
    node.gate.add_permits(4);
    tokio::time::timeout_at(
        cleanup,
        node.raft.wait(None).applied_index(Some(384), "complete successor prefix"),
    )
    .await??;
    tokio::time::timeout_at(cleanup, node.raft.shutdown()).await??;
    tokio::time::timeout_at(cleanup, node.released).await??;
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..385).collect::<Vec<_>>()
    );
    assert!(node.maximum.load(Ordering::Acquire) <= 64);
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_deferred_snapshot_preserves_vote_and_append_before_response() -> Result<()> {
    let mut node = ObservedNode::new(page_config()?).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    assert!(
        tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix()))
            .await??
            .is_success()
    );
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 1)).await??;
    tokio::time::timeout_at(deadline, node.raft.trigger().snapshot()).await??;
    tokio::time::timeout_at(deadline, node.raft.with_raft_state(|_| ())).await??;
    node.persistence.enabled.store(true, Ordering::Release);
    let mut voting = node.persistence.vote_entered.subscribe();
    let mut appending = node.persistence.append_entered.subscribe();
    let next_id = LogId::new(CommittedLeaderId::new(2, 2), 129);
    let next_vote = Vote::new_committed(2, 2);
    let mut append = std::pin::pin!(node.raft.append_entries(AppendEntriesRequest {
        vote: next_vote,
        prev_log_id: Some(page_id(128)),
        entries: vec![Entry {
            log_id: next_id,
            payload: EntryPayload::Blank
        }],
        leader_commit: Some(next_id),
    }));
    let ordering: Result<()> = async {
        anyhow::ensure!(
            futures::poll!(&mut append).is_pending(),
            "unexpected immediate response"
        );
        tokio::time::timeout_at(deadline, voting.wait_for(|n| *n == 1)).await??;
        anyhow::ensure!(
            futures::poll!(&mut append).is_pending(),
            "response preceded durable vote"
        );
        anyhow::ensure!(node.store.clone().read_vote().await? == Some(Vote::new_committed(1, 2)));
        node.persistence.vote_gate.add_permits(1);
        tokio::time::timeout_at(deadline, appending.wait_for(|n| *n == 1)).await??;
        anyhow::ensure!(
            futures::poll!(&mut append).is_pending(),
            "response preceded durable append"
        );
        anyhow::ensure!(node.store.clone().read_vote().await? == Some(next_vote));
        anyhow::ensure!(node.store.clone().get_log_state().await?.last_log_id == Some(page_id(128)));
        node.persistence.append_gate.add_permits(1);
        anyhow::ensure!(tokio::time::timeout_at(deadline, &mut append).await??.is_success());
        anyhow::ensure!(node.store.clone().get_log_state().await?.last_log_id == Some(next_id));
        anyhow::ensure!(node.store.get_state_machine().await.last_applied_log.is_none());
        Ok(())
    }
    .await;
    let cleanup = tokio::time::Instant::now() + Duration::from_secs(10);
    node.persistence.vote_gate.add_permits(1);
    node.persistence.append_gate.add_permits(1);
    node.gate.add_permits(4);
    tokio::time::timeout_at(
        cleanup,
        node.raft.wait(None).applied_index(Some(129), "persisted successor"),
    )
    .await??;
    tokio::time::timeout_at(cleanup, node.raft.shutdown()).await??;
    tokio::time::timeout_at(cleanup, node.released).await??;
    assert!(ordering.is_ok(), "BOUNDED_APPLY_DURABILITY: {ordering:?}");
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..130).collect::<Vec<_>>()
    );
    assert_eq!(*node.builder_frontiers.lock().expect("test observation mutex"), vec![
        Some(page_id(128))
    ]);
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_deferred_snapshot_keeps_conflict_and_response_order() -> Result<()> {
    let mut node = ObservedNode::new(page_config()?).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    assert!(
        tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix()))
            .await??
            .is_success()
    );
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 1)).await??;
    tokio::time::timeout_at(deadline, node.raft.trigger().snapshot()).await??;
    let suffix = AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: Some(page_id(128)),
        entries: vec![Entry {
            log_id: page_id(129),
            payload: EntryPayload::Blank,
        }],
        leader_commit: Some(page_id(128)),
    };
    assert!(tokio::time::timeout_at(deadline, node.raft.append_entries(suffix)).await??.is_success());
    let next_id = LogId::new(CommittedLeaderId::new(2, 2), 129);
    let mut conflict = std::pin::pin!(node.raft.append_entries(AppendEntriesRequest {
        vote: Vote::new_committed(2, 2),
        prev_log_id: Some(page_id(128)),
        entries: vec![Entry {
            log_id: next_id,
            payload: EntryPayload::Blank
        }],
        leader_commit: Some(next_id),
    }));
    assert!(futures::poll!(&mut conflict).is_pending());
    tokio::time::timeout_at(deadline, node.raft.with_raft_state(|_| ())).await??;
    let early_response = futures::poll!(&mut conflict);
    let early_log = node.store.clone().try_get_log_entries(129..130).await?;
    let early_maintenance = node.maintenance_frontiers.lock().expect("test observation mutex").clone();
    node.gate.add_permits(4);
    let was_pending = early_response.is_pending();
    let response = match early_response {
        std::task::Poll::Ready(result) => result?,
        std::task::Poll::Pending => tokio::time::timeout_at(deadline, &mut conflict).await??,
    };
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).applied_index(Some(129), "replacement applied"),
    )
    .await??;
    tokio::time::timeout_at(deadline, node.raft.shutdown()).await??;
    tokio::time::timeout_at(deadline, node.released).await??;
    assert!(response.is_success());
    assert!(
        was_pending,
        "BOUNDED_APPLY_CONFLICT: response crossed unperformed truncation"
    );
    assert!(
        early_maintenance.is_empty(),
        "BOUNDED_APPLY_CONFLICT: removal crossed pending apply"
    );
    assert_eq!(early_log[0].log_id, page_id(129));
    assert_eq!(
        *node.maintenance_frontiers.lock().expect("test observation mutex"),
        vec![("truncate", Some(page_id(128)))]
    );
    assert_eq!(*node.builder_frontiers.lock().expect("test observation mutex"), vec![
        Some(page_id(128))
    ]);
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..130).collect::<Vec<_>>()
    );
    assert_eq!(node.store.get_state_machine().await.last_applied_log, Some(next_id));
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_deferred_snapshot_keeps_install_and_response_order() -> Result<()> {
    let mut source = Arc::new(MemStore::new());
    let mut entries = committed_page_prefix().entries;
    entries.extend((129..193).map(|index| Entry {
        log_id: page_id(index),
        payload: EntryPayload::Blank,
    }));
    source.apply_to_state_machine(&entries).await?;
    let snapshot = source.get_snapshot_builder().await.build_snapshot().await?;
    let mut node = ObservedNode::new(page_config()?).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let install_permit = node.install_gate.clone().acquire_owned().await?;
    assert!(
        tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix()))
            .await??
            .is_success()
    );
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 1)).await??;
    tokio::time::timeout_at(deadline, node.raft.trigger().snapshot()).await??;
    let mut install = std::pin::pin!(node.raft.install_full_snapshot(Vote::new_committed(1, 2), snapshot));
    assert!(futures::poll!(&mut install).is_pending());
    tokio::time::timeout_at(deadline, node.raft.with_raft_state(|_| ())).await??;
    let mut heartbeat = std::pin::pin!(node.raft.append_entries(AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: Some(page_id(192)),
        entries: vec![],
        leader_commit: Some(page_id(192)),
    }));
    assert!(futures::poll!(&mut heartbeat).is_pending());
    tokio::time::timeout_at(deadline, node.raft.with_raft_state(|_| ())).await??;
    let early_maintenance = node.maintenance_frontiers.lock().expect("test observation mutex").clone();
    node.gate.add_permits(3);
    tokio::time::timeout_at(deadline, node.install_entered.wait_for(|n| *n == 1)).await??;
    tokio::time::timeout_at(deadline, node.raft.with_raft_state(|_| ())).await??;
    let early_install = futures::poll!(&mut install);
    let early_heartbeat = futures::poll!(&mut heartbeat);
    let was_pending = early_install.is_pending() && early_heartbeat.is_pending();
    drop(install_permit);
    match early_install {
        std::task::Poll::Ready(result) => result?,
        std::task::Poll::Pending => tokio::time::timeout_at(deadline, &mut install).await??,
    };
    let response = match early_heartbeat {
        std::task::Poll::Ready(result) => result?,
        std::task::Poll::Pending => tokio::time::timeout_at(deadline, &mut heartbeat).await??,
    };
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).applied_index(Some(192), "installed snapshot"),
    )
    .await??;
    tokio::time::timeout_at(deadline, node.raft.shutdown()).await??;
    tokio::time::timeout_at(deadline, node.released).await??;
    assert!(response.is_success());
    assert!(
        was_pending,
        "BOUNDED_APPLY_INSTALL: responses crossed unperformed installation"
    );
    assert!(
        early_maintenance.is_empty(),
        "BOUNDED_APPLY_INSTALL: maintenance crossed pending apply"
    );
    let maintenance = node.maintenance_frontiers.lock().expect("test observation mutex").clone();
    assert!(maintenance.contains(&("install", Some(page_id(128)))));
    assert!(maintenance.iter().all(|(_, frontier)| *frontier >= Some(page_id(128))));
    assert_eq!(*node.builder_frontiers.lock().expect("test observation mutex"), vec![
        Some(page_id(128))
    ]);
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..129).collect::<Vec<_>>()
    );
    assert_eq!(
        node.store.get_state_machine().await.last_applied_log,
        Some(page_id(192))
    );
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_snapshot_barrier_and_purge_preserve_complete_prefix() -> Result<()> {
    let mut node = ObservedNode::new(page_config()?).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let ack = tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix())).await??;
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 1)).await??;
    tokio::time::timeout_at(deadline, node.raft.trigger().snapshot()).await??;
    // A subsequent core callback proves that the earlier trigger was consumed,
    // without sleeps or a timing-based negative observation.
    tokio::time::timeout_at(deadline, node.raft.with_raft_state(|_| ())).await??;
    assert!(node.builder_frontiers.lock().expect("test observation mutex").is_empty());
    node.gate.add_permits(3);
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).snapshot(page_id(128), "complete snapshot frontier"),
    )
    .await??;
    tokio::time::timeout_at(deadline, node.raft.trigger().purge_log(128)).await??;
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).purged(Some(page_id(128)), "purge completed snapshot"),
    )
    .await??;
    // Applying the first successor after snapshot/purge must still advance the
    // command sequence and preserve the previous membership and log frontier.
    node.gate.add_permits(1);
    let next = tokio::time::timeout_at(
        deadline,
        node.raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(1, 2),
            prev_log_id: Some(page_id(128)),
            entries: vec![Entry {
                log_id: page_id(129),
                payload: EntryPayload::Blank,
            }],
            leader_commit: Some(page_id(129)),
        }),
    )
    .await??;
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).applied_index(Some(129), "successor after purge"),
    )
    .await??;
    tokio::time::timeout_at(deadline, node.raft.shutdown()).await??;
    tokio::time::timeout_at(deadline, node.released).await??;
    assert!(ack.is_success() && next.is_success());
    assert_eq!(
        *node.builder_frontiers.lock().expect("test observation mutex"),
        vec![Some(page_id(128))],
        "BOUNDED_APPLY_SNAPSHOT: builder must observe the entire preceding committed range"
    );
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..130).collect::<Vec<_>>()
    );
    assert!(node.maximum.load(Ordering::Acquire) <= 64);
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_shutdown_and_restart_replay_only_unapplied_suffix() -> Result<()> {
    let config = page_config()?;
    let mut node = ObservedNode::new(config.clone()).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let ack = tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix())).await??;
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 1)).await??;
    node.gate.add_permits(1);
    tokio::time::timeout_at(deadline, node.entered.wait_for(|n| *n == 2)).await??;
    tokio::time::timeout_at(deadline, node.raft.shutdown()).await??;
    // The already-dispatched page may complete after core shutdown. Drain its
    // actual owner before reopening; queued scalar work is recovered from logs.
    node.gate.add_permits(1);
    tokio::time::timeout_at(deadline, node.released).await??;
    assert!(ack.is_success());
    assert_eq!(
        node.store.get_state_machine().await.last_applied_log,
        Some(page_id(127))
    );
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..128).collect::<Vec<_>>()
    );
    let (log, _) = Adaptor::new(node.store.clone());
    let (entered, _) = watch::channel(0);
    let (released, observed_release) = oneshot::channel();
    let state = ObservedStore {
        inner: node.store.clone(),
        gate: Arc::new(Semaphore::new(1)),
        entered,
        maximum: node.maximum.clone(),
        applied: node.applied.clone(),
        builder_frontiers: node.builder_frontiers.clone(),
        released: Some(released),
        read_probe: None,
        response_mode: Arc::new(Mutex::new(None)),
        persistence: None,
        maintenance_frontiers: node.maintenance_frontiers.clone(),
        install_gate: Arc::new(Semaphore::new(1)),
        install_entered: watch::channel(0).0,
    };
    let (_, state) = Adaptor::new(state);
    let network = RaftRouter::new(config.clone());
    let reopened = tokio::time::timeout_at(deadline, Raft::<TypeConfig>::new(0, config, network, log, state)).await??;
    tokio::time::timeout_at(
        deadline,
        reopened.wait(None).applied_index(Some(128), "replayed committed suffix"),
    )
    .await??;
    tokio::time::timeout_at(deadline, reopened.shutdown()).await??;
    tokio::time::timeout_at(deadline, observed_release).await??;
    assert_eq!(
        node.store.get_state_machine().await.last_applied_log,
        Some(page_id(128))
    );
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..129).collect::<Vec<_>>(),
        "BOUNDED_APPLY_RESTART: resume the retained committed suffix without loss or duplicate apply"
    );
    assert!(node.maximum.load(Ordering::Acquire) <= 64);
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_honors_shorter_storage_pages_through_adapter() -> Result<()> {
    let probe = ReadProbe::new(ReadMode::Prefix(7));
    let node = ObservedNode::new_with_reader(page_config()?, Some(probe.clone())).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    node.gate.add_permits(129);
    assert!(
        tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix()))
            .await??
            .is_success()
    );
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).applied_index(Some(128), "short pages complete"),
    )
    .await??;
    tokio::time::timeout_at(deadline, node.raft.shutdown()).await??;
    tokio::time::timeout_at(deadline, node.released).await??;
    assert_eq!(
        *probe.requests.lock().expect("test observation mutex"),
        (0..129).step_by(7).map(|start| (start, (start + 64).min(129))).collect::<Vec<_>>(),
        "BOUNDED_APPLY_READER: preserve the underlying limited-reader contract"
    );
    assert!(node.maximum.load(Ordering::Acquire) <= 7);
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..129).collect::<Vec<_>>()
    );
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_rejects_malformed_storage_pages_before_application() -> Result<()> {
    for mode in [ReadMode::Empty, ReadMode::Gap, ReadMode::Duplicate, ReadMode::Oversized] {
        let probe = ReadProbe::new(mode);
        let node = ObservedNode::new_with_reader(page_config()?, Some(probe.clone())).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        // If validation is removed, allow erroneous work to finish rather than
        // leaving the worker blocked at a test gate.
        node.gate.add_permits(129);
        // Durable append acknowledgement can precede the apply read failure.
        let _ack = tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix())).await?;
        let metrics = tokio::time::timeout_at(
            deadline,
            node.raft.wait(None).metrics(|m| m.running_state.is_err(), "invalid page stops the core"),
        )
        .await??;
        tokio::time::timeout_at(deadline, node.raft.shutdown()).await??;
        tokio::time::timeout_at(deadline, node.released).await??;
        assert!(
            matches!(metrics.running_state, Err(Fatal::StorageError(_))),
            "BOUNDED_APPLY_INVALID_PAGE: {mode:?} must fail with a storage error, got {:?}",
            metrics.running_state
        );
        assert_eq!(*probe.requests.lock().expect("test observation mutex"), vec![(0, 64)]);
        assert_eq!(
            node.maximum.load(Ordering::Acquire),
            0,
            "malformed input reached the state machine"
        );
        assert!(node.applied.lock().expect("test observation mutex").is_empty());
    }
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_unset_preserves_full_range_reader() -> Result<()> {
    let mut config = (*page_config()?).clone();
    config.max_apply_entries = None;
    let probe = ReadProbe::new(ReadMode::Empty);
    let node = ObservedNode::new_with_reader(Arc::new(config.validate()?), Some(probe.clone())).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    node.gate.add_permits(1);
    assert!(
        tokio::time::timeout_at(deadline, node.raft.append_entries(committed_page_prefix()))
            .await??
            .is_success()
    );
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).applied_index(Some(128), "default full range"),
    )
    .await??;
    tokio::time::timeout_at(deadline, node.raft.shutdown()).await??;
    tokio::time::timeout_at(deadline, node.released).await??;
    assert!(probe.requests.lock().expect("test observation mutex").is_empty());
    assert_eq!(node.maximum.load(Ordering::Acquire), 129);
    assert_eq!(
        *node.applied.lock().expect("test observation mutex"),
        (0..129).collect::<Vec<_>>()
    );
    Ok(())
}

async fn assert_invalid_response_stops_worker_and_client(mode: ResponseMode) -> Result<()> {
    let node = ObservedNode::new(page_config()?).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    node.gate.add_permits(8);
    tokio::time::timeout_at(deadline, node.raft.initialize(btreeset! {0})).await??;
    // Initialization starts an election; its response does not establish leadership.
    tokio::time::timeout_at(
        deadline,
        node.raft.wait(None).current_leader(0, "ready for response fault injection"),
    )
    .await??;
    let initial = tokio::time::timeout_at(
        deadline,
        node.raft.client_write(ClientRequest::make_request("client", 0)),
    )
    .await??;
    *node.response_mode.lock().expect("test observation mutex") = Some(mode);
    let result = tokio::time::timeout_at(
        deadline,
        node.raft.client_write(ClientRequest::make_request("client", 1)),
    )
    .await;
    // Cleanup also runs for the panic/timeout regression. Shutdown is bounded
    // separately from the client operation so a failed assertion leaks no task.
    tokio::time::timeout(Duration::from_secs(2), node.raft.shutdown()).await??;
    tokio::time::timeout(Duration::from_secs(2), node.released).await??;
    assert!(
        matches!(&result, Ok(Err(err)) if matches!(err.fatal(), Some(Fatal::StorageError(_)))),
        "BOUNDED_APPLY_RESPONSE: {mode:?} must release the client with a storage error, got {result:?}"
    );
    let metrics = node.raft.metrics().borrow().clone();
    assert!(matches!(metrics.running_state, Err(Fatal::StorageError(_))));
    assert_eq!(
        metrics.last_applied,
        Some(initial.log_id),
        "an invalid response must not advance applied metrics"
    );
    assert_eq!(
        node.store.get_state_machine().await.last_applied_log.index(),
        Some(initial.log_id.index + 1)
    );
    Ok(())
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_short_response_releases_client_and_worker() -> Result<()> {
    assert_invalid_response_stops_worker_and_client(ResponseMode::Short).await
}

#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn bounded_apply_extra_response_releases_client_and_worker() -> Result<()> {
    assert_invalid_response_stops_worker_and_client(ResponseMode::Extra).await
}
