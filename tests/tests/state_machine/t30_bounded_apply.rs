//! Engine scheduling qualification. Memstore is a test double here, not native
//! SDK storage, durability, transport, or allocated-byte capacity evidence.

use std::num::NonZeroU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
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
use openraft::RaftStorage;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::SnapshotPolicy;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::Vote;
use openraft_memstore::ClientResponse;
use openraft_memstore::MemStore;
use openraft_memstore::TypeConfig;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Semaphore;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

struct ObservedStore {
    inner: Arc<MemStore>,
    gate: Arc<Semaphore>,
    entered: watch::Sender<usize>,
    maximum: Arc<AtomicUsize>,
    applied: Arc<Mutex<Vec<u64>>>,
    builder_frontiers: Arc<Mutex<Vec<Option<LogId<u64>>>>>,
    released: Option<oneshot::Sender<()>>,
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
}

impl RaftStorage<TypeConfig> for ObservedStore {
    type LogReader = Arc<MemStore>;
    type SnapshotBuilder = Arc<MemStore>;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
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
        self.inner.append_to_log(entries).await
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.delete_conflict_logs_since(log_id).await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
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
        let results = self.inner.apply_to_state_machine(entries).await?;
        self.applied.lock().expect("test observation mutex").extend(indices);
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let (frontier, _) = self.inner.last_applied_state().await.expect("test applied observation");
        self.builder_frontiers.lock().expect("test observation mutex").push(frontier);
        self.inner.get_snapshot_builder().await
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        self.inner.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, ()>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
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
}

impl ObservedNode {
    async fn new(config: Arc<Config>) -> Result<Self> {
        let store = Arc::new(MemStore::new());
        store.enable_saving_committed.store(true, Ordering::Release);
        let (log, _) = Adaptor::new(store.clone());
        let gate = Arc::new(Semaphore::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let applied = Arc::new(Mutex::new(Vec::new()));
        let builder_frontiers = Arc::new(Mutex::new(Vec::new()));
        let (entered, observed) = watch::channel(0);
        let (released, release_observation) = oneshot::channel();
        let state = ObservedStore {
            inner: store.clone(),
            gate: gate.clone(),
            entered,
            maximum: maximum.clone(),
            applied: applied.clone(),
            builder_frontiers: builder_frontiers.clone(),
            released: Some(released),
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
