//! Keep fixture inspection and store replacement available with either storage API.
//!
//! The v2 wrapper forwards to native v2 implementations. Its lock has the same
//! ownership as the legacy adapter; tests that need independent log and state
//! machine ownership construct separate wrappers.

#[cfg(not(feature = "storage-v2"))]
pub use openraft::storage::Adaptor as Store;
#[cfg(feature = "storage-v2")]
pub use v2::Store;

#[cfg(feature = "storage-v2")]
mod v2 {
    use std::fmt::Debug;
    use std::marker::PhantomData;
    use std::ops::RangeBounds;
    use std::sync::Arc;

    use openraft::storage::LogFlushed;
    use openraft::storage::RaftLogStorage;
    use openraft::storage::RaftStateMachine;
    use openraft::LogId;
    use openraft::LogState;
    use openraft::OptionalSend;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::Snapshot;
    use openraft::SnapshotMeta;
    use openraft::StorageError;
    use openraft::StoredMembership;
    use openraft::Vote;
    use tokio::sync::RwLock;
    use tokio::sync::RwLockReadGuard;
    use tokio::sync::RwLockWriteGuard;

    #[derive(Clone)]
    pub struct Store<C, S> {
        storage: Arc<RwLock<S>>,
        config: PhantomData<C>,
    }

    impl<C, S> Store<C, S> {
        pub fn new(store: S) -> (Self, Self) {
            let storage = Arc::new(RwLock::new(store));
            (
                Self {
                    storage: storage.clone(),
                    config: PhantomData,
                },
                Self {
                    storage,
                    config: PhantomData,
                },
            )
        }

        pub async fn storage(&self) -> RwLockReadGuard<'_, S> {
            self.storage.read().await
        }

        pub async fn storage_mut(&self) -> RwLockWriteGuard<'_, S> {
            self.storage.write().await
        }
    }

    impl<C, S> RaftLogReader<C> for Store<C, S>
    where
        C: RaftTypeConfig,
        S: RaftLogStorage<C>,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
            self.storage_mut().await.try_get_log_entries(range).await
        }

        async fn limited_get_log_entries(
            &mut self,
            start: u64,
            end: u64,
        ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
            self.storage_mut().await.limited_get_log_entries(start, end).await
        }
    }

    impl<C, S> RaftLogStorage<C> for Store<C, S>
    where
        C: RaftTypeConfig,
        S: RaftLogStorage<C>,
    {
        type LogReader = S::LogReader;

        async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
            self.storage_mut().await.get_log_state().await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.storage_mut().await.get_log_reader().await
        }

        async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            self.storage_mut().await.save_vote(vote).await
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
            self.storage_mut().await.read_vote().await
        }

        async fn save_committed(&mut self, committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C::NodeId>> {
            self.storage_mut().await.save_committed(committed).await
        }

        async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
            self.storage_mut().await.read_committed().await
        }

        async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C::NodeId>>
        where
            I: IntoIterator<Item = C::Entry> + OptionalSend,
            I::IntoIter: OptionalSend,
        {
            self.storage_mut().await.append(entries, callback).await
        }

        async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            self.storage_mut().await.truncate(log_id).await
        }

        async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            self.storage_mut().await.purge(log_id).await
        }
    }

    impl<C, S> RaftStateMachine<C> for Store<C, S>
    where
        C: RaftTypeConfig,
        S: RaftStateMachine<C>,
    {
        type SnapshotBuilder = S::SnapshotBuilder;

        async fn applied_state(
            &mut self,
        ) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>), StorageError<C::NodeId>> {
            self.storage_mut().await.applied_state().await
        }

        async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
        where
            I: IntoIterator<Item = C::Entry> + OptionalSend,
            I::IntoIter: OptionalSend,
        {
            self.storage_mut().await.apply(entries).await
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            self.storage_mut().await.get_snapshot_builder().await
        }

        async fn begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>> {
            self.storage_mut().await.begin_receiving_snapshot().await
        }

        async fn install_snapshot(
            &mut self,
            meta: &SnapshotMeta<C::NodeId, C::Node>,
            snapshot: Box<C::SnapshotData>,
        ) -> Result<(), StorageError<C::NodeId>> {
            self.storage_mut().await.install_snapshot(meta, snapshot).await
        }

        async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>> {
            self.storage_mut().await.get_current_snapshot().await
        }
    }
}
