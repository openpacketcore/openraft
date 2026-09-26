//! Provide `LogStore`, which is a in-memory implementation of `RaftLogStore` for demonstration
//! purpose only.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::RaftTypeConfig;
use openraft::Vote;
use tokio::sync::Mutex;

/// RaftLogStore implementation with a in-memory storage
#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Debug)]
pub struct LogStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogId<C::NodeId>>,

    /// The Raft log.
    log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    committed: Option<LogId<C::NodeId>>,

    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> LogStoreInner<C> {
    fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(&mut self, range: RB) -> Vec<C::Entry>
    where C::Entry: Clone {
        self.log.range(range.clone()).map(|(_, val)| val.clone()).collect()
    }

    fn get_log_state(&mut self) -> LogState<C> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.get_log_id().clone());

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        }
    }

    fn save_committed(&mut self, committed: Option<LogId<C::NodeId>>) {
        self.committed = committed;
    }

    fn read_committed(&mut self) -> Option<LogId<C::NodeId>> {
        self.committed.clone()
    }

    fn save_vote(&mut self, vote: &Vote<C::NodeId>) {
        self.vote = Some(vote.clone());
    }

    fn read_vote(&mut self) -> Option<Vote<C::NodeId>> {
        self.vote.clone()
    }

    fn append<I>(&mut self, entries: I, callback: LogFlushed<C>)
    where I: IntoIterator<Item = C::Entry> {
        // Simple implementation that calls the flush-before-return `append_to_log`.
        for entry in entries {
            self.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
    }

    fn truncate(&mut self, log_id: LogId<C::NodeId>) {
        let keys = self.log.range(log_id.index..).map(|(k, _v)| *k).collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }
    }

    fn purge(&mut self, log_id: LogId<C::NodeId>) {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

        {
            let keys = self.log.range(..=log_id.index).map(|(k, _v)| *k).collect::<Vec<_>>();
            for key in keys {
                self.log.remove(&key);
            }
        }
    }
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::ops::RangeBounds;

    use openraft::storage::LogFlushed;
    use openraft::storage::RaftLogStorage;
    use openraft::LogId;
    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::StorageError;
    use openraft::Vote;

    use crate::log_store::LogStore;

    impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
    where C::Entry: Clone
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            Ok(inner.try_get_log_entries(range))
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
    where C::Entry: Clone
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            Ok(inner.get_log_state())
        }

        async fn save_committed(&mut self, committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.save_committed(committed);
            Ok(())
        }

        async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            Ok(inner.read_committed())
        }

        async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.save_vote(vote);
            Ok(())
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            Ok(inner.read_vote())
        }

        async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C::NodeId>>
        where I: IntoIterator<Item = C::Entry> {
            let mut inner = self.inner.lock().await;
            inner.append(entries, callback);
            Ok(())
        }

        async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.truncate(log_id);
            Ok(())
        }

        async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id);
            Ok(())
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
