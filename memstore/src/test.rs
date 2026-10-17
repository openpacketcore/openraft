use openraft::testing::Suite;
use openraft::StorageError;

use crate::MemNodeId;
use crate::MemStore;

#[cfg(feature = "storage-v2")]
struct MemStoreBuilder;

#[cfg(feature = "storage-v2")]
impl openraft::testing::StoreBuilder<crate::TypeConfig, std::sync::Arc<MemStore>, std::sync::Arc<MemStore>>
    for MemStoreBuilder
{
    async fn build(&self) -> Result<((), std::sync::Arc<MemStore>, std::sync::Arc<MemStore>), StorageError<MemNodeId>> {
        let store = MemStore::new_async().await;
        Ok(((), store.clone(), store))
    }
}

#[test]
#[allow(clippy::result_large_err)]
pub fn test_mem_store() -> Result<(), StorageError<MemNodeId>> {
    #[cfg(not(feature = "storage-v2"))]
    let builder = MemStore::new_async;
    #[cfg(feature = "storage-v2")]
    let builder = MemStoreBuilder;
    Suite::test_all(builder)
}
