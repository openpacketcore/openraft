//! Exercise the public capture and the real core's separate quorum-probe path.

use super::*;
use crate::error::Fatal;

struct Traffic {
    hold_stream_three: AtomicBool,
    refuse_stream_two: AtomicBool,
    refuse_probe_two: AtomicBool,
    probes_three: AtomicUsize,
    stream_entered: tokio::sync::Notify,
    release_stream: tokio::sync::Semaphore,
}

impl Default for Traffic {
    fn default() -> Self {
        Self {
            hold_stream_three: AtomicBool::new(true),
            refuse_stream_two: AtomicBool::new(false),
            refuse_probe_two: AtomicBool::new(false),
            probes_three: AtomicUsize::new(0),
            stream_entered: tokio::sync::Notify::new(),
            release_stream: tokio::sync::Semaphore::new(0),
        }
    }
}

struct ObservedNetwork {
    inner: Network,
    traffic: Arc<Traffic>,
}

struct ObservedPeer {
    inner: Peer,
    traffic: Arc<Traffic>,
}

impl RaftNetworkFactory<TestConfig> for ObservedNetwork {
    type Network = ObservedPeer;

    async fn new_client(&mut self, node_id: u64, node: &EmptyNode) -> Self::Network {
        ObservedPeer {
            inner: self.inner.new_client(node_id, node).await,
            traffic: self.traffic.clone(),
        }
    }
}

fn unreachable() -> RpcFailure {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other("controlled peer partition")))
}

impl RaftNetwork<TestConfig> for ObservedPeer {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TestConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RpcFailure> {
        assert!(
            rpc.entries.is_empty(),
            "only quorum probes use the bare method in this fixture"
        );
        if self.inner.node_id == 2 && self.traffic.refuse_probe_two.load(Ordering::SeqCst) {
            return Err(unreachable());
        }
        if self.inner.node_id == 3 {
            self.traffic.probes_three.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.append_entries(rpc, option).await
    }

    async fn append_entries_with_runtime(
        &mut self,
        rpc: AppendEntriesRequest<TestConfig>,
        option: RPCOption,
    ) -> Result<(AppendEntriesResponse<u64>, Option<[u8; 16]>), RpcFailure> {
        if self.inner.node_id == 2 && self.traffic.refuse_stream_two.load(Ordering::SeqCst) {
            return Err(unreachable());
        }
        if self.inner.node_id == 3 && self.traffic.hold_stream_three.load(Ordering::SeqCst) {
            self.traffic.stream_entered.notify_one();
            self.traffic.release_stream.acquire().await.unwrap().forget();
        }
        let response = self.inner.append_entries(rpc, option).await?;
        Ok((response, Some([self.inner.node_id as u8; 16])))
    }

    async fn vote(&mut self, rpc: VoteRequest<u64>, option: RPCOption) -> Result<VoteResponse<u64>, RpcFailure> {
        self.inner.vote(rpc, option).await
    }

    #[cfg(not(feature = "generic-snapshot-data"))]
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TestConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RpcFailure<InstallSnapshotError>> {
        self.inner.install_snapshot(rpc, option).await
    }

    #[cfg(feature = "generic-snapshot-data")]
    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TestConfig>,
        cancel: impl Future<Output = raft::error::ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<raft::raft::SnapshotResponse<u64>, raft::error::StreamingError<TestConfig, Fatal<u64>>> {
        self.inner.full_snapshot(vote, snapshot, cancel, option).await
    }
}

fn config() -> Arc<raft::Config> {
    Arc::new(
        raft::Config {
            enable_tick: false,
            enable_elect: false,
            snapshot_policy: raft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

#[tokio::test(start_paused = true)]
async fn participation_public_capture_excludes_a_real_quorum_probe_while_replication_is_held() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let traffic = Arc::new(Traffic::default());
        let network = ObservedNetwork {
            inner: Network(Arc::default()),
            traffic: traffic.clone(),
        };
        let store = MemoryStore::default();
        let raft = raft::Raft::<TestConfig>::new(1, config(), network, store.clone(), store).await.unwrap();
        raft.initialize(BTreeSet::from([1, 2, 3])).await.unwrap();
        raft.trigger().elect().await.unwrap();
        raft.wait(Some(Duration::from_secs(2))).current_leader(1, "leader elected").await.unwrap();
        raft.client_write(0).await.unwrap();
        traffic.stream_entered.notified().await;
        let captured = raft.replication_readback().await.unwrap();
        assert!(!captured.observations.contains_key(&3));
        assert_eq!(captured.observations[&2].runtime_id, [2; 16]);

        // Force the quorum round to use peer 3, whose stream response is still
        // held. Its empty read probe must neither trip the stream fixture nor
        // create or refresh a participation row.
        traffic.refuse_probe_two.store(true, Ordering::SeqCst);
        let before = traffic.probes_three.load(Ordering::SeqCst);
        raft.get_read_log_id().await.unwrap();
        assert!(traffic.probes_three.load(Ordering::SeqCst) > before);
        let confirmed = raft.replication_readback().await.unwrap();
        assert_eq!(confirmed.vote, captured.vote);
        assert!(!confirmed.observations.contains_key(&3));

        // Peer 3 now becomes essential for the next commit. Completion proves
        // its stream reply has crossed the core's acceptance point before capture.
        traffic.refuse_stream_two.store(true, Ordering::SeqCst);
        traffic.hold_stream_three.store(false, Ordering::SeqCst);
        traffic.release_stream.add_permits(1);
        let written = raft.client_write(1).await.unwrap();
        let observed = raft.replication_readback().await.unwrap();
        assert_eq!(observed.committed, Some(written.log_id));
        assert_eq!(observed.observations[&3].runtime_id, [3; 16]);
        assert!(observed.observations[&3].matching.unwrap() >= observed.observations[&3].target);
        raft.shutdown().await.unwrap();
        assert!(matches!(
            raft.replication_readback().await,
            Err(RaftError::Fatal(Fatal::Stopped))
        ));
    })
    .await
    .expect("public capture hang guard");
}

#[tokio::test(start_paused = true)]
async fn participation_public_follower_forwards_and_legacy_transports_omit_metadata() {
    let mut legacy = Peer {
        node_id: 3,
        handoff: Arc::default(),
    };
    let (response, runtime_id) = legacy
        .append_entries_with_runtime(
            AppendEntriesRequest {
                vote: Vote::new_committed(4, 1),
                prev_log_id: None,
                leader_commit: None,
                entries: vec![],
            },
            RPCOption::new(Duration::from_millis(100)),
        )
        .await
        .unwrap();
    assert_eq!(response, AppendEntriesResponse::Success);
    assert_eq!(runtime_id, None);

    let store = MemoryStore::default();
    let raft = raft::Raft::<TestConfig>::new(2, config(), Network(Arc::default()), store.clone(), store)
        .await
        .unwrap();
    let membership = LogId::new(raft::CommittedLeaderId::new(4, 1), 0);
    raft.append_entries(AppendEntriesRequest {
        vote: Vote::new_committed(4, 1),
        prev_log_id: None,
        leader_commit: Some(membership),
        entries: vec![Entry {
            log_id: membership,
            payload: EntryPayload::Membership(raft::Membership::new(vec![BTreeSet::from([1, 2, 3])], None)),
        }],
    })
    .await
    .unwrap();
    match raft.replication_readback().await.unwrap_err() {
        RaftError::APIError(forward) => assert_eq!(forward.leader_id, Some(1)),
        other => panic!("expected leader forwarding, got {other:?}"),
    }
    raft.shutdown().await.unwrap();
}
