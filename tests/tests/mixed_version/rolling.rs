use super::*;

// Preserve the exact vote, complete log and applied prefix across a binary
// replacement. Both stores implement the real storage interface; translating
// their identical serde forms does not create grants, logs or votes.
struct Records {
    vote: Vec<u8>,
    log: Vec<u8>,
    applied: Option<u64>,
    machine: serde_json::Value,
}

enum Store {
    Current(Arc<openraft_memstore::MemStore>),
    Legacy(Arc<legacy_memstore::MemStore>),
}

impl Store {
    async fn export(&self) -> Records {
        macro_rules! export {
            ($raft:ident, $store:expr) => {{
                use $raft::storage::RaftLogReader;
                use $raft::RaftStorage;
                let mut store = $store.clone();
                let machine = store.get_state_machine().await;
                Records {
                    vote: encode(&store.read_vote().await.unwrap()),
                    log: encode(&store.get_log_reader().await.try_get_log_entries(..).await.unwrap()),
                    applied: machine.last_applied_log.map(|id| id.index),
                    machine: serde_json::to_value(machine).unwrap(),
                }
            }};
        }
        match self {
            Self::Current(store) => export!(new, store),
            Self::Legacy(store) => export!(old, store),
        }
    }
}

struct Node {
    endpoint: Endpoint,
    store: Store,
}

async fn start(id: u64, current: bool, state: Arc<NetworkState>, records: Option<Records>) -> Node {
    macro_rules! start {
        ($raft:ident, $mem:ident, $config:ty, $variant:ident) => {{
            use $raft::RaftStorage;
            let mut store = Arc::new($mem::MemStore::default());
            if let Some(records) = records {
                let vote: Option<$raft::Vote<u64>> = decode(&records.vote);
                if let Some(vote) = vote {
                    store.save_vote(&vote).await.unwrap();
                }
                let entries: Vec<$raft::Entry<$config>> = decode(&records.log);
                store.append_to_log(entries.iter().cloned()).await.unwrap();
                let applied = entries.iter().take_while(|entry| Some(entry.log_id.index) <= records.applied).count();
                store.apply_to_state_machine(&entries[..applied]).await.unwrap();
                assert_eq!(
                    records.machine,
                    serde_json::to_value(store.get_state_machine().await).unwrap()
                );
                assert_eq!(records.vote, encode(&store.read_vote().await.unwrap()));
                assert_eq!(records.log, encode(&entries));
            }
            let (log, machine) = $raft::storage::Adaptor::new(store.clone());
            let raft = $raft::Raft::new(
                id,
                Arc::new(
                    $raft::Config {
                        heartbeat_interval: 2_000,
                        election_timeout_min: 5_000,
                        election_timeout_max: 8_000,
                        ..Default::default()
                    }
                    .validate()
                    .unwrap(),
                ),
                Network {
                    source: id,
                    state: state.clone(),
                },
                log,
                machine,
            )
            .await
            .unwrap();
            Node {
                endpoint: Endpoint::$variant(raft),
                store: Store::$variant(store),
            }
        }};
    }
    let node = if current {
        start!(new, openraft_memstore, NewConfig, Current)
    } else {
        start!(old, legacy_memstore, OldConfig, Legacy)
    };
    state.nodes.lock().unwrap().insert(id, node.endpoint.clone());
    node
}

impl Endpoint {
    async fn initialize(&self) {
        let voters = [0, 1, 2].into_iter().collect::<std::collections::BTreeSet<_>>();
        match self {
            Self::Current(node) => node.initialize(voters).await.unwrap(),
            Self::Legacy(node) => node.initialize(voters).await.unwrap(),
        }
    }

    async fn wait_leader(&self) -> anyhow::Result<()> {
        match self {
            Self::Current(node) => {
                node.wait(Some(Duration::from_secs(26))).state(new::ServerState::Leader, "leader").await?;
                node.ensure_linearizable().await?;
            }
            Self::Legacy(node) => {
                node.wait(Some(Duration::from_secs(26))).state(old::ServerState::Leader, "leader").await?;
                node.ensure_linearizable().await?;
            }
        }
        Ok(())
    }

    async fn write(&self, client: &str) -> anyhow::Result<u64> {
        match self {
            Self::Current(node) => Ok(node
                .client_write(openraft_memstore::ClientRequest {
                    client: client.into(),
                    serial: 1,
                    status: "retained".into(),
                })
                .await?
                .log_id
                .index),
            Self::Legacy(node) => Ok(node
                .client_write(legacy_memstore::ClientRequest {
                    client: client.into(),
                    serial: 1,
                    status: "retained".into(),
                })
                .await?
                .log_id
                .index),
        }
    }

    async fn applied(&self, index: u64) -> anyhow::Result<()> {
        match self {
            Self::Current(node) => {
                node.wait(Some(Duration::from_secs(26))).applied_index(Some(index), "applied prefix").await?;
            }
            Self::Legacy(node) => {
                node.wait(Some(Duration::from_secs(26))).applied_index(Some(index), "applied prefix").await?;
            }
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn upgrade_preserves_divergent_logs_and_recovers_after_legacy_leader_loss() -> anyhow::Result<()> {
    replace_and_recover(true).await
}

#[tokio::test(start_paused = true)]
async fn downgrade_preserves_divergent_logs_and_recovers_after_upgraded_leader_loss() -> anyhow::Result<()> {
    replace_and_recover(false).await
}

async fn replace_and_recover(upgrade: bool) -> anyhow::Result<()> {
    let state = Arc::new(NetworkState::default());
    let mut nodes = Vec::new();
    for id in 0..3 {
        nodes.push(start(id, !upgrade, state.clone(), None).await);
    }
    nodes[0].endpoint.initialize().await;
    nodes[0].endpoint.wait_leader().await?;
    let prefix = nodes[0].endpoint.write("common-prefix").await?;
    for node in &nodes {
        node.endpoint.applied(prefix).await?;
    }
    state.lagging.store(true, Ordering::SeqCst);
    let acknowledged = nodes[0].endpoint.write("acknowledged-before-loss").await?;
    nodes[1].endpoint.applied(acknowledged).await?;
    assert_eq!(nodes[2].store.export().await.applied, Some(prefix));

    // Upgrade the fresh survivor, or downgrade the stale survivor, with the
    // divergent committed prefixes already established. No pre-fault catchup.
    let replaced = if upgrade { 1 } else { 2 };
    state.nodes.lock().unwrap().remove(&(replaced as u64));
    nodes[replaced].endpoint.shutdown().await;
    let records = nodes[replaced].store.export().await;
    nodes[replaced] = start(replaced as u64, upgrade, state.clone(), Some(records)).await;
    assert!(matches!(nodes[1].endpoint, Endpoint::Current(_)));
    assert!(matches!(nodes[2].endpoint, Endpoint::Legacy(_)));
    assert_eq!(nodes[1].store.export().await.applied, Some(acknowledged));
    assert_eq!(nodes[2].store.export().await.applied, Some(prefix));

    let lost_at = Instant::now();
    state.nodes.lock().unwrap().remove(&0);
    nodes[0].endpoint.shutdown().await;
    state.legacy_campaigns.store(0, Ordering::SeqCst);
    state.new_campaigns.store(0, Ordering::SeqCst);
    let recovered = tokio::time::timeout_at(lost_at + Duration::from_secs(26), async {
        nodes[1].endpoint.wait_leader().await?;
        let committed = nodes[1].endpoint.write("after-loss").await?;
        nodes[2].endpoint.applied(committed).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await;
    let elapsed = lost_at.elapsed();
    let endpoints = std::mem::take(&mut *state.nodes.lock().unwrap());
    for endpoint in endpoints.values() {
        endpoint.shutdown().await;
    }
    assert!(state.legacy_campaigns.load(Ordering::SeqCst) > 0);
    assert!(state.unsupported_inquiries.load(Ordering::SeqCst) > 0);
    assert!(
        recovered.as_ref().is_ok_and(|result| result.is_ok()),
        "replacement upgrade={upgrade} failed to recover within 26 seconds: {recovered:?}"
    );
    assert!(state.new_campaigns.load(Ordering::SeqCst) > 0);
    for node in &nodes[1..] {
        let records = node.store.export().await;
        for client in ["common-prefix", "acknowledged-before-loss", "after-loss"] {
            assert_eq!(records.machine["client_status"][client], "retained");
        }
    }
    println!("replacement upgrade={upgrade}: fresh authority, new commit and reconciliation in {elapsed:?}");
    Ok(())
}
