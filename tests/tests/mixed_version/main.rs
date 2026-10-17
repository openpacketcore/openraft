//! Execute the preceding fork beside the candidate, with no PreVote shim.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use legacy_openraft as old;
use openraft as new;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::time::Instant;

type NewConfig = openraft_memstore::TypeConfig;
type OldConfig = legacy_memstore::TypeConfig;

#[derive(Clone)]
enum Endpoint {
    Current(new::Raft<NewConfig>),
    Legacy(old::Raft<OldConfig>),
}

fn encode(value: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn decode<T: DeserializeOwned>(value: &[u8]) -> T {
    serde_json::from_slice(value).unwrap()
}

fn unavailable(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

impl Endpoint {
    async fn append(&self, request: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Self::Current(node) => node.append_entries(decode(request)).await.map(|r| encode(&r)).map_err(unavailable),
            Self::Legacy(node) => node.append_entries(decode(request)).await.map(|r| encode(&r)).map_err(unavailable),
        }
    }

    async fn vote(&self, request: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Self::Current(node) => node.vote(decode(request)).await.map(|r| encode(&r)).map_err(unavailable),
            Self::Legacy(node) => node.vote(decode(request)).await.map(|r| encode(&r)).map_err(unavailable),
        }
    }

    async fn shutdown(&self) {
        match self {
            Self::Current(node) => node.shutdown().await.unwrap(),
            Self::Legacy(node) => node.shutdown().await.unwrap(),
        }
    }
}

#[derive(Default)]
struct NetworkState {
    nodes: Mutex<BTreeMap<u64, Endpoint>>,
    lagging: AtomicBool,
    unsupported_inquiries: AtomicUsize,
    legacy_campaigns: AtomicUsize,
    new_campaigns: AtomicUsize,
}

#[derive(Clone)]
struct Network {
    source: u64,
    state: Arc<NetworkState>,
}

struct Connection {
    network: Network,
    target: u64,
}

impl Connection {
    async fn endpoint(&self) -> io::Result<Endpoint> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let nodes = self.network.state.nodes.lock().unwrap();
        if !nodes.contains_key(&self.network.source) {
            return Err(unavailable("sender has stopped"));
        }
        nodes.get(&self.target).cloned().ok_or_else(|| unavailable("receiver has stopped"))
    }

    async fn append(&self, bytes: &[u8], has_entries: bool) -> io::Result<Vec<u8>> {
        if self.network.source == 0
            && self.target == 2
            && has_entries
            && self.network.state.lagging.load(Ordering::SeqCst)
        {
            return Err(unavailable("replication to the stale survivor is delayed"));
        }
        self.endpoint().await?.append(bytes).await
    }
}

// Both versions run their own real Raft engine. JSON only bridges their
// unchanged ordinary RPC types; the legacy implementation has no inquiry RPC.
macro_rules! network {
    ($raft:ident, $config:ty, $counter:ident, $($extra:tt)*) => {
        impl $raft::network::RaftNetworkFactory<$config> for Network {
            type Network = Connection;

            async fn new_client(&mut self, target: u64, _node: &()) -> Connection {
                Connection { network: self.clone(), target }
            }
        }

        impl $raft::network::RaftNetwork<$config> for Connection {
            async fn append_entries(
                &mut self,
                request: $raft::raft::AppendEntriesRequest<$config>,
                _option: $raft::network::RPCOption,
            ) -> Result<$raft::raft::AppendEntriesResponse<u64>, $raft::error::RPCError<u64, (), $raft::error::RaftError<u64>>> {
                self.append(&encode(&request), !request.entries.is_empty())
                    .await.map(|bytes| decode(&bytes))
                    .map_err(|error| $raft::error::Unreachable::new(&error).into())
            }

            async fn vote(
                &mut self,
                request: $raft::raft::VoteRequest<u64>,
                _option: $raft::network::RPCOption,
            ) -> Result<$raft::raft::VoteResponse<u64>, $raft::error::RPCError<u64, (), $raft::error::RaftError<u64>>> {
                self.network.state.$counter.fetch_add(1, Ordering::SeqCst);
                let response = async { self.endpoint().await?.vote(&encode(&request)).await }.await;
                response.map(|bytes| decode(&bytes))
                    .map_err(|error| $raft::error::Unreachable::new(&error).into())
            }

            async fn install_snapshot(
                &mut self,
                _request: $raft::raft::InstallSnapshotRequest<$config>,
                _option: $raft::network::RPCOption,
            ) -> Result<
                $raft::raft::InstallSnapshotResponse<u64>,
                $raft::error::RPCError<u64, (), $raft::error::RaftError<u64, $raft::error::InstallSnapshotError>>,
            > {
                panic!("the small rolling-election fixture must not transfer snapshots")
            }

            $($extra)*
        }
    };
}

network!(old, OldConfig, legacy_campaigns,);
network!(
    new,
    NewConfig,
    new_campaigns,
    async fn pre_vote(
        &mut self,
        request: new::raft::PreVoteRequest<u64>,
        _option: new::network::RPCOption,
    ) -> Result<new::raft::PreVoteResponse<u64>, new::error::RPCError<u64, (), new::error::RaftError<u64>>> {
        let response = async {
            match self.endpoint().await? {
                Endpoint::Current(node) => node.pre_vote(request).await.map_err(unavailable),
                Endpoint::Legacy(_) => {
                    self.network.state.unsupported_inquiries.fetch_add(1, Ordering::SeqCst);
                    Err(unavailable("the preceding fork has no PreVote RPC"))
                }
            }
        }
        .await;
        response.map_err(|error| new::error::Unreachable::new(&error).into())
    }
);

#[tokio::test(start_paused = true)]
async fn fresh_upgraded_survivor_can_elect_with_a_stale_legacy_survivor() -> anyhow::Result<()> {
    let state = Arc::new(NetworkState::default());
    let current_store = Arc::new(openraft_memstore::MemStore::default());
    let (log, machine) = new::storage::Adaptor::new(current_store.clone());
    let current = new::Raft::new(
        1,
        Arc::new(
            new::Config {
                heartbeat_interval: 2_000,
                election_timeout_min: 5_000,
                election_timeout_max: 8_000,
                ..Default::default()
            }
            .validate()?,
        ),
        Network {
            source: 1,
            state: state.clone(),
        },
        log,
        machine,
    )
    .await?;
    state.nodes.lock().unwrap().insert(1, Endpoint::Current(current.clone()));

    let mut legacy_nodes = Vec::new();
    let mut legacy_stores = Vec::new();
    for id in [0, 2] {
        let store = Arc::new(legacy_memstore::MemStore::default());
        let (log, machine) = old::storage::Adaptor::new(store.clone());
        let node = old::Raft::new(
            id,
            Arc::new(
                old::Config {
                    heartbeat_interval: 2_000,
                    election_timeout_min: 5_000,
                    election_timeout_max: 8_000,
                    ..Default::default()
                }
                .validate()?,
            ),
            Network {
                source: id,
                state: state.clone(),
            },
            log,
            machine,
        )
        .await?;
        state.nodes.lock().unwrap().insert(id, Endpoint::Legacy(node.clone()));
        legacy_nodes.push(node);
        legacy_stores.push(store);
    }
    let leader = &legacy_nodes[0];
    leader.initialize([0, 1, 2].into_iter().collect::<std::collections::BTreeSet<_>>()).await?;
    leader
        .wait(Some(Duration::from_secs(5)))
        .state(old::ServerState::Leader, "initial legacy leader")
        .await?;
    let prefix = leader
        .client_write(legacy_memstore::ClientRequest {
            client: "common-prefix".into(),
            serial: 1,
            status: "retained".into(),
        })
        .await?;
    current
        .wait(Some(Duration::from_secs(5)))
        .applied_index(Some(prefix.log_id.index), "common prefix")
        .await?;
    legacy_nodes[1]
        .wait(Some(Duration::from_secs(5)))
        .applied_index(Some(prefix.log_id.index), "common prefix")
        .await?;

    state.lagging.store(true, Ordering::SeqCst);
    let acknowledged = leader
        .client_write(legacy_memstore::ClientRequest {
            client: "acknowledged-before-loss".into(),
            serial: 1,
            status: "must-survive".into(),
        })
        .await?;
    current
        .wait(Some(Duration::from_secs(5)))
        .applied_index(Some(acknowledged.log_id.index), "fresh survivor")
        .await?;
    let fresh = current_store.get_state_machine().await;
    let stale = legacy_stores[1].get_state_machine().await;
    assert!(fresh.last_applied_log.unwrap().index > stale.last_applied_log.unwrap().index);
    assert_eq!(fresh.client_status["acknowledged-before-loss"], "must-survive");
    assert!(!stale.client_status.contains_key("acknowledged-before-loss"));

    let lost_at = Instant::now();
    let stopped = state.nodes.lock().unwrap().remove(&0).unwrap();
    stopped.shutdown().await;
    state.legacy_campaigns.store(0, Ordering::SeqCst);
    state.new_campaigns.store(0, Ordering::SeqCst);
    let deadline = lost_at + Duration::from_secs(26);
    let recovered = tokio::time::timeout_at(deadline, async {
        current
            .wait(Some(Duration::from_secs(26)))
            .state(new::ServerState::Leader, "mixed survivors elect")
            .await?;
        current.ensure_linearizable().await?;
        let committed = current
            .client_write(openraft_memstore::ClientRequest {
                client: "after-loss".into(),
                serial: 1,
                status: "committed".into(),
            })
            .await?;
        legacy_nodes[1]
            .wait(Some(Duration::from_secs(26)))
            .applied_index(Some(committed.log_id.index), "legacy survivor reconciles")
            .await?;
        Ok::<(), anyhow::Error>(())
    })
    .await;
    let legacy_campaigns = state.legacy_campaigns.load(Ordering::SeqCst);
    let new_campaigns = state.new_campaigns.load(Ordering::SeqCst);
    let unsupported = state.unsupported_inquiries.load(Ordering::SeqCst);
    let endpoints = std::mem::take(&mut *state.nodes.lock().unwrap());
    for endpoint in endpoints.values() {
        endpoint.shutdown().await;
    }
    assert!(legacy_campaigns > 0, "the actual previous fork sent ordinary votes");
    assert!(
        unsupported > 0,
        "the upgraded voter encountered a genuinely unsupported inquiry"
    );
    assert!(recovered.as_ref().is_ok_and(|result| result.is_ok()), "live mixed quorum failed to recover: legacy votes={legacy_campaigns}, upgraded votes={new_campaigns}, unsupported inquiries={unsupported}");
    recovered.unwrap()?;
    assert!(new_campaigns > 0, "the fresh voter must obtain real votes");
    for machine in [
        encode(&current_store.get_state_machine().await),
        encode(&legacy_stores[1].get_state_machine().await),
    ] {
        let machine: openraft_memstore::MemStoreStateMachine = decode(&machine);
        assert_eq!(machine.client_status["common-prefix"], "retained");
        assert_eq!(machine.client_status["acknowledged-before-loss"], "must-survive");
        assert_eq!(machine.client_status["after-loss"], "committed");
    }
    Ok(())
}
