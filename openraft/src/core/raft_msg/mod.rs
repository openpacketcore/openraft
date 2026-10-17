use std::collections::BTreeMap;

use crate::core::raft_msg::external_command::ExternalCommand;
use crate::error::CheckIsLeaderError;
use crate::error::Infallible;
use crate::error::InitializeError;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendEntriesResponse;
use crate::raft::BoxCoreFn;
use crate::raft::PreVoteRequest;
use crate::raft::PreVoteResponse;
use crate::raft::SnapshotResponse;
use crate::raft::TransferLeaderError;
use crate::raft::TransferLeaderRequest;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::NodeIdOf;
use crate::type_config::alias::NodeOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::ResponderOf;
use crate::type_config::alias::SnapshotDataOf;
use crate::ChangeMembers;
use crate::MessageSummary;
use crate::PeerResponseFence;
use crate::PeerResponseFenceError;
use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::Vote;

pub(crate) mod external_command;

/// A oneshot TX to send result from `RaftCore` to external caller, e.g. `Raft::append_entries`.
pub(crate) type ResultSender<C, T, E = Infallible> = OneshotSenderOf<C, Result<T, E>>;

/// TX for Vote Response
pub(crate) type VoteTx<C> = ResultSender<C, VoteResponse<NodeIdOf<C>>>;

/// TX for a read-only speculative vote response.
pub(crate) type PreVoteTx<C> = ResultSender<C, PreVoteResponse<NodeIdOf<C>>>;

/// TX for Append Entries Response
pub(crate) type AppendEntriesTx<C> = ResultSender<C, AppendEntriesResponse<NodeIdOf<C>>>;

/// Linearizable read result, published by the serialized core.
pub(crate) type ClientReadResult<C> =
    Result<(Option<LogIdOf<C>>, Option<LogIdOf<C>>), CheckIsLeaderError<NodeIdOf<C>, NodeOf<C>>>;

/// TX for Linearizable Read Response
pub(crate) type ClientReadTx<C> = OneshotSenderOf<C, ClientReadResult<C>>;

/// A message sent by application to the [`RaftCore`].
///
/// [`RaftCore`]: crate::core::RaftCore
pub(crate) enum RaftMsg<C>
where C: RaftTypeConfig
{
    FencePeerResponses {
        peer: C::NodeId,
        tx: ResultSender<C, PeerResponseFence<C::NodeId>, PeerResponseFenceError>,
    },

    ReleasePeerResponseFence {
        fence: PeerResponseFence<C::NodeId>,
        tx: ResultSender<C, bool>,
    },

    AppendEntries {
        rpc: AppendEntriesRequest<C>,
        tx: AppendEntriesTx<C>,
    },

    RequestVote {
        rpc: VoteRequest<C::NodeId>,
        tx: VoteTx<C>,
    },

    PreVote {
        rpc: PreVoteRequest<C::NodeId>,
        tx: PreVoteTx<C>,
    },

    InstallFullSnapshot {
        vote: Vote<C::NodeId>,
        snapshot: Snapshot<C>,
        tx: ResultSender<C, SnapshotResponse<C::NodeId>>,
    },

    /// Begin receiving a snapshot from the leader.
    ///
    /// Returns a snapshot data handle for receiving data.
    ///
    /// It does not check [`Vote`] because it is a read operation
    /// and does not break raft protocol.
    BeginReceivingSnapshot {
        tx: ResultSender<C, Box<SnapshotDataOf<C>>, Infallible>,
    },

    ClientWriteRequest {
        app_data: C::D,
        tx: ResponderOf<C>,
    },

    CheckIsLeaderRequest {
        tx: ClientReadTx<C>,
    },

    Initialize {
        members: BTreeMap<C::NodeId, C::Node>,
        tx: ResultSender<C, (), InitializeError<C::NodeId, C::Node>>,
    },

    ChangeMembership {
        changes: ChangeMembers<C::NodeId, C::Node>,

        /// If `retain` is `true`, then the voters that are not in the new
        /// config will be converted into learners, otherwise they will be removed.
        retain: bool,

        tx: ResponderOf<C>,
    },

    BeginLeadershipTransfer {
        to: C::NodeId,
        tx: ResultSender<C, TransferLeaderRequest<C::NodeId>, TransferLeaderError>,
    },

    PrepareShutdown {
        to: Option<C::NodeId>,
        tx: ResultSender<C, Option<TransferLeaderRequest<C::NodeId>>, TransferLeaderError>,
    },

    HandleLeadershipTransfer {
        request: TransferLeaderRequest<C::NodeId>,
        tx: ResultSender<C, (), TransferLeaderError>,
    },

    ReleaseLeaderLease {
        sender: C::NodeId,
        vote: Vote<C::NodeId>,
        membership_log_id: Option<LogIdOf<C>>,
        tx: ResultSender<C, bool>,
    },

    ElectIfLeaderResigned {
        sender: C::NodeId,
        vote: Vote<C::NodeId>,
        membership_log_id: Option<LogIdOf<C>>,
        tx: ResultSender<C, bool>,
    },

    ExternalCoreRequest {
        req: BoxCoreFn<C>,
    },

    ExternalCommand {
        cmd: ExternalCommand<C>,
    },
}

impl<C> MessageSummary<RaftMsg<C>> for RaftMsg<C>
where C: RaftTypeConfig
{
    fn summary(&self) -> String {
        match self {
            RaftMsg::FencePeerResponses { peer, .. } => format!("FencePeerResponses: {}", peer),
            RaftMsg::ReleasePeerResponseFence { fence, .. } => format!("ReleasePeerResponseFence: {}", fence.peer),
            RaftMsg::AppendEntries { rpc, .. } => {
                format!("AppendEntries: {}", rpc.summary())
            }
            RaftMsg::RequestVote { rpc, .. } => {
                format!("RequestVote: {}", rpc.summary())
            }
            RaftMsg::PreVote { rpc, .. } => format!("PreVote: {}", rpc.summary()),
            RaftMsg::BeginReceivingSnapshot { .. } => "BeginReceivingSnapshot".to_string(),
            RaftMsg::InstallFullSnapshot { vote, snapshot, .. } => {
                format!("InstallFullSnapshot: vote: {}, snapshot: {}", vote, snapshot)
            }
            RaftMsg::ClientWriteRequest { .. } => "ClientWriteRequest".to_string(),
            RaftMsg::CheckIsLeaderRequest { .. } => "CheckIsLeaderRequest".to_string(),
            RaftMsg::Initialize { members, .. } => {
                format!("Initialize: {:?}", members)
            }
            RaftMsg::ChangeMembership {
                changes: members,
                retain,
                ..
            } => {
                format!("ChangeMembership: members: {:?}, retain: {}", members, retain,)
            }
            RaftMsg::BeginLeadershipTransfer { .. } => "BeginLeadershipTransfer".to_string(),
            RaftMsg::PrepareShutdown { .. } => "PrepareShutdown".to_string(),
            RaftMsg::HandleLeadershipTransfer { .. } => "HandleLeadershipTransfer".to_string(),
            RaftMsg::ReleaseLeaderLease { .. } => "ReleaseLeaderLease".to_string(),
            RaftMsg::ElectIfLeaderResigned { .. } => "ElectIfLeaderResigned".to_string(),
            RaftMsg::ExternalCoreRequest { .. } => "External Request".to_string(),
            RaftMsg::ExternalCommand { cmd } => {
                format!("ExternalCommand: {:?}", cmd)
            }
        }
    }
}
