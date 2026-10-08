use std::collections::BTreeMap;
use std::sync::Arc;

use crate::NodeId;

/// A process-local receipt for one remote peer response fence.
///
/// Created by [`Raft::fence_peer_responses`](crate::Raft::fence_peer_responses).
/// Cloning the receipt does not create another fence. A receipt can release only
/// its own, still-current fence in the same Raft instance. It is deliberately
/// neither serializable nor constructible by callers.
#[derive(Clone, Debug)]
pub struct PeerResponseFence<NID: NodeId> {
    pub(crate) peer: NID,
    pub(crate) token: ResponseGeneration,
}

impl<NID: NodeId> PeerResponseFence<NID> {
    /// The remote peer whose responses this receipt fenced.
    pub fn peer_id(&self) -> &NID {
        &self.peer
    }
}

/// A peer response fence can only exclude a remote peer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PeerResponseFenceError {
    /// Local storage acknowledgements must remain authoritative.
    #[error("cannot fence responses from the local node")]
    LocalNode,
}

/// Allocation identity cannot wrap or be reused while an old result retains it.
#[derive(Clone, Debug, Default)]
pub(crate) struct ResponseGeneration(Arc<()>);

impl ResponseGeneration {
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Owned and mutated only by RaftCore, in the same serialized loop as results.
#[derive(Debug)]
pub(crate) struct PeerAdmission<NID: NodeId> {
    pub(crate) generation: ResponseGeneration,
    closed: BTreeMap<NID, ResponseGeneration>,
}

impl<NID: NodeId> Default for PeerAdmission<NID> {
    fn default() -> Self {
        Self {
            generation: ResponseGeneration::default(),
            closed: BTreeMap::new(),
        }
    }
}

impl<NID: NodeId> PeerAdmission<NID> {
    pub(crate) fn allows(&self, peer: &NID) -> bool {
        !self.closed.contains_key(peer)
    }

    pub(crate) fn accepts(&self, peer: &NID, generation: &ResponseGeneration) -> bool {
        self.allows(peer) && self.generation.same_as(generation)
    }

    pub(crate) fn fence(&mut self, peer: NID) -> PeerResponseFence<NID> {
        let token = ResponseGeneration::default();
        self.closed.insert(peer.clone(), token.clone());
        self.generation = ResponseGeneration::default();
        PeerResponseFence { peer, token }
    }

    pub(crate) fn release(&mut self, fence: &PeerResponseFence<NID>) -> bool {
        if !self.closed.get(&fence.peer).is_some_and(|token| token.same_as(&fence.token)) {
            return false;
        }
        self.closed.remove(&fence.peer);
        self.generation = ResponseGeneration::default();
        true
    }
}
