use std::collections::BTreeMap;
use std::sync::Arc;

use crate::type_config::alias::InstantOf;
use crate::EffectiveMembership;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::RaftTypeConfig;
use crate::Vote;

/// The last accepted replication-stream response from one remote process.
///
/// This is volatile observation data, separate from Raft's matching and quorum
/// accounting. It does not itself establish leadership or application readiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationObservation<C: RaftTypeConfig> {
    /// Opaque process identity supplied with the accepted response.
    pub runtime_id: [u8; 16],

    /// Leader-local monotonic instant at which that response's request was sent.
    ///
    /// Compute its age when constructing the application reply, after any quorum
    /// confirmation. Do not transmit this instant or compare it with another host.
    pub sent_at: InstantOf<C>,

    /// Prefix positively confirmed by this process in this leadership context.
    ///
    /// `None` means no positive proof; `Some(None)` confirms an empty prefix.
    /// `Some(Some(log_id))` confirms that log and its predecessors.
    pub matching: Option<Option<LogId<C::NodeId>>>,

    /// Fixed catch-up target sampled before the first response updates progress.
    ///
    /// This is the larger of the committed log and the leader's first own-term
    /// entry, following the engine's read-index rule. Later commits do not move it.
    pub target: Option<LogId<C::NodeId>>,
}

/// One serialized capture of a leader's replication observations and Raft state.
///
/// Missing peers are unobserved. Use `membership` to distinguish voters in each
/// joint configuration from learners; the local leader has no remote observation.
/// All observations are discarded when leadership changes.
///
/// This capture alone is not a linearizable leadership proof. After capturing it,
/// run a fresh [`Raft::get_read_log_id()`](crate::Raft::get_read_log_id) quorum
/// round and recheck the same local leader and vote, under the caller's deadline.
/// Engine observations do not require a local state-machine apply wait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationReadback<C: RaftTypeConfig> {
    /// The local leader's committed vote at capture.
    pub vote: Vote<C::NodeId>,

    /// Effective membership, including the membership log ID and all voter roles.
    pub membership: Arc<EffectiveMembership<C::NodeId, C::Node>>,

    /// Current commit position at capture; it is not a moving catch-up target.
    pub committed: Option<LogId<C::NodeId>>,

    /// Accepted, currently visible observations, keyed by remote Raft node ID.
    pub observations: BTreeMap<C::NodeId, ReplicationObservation<C>>,
}

/// A fence hides evidence but retains the current runtime's fixed target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PeerParticipation<C: RaftTypeConfig> {
    observation: ReplicationObservation<C>,
    visible: bool,
}

impl<C: RaftTypeConfig> PeerParticipation<C> {
    pub(crate) fn new(runtime_id: [u8; 16], sent_at: InstantOf<C>, target: Option<LogId<C::NodeId>>) -> Self {
        Self {
            observation: ReplicationObservation {
                runtime_id,
                sent_at,
                matching: None,
                target,
            },
            visible: true,
        }
    }

    pub(crate) fn observe(
        &mut self,
        runtime_id: [u8; 16],
        sent_at: InstantOf<C>,
        target: Option<LogId<C::NodeId>>,
        result: &Result<Option<LogId<C::NodeId>>, LogId<C::NodeId>>,
    ) {
        // Replication-stream appends are serialized. This guard additionally
        // rejects older-send callbacks without changing ordinary Raft handling.
        if sent_at < self.observation.sent_at {
            return;
        }
        if self.observation.runtime_id != runtime_id {
            *self = Self::new(runtime_id, sent_at, target);
        }
        self.observation.sent_at = sent_at;
        self.visible = true;
        match result {
            Ok(matching) => {
                let confirmed = self.observation.matching.get_or_insert(None);
                if matching.index() > confirmed.index() {
                    *confirmed = matching.clone();
                }
            }
            Err(conflict) => {
                if self
                    .observation
                    .matching
                    .as_ref()
                    .and_then(Option::as_ref)
                    .is_some_and(|confirmed| conflict.index <= confirmed.index)
                {
                    self.observation.matching = None;
                }
            }
        }
    }

    pub(crate) fn fence(&mut self) {
        self.visible = false;
        self.observation.matching = None;
    }

    pub(crate) fn visible(&self) -> Option<&ReplicationObservation<C>> {
        self.visible.then_some(&self.observation)
    }
}
