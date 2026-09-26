use std::collections::VecDeque;

use crate::core::sm::CommandSeq;
use crate::engine::Command;
use crate::RaftTypeConfig;

/// The entry of output from Engine to the runtime.
#[derive(Debug, Default)]
pub(crate) struct EngineOutput<C>
where C: RaftTypeConfig
{
    /// A Engine level sequence number for identifying a command.
    pub(crate) seq: CommandSeq,

    /// Command queue that need to be executed by `RaftRuntime`.
    pub(crate) commands: VecDeque<Command<C>>,
}

impl<C> EngineOutput<C>
where C: RaftTypeConfig
{
    /// Generate the next command seq of an sm::Command.
    pub(crate) fn next_sm_seq(&mut self) -> CommandSeq {
        self.seq += 1;
        self.seq
    }

    /// Get the last used sm::Command seq
    pub(crate) fn last_sm_seq(&self) -> CommandSeq {
        self.seq
    }

    pub(crate) fn new(command_buffer_size: usize) -> Self {
        Self {
            seq: 0,
            commands: VecDeque::with_capacity(command_buffer_size),
        }
    }

    /// Push a command to the queue.
    pub(crate) fn push_command(&mut self, mut cmd: Command<C>) {
        tracing::debug!("push command: {:?}", cmd);

        match &mut cmd {
            Command::StateMachine { command } => {
                let seq = self.next_sm_seq();
                tracing::debug!("next_seq: {}", seq);
                command.set_seq(seq);
            }
            Command::BecomeLeader => {}
            Command::QuitLeader => {}
            Command::AppendInputEntries { .. } => {}
            Command::ReplicateCommitted { .. } => {}
            Command::Commit { .. } => {}
            Command::Replicate { .. } => {}
            Command::RebuildReplicationStreams { .. } => {}
            Command::SaveVote { .. } => {}
            Command::SendVote { .. } => {}
            Command::PurgeLog { .. } => {}
            Command::DeleteConflictLog { .. } => {}
            Command::Respond { .. } => {}
        }
        self.commands.push_back(cmd)
    }

    /// Put back a command at its original position in the queue.
    ///
    /// This will be used when the command is not ready to be executed.
    pub(crate) fn postpone_command(&mut self, index: usize, cmd: Command<C>) {
        tracing::debug!("postpone command: {:?}", cmd);
        self.commands.insert(index, cmd)
    }

    /// Take the next command to run, leaving any earlier deferred commands in place.
    pub(crate) fn pop_command(&mut self, index: usize) -> Option<Command<C>> {
        self.commands.remove(index)
    }

    /// Coalesce deferred commits without crossing an ordering barrier.
    ///
    /// The combined range stays at the latest commit's position, after all its
    /// append/vote dependencies. Keep one scalar range per SM/log/condition
    /// interval, even if a blocked snapshot receives many successor commits.
    /// Independently requested SM operations still occupy their existing queue;
    /// this is not a global bound on that queue or on API admission.
    pub(crate) fn coalesce_commits(&mut self) {
        let mut previous = None;
        let mut index = 0;
        while index < self.commands.len() {
            match &self.commands[index] {
                Command::Commit {
                    seq,
                    already_committed,
                    upto,
                } => {
                    if let Some(prior) = previous {
                        let merge = matches!(
                            &self.commands[prior],
                            Command::Commit { seq: old_seq, upto: old_upto, .. }
                                if old_seq < seq
                                    && already_committed.as_ref() == Some(old_upto)
                                    && old_upto < upto
                                    && old_upto.index < upto.index
                        );
                        if merge {
                            let Some(Command::Commit { already_committed, .. }) = self.commands.remove(prior) else {
                                unreachable!("previous commit was inspected above");
                            };
                            index -= 1;
                            if let Command::Commit {
                                already_committed: start,
                                ..
                            } = &mut self.commands[index]
                            {
                                *start = already_committed;
                            }
                        }
                    }
                    previous = Some(index);
                }
                Command::StateMachine { .. }
                | Command::PurgeLog { .. }
                | Command::DeleteConflictLog { .. }
                | Command::Respond { when: Some(_), .. } => previous = None,
                _ => {}
            }
            index += 1;
        }
    }

    /// Iterate all queued commands.
    pub(crate) fn iter_commands(&self) -> impl Iterator<Item = &Command<C>> {
        self.commands.iter()
    }

    /// Take all queued commands and clear the queue.
    #[cfg(test)]
    pub(crate) fn take_commands(&mut self) -> Vec<Command<C>> {
        self.commands.drain(..).collect()
    }

    /// Clear all queued commands.
    #[cfg(test)]
    pub(crate) fn clear_commands(&mut self) {
        self.commands.clear()
    }
}

#[cfg(test)]
mod bounded_apply_tests {
    use super::*;
    use crate::core::sm;
    use crate::engine::testing::UTConfig;
    use crate::engine::Condition;
    use crate::engine::Respond;
    use crate::raft::AppendEntriesResponse;
    use crate::testing::log_id;
    use crate::type_config::TypeConfigExt;
    use crate::Entry;
    use crate::EntryPayload;
    use crate::Snapshot;
    use crate::SnapshotMeta;
    use crate::Vote;

    fn commit(seq: u64, since: u64, end: u64) -> Command<UTConfig> {
        Command::Commit {
            seq,
            already_committed: since.checked_sub(1).map(|i| log_id(1, 1, i)),
            upto: log_id(1, 1, end - 1),
        }
    }

    #[test]
    fn bounded_apply_deferred_commits_remain_constant_size() {
        let mut output = EngineOutput::<UTConfig>::default();
        output.push_command(sm::Command::build_snapshot().into());
        for i in 1..=4096 {
            output.push_command(commit(i + 1, i, i + 1));
            output.coalesce_commits();
            assert_eq!(
                output.commands.len(),
                2,
                "BOUNDED_APPLY_DEFERRED_RANGE: one marker retains one successor range"
            );
        }
        assert_eq!(output.commands[1], commit(4097, 1, 4097));
    }

    #[test]
    fn bounded_apply_coalesced_commit_stays_after_its_persistence() {
        let mut output = EngineOutput::<UTConfig>::default();
        output.push_command(commit(1, 0, 1));
        output.push_command(Command::SaveVote {
            vote: Vote::new_committed(2, 1),
        });
        output.push_command(Command::AppendInputEntries {
            vote: Vote::new_committed(2, 1),
            entries: vec![Entry {
                log_id: log_id(1, 1, 1),
                payload: EntryPayload::Blank,
            }],
        });
        output.push_command(commit(2, 1, 2));
        output.coalesce_commits();
        assert!(matches!(output.commands[0], Command::SaveVote { .. }));
        assert!(matches!(output.commands[1], Command::AppendInputEntries { .. }));
        assert_eq!(output.commands[2], commit(2, 0, 2));
    }

    #[test]
    fn bounded_apply_commit_coalescing_keeps_ordering_barriers() {
        let (receive, _) = UTConfig::oneshot();
        let (respond, _) = UTConfig::oneshot();
        let barriers = vec![
            sm::Command::build_snapshot().into(),
            sm::Command::begin_receiving_snapshot(receive).into(),
            sm::Command::install_full_snapshot(Snapshot {
                meta: SnapshotMeta::default(),
                snapshot: Box::default(),
            })
            .into(),
            Command::PurgeLog { upto: log_id(1, 1, 0) },
            Command::DeleteConflictLog { since: log_id(1, 1, 2) },
            Command::Respond {
                when: Some(Condition::StateMachineCommand { command_seq: 1 }),
                resp: Respond::new(Ok(AppendEntriesResponse::Success), respond),
            },
        ];
        for barrier in barriers {
            let mut output = EngineOutput::<UTConfig>::default();
            output.push_command(commit(1, 0, 1));
            output.push_command(barrier);
            output.push_command(commit(3, 1, 2));
            output.coalesce_commits();
            assert_eq!(output.commands.len(), 3);
            assert_eq!(output.commands[0], commit(1, 0, 1));
            assert_eq!(output.commands[2], commit(3, 1, 2));
        }
    }

    #[test]
    fn bounded_apply_commit_coalescing_rejects_gaps_overlap_and_sequence_reuse() {
        for next in [commit(2, 2, 3), commit(2, 0, 2), commit(1, 1, 2), commit(2, 1, 1)] {
            let mut output = EngineOutput::<UTConfig>::default();
            output.push_command(commit(1, 0, 1));
            output.push_command(next);
            output.coalesce_commits();
            assert_eq!(output.commands.len(), 2);
            assert_eq!(output.commands[0], commit(1, 0, 1));
        }
    }
}
