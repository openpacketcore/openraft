[`RaftStateMachine`] serves as the core API for managing the state machine and
snapshot functionalities.
It directly ties the concepts of state management and snapshotting,
acknowledging that snapshots are often simply persisted states of the state
machine.


## Key Responsibilities

The [`RaftStateMachine`] encapsulates several critical responsibilities:

1. **Log Application**: It requires an implementation of the [`apply`] method,
   where the state machine processes and applies committed log entries.  This
   method is central to maintaining the state machine's integrity and ensuring
   that all state transitions are based on the replicated and committed log
   entries.

2. **Querying State and Snapshots**: [`applied_state`] allow querying the
   current state of the state machine.

3. **Snapshot Handling**: Through methods like [`get_snapshot_builder`],
   [`begin_receiving_snapshot`], [`get_current_snapshot`] and
   [`install_snapshot`] defines a comprehensive approach to managing snapshots.
   These methods cover creating snapshots, handling incoming snapshot data from
   the leader, and installing snapshots to bring the state machine to a specific
   state.


## State Management in Raft State Machines

- **State Reversion and Recovery**:
  The state machine in the Raft application is typically an in-memory component.
  Upon startup, the state machine may revert to a previous state. This setup is
  generally acceptable because the combination of a persistent snapshot and the
  Raft logs provides sufficient information to reconstruct the state. This process
  involves first rebuilding the state machine from the snapshot and then
  reapplying any logs that are not included in the snapshot.

  Afterwards, Raft log entries are applied to update the state machine to its
  current state.

- **Distinct Management of Membership and Normal Logs**:
  Within the state machine, the state of membership configuration logs and the
  state of normal logs are managed separately, though they are stored together.
  These can be thought of as two distinct sections.

- **Membership Config State Beyond the Last Applied**:
  It is acceptable for the membership to return with a log ID greater than the
  last applied log ID, provided that the corresponding Raft logs have not been
  purged and can thus be reapplied to the state machine. Upon startup, the most
  recent membership configuration is loaded by scanning the logs starting from the
  `last-applied-log-id`.


## Bounded apply pages

`Config::max_apply_entries` optionally bounds each runtime apply read. Unset
preserves the original behavior. When enabled, Openraft asks the storage
`limited_get_log_entries` reader for at most that many entries; the reader may
return a shorter nonempty prefix because of its byte limit. The legacy storage
`Adaptor` forwards this method to the underlying store. Storage must return
contiguous, complete entries and retain any per-operation deadline across pages.
An empty, oversized or noncontiguous page terminates the core with a storage
error before that page reaches the state machine.
If the state machine returns a different number of results than entries, the
worker reports a storage error and stops. Pending clients receive that error;
the core does not acknowledge an applied frontier for the invalid response.

The core retains one active committed range and one coalesced pending range,
loads one page, and consumes its ordered responses before loading another.
Partial pages advance the applied log frontier, but do not complete the whole
state-machine command. Every accepted entry still gets one ordered result.
Durable replication acknowledgements do not wait for application. Snapshot
building and receive preparation retain the full preceding apply range while
later append/vote persistence and independent responses can proceed in order.
Successor commits stay after that snapshot and coalesce into one scalar command
per existing state-machine, log-removal or explicit response-condition interval.
Coalescing retains the first start, latest end and latest command sequence, and
keeps the combined command after all of its persistence dependencies. This does
not bound the number of independently requested state-machine operations.
Snapshot installation, conflict truncations and unsatisfied response conditions
retain their ordering barriers. Their waits return to the normal event loop so
remaining pages can make progress. Physical log purge waits for completed apply
and every retired replication reader, while independent persistence and responses
can proceed. Releasing either owner alone does not permit early deletion.

Membership rebuild retires old replication tasks without waiting for their I/O.
The core owns all generations until their log readers and snapshot children have
returned. Leadership changes join those owners before destructive storage work;
normal and fatal shutdown join them before reporting the Shutdown state. A fatal
cause is published before cleanup. New and already waiting API calls observe that
failure without waiting for retained readers or responder owners, while a reply
already delivered remains usable. Application-defined receivers returned by
`client_write_ff` retain their own completion contract. Cancellation requests a
snapshot task to stop; only its completed join establishes that its data has been released. A local
snapshot storage error remains in the owned task result even when retirement has
closed its callback channel.

This option applies to runtime application. Startup recovery continues to use
its existing 64-entry chunks through `try_get_log_entries`, independently of
`max_apply_entries` and the runtime limited reader.

This option bounds apply entry and response populations, not process RSS.
Applications must separately bound individual entries/results, API admission,
cancelled accepted work, peer fan-out, transport, snapshots and storage caches.
It does not change the stored log or snapshot format, a storage durability
mode, election settings, operation deadlines, or replication payload limits.

[`RaftStateMachine`]:         `crate::storage::RaftStateMachine`
[`apply`]:                    `crate::storage::RaftStateMachine::apply`
[`applied_state`]:            `crate::storage::RaftStateMachine::applied_state`
[`get_snapshot_builder`]:     `crate::storage::RaftStateMachine::get_snapshot_builder`
[`begin_receiving_snapshot`]: `crate::storage::RaftStateMachine::begin_receiving_snapshot`
[`get_current_snapshot`]:     `crate::storage::RaftStateMachine::get_current_snapshot`
[`install_snapshot`]:         `crate::storage::RaftStateMachine::install_snapshot`
