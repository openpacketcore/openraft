# Planned voter shutdown

Stopping a leader without preparation leaves the surviving voters to detect its
failure using the normal election and leader-lease timers. Applications that need
to retire a voter while its replication transport is still available can use
[`Raft::prepare_shutdown`][prepare]. This does not change membership or shorten
failure-detection timers.

1. Stop admission at the application boundary and permanently disable reuse of
   any application-cached leader read proof. Account for previously accepted
   mutations through their original response or recovery path; do not replay them.
2. Call `prepare_shutdown(Some(successor))` with a different current voter.
   The engine decides the local role atomically. A leader stops new proposals,
   reads and heartbeats and returns a [`TransferLeaderRequest`][request]. A
   follower or candidate returns `None` and cancels any pending campaign. Neither
   future election triggers nor delayed vote grants can restore its candidacy.
3. Deliver a returned request through an authenticated consensus transport to the
   current voters and the intended successor. Authenticate the sender as the
   request's issuing leader and preserve the application's cluster and membership
   scope. Receivers call [`Raft::handle_leadership_transfer`][receive]. Delivering
   to the other surviving voters before the target avoids their old leader leases
   unnecessarily delaying its normal election. Delivery acknowledgements do not
   prove that the target became leader.
4. Keep the old replication listener and storage alive while observing fresh
   successor leadership and an engine quorum read proof. Check that the surviving
   deployment has enough available voters for its required service level. A proof
   obtained while the retiring voter is alive may include that voter's response;
   it does not by itself certify a quorum after that voter stops.
5. Remove the old listener, then call [`Raft::shutdown`][shutdown] to join the
   engine before closing its storage. Handoff initiation does not close storage.

The request binds the exact committed vote, a stable committed membership and all
log entries accepted before admission stopped. The successor must have applied
that entire prefix before it can campaign. It uses the ordinary election,
persistence and quorum code. A lagging target returns `LogNotApplied` without
releasing its lease or changing its vote; ordinary replication may catch it up
before the same request is delivered again under the original deadline. No
application mutation is resubmitted by the engine.

Other voters release only the exact issuing vote's election lease. An in-flight
heartbeat for that vote cannot restore the lease. A greater vote has ordinary
lease behavior. Stale votes, changed or uncommitted membership, learners, missing
targets and already retiring targets are rejected without those effects.

Retirement is irreversible for the lifetime of an engine instance. Cancelling the
API future after dispatch does not undo preparation. Calling it again retrieves
the same result, even after the node observes a successor vote; a different target
cannot replace a prepared handoff. Prepared nodes still replicate and vote for
other candidates until the application actually shuts them down. They cannot be
selected as handoff targets. Reopening a voter constructs a new Raft instance
under the application's existing storage and ownership rules.

[`Raft::begin_leadership_transfer`][begin] is a narrower leader-only operation.
It stops admission for the current leader vote but does not permanently retire
the node. Use `prepare_shutdown` for planned retirement rather than checking role
in the application and then calling the leader-only operation: a candidate can
win between that role check and shutdown.

[prepare]: crate::Raft::prepare_shutdown
[begin]: crate::Raft::begin_leadership_transfer
[receive]: crate::Raft::handle_leadership_transfer
[shutdown]: crate::Raft::shutdown
[request]: crate::raft::TransferLeaderRequest
