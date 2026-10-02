# Pre-Vote

Pre-Vote is an optional round that a voter runs **before** it increments its
term and starts a real election. It asks whether a quorum *would* grant it a
vote, without changing any state. Only if a quorum would grant does the voter
start the real election.

It is disabled by default and enabled with
[`Config::enable_pre_vote`](`crate::Config::enable_pre_vote`).


## The problem it solves

A voter that cannot currently win an election, because it is cut off from the
other voters, was restarted, or is behind on the log, still increments its term
every time its election timer fires. When it can be reached again, that higher
term makes the healthy leader step down: the leader sees the higher term in an
AppendEntries response from the returning voter. The cluster then holds an
election it did not need.

The [leader lease](`crate::docs::protocol::replication::leader_lease`) already
keeps such a voter from *winning*: the other voters reject its vote requests
while their lease from the current leader is valid. Pre-Vote keeps its term
from climbing at all, so there is no higher term to depose the leader with.


## How it works

When the election timer fires and Pre-Vote is enabled, a voter of a cluster
with more than one voter sends a Pre-Vote request to the other voters: *would
you grant me a vote at `term + 1`?* The request carries that hypothetical vote
and the voter's last log id.

The round starts only when the voter's own leader lease has expired: while it
is valid, a leader is serving the voter and every voter applying the same rule
would reject the round.

A voter answers with the rules it uses for a real vote request:

- If it holds a committed vote whose
  [leader lease](`crate::docs::protocol::replication::leader_lease`) has not
  expired, it would not grant: a leader is serving it.
- If it is the leader and a quorum acknowledged it within the leader lease, it
  would not grant. A leader never renews the lease on its own vote, because
  acknowledgements renew it only on followers, so it uses this
  quorum-acknowledged lease instead. A real vote request is judged the same
  way, except that a planned leadership transfer releases the lease.
- If the requester's last log id is behind its own, it would not grant.
- If the requested vote is not greater than or equal to its own vote, it would
  not grant.
- Otherwise it would grant.

Unlike a real vote, a Pre-Vote request **persists nothing and changes
nothing**: no new term, no saved vote and no renewed lease. The requester does
not change its own state either; it stays a follower in its term until a quorum
would grant.

If a quorum would grant, the voter starts the real election, increments its
term and persists its vote as usual. A response with a strictly higher vote
catches the requester up to that vote in non-committed form, as a rejected real
vote does, unless it is a vote for the requester itself: no voter holds one for
a term the requester never campaigned in. A rejection that reports a greater
log delays the next attempt. Any
vote the requester accepts afterwards, including a heartbeat from the current
leader, ends the round. Otherwise the round is retried after one newly sampled
election timeout.

Only a response counts toward the quorum. An error, including an unreachable
peer, is never a grant, so a voter that is cut off cannot assemble one.

A voter that is the only voter wins its own Pre-Vote and elects at once.


## Voters that cannot answer Pre-Vote

A network may reach voters that cannot answer a Pre-Vote request, such as
voters of a release without Pre-Vote during a rolling upgrade. Such a voter
campaigns with the real vote alone and grants real votes by the rules above. A
network that knows a voter is one of them answers its Pre-Vote locally, as a
rejection that carries no vote to catch up to, and never sends it a request it
could not decode.

Counting such a voter as rejecting keeps a voter that runs Pre-Vote from
campaigning, and raising its term, before a voter without Pre-Vote that may
hold a more up-to-date log campaigns. That voter then wins with the real vote.
When its log is behind, it cannot win, and a voter whose log is more up to date
must campaign instead, although its own Pre-Vote may never reach a quorum:

- A voter that rejects a real vote request because the candidate's log is
  behind its own, while it has no leader, records that candidate's term.
- If it hears from no leader for one election timeout after the first such
  rejection, it campaigns without Pre-Vote, in a term above every recorded
  term. The candidate voted for itself in its own term, so a campaign in that
  term could not win its vote.
- The wait lets that candidate still win with the other voters' grants, without
  being disrupted.
- Hearing from a leader, granting a vote, leading or campaigning clears the
  record.

The rule changes no vote rule: a voter still grants only a candidate whose log
is at least as up to date as its own. It only decides when a voter campaigns.


## Networks without Pre-Vote

Pre-Vote uses the separate
[`RaftNetwork::pre_vote`](`crate::network::RaftNetwork::pre_vote`) RPC. Its
default implementation reports a granted Pre-Vote without contacting the
target, so a network that does not implement it keeps the election behavior
without Pre-Vote.
