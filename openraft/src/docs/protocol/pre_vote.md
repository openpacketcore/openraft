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
log delays the next attempt. Any vote the requester accepts afterwards,
including a heartbeat from the current leader, ends the round.

Each round has its own identifier, which every response names. Rounds in one
term propose the same hypothetical vote, so the vote cannot tell a delayed
response from a current one; a response to any round but the one in flight is
ignored. A grant from a node that does not vote in the round never counts.

Only a response counts toward the quorum. An error, including an unreachable
peer, is never a grant, so a voter that is cut off cannot assemble one.

A voter that is the only voter wins its own Pre-Vote and elects at once.


## Retrying a round

A round that has not reached a quorum is held for the width of the
election-timeout window, `election_timeout_max - election_timeout_min`, and
then retried on a later tick while the election timer stays expired.

After an unplanned leader loss, a round fails while another voter's lease
still runs. Every survivor's lease runs out within the minimum election timeout
of the loss. A survivor starts its first round within the maximum election
timeout and a tick of its last leader contact, and each retry within the
window's width and a tick of the round before it. So the survivor with the most
up-to-date log starts a round that no lease rejects within the maximum election
timeout and a tick of the loss, the same bound as its first round. Holding a
rejected round for a whole sampled election timeout instead could nearly double
that.


## Voters that cannot answer Pre-Vote

A network may reach voters that cannot answer a Pre-Vote request, such as
voters of a release without Pre-Vote during a rolling upgrade. Such a voter
campaigns with the real vote alone and grants real votes by the rules above,
but it can never grant a Pre-Vote. Pre-Vote is therefore used only while every
voter that can be reached is positively known to answer it:

- [`RaftNetwork::pre_vote`](`crate::network::RaftNetwork::pre_vote`) sends the
  request and returns [`PreVoteReply::Answered`](`crate::raft::PreVoteReply`)
  only for a target that is positively known to answer Pre-Vote: the
  capability was negotiated on the connection to it, or it is an in-process
  peer that passes the request to
  [`Raft::pre_vote`](`crate::Raft::pre_vote`).
- For any other target that it can reach, it sends nothing and returns
  [`PreVoteReply::Unsupported`](`crate::raft::PreVoteReply`). The voter then
  ends the round and runs the classic election for this campaign at once:
  waiting for a Pre-Vote quorum that needs that target could take forever, for
  example when the target still believes it leads and never campaigns itself.
- A target that cannot be reached is an error. It can grant neither a Pre-Vote
  nor a vote, so it neither counts as a grant nor forces the classic election.

Each campaign decides again, so a voter set that no longer holds such a voter
uses Pre-Vote again.


## Networks without Pre-Vote

The default implementation of
[`RaftNetwork::pre_vote`](`crate::network::RaftNetwork::pre_vote`) returns
[`PreVoteReply::Unsupported`](`crate::raft::PreVoteReply`) without contacting
the target, so a network that does not implement it keeps the election
behavior without Pre-Vote: every campaign runs the classic election.
