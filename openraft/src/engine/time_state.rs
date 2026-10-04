use std::time::Duration;

#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct Config {
    /// The sampled time interval after which the next election will be initiated once the current
    /// lease has expired. A new interval is sampled whenever an election campaign begins.
    pub(crate) election_timeout: Duration,

    /// If this node has a smaller last-log-id than others, it will be less likely to be elected as
    /// a leader. In this case, it is necessary to sleep for a longer period of time
    /// `smaller_log_timeout` so that other nodes with a greater last-log-id have a chance to elect
    /// themselves.
    ///
    /// Note that this value should be greater than the `election_timeout` of every other node.
    pub(crate) smaller_log_timeout: Duration,

    /// The duration of an active leader's lease.
    ///
    /// When a follower or learner perceives an active leader, such as by receiving an AppendEntries
    /// message, it should not grant another candidate to become the leader during this period.
    ///
    /// It is the minimum election timeout, so every sampled `election_timeout` covers it.
    pub(crate) leader_lease: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(150),
            smaller_log_timeout: Duration::from_millis(600),
            leader_lease: Duration::from_millis(150),
        }
    }
}

impl Config {
    /// Return how long a voter waits after the last update of its vote before it campaigns.
    ///
    /// A committed vote carries the leader lease. The lease and the sampled election timeout both
    /// start at the vote's last update and run in parallel, so the voter waits for the longer of
    /// the two, never for their sum. While the lease is valid the voter also rejects other
    /// candidates. An uncommitted vote, such as a vote granted to a candidate, has no lease.
    pub(crate) fn election_wait(&self, leased: bool) -> Duration {
        if leased {
            std::cmp::max(self.leader_lease, self.election_timeout)
        } else {
            self.election_timeout
        }
    }
}
