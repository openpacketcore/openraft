#![cfg_attr(feature = "bt", feature(error_generic_member_access))]
#![cfg_attr(feature = "bt", allow(unused_features))]

#[macro_use]
#[path = "../fixtures/mod.rs"]
mod fixtures;

// The number indicate the preferred running order for these case.
// The later tests may depend on the earlier ones.

mod t10_elect_compare_last_log;
mod t11_elect_seize_leadership;
mod t13_elect_while_leader;
mod t20_lease_and_timeout_overlap;
mod t21_pre_vote;
mod t22_one_way_isolation;
mod t23_classic_voter_stale_log;
mod t24_classic_leader_one_way;
mod t25_rejected_pre_vote_retry;
mod t26_delayed_pre_vote_replies;
mod t27_fresher_classic_candidate;
