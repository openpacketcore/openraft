mod bounded_apply;

pub(crate) use bounded_apply::BoundedApply;

#[derive(Debug, Clone)]
#[derive(Default)]
pub(crate) struct CommandState {
    /// The sequence number of the last finished sm command.
    pub(crate) finished_sm_seq: u64,

    /// Scalar ranges only; entry and response ownership is one page at a time.
    pub(crate) bounded_apply: BoundedApply,
}
