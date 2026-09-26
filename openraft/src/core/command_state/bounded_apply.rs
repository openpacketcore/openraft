//! Constant-size bookkeeping for an opt-in paged committed prefix.
//!
//! The core owns one active range and one coalesced pending range. It sends only
//! one page to the state machine, then consumes that page's response before
//! loading another. Commands that order state-machine writes or remove logs
//! remain barriers in the existing engine output queue.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Range {
    seq: u64,
    next: u64,
    end: u64,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct BoundedApply {
    active: Option<Range>,
    pending: Option<Range>,
    in_flight_end: Option<u64>,
}

impl BoundedApply {
    pub(crate) fn is_pending(&self) -> bool {
        self.active.is_some()
    }

    /// Enqueue only a committed range; never store entries or per-entry metadata.
    /// Adjacent commits may coalesce only before another SM/log-removal barrier.
    pub(crate) fn enqueue(&mut self, seq: u64, since: u64, end: u64) -> Result<(), &'static str> {
        if since >= end {
            return Err("empty or reversed bounded apply range");
        }
        let next = Range { seq, next: since, end };
        match (self.active, self.pending) {
            (None, None) => self.active = Some(next),
            (Some(active), None) if active.end == since && active.seq < seq => self.pending = Some(next),
            (Some(_), Some(pending)) if pending.end == since && pending.seq < seq => {
                self.pending = Some(Range { seq, end, ..pending });
            }
            _ => return Err("noncontiguous or unordered bounded apply range"),
        }
        Ok(())
    }

    /// `None` while a page (including its response) is still owned by the worker.
    pub(crate) fn next_page(&self, limit: std::num::NonZeroU64) -> Option<(u64, u64, u64)> {
        if self.in_flight_end.is_some() {
            return None;
        }
        let range = self.active?;
        Some((
            range.seq,
            range.next,
            range.next.saturating_add(limit.get()).min(range.end),
        ))
    }

    /// The limited reader can return fewer entries because of its byte budget.
    pub(crate) fn sent_page(&mut self, end: u64) -> Result<(), &'static str> {
        let range = self.active.ok_or("missing bounded apply range")?;
        if self.in_flight_end.is_some() || end <= range.next || end > range.end {
            return Err("invalid bounded apply page dispatch");
        }
        self.in_flight_end = Some(end);
        Ok(())
    }

    /// Return a completed command sequence only after its complete range applies.
    /// The caller consumes every response before driving the next page.
    pub(crate) fn complete_page(&mut self, seq: u64, since: u64, end: u64) -> Result<Option<u64>, &'static str> {
        let range = self.active.ok_or("unexpected bounded apply completion")?;
        if range.seq != seq || range.next != since || self.in_flight_end != Some(end) {
            return Err("unordered bounded apply completion");
        }
        self.in_flight_end = None;
        if end == range.end {
            self.active = self.pending.take();
            Ok(Some(seq))
        } else {
            self.active = Some(Range { next: end, ..range });
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit() -> std::num::NonZeroU64 {
        std::num::NonZeroU64::new(64).unwrap()
    }

    #[test]
    fn bounded_apply_does_not_release_page_or_command_before_exact_completion() {
        let mut apply = BoundedApply::default();
        apply.enqueue(4, 20, 85).unwrap();
        assert_eq!(apply.next_page(limit()), Some((4, 20, 84)));
        // An actual byte-limited read may be shorter than the requested page.
        apply.sent_page(21).unwrap();
        assert_eq!(apply.next_page(limit()), None);
        assert!(apply.sent_page(22).is_err());
        for (seq, since, end) in [(5, 20, 21), (4, 19, 21), (4, 20, 22)] {
            assert!(apply.complete_page(seq, since, end).is_err());
            assert_eq!(apply.next_page(limit()), None);
        }
        assert_eq!(apply.complete_page(4, 20, 21).unwrap(), None);
        assert_eq!(apply.next_page(limit()), Some((4, 21, 85)));
        apply.sent_page(85).unwrap();
        assert_eq!(apply.complete_page(4, 21, 85).unwrap(), Some(4));
        assert!(!apply.is_pending());
        assert!(apply.complete_page(4, 21, 85).is_err());
    }

    #[test]
    fn bounded_apply_coalesces_a_long_backlog_without_storing_entries_or_ranges() {
        let mut apply = BoundedApply::default();
        apply.enqueue(1, 0, 1).unwrap();
        apply.sent_page(1).unwrap();
        for index in 1..4096 {
            apply.enqueue(index + 1, index, index + 1).unwrap();
        }
        assert_eq!(
            apply.pending,
            Some(Range {
                seq: 4096,
                next: 1,
                end: 4096
            })
        );
        assert_eq!(apply.next_page(limit()), None);
        assert_eq!(apply.complete_page(1, 0, 1).unwrap(), Some(1));
        let mut next = 1;
        let mut complete = None;
        while let Some((seq, since, end)) = apply.next_page(limit()) {
            assert_eq!(since, next);
            assert!(end - since <= 64);
            apply.sent_page(end).unwrap();
            complete = apply.complete_page(seq, since, end).unwrap();
            next = end;
            assert_eq!(complete.is_some(), end == 4096);
        }
        assert_eq!((next, complete), (4096, Some(4096)));
        assert!(!apply.is_pending());
    }

    #[test]
    fn bounded_apply_rejects_gaps_overlap_and_sequence_reuse_without_changing_work() {
        let mut apply = BoundedApply::default();
        assert!(apply.enqueue(1, 0, 0).is_err());
        assert!(apply.enqueue(1, 2, 1).is_err());
        apply.enqueue(4, 0, 64).unwrap();
        for (seq, since, end) in [(4, 64, 65), (5, 63, 65), (5, 65, 66)] {
            assert!(apply.enqueue(seq, since, end).is_err());
            assert!(apply.pending.is_none());
        }
        apply.enqueue(5, 64, 65).unwrap();
        assert!(apply.enqueue(5, 65, 66).is_err());
        assert_eq!(
            apply.pending,
            Some(Range {
                seq: 5,
                next: 64,
                end: 65
            })
        );
        assert_eq!(apply.next_page(limit()), Some((4, 0, 64)));
    }

    #[test]
    fn bounded_apply_last_page_uses_checked_domain_without_overflow() {
        let mut apply = BoundedApply::default();
        apply.enqueue(1, u64::MAX - 1, u64::MAX).unwrap();
        assert_eq!(apply.next_page(limit()), Some((1, u64::MAX - 1, u64::MAX)));
        apply.sent_page(u64::MAX).unwrap();
        assert_eq!(apply.complete_page(1, u64::MAX - 1, u64::MAX).unwrap(), Some(1));
    }

    #[test]
    fn bounded_apply_configuration_is_opt_in_and_rejects_zero() {
        assert_eq!(crate::Config::default().max_apply_entries, None);
        assert_eq!(
            crate::Config::build(&["test", "--max-apply-entries", "64"]).unwrap().max_apply_entries,
            Some(limit())
        );
        assert!(crate::Config::build(&["test", "--max-apply-entries", "0"]).is_err());
    }
}
