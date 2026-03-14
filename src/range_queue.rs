//! Range queue for adaptive stream transfer. Coordinator owns the queue; workers pop ranges
//! and report completion or requeue on failure. Enables many ranges with fewer streams and scale-up.

use crate::base::FileRange;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Identifies a stream worker (index used for TCP port or stream slot).
pub type WorkerId = usize;

/// Per-chunk (or per-interval) report from a worker for bandwidth estimation and stall detection.
#[derive(Debug, Clone)]
pub struct StreamReport {
    pub worker_id: WorkerId,
    pub bytes_this_interval: u64,
    pub elapsed_ms: u64,
}

/// Shared queue of file ranges. Workers pop from pending, mark in_flight, then complete or requeue.
pub struct RangeQueue {
    pending: Mutex<VecDeque<FileRange>>,
    in_flight: Mutex<HashMap<WorkerId, FileRange>>,
}

impl RangeQueue {
    /// Build queue from the list of ranges still to transfer (e.g. checkpoint.get_missing_ranges).
    pub fn new(ranges: Vec<FileRange>) -> Self {
        let pending = Mutex::new(ranges.into_iter().collect());
        let in_flight = Mutex::new(HashMap::new());
        Self { pending, in_flight }
    }

    /// Take the next range to transfer. Returns None when pending is empty.
    pub fn pop(&self) -> Option<FileRange> {
        self.pending.lock().unwrap().pop_front()
    }

    /// Record that worker `id` is now transferring `range`. Call after pop().
    pub fn mark_in_flight(&self, id: WorkerId, range: FileRange) {
        self.in_flight.lock().unwrap().insert(id, range);
    }

    /// Worker finished the range successfully. Removes from in_flight.
    pub fn complete(&self, id: WorkerId) {
        self.in_flight.lock().unwrap().remove(&id);
    }

    /// Worker failed or stalled: put this worker's range back on pending and remove from in_flight.
    pub fn requeue(&self, id: WorkerId) {
        let _ = self.requeue_returning_range(id);
    }

    /// Like requeue but returns the range that was requeued (for coordinator to mark stale completions).
    pub fn requeue_returning_range(&self, id: WorkerId) -> Option<FileRange> {
        let mut in_flight = self.in_flight.lock().unwrap();
        let range = in_flight.remove(&id);
        drop(in_flight);
        if let Some(r) = range {
            self.pending.lock().unwrap().push_front(r);
            Some(r)
        } else {
            None
        }
    }

    /// True when there is no work left: pending empty and no in-flight ranges.
    pub fn is_done(&self) -> bool {
        let pending = self.pending.lock().unwrap();
        let in_flight = self.in_flight.lock().unwrap();
        pending.is_empty() && in_flight.is_empty()
    }

    /// Number of workers that have a range in flight (for coordinator scale-up / reaping).
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.lock().unwrap().len()
    }
}
