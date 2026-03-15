//! Range queue for adaptive stream transfer. Coordinator owns the queue; workers pop ranges
//! and report completion or requeue on failure. Single mutex guards both pending and in_flight
//! so pop_and_mark is atomic and no two workers can get the same range.

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

struct Inner {
    pending: VecDeque<FileRange>,
    in_flight: HashMap<WorkerId, FileRange>,
}

pub struct RangeQueue(Mutex<Inner>);

impl RangeQueue {
    pub fn new(ranges: Vec<FileRange>) -> Self {
        Self(Mutex::new(Inner {
            pending: ranges.into_iter().collect(),
            in_flight: HashMap::new(),
        }))
    }

    /// Atomically pop the next range and mark it in-flight for worker `id`. Single lock.
    pub fn pop_and_mark(&self, id: WorkerId) -> Option<FileRange> {
        let mut inner = self.0.lock().unwrap();
        let range = inner.pending.pop_front()?;
        inner.in_flight.insert(id, range);
        Some(range)
    }

    pub fn complete(&self, id: WorkerId) {
        self.0.lock().unwrap().in_flight.remove(&id);
    }

    pub fn requeue(&self, id: WorkerId) {
        let mut inner = self.0.lock().unwrap();
        if let Some(range) = inner.in_flight.remove(&id) {
            inner.pending.push_front(range);
        }
    }

    /// Returns the range that was requeued (for coordinator to mark stale completions).
    pub fn requeue_returning_range(&self, id: WorkerId) -> Option<FileRange> {
        let mut inner = self.0.lock().unwrap();
        let range = inner.in_flight.remove(&id);
        if let Some(r) = range {
            inner.pending.push_front(r);
            Some(r)
        } else {
            None
        }
    }

    pub fn is_done(&self) -> bool {
        let inner = self.0.lock().unwrap();
        inner.pending.is_empty() && inner.in_flight.is_empty()
    }

    pub fn pending_count(&self) -> usize {
        self.0.lock().unwrap().pending.len()
    }

    pub fn in_flight_count(&self) -> usize {
        self.0.lock().unwrap().in_flight.len()
    }
}
