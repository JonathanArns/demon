use std::collections::HashMap;

use crate::network::NodeId;

pub mod counters;

/// A snapshot identifier that enables processes to construct the snapshot
pub struct Snapshot {
    vec: Vec<u64>,
}

impl Snapshot {
    pub fn merge(&self, other: &Self) -> Self {
        let mut vector = vec![];
        for i in 0..self.vec.len() {
            vector.push(self.vec[i].max(other.vec[i]));
        }
        Self { vec: vector }
    }

    pub fn merge_inplace(&mut self, other: &Self) {
        for i in 0..self.vec.len() {
            self.vec[i] = self.vec[i].max(other.vec[i]);
        }
    }
}

pub trait Operation {
    type State: Default;
    type ReadVal;

    fn is_weak(&self) -> bool;
    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal>;
}

pub struct Transaction<O: Operation> {
    ops: Vec<O>,
    snapshot: Snapshot,
}

/// A deterministic in-memory storage layer, that combines weak and strong operations.
///
/// TODO: make this storage thread-safe or something
pub struct Storage<O: Operation>{
    /// The weak operation logs (offset, log)
    weak_logs: HashMap<NodeId, (usize, Vec<O>)>,
    /// The snapshot that the latest transaction was executed on.
    /// weak_logs may be truncated up to this snapshot, because we will never
    /// need to re-execute anything before this snapshot.
    latest_transaction_snapshot: Snapshot,
    /// The state at the latest transaction snapshot.
    latest_transaction_snapshot_state: O::State,
}

impl<O: Operation> Storage<O> {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        let mut weak_logs = HashMap::new();
        for id in &nodes {
            weak_logs.insert(*id, (0, vec![]));
        }
        Self {
            weak_logs,
            latest_transaction_snapshot: Snapshot{vec: vec![0; nodes.len()]},
            latest_transaction_snapshot_state: O::State::default(),
        }
    }

    pub fn current_snapshot(&self) -> Snapshot {
        todo!()
    }

    /// Stores a weak operation, without computing a possible result.
    pub fn store_weak(&mut self, op: O, from: NodeId) {
        self.weak_logs.get_mut(&from).unwrap().1.push(op);
    }

    /// Stores and executes a weak operation, returning a possible result.
    pub fn exec_weak(&mut self, op: O, from: NodeId) -> Option<O::ReadVal> {
        todo!()
    }

    /// Stores and executes a transaction, returning possible read values.
    pub fn exec_transaction(&mut self, t: Transaction<O>) -> Vec<O::ReadVal> {
        self.latest_transaction_snapshot.merge_inplace(&t.snapshot);
        todo!()
    }
}
