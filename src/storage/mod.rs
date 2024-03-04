use std::{collections::HashMap, fmt::Debug};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use omnipaxos::storage::{Entry, NoSnapshot};

use crate::{network::NodeId, weak_replication::{WeakLogEntry, WeakLogStorage}};

pub mod counters;

/// A snapshot identifier that enables processes to construct the snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    // /// (term, idx) per weak log
    // pub vec: Vec<(u32, u64)>,
    /// idx per weak log
    pub vec: Vec<u64>,
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

    /// Returns None if they are concurrent.
    /// Returns Some(true) if self fully includes other.
    pub fn greater(&self, other: &Self) -> Option<bool> {
        let mut result = None;
        for i in 0..self.vec.len() {
            if self.vec[i] > other.vec[i] {
                if result == Some(false) {
                    return None
                }
                result = Some(true);
            }
            if self.vec[i] < other.vec[i] {
                if result == Some(true) {
                    return None
                }
                result = Some(false)
            }
        }
        result
    }
}

pub trait Operation: Clone + Debug + Sync + Send + Serialize + DeserializeOwned + 'static {
    type State: Default + Clone + Sync + Send;
    type ReadVal: Clone + Serialize;

    fn is_weak(&self) -> bool;
    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedOperation<O> {
    pub node: NodeId,
    pub idx: u64,
    pub op: O,
}

impl<O: Operation> WeakLogEntry for TaggedOperation<O> {
    fn update_snapshot(&self, snapshot: &mut Snapshot) -> bool {
        if self.idx >= snapshot.vec[self.node.0 as usize] {
            snapshot.vec[self.node.0 as usize] = self.idx;
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query<O> {
    pub ops: Vec<O>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Response<O: Operation> {
    pub values: Vec<O::ReadVal>,
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction<O> {
    pub query: Query<O>,
    pub snapshot: Snapshot,
}

impl<O: Operation> Entry for Transaction<O> {
    type Snapshot = NoSnapshot;
}


/// A deterministic in-memory storage layer, that combines weak and strong operations.
///
/// TODO: make this storage thread-safe or something
#[derive(Debug)]
pub struct Storage<O: Operation> {
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
    pub fn store_weak(&mut self, op: TaggedOperation<O>) {
        self.weak_logs.get_mut(&op.node).unwrap().1.push(op.op);
    }

    /// Stores and executes a weak operation, returning a possible result.
    pub fn exec_weak(&mut self, op: TaggedOperation<O>) -> Response<O> {
        self.weak_logs.get_mut(&op.node).unwrap().1.push(op.op);
        todo!()
    }

    /// Stores and executes a transaction, returning possible read values.
    pub fn exec_transaction(&mut self, t: Transaction<O>) -> Response<O> {
        self.latest_transaction_snapshot.merge_inplace(&t.snapshot);
        todo!()
    }
}

impl<O: Operation> WeakLogStorage<TaggedOperation<O>> for Storage<O> {
    fn read(&self, log: NodeId, from: u64) -> Vec<TaggedOperation<O>> {
        todo!()
    }
}
