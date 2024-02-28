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

pub struct Storage<O: Operation>{
    weak_logs: HashMap<NodeId, Vec<O>>,
    transactions: Vec<Transaction<O>>,
}

impl<O: Operation> Storage<O> {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        let mut weak_logs = HashMap::new();
        for id in nodes {
            weak_logs.insert(id, vec![]);
        }
        Self {
            weak_logs,
            transactions: vec![],
        }
    }

    pub fn current_snapshot(&self) -> Snapshot {
        todo!()
    }

    pub fn exec_weak(&mut self, op: O) -> Option<O::ReadVal> {
        todo!()
    }

    pub fn exec_transaction(&mut self, t: Transaction<O>) -> Vec<O::ReadVal> {
        todo!()
    }
}
