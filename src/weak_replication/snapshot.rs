use serde::{Deserialize, Serialize};

use crate::network::NodeId;

/// A snapshot identifier that enables processes to construct the snapshot.
/// Represents the number of known entries per weak log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    /// TODO: make this field private and enforce correct indexing via NodeId
    pub vec: Vec<u64>,
}

impl Snapshot {
    pub fn new(nodes: &[NodeId]) -> Self {
        Self {
            vec: vec![0; nodes.len()]
        }
    }

    pub fn increment(&mut self, node: NodeId, amount: u64) {
        self.vec[node.0 as usize - 1] += amount;
    }

    pub fn get(&self, node: NodeId) -> u64 {
        self.vec[node.0 as usize - 1]
    }

    pub fn get_mut(&mut self, node: NodeId) -> &mut u64 {
        &mut self.vec[node.0 as usize - 1]
    }

    pub fn entries(&self) -> Vec<(NodeId, u64)> {
        self.vec.clone().into_iter().enumerate().map(|(i, value)| {
            (NodeId(i as u32 + 1), value)
        }).collect()
    }

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

    /// Returns true if self includes any operations that are not in other.
    pub fn greater(&self, other: &Self) -> bool {
        for i in 0..self.vec.len() {
            if self.vec[i] > other.vec[i] {
                return true
            }
        }
        false
    }
}

