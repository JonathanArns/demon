use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use crate::{network::NodeId, causal_replication::Snapshot};
use super::{QueryResult, Transaction};
use crate::rdts::Operation;

/// A storage layer for our RedBlue consistency implementation
#[derive(Debug)]
pub struct Storage<O: Operation> {
    current_snapshot: Arc<RwLock<Snapshot>>,
    state: Arc<RwLock<O::State>>,
}

impl<O: Operation> Storage<O> {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        Self {
            current_snapshot: Arc::new(RwLock::new(Snapshot::new(&nodes))),
            state: Arc::new(RwLock::new(O::State::default())),
        }
    }

    /// Executes a blue operation.
    pub async fn exec_blue(&self, op: O, from: NodeId) -> QueryResult<O> {
        let mut snapshot = self.current_snapshot.write().await;
        let output = op.apply(&mut *self.state.write().await);
        if op.is_writing() {
            snapshot.increment(from, 1);
        }
        QueryResult{ value: output }
    }

    /// Executes a blue operation.
    pub async fn exec_blue_remote(&self, op: O, causality: &Snapshot) -> QueryResult<O> {
        let mut snapshot = self.current_snapshot.write().await;
        let output = op.apply(&mut *self.state.write().await);
        if op.is_writing() {
            snapshot.merge_inplace(causality);
        }
        QueryResult{ value: output }
    }

    /// Stores and executes a red transaction, returning possible read values.
    pub async fn exec_red(&self, t: Transaction<O>) -> QueryResult<O> {
        if let Some(op) = t.op {
            // wait until all causally required weak ops are here
            loop {
                if !t.snapshot.greater(&*self.current_snapshot.read().await) {
                    break
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let output = op.apply(&mut *self.state.write().await);
            QueryResult{ value: output }
        } else {
            QueryResult{ value: None }
        }
    }

    /// Generates the shadow op for `op` on the current state.
    /// Also returns the snapshot on which this shadow operation was generated.
    pub async fn generate_shadow(&self, op: O) -> Option<(O, Snapshot)> {
        let lock = self.current_snapshot.read().await;
        let shadow = op.generate_shadow(&*self.state.read().await);
        shadow.map(|x| (x, lock.clone()))
    }

    pub async fn get_current_snapshot(&self) -> Snapshot {
        self.current_snapshot.read().await.clone()
    }
}
