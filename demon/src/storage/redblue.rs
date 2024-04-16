use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use crate::{network::NodeId, weak_replication::Snapshot};
use super::{Operation, Response, Transaction};

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
    pub async fn exec_blue(&self, op: O, from: NodeId) -> Response<O> {
        let mut snapshot = self.current_snapshot.write().await;
        let output = op.apply(&mut *self.state.write().await);
        snapshot.increment(from, 1);
        Response{ value: output }
    }

    /// Stores and executes a red transaction, returning possible read values.
    pub async fn exec_red(&self, t: Transaction<O>) -> Response<O> {
        // wait until all causally required weak ops are here
        loop {
            if !t.snapshot.greater(&*self.current_snapshot.read().await) {
                break
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let output = t.op.apply(&mut *self.state.write().await);
        Response{ value: output }
    }

    pub async fn get_current_snapshot(&self) -> Snapshot {
        self.current_snapshot.read().await.clone()
    }
}
