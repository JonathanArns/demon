use std::{collections::HashMap, fmt::Debug};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use omnipaxos::storage::{Entry, NoSnapshot};

use crate::{demon::TransactionId, network::NodeId};

pub mod counters;
pub mod snapshot;
pub use snapshot::Snapshot;

pub trait Operation: Clone + Debug + Sync + Send + Serialize + DeserializeOwned + 'static {
    type State: Default + Clone + Sync + Send;
    type ReadVal: Clone + Serialize;

    fn is_weak(&self) -> bool;
    fn is_writing(&self) -> bool;
    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal>;
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
    pub id: TransactionId,
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

    /// Executes a weak query.
    ///
    /// To be called for all weak queries that are delivered by weak replication.
    pub fn exec_remote_weak_query(&mut self, op: O) {
        // self.weak_logs.get_mut(&op.node).unwrap().1.push(op.op);
        println!("TODO: execute weak remote op");
    }

    /// Executes a weak query from the client.
    /// 
    /// Returns possible read values and a vector of operations to replicate asyncronously.
    pub fn exec_weak_query(&mut self, query: Query<O>, from: NodeId) -> (Response<O>, Vec<O>) {
        let mut output = vec![];
        let mut entries_to_replicate = vec![];
        let mut state = O::State::default(); // TODO: execute on the correct state
        let (_offset, log) = self.weak_logs.get_mut(&from).unwrap();
        for op in query.ops {
            if let Some(result) = op.apply(&mut state) {
                output.push(result);
            }
            if op.is_writing() {
                log.push(op.clone());
                entries_to_replicate.push(op);
            }
        }
        (Response{ values: output }, entries_to_replicate)
    }

    /// Stores and executes a transaction, returning possible read values.
    pub fn exec_transaction(&mut self, t: Transaction<O>) -> Response<O> {
        self.latest_transaction_snapshot.merge_inplace(&t.snapshot);
        println!("TODO: execute transaction");
        Response{ values: vec![] }
    }
}
