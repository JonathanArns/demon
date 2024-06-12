use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use crate::{network::NodeId, causal_replication::{TaggedEntry, Snapshot}};
use super::{QueryResult, Transaction};
use crate::rdts::Operation;

/// A deterministic in-memory storage layer, that combines weak and strong operations.
///
/// This is supposed to be a very naive, but correct implementation.
#[derive(Debug)]
pub struct Storage<O: Operation> {
    /// Weak operations that are not part of a transaction snapshot yet.
    uncommitted_weak_ops: Arc<RwLock<Vec<TaggedEntry<O>>>>,
    latest_weak_snapshot: Arc<RwLock<Snapshot>>,
    /// The snapshot that the latest transaction was executed on.
    /// weak_logs may be truncated up to this snapshot, because we will never
    /// need to re-execute anything before this snapshot.
    latest_transaction_snapshot: Arc<RwLock<Snapshot>>,
    /// The state at the latest transaction snapshot.
    latest_transaction_snapshot_state: Arc<RwLock<O::State>>,
}

impl<O: Operation> Storage<O> {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        Self {
            uncommitted_weak_ops: Default::default(),
            latest_transaction_snapshot: Arc::new(RwLock::new(Snapshot::new(&nodes))),
            latest_weak_snapshot: Arc::new(RwLock::new(Snapshot::new(&nodes))),
            latest_transaction_snapshot_state: Default::default(),
        }
    }

    /// Executes a weak query.
    ///
    /// To be called for all weak queries that are delivered by weak replication.
    pub async fn exec_remote_weak_query(&self, op: TaggedEntry<O>) {
        self.uncommitted_weak_ops.write().await.push(op.clone());
        self.latest_weak_snapshot.write().await.increment(op.from, 1);
    }

    /// Executes a weak query from the client.
    /// 
    /// Returns possible read value.
    pub async fn exec_weak_query(&self, op: O, from: NodeId) -> QueryResult<O> {
        let mut latch = self.uncommitted_weak_ops.write().await;

        let mut state = self.latest_transaction_snapshot_state.read().await.clone(); // TODO: execute on the correct state
        for op in latch.iter().filter(|o| o.value.is_conflicting(&op)) {
            op.value.apply(&mut state);
        }
        
        // now we execute the actual query and store the write ops
        let output = op.apply(&mut state);
        if op.is_writing() {
            let mut snapshot = self.latest_weak_snapshot.write().await;
            // compute the weak log idx that the first write operation in this query will get
            let tagged_op = TaggedEntry {
                value: op,
                from,
                causality: snapshot.clone(),
            };
            snapshot.increment(from, 1);
            latch.push(tagged_op);
        }
        QueryResult{ value: output }
    }

    /// Stores and executes a transaction, returning possible read values.
    pub async fn exec_transaction(&self, t: Transaction<O>) -> QueryResult<O> {
        // wait until all required weak ops are here
        loop {
            if !t.snapshot.greater(&*self.latest_weak_snapshot.read().await) {
                break
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let mut snapshot_latch = self.latest_transaction_snapshot.write().await;
        snapshot_latch.merge_inplace(&t.snapshot);

        // update local snapshot state and filter weak ops
        let mut weak_latch = self.uncommitted_weak_ops.write().await;
        let mut state_latch = self.latest_transaction_snapshot_state.write().await;
        let mut i = 0;
        while i < weak_latch.len() {
            let op = &weak_latch[i];
            if op.causality.included_in(&snapshot_latch) {
                op.value.apply(&mut state_latch);
                weak_latch.remove(i);
            } else {
                i += 1;
            }
        }

        // execute the transaction
        if let Some(op) = t.op {
            let output = op.apply(&mut state_latch);
            QueryResult{ value: output }
        } else {
            QueryResult{ value: None }
        }
    }
}
