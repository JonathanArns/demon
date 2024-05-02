use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use crate::{network::NodeId, weak_replication::{TaggedEntry, Snapshot}};
use super::{QueryResult, Transaction};
use crate::rdts::Operation;

/// A deterministic in-memory storage layer, that combines weak and strong operations.
///
/// This is supposed to be a very naive, but correct implementation.
#[derive(Debug)]
pub struct Storage<O: Operation> {
    /// Weak operations that are not part of a transaction snapshot yet.
    uncommitted_weak_ops: Arc<RwLock<Vec<TaggedEntry<O>>>>,
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
            latest_transaction_snapshot_state: Default::default(),
        }
    }

    /// Executes a weak query.
    ///
    /// To be called for all weak queries that are delivered by weak replication.
    pub async fn exec_remote_weak_query(&self, op: TaggedEntry<O>) {
        self.uncommitted_weak_ops.write().await.push(op);
    }

    /// Executes a weak query from the client.
    /// 
    /// Returns possible read value.
    pub async fn exec_weak_query(&self, op: O, from: NodeId) -> QueryResult<O> {
        let snapshot_val = self.latest_transaction_snapshot.read().await.get(from);
        let mut latch = self.uncommitted_weak_ops.write().await;

        // TODO: is this correct? since we have locked the weak ops...
        let mut state = self.latest_transaction_snapshot_state.read().await.clone(); // TODO: execute on the correct state
        for op in latch.iter().filter(|o| o.value.is_conflicting(&op)) {
            op.value.apply(&mut state);
        }
        
        // now we execute the actual query and store the write ops
        let output = op.apply(&mut state);
        if op.is_writing() {
            // compute the weak log idx that the first write operation in this query will get
            let next_op_idx = latch.iter()
                .filter(|o| o.node == from)
                .map(|o| o.idx)
                .max()
                .map(|idx| idx + 1)
                .unwrap_or(snapshot_val);
            let tagged_op = TaggedEntry {
                value: op,
                node: from,
                idx: next_op_idx,
            };
            latch.push(tagged_op);
        }
        QueryResult{ value: output }
    }

    /// Stores and executes a transaction, returning possible read values.
    pub async fn exec_transaction(&self, t: Transaction<O>) -> QueryResult<O> {
        // wait until all required weak ops are here
        loop {
            {
                let mut has_all_entries = true;
                let weak_latch = self.uncommitted_weak_ops.read().await;
                let snapshot_latch = self.latest_transaction_snapshot.read().await;
                for (node, len) in t.snapshot.entries() {
                    if snapshot_latch.get(node) >= len {
                        continue
                    }
                    if let None = weak_latch.iter().find(|e| e.node == node && e.idx == len-1) {
                        has_all_entries = false;
                        break
                    }
                }
                if has_all_entries {
                    break
                }
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
            if op.is_in_snapshot(&snapshot_latch) {
                op.value.apply(&mut state_latch);
                weak_latch.remove(i);
            } else {
                i += 1;
            }
        }

        // execute the transaction
        let output = t.op.apply(&mut state_latch);
        QueryResult{ value: output }
    }
}
