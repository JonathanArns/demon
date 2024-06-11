use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use crate::{network::NodeId, causal_replication::{TaggedEntry, Snapshot}};
use super::{QueryResult, Transaction};
use crate::rdts::Operation;

/// A deterministic in-memory storage layer, that combines weak and strong operations.
///
/// This is supposed to be a less naive implementation, that keeps a committed and an applied
/// state and only re-executes conflicting operations.
///
/// TODO: future possible optimization: batch strong operations
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
    /// The latest weak snapshot
    latest_weak_snapshot_state: Arc<RwLock<O::State>>,
    latest_weak_snapshot: Arc<RwLock<Snapshot>>,
}

impl<O: Operation> Storage<O> {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        Self {
            uncommitted_weak_ops: Default::default(),
            latest_transaction_snapshot: Arc::new(RwLock::new(Snapshot::new(&nodes))),
            latest_weak_snapshot: Arc::new(RwLock::new(Snapshot::new(&nodes))),
            latest_transaction_snapshot_state: Default::default(),
            latest_weak_snapshot_state: Default::default(),
        }
    }

    /// Executes a weak query.
    ///
    /// To be called for all weak queries that are delivered by weak replication.
    pub async fn exec_remote_weak_query(&self, op: TaggedEntry<O>) {
        op.value.apply(&mut *self.latest_weak_snapshot_state.write().await);
        self.latest_weak_snapshot.write().await.merge_inplace(&op.causality);
        self.uncommitted_weak_ops.write().await.push(op);
    }

    /// Executes a weak query from the client.
    /// 
    /// Returns possible read value.
    pub async fn exec_weak_query(&self, op: O, from: NodeId) -> QueryResult<O> {
        let mut state_latch = self.latest_weak_snapshot_state.write().await;

        // now we execute the actual query and store the write ops
        let output = op.apply(&mut state_latch);
        if op.is_writing() {
            let mut snapshot = self.latest_weak_snapshot.write().await;
            let mut log_latch = self.uncommitted_weak_ops.write().await;
            snapshot.increment(from, 1);
            let tagged_op = TaggedEntry {
                value: op,
                causality: snapshot.clone(),
            };
            log_latch.push(tagged_op);
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

        // get latches
        let mut weak_state_latch = self.latest_weak_snapshot_state.write().await;
        let mut snapshot_latch = self.latest_transaction_snapshot.write().await;
        let mut weak_latch = self.uncommitted_weak_ops.write().await;
        let mut state_latch = self.latest_transaction_snapshot_state.write().await;

        if t.snapshot.greater(&snapshot_latch) {
            // update snapshot
            snapshot_latch.merge_inplace(&t.snapshot);

            // update local snapshot state and filter weak ops
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
        }

        // execute the transaction
        let output = if let Some(op) = t.op {
            let output = op.apply(&mut state_latch);

            // update weak snapshot state
            op.rollback_conflicting_state(&state_latch, &mut weak_state_latch);
            for weak in weak_latch.iter() {
                if op.is_conflicting(&weak.value) {
                    weak.value.apply(&mut weak_state_latch);
                }
            }
            output
        } else {
            None
        };

        QueryResult{ value: output }
    }

    /// Generates the shadow op for `op` on the current state.
    pub async fn generate_shadow(&self, op: O) -> Option<O> {
        op.generate_shadow(&*self.latest_weak_snapshot_state.read().await)
    }
}
