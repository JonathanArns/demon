use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use crate::{network::NodeId, causal_replication::{TaggedEntry, Snapshot}};
use super::{QueryResult, Transaction};
use crate::rdts::Operation;

/// An in-memory storage layer that can generate shadow operations based on a deterministically
/// chosen snapshot.
#[derive(Debug)]
pub struct Storage<O: Operation> {
    /// The actual current state.
    state: Arc<RwLock<O::State>>,
    /// A snapshot vector describing the current state.
    state_snapshot: Arc<RwLock<Snapshot>>,
    /// The state at the latest transaction snapshot.
    red_shadow_state: Arc<RwLock<O::State>>,
    /// A snapshot vector describing the red_shadow_state.
    red_shadow_snapshot: Arc<RwLock<Snapshot>>,
    /// Blue operations that have been applied to state, but are not part of the red shadow snapshot state yet.
    uncommitted_blue_ops: Arc<RwLock<Vec<TaggedEntry<O>>>>,
}

impl<O: Operation> Storage<O> {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        let state = O::State::default();
        Self {
            state: Arc::new(RwLock::new(state.clone())),
            red_shadow_state: Arc::new(RwLock::new(state)),
            state_snapshot: Arc::new(RwLock::new(Snapshot::new(&nodes))),
            red_shadow_snapshot: Arc::new(RwLock::new(Snapshot::new(&nodes))),
            uncommitted_blue_ops: Default::default(),
        }
    }

    /// Gets the current snapshot.
    pub async fn get_current_snapshot(&self) -> Snapshot {
        self.state_snapshot.read().await.clone()
    }

    /// Generates the shadow op for `op` on the current state.
    pub async fn generate_blue_shadow(&self, op: O) -> Option<O> {
        op.generate_shadow(&mut *self.state.write().await)
    }

    /// Executes a blue query.
    /// 
    /// Returns possible read value.
    pub async fn exec_blue_shadow(&self, op: O, causality: Snapshot, from: NodeId) -> QueryResult<O> {
        let mut state_latch = self.state.write().await;
        let output = op.apply(&mut state_latch);
        if op.is_writing() {
            // compute the weak log idx that this query will get
            let mut snapshot_latch = self.state_snapshot.write().await;
            self.uncommitted_blue_ops.write().await.push(
                TaggedEntry { causality, value: op, from }
            );
            snapshot_latch.increment(from, 1);
        }
        QueryResult{ value: output }
    }

    /// Deterministically generates and executes red shadow operations.
    pub async fn exec_red(&self, t: Transaction<O>) -> QueryResult<O> {
        // wait until all required blue ops are here
        loop {
            if !t.snapshot.greater(&*self.state_snapshot.read().await) {
                break
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // get latches
        let mut state = self.state.write().await;
        let mut red_shadow_snapshot = self.red_shadow_snapshot.write().await;
        let mut red_shadow_state = self.red_shadow_state.write().await;
        let mut uncommitted_blue_ops = self.uncommitted_blue_ops.write().await;

        // update the red shadow state and snapshot
        if t.snapshot.greater(&red_shadow_snapshot) {
            // update snapshot
            red_shadow_snapshot.merge_inplace(&t.snapshot);

            // update local snapshot state and filter weak ops
            let mut i = 0;
            while i < uncommitted_blue_ops.len() {
                let op = &uncommitted_blue_ops[i];
                if op.causality.included_in(&red_shadow_snapshot) {
                    op.value.apply(&mut red_shadow_state);
                    uncommitted_blue_ops.remove(i);
                } else {
                    i += 1;
                }
            }
        }

        // generate the shadow operation
        let mut output = None;
        if let Some(op) = t.op {
            if let Some(shadow) = op.generate_shadow(&mut red_shadow_state) {
                // apply the shadow operation
                let _ = shadow.apply(&mut red_shadow_state);
                output = shadow.apply(&mut state);
            }
        }

        QueryResult{ value: output }
    }
}
