use std::{fmt::Debug, time::Duration};

use serde::{de::DeserializeOwned, Serialize};

use crate::api::http::BenchSettings;

pub mod counters;
pub mod rubis_rdt;
pub mod tpcc;
pub mod non_negative_counter;
pub mod or_set;
pub mod co_editor;

/// A generic operations first approach to defining replicated data types.
pub trait Operation: Clone + Debug + Sync + Send + Serialize + DeserializeOwned + 'static {
    type State: Default + Clone + Sync + Send;
    type ReadVal: Clone + Serialize + Debug + Sync + Send;
    type QueryState: Default + Send;

    /// Parse a query string to an operation, or return None for bad queries.
    fn parse(text: &str) -> anyhow::Result<Self>;
    /// Applies this operation to the state and returns a result if one exists.
    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal>;
    /// Indicates that the operation is writing and thus needs to be replicated in any case.
    fn is_writing(&self) -> bool;
    /// Indicates that this operation needs to be strong in semi-serializability.
    fn is_semiserializable_strong(&self) -> bool;
    /// Indicates whether either operation might read or overwrite the other.
    fn is_conflicting(&self, other: &Self) -> bool;
    /// Takes any state in `source` that this operation might touch, and merges it into `target`.
    /// This is used to effectively roll back all operations between `source` and `target`
    /// that conflict with `self`.
    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State);
    /// Indicates that this operation needs to be red in RedBlue.
    fn is_red(&self) -> bool;
    /// Indicates an ordering restriction in PoR.
    fn is_por_conflicting(&self, other: &Self) -> bool;
    /// Used to turn client operations into shadow operations.
    /// Must not perform client-visible state mutations, but may perform "bookkeeping" for CRDT
    /// correctness.
    fn generate_shadow(&self, state: &mut Self::State) -> Option<Self>;
    /// Used to generate benchmark workloads
    fn gen_query(settings: &BenchSettings, state: &mut Self::QueryState) -> Self;
    fn update_query_state(state: &mut Self::QueryState, val: Self::ReadVal) {}
    /// Used to record operation names in measurements.
    fn name(&self) -> String;
    /// Used to generate the periodic strong no-op executed by Demon.
    fn gen_periodic_strong_op() -> Option<Self> {
        None
    }
    /// The frequency of periodic strong ops in Demon.
    fn periodic_strong_op_interval() -> Duration {
        Duration::from_millis(20)
    }
    /// Can be used as an optimization to tell DeMon that an RDT does not
    /// observe any state for shadow op generation.
    fn uses_state_for_shadow_generation() -> bool {
        true
    }
}
