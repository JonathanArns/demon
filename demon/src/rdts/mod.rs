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

    // required methods to replicate via DeMon protocol

    /// Applies this operation to the state and returns a result if one exists.
    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal>;
    /// Indicates that the operation is writing and thus needs to be replicated in any case.
    fn is_writing(&self) -> bool;
    /// Indicates that this operation needs to be strong in semi-serializability.
    fn is_semiserializable_strong(&self) -> bool;
    /// Used to turn client operations into shadow operations.
    /// Must not perform client-visible state mutations, but may perform "bookkeeping" for CRDT
    /// correctness.
    fn generate_shadow(&self, state: &mut Self::State) -> Option<Self>;

    // optional methods for DeMon, to tune the protocol and minimize rollbacks to conflicting operations

    /// Indicates whether either operation might read or overwrite the other.
    fn is_conflicting(&self, _other: &Self) -> bool { true }
    /// Takes any state in `source` that this operation might touch, and merges it into `target`.
    /// This is used to effectively roll back all operations between `source` and `target`
    /// that conflict with `self`.
    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        *target = source.clone()
    }
    /// Used to generate the periodic strong no-op executed by Demon.
    fn gen_periodic_strong_op() -> Option<Self> {
        None
    }
    /// The frequency of periodic strong ops in Demon.
    fn periodic_strong_op_interval() -> Duration {
        Duration::from_millis(20)
    }

    // optional methods used for benchmarking and query interface

    /// Used to generate benchmark workloads
    fn gen_query(_settings: &BenchSettings, _state: &mut Self::QueryState) -> Self {
        unimplemented!("must implement to support benchmarks")
    }
    fn update_query_state(_state: &mut Self::QueryState, _val: Self::ReadVal) {}
    /// Used to record operation names in measurements.
    fn name(&self) -> String {
        unimplemented!("must implement to support benchmarks")
    }
    /// Parse a query string to an operation, or return None for bad queries.
    fn parse(_text: &str) -> anyhow::Result<Self> {
        unimplemented!("must implement for an interactive text-based query interface")
    }

    // otional methods for RedBlue and PoR consistency

    /// Indicates that this operation needs to be red in RedBlue.
    fn is_red(&self) -> bool {
        unimplemented!("must implement to support RedBlue consistency")
    }
    /// Indicates an ordering restriction in PoR.
    fn is_por_conflicting(&self, _other: &Self) -> bool{
        unimplemented!("must implement to support PoR consistency")
    }
}
