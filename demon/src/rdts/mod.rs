use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

pub mod counters;
pub mod rubis;

/// A generic operations first approach to defining replicated data types.
pub trait Operation: Clone + Debug + Sync + Send + Serialize + DeserializeOwned + 'static {
    type State: Default + Clone + Sync + Send;
    type ReadVal: Clone + Serialize + Debug + Sync + Send;

    /// Parse a query string to an operation, or return None for bad queries.
    fn parse(text: &str) -> anyhow::Result<Self>;
    /// Applies this operation to the state and returns a result if one exists.
    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal>;
    /// Indicates that the operation is writing and thus needs to be replicated in any case.
    fn is_writing(&self) -> bool;
    /// Indicates that this operation needs to be strong in semi-serializability.
    fn is_semiserializable_strong(&self) -> bool;
    /// Indicates whether eitehr operation might read or overwrite the other.
    fn is_conflicting(&self, other: &Self) -> bool;
    /// Takes any state in `source` that this operation might touch, and merges it into `target`.
    /// This is used to effectively roll back all operations between `source` and `target`
    /// that conflict with `self`.
    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State);
    /// Indicates that this operation needs to be red in RedBlue.
    fn is_red(&self) -> bool;
    /// Indicates an ordering restriction in PoR.
    fn is_por_conflicting(&self, other: &Self) -> bool;
}
