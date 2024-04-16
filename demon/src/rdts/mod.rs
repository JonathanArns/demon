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
    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal>;
    fn is_writing(&self) -> bool;
    fn is_semiserializable_strong(&self) -> bool;
    fn is_red(&self) -> bool;
    fn is_por_conflicting(&self, other: &Self) -> bool;
}
