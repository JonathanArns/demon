use std::fmt::Debug;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use omnipaxos::storage::{Entry, NoSnapshot};

use crate::{protocols::TransactionId, weak_replication::Snapshot};

pub mod counters;
pub mod demon;
pub mod basic;
pub mod redblue;

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
}

#[derive(Clone, Debug, Serialize)]
pub struct Response<O: Operation> {
    pub value: Option<O::ReadVal>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction<O> {
    pub id: TransactionId,
    pub op: O,
    pub snapshot: Snapshot,
}

impl<O: Operation> Entry for Transaction<O> {
    type Snapshot = NoSnapshot;
}
