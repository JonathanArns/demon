use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use omnipaxos::storage::{Entry, NoSnapshot};

use crate::{protocols::TransactionId, rdts::Operation, causal_replication::Snapshot};

pub mod demon;
pub mod demon_old;
pub mod basic;
pub mod redblue;
pub mod deterministic_redblue;

#[derive(Clone, Debug, Serialize)]
pub struct QueryResult<O: Operation> {
    pub value: Option<O::ReadVal>,
}

/// A strong operation.
/// Can be a no-op, which is needed to periodically commit weak ops without strong ops.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction<O> {
    pub id: TransactionId,
    pub op: Option<O>,
    pub snapshot: Snapshot,
}

impl<O: Operation> Entry for Transaction<O> {
    type Snapshot = NoSnapshot;
}
