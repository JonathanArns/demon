use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use omnipaxos::storage::{Entry, NoSnapshot};

use crate::{protocols::TransactionId, rdts::Operation, weak_replication::Snapshot};

pub mod demon;
pub mod demon_old;
pub mod basic;
pub mod redblue;
pub mod unistore;

#[derive(Clone, Debug, Serialize)]
pub struct QueryResult<O: Operation> {
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
