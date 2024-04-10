use crate::{network::NodeId, storage::counters::CounterOp};
use serde::{Serialize, Deserialize};

pub mod demon;
pub mod redblue;
pub mod strong;
pub mod causal;

/// The Operation type to be used by all protocols
type Op = CounterOp;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TransactionId(NodeId, u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Component {
    Sequencer,
    WeakReplication,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub payload: Vec<u8>,
    pub component: Component,
}
