use crate::network::NodeId;
use serde::{Serialize, Deserialize};

pub mod demon;
pub mod redblue;
pub mod strict;
pub mod causal;
pub mod unistore;

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
