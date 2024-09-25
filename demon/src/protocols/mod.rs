use crate::network::NodeId;
use serde::{Serialize, Deserialize};

pub mod demon;
pub mod strict;
pub mod causal;
pub mod gemini;
pub mod unistore;
pub mod deterministic_redblue;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TransactionId(NodeId, u64);

impl TransactionId {
    fn to_string(&self) -> String {
        let mut s = String::with_capacity(32);
        s.push_str(&self.0.0.to_string());
        s.push(',');
        s.push_str(&self.1.to_string());
        s
    }
    
    fn zero() -> Self {
        Self(NodeId(0), 0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Component {
    Sequencer,
    WeakReplication,
    Protocol,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub payload: Vec<u8>,
    pub component: Component,
}
