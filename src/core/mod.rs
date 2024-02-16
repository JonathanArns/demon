use serde::{Serialize, Deserialize};

mod event_handler;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

/// The top-level event type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Event {
    Internal(Message),
    Client(ClientRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Sequencer,
    Gossip,
    Membership,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientRequest {}


