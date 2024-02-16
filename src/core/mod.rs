use serde::{Serialize, Deserialize};

mod event_handler;
pub use event_handler::handle;

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


