use std::{sync::Arc, time::Duration};

use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, Mutex};

use crate::{demon::{DeMon, Message}, network::{Network, NodeId}};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotVec {
    pub vec: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum GossipMsg {
    Gossip,
    // Snapshot(SnapshotVec),
}

#[derive(Debug, Clone)]
pub enum GossipEvent {
    // Durable()
}

/// A transaction sequencer that creates a replicated log of transactions.
pub struct Gossiper {
    network: Network<Message, DeMon>,
    event_sender: Sender<GossipEvent>,
    current_snapshot: Arc<Mutex<SnapshotVec>>,
}

impl Clone for Gossiper {
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            event_sender: self.event_sender.clone(),
            current_snapshot: self.current_snapshot.clone(),
        }
    }
}

impl Gossiper {
    pub async fn new(network: Network<Message, DeMon>) -> (Self, Receiver<GossipEvent>) {
        let (sender, receiver) = channel(1000);

        // TODO: correctly initialize snapshot
        let current_snapshot = Arc::new(Mutex::new(SnapshotVec{vec: vec![]}));
        let gossiper = Self {
            network,
            event_sender: sender,
            current_snapshot,
        };
        // tokio::task::spawn(gossiper.clone().background_task());
        (gossiper, receiver)
    }

    /// Handle an incoming message.
    pub async fn handle_msg(&self, from: NodeId, data: Vec<u8>) {
        let msg: GossipMsg = bincode::deserialize(&data).unwrap();
        match msg {
            GossipMsg::Gossip => {

            },
            
        }
    }

    /// Periodically send gossip.
    ///
    /// Should be called with a cloned handle to the sequencer.
    async fn run_gossip(self) {
        loop {
            tokio::time::sleep(Duration::from_micros(1000)).await;
        }
    }
}
