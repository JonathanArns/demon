use std::{sync::Arc, time::Duration};

use omnipaxos::{messages::Message as PaxosMessage, storage::{Entry, NoSnapshot}, ClusterConfig, OmniPaxos, ServerConfig, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;

use crate::{demon::{DeMon, Message}, network::{Network, NodeId}};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {

}

impl Entry for Transaction {
    type Snapshot = NoSnapshot;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SequencerMsg {
    Omnipaxos(PaxosMessage<Transaction>)
}

/// A transaction sequencer that creates a replicated log of transactions.
pub struct Sequencer {
    omnipaxos: Arc<Mutex<OmniPaxos<Transaction, MemoryStorage<Transaction>>>>,
    network: Network<Message, DeMon>,
}

impl Clone for Sequencer {
    fn clone(&self) -> Self {
        Self {
            omnipaxos: self.omnipaxos.clone(),
            network: self.network.clone(),
        }
    }
}

impl Sequencer {
    pub async fn new(network: Network<Message, DeMon>) -> Self {
        let configuration_id = 1;
        let server_config = ServerConfig {
            pid: network.my_id().await.0.into(),
            ..Default::default()
        };
        let cluster_config = ClusterConfig {
            configuration_id,
            nodes: network.nodes().await.into_iter().map(|n| n.0 as u64).collect(),
            ..Default::default()
        };
        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config
        };
        let op = op_config.build(MemoryStorage::default()).unwrap();
        let sequencer = Self {
            network,
            omnipaxos: Arc::new(Mutex::new(op)),
        };
        tokio::task::spawn(sequencer.clone().drive_omnipaxos());
        sequencer
    }

    /// Handle an incoming message.
    pub async fn handle_msg(&self, msg: SequencerMsg) {
        match msg {
            SequencerMsg::Omnipaxos(m) => {
                self.omnipaxos.lock().await.handle_incoming(m);
            }
        }
    }

    pub async fn append(&self, transaction: Transaction) {
        todo!()
    }

    /// Increments omnipaxos' internal clock and sends messages.
    ///
    /// Should be called with a cloned handle to the sequencer.
    async fn drive_omnipaxos(self) {
        loop {
            tokio::time::sleep(Duration::from_micros(1000)).await;
            let mut latch = self.omnipaxos.lock().await;
            latch.tick();
            let messages = latch.outgoing_messages().into_iter().map(|m| {
                (NodeId(m.get_receiver() as u32), Message::Sequencer(SequencerMsg::Omnipaxos(m)))
            }).collect::<Vec<_>>();
            self.network.send_batch(messages).await;
        }
    }
}
