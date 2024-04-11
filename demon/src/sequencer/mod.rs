use std::{ops::RangeBounds, sync::Arc, time::Duration};

use omnipaxos::{messages::Message as PaxosMessage, storage::Entry, util::LogEntry, ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, Mutex};

use crate::{protocols::{Component, Message}, network::{Network, NodeId}};


#[derive(Debug, Clone, Serialize, Deserialize)]
enum SequencerMsg<T: Entry> {
    Omnipaxos(PaxosMessage<T>)
}

#[derive(Debug, Clone)]
pub enum SequencerEvent<T> {
    /// Is triggered whenever the decided index increases.
    /// Contains the index range of newly decided entries.
    Decided(Vec<T>),
}

/// A transaction sequencer that creates a replicated log of transactions.
pub struct Sequencer<T>
where
    T: Entry + Serialize + DeserializeOwned + Send + 'static,
    T::Snapshot: Send,
{
    omnipaxos: Arc<Mutex<OmniPaxos<T, MemoryStorage<T>>>>,
    network: Network<Message>,
    event_sender: Sender<SequencerEvent<T>>,
    decided_idx: Arc<Mutex<u64>>,
}

impl<T> Clone for Sequencer<T>
where
    T: Entry + Serialize + DeserializeOwned + Send + 'static,
    T::Snapshot: Send,
{
    fn clone(&self) -> Self {
        Self {
            omnipaxos: self.omnipaxos.clone(),
            network: self.network.clone(),
            event_sender: self.event_sender.clone(),
            decided_idx: self.decided_idx.clone(),
        }
    }
}

impl<T> Sequencer<T>
where
    T: Entry + Serialize + DeserializeOwned + Send + 'static,
    T::Snapshot: Send,
{
    pub async fn new(network: Network<Message>) -> (Self, Receiver<SequencerEvent<T>>) {
        let configuration_id = 1;
        let my_id = network.my_id().await;
        let server_config = ServerConfig {
            pid: my_id.0.into(),
            election_tick_timeout: 100,
            resend_message_tick_timeout: 100,
            leader_priority: if my_id.0 == 1 { 1 } else { 0 },
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
        let (sender, receiver) = channel(1000);
        let sequencer = Self {
            network,
            omnipaxos: Arc::new(Mutex::new(op)),
            event_sender: sender,
            decided_idx: Arc::new(Mutex::new(0)),
        };
        tokio::task::spawn(sequencer.clone().drive_omnipaxos());
        (sequencer, receiver)
    }

    /// Handle an incoming message.
    pub async fn handle_msg(&self, data: Vec<u8>) {
        let msg: SequencerMsg<T> = bincode::deserialize(&data).unwrap();
        match msg {
            SequencerMsg::Omnipaxos(m) => {
                let mut latch = self.omnipaxos.lock().await;
                latch.handle_incoming(m);
                let decided_idx = latch.get_decided_idx();
                let mut my_decided_idx = self.decided_idx.lock().await;
                if decided_idx > *my_decided_idx {
                    let old_decided_idx = *my_decided_idx;
                    *my_decided_idx = decided_idx;
                    let decided_entries = if let Some(entries) = latch.read_entries(old_decided_idx..decided_idx) {
                        entries.into_iter().filter(|e| {
                            match e {
                                LogEntry::Decided(_) => true,
                                _ => false,
                            }
                        }).map(|e| {
                            match e {
                                LogEntry::Decided(t) => t,
                                _ => unreachable!("should have been filtered out"),
                            }
                        }).collect()
                    } else {
                        vec![]
                    };
                    self.event_sender.send(SequencerEvent::Decided(decided_entries)).await.unwrap();
                }
                // flush OmniPaxos messages
                let messages = latch.outgoing_messages().into_iter().map(|m| {
                    (NodeId(m.get_receiver() as u32), Message{payload: bincode::serialize(&SequencerMsg::<T>::Omnipaxos(m)).unwrap(), component: Component::Sequencer})
                }).collect::<Vec<_>>();
                if messages.len() > 0 {
                    self.network.send_batch(messages).await;
                }
            }
        }
    }

    /// Schedules a new transaction for sequencing, so that it will eventually be decided.
    pub async fn append(&self, transaction: T) {
        let mut latch = self.omnipaxos.lock().await;
        latch.append(transaction).unwrap();
        // flush OmniPaxos messages
        let messages = latch.outgoing_messages().into_iter().map(|m| {
            (NodeId(m.get_receiver() as u32), Message{payload: bincode::serialize(&SequencerMsg::<T>::Omnipaxos(m)).unwrap(), component: Component::Sequencer})
        }).collect::<Vec<_>>();
        if messages.len() > 0 {
            self.network.send_batch(messages).await;
        }
    }

    /// Read decided transactions in a range from the log.
    /// This might return fewer entries than the range's length, for example if some entries are
    /// not decided yet, or if there are omnipaxos internal entries there.
    pub async fn read<R: RangeBounds<u64>>(&self, range: R) -> Vec<T> {
        if let Some(entries) = self.omnipaxos.lock().await.read_entries(range) {
            entries.into_iter().filter(|e| {
                match e {
                    LogEntry::Decided(_) => true,
                    _ => false,
                }
            }).map(|e| {
                match e {
                    LogEntry::Decided(t) => t,
                    _ => unreachable!("should have been filtered out"),
                }
            }).collect()
        } else {
            vec![]
        }
    }

    /// Increments omnipaxos' internal clock and sends messages.
    ///
    /// Should be called with a cloned handle to the sequencer.
    async fn drive_omnipaxos(self) {
        loop {
            tokio::time::sleep(Duration::from_micros(10000)).await;
            let mut latch = self.omnipaxos.lock().await;
            latch.tick();
            // flush OmniPaxos messages
            let messages = latch.outgoing_messages().into_iter().map(|m| {
                (NodeId(m.get_receiver() as u32), Message{payload: bincode::serialize(&SequencerMsg::<T>::Omnipaxos(m)).unwrap(), component: Component::Sequencer})
            }).collect::<Vec<_>>();
            if messages.len() > 0 {
                self.network.send_batch(messages).await;
            }
        }
    }
}
