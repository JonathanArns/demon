use std::{collections::HashMap, marker::PhantomData, ops::RangeBounds, sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, Mutex};

use crate::{demon::{DeMon, Message}, network::{Network, NodeId}, storage::Snapshot};


/// The weak replication layer needs a limited amount of storage access through this trait.
pub trait WeakLogStorage<T> {
    fn append(&self, item: T);
    fn read<R: RangeBounds<u64>>(&self, log: NodeId, range: R) -> Vec<T>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WeakMsg<T> {
    Snapshot(Snapshot),
    Entries(Vec<T>),
}

#[derive(Debug, Clone)]
pub enum WeakEvent {
    Deliver,
    Uniform(Snapshot),
}

/// An eventually consistent replication layer that replicates one totally ordered log per
/// participant, and keeps track of which entries are quorum-replicated.
pub struct WeakReplication<T>
where T: Serialize + DeserializeOwned + Send + 'static {
    network: Network<Message, DeMon>,
    event_sender: Sender<WeakEvent>,
    quorum_replicated_snapshot: Arc<Mutex<Snapshot>>,
    current_snapshot: Arc<Mutex<Snapshot>>,
    peer_snapshots: Arc<Mutex<HashMap<NodeId, Snapshot>>>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for WeakReplication<T>
where T: Serialize + DeserializeOwned + Send + 'static {
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            event_sender: self.event_sender.clone(),
            quorum_replicated_snapshot: self.quorum_replicated_snapshot.clone(),
            current_snapshot: self.current_snapshot.clone(),
            peer_snapshots: self.peer_snapshots.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> WeakReplication<T>
where T: Serialize + DeserializeOwned + Send + 'static {
    pub async fn new(network: Network<Message, DeMon>) -> (Self, Receiver<WeakEvent>) {
        let (sender, receiver) = channel(1000);

        // TODO: correctly initialize snapshots
        let current_snapshot = Arc::new(Mutex::new(Snapshot{vec: vec![]}));
        let quorum_replicated_snapshot = Arc::new(Mutex::new(Snapshot{vec: vec![]}));
        let peer_snapshots = Arc::new(Mutex::new(HashMap::new()));
        let gossiper = Self {
            network,
            event_sender: sender,
            quorum_replicated_snapshot,
            current_snapshot,
            peer_snapshots,
            _phantom: PhantomData,
        };
        // tokio::task::spawn(gossiper.clone().background_task());
        (gossiper, receiver)
    }

    /// Handle an incoming message.
    pub async fn handle_msg(&self, from: NodeId, data: Vec<u8>) {
        let msg: WeakMsg<T> = bincode::deserialize(&data).unwrap();
        match msg {
            WeakMsg::Snapshot(s) => {
                
            },
            WeakMsg::Entries(e) => {

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
