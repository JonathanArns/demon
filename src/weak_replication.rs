use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, Mutex, RwLock};

use crate::{demon::{Component, DeMon, Message}, network::{Network, NodeId}, storage::Snapshot};


/// The weak replication layer needs a limited amount of storage access through this trait.
pub trait WeakLogStorage<T>: Send + Sync + 'static {
    fn read(&self, log: NodeId, from: u64) -> Vec<T>;
}

/// The weak replication layer needs some ordering information about the entries it is replicating
/// through this trait.
pub trait WeakLogEntry: Serialize + DeserializeOwned + Send + 'static {
    /// Checks if this entry is already part of the snapshot.
    /// If not, the snapshot is incremented accordingly.
    ///
    /// Returns true iff the entry was not part of the snapshot before.
    fn update_snapshot(&self, snapshot: &mut Snapshot) -> bool;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WeakMsg<T> {
    Snapshot(Snapshot),
    Entries(Vec<T>),
}

#[derive(Debug, Clone)]
pub enum WeakEvent<T> {
    Deliver(T),
    QuorumReplicated(Snapshot),
}

/// An eventually consistent replication layer that replicates one totally ordered log per
/// participant, and keeps track of which entries are quorum-replicated.
pub struct WeakReplication<T, S>
where
    T: WeakLogEntry,
    S: WeakLogStorage<T>,
{
    network: Network<Message, DeMon>,
    event_sender: Sender<WeakEvent<T>>,
    quorum_replicated_snapshot: Arc<Mutex<Snapshot>>,
    current_snapshot: Arc<Mutex<Snapshot>>,
    peer_snapshots: Arc<Mutex<HashMap<NodeId, Snapshot>>>,
    storage: Arc<RwLock<S>>,
    quorum_size: u64,
    _phantom: PhantomData<T>,
}

impl<T, S> Clone for WeakReplication<T, S>
where
    T: WeakLogEntry,
    S: WeakLogStorage<T>,
{
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            event_sender: self.event_sender.clone(),
            quorum_replicated_snapshot: self.quorum_replicated_snapshot.clone(),
            current_snapshot: self.current_snapshot.clone(),
            peer_snapshots: self.peer_snapshots.clone(),
            storage: self.storage.clone(),
            quorum_size: self.quorum_size,
            _phantom: PhantomData,
        }
    }
}

impl<T, S> WeakReplication<T, S>
where
    T: WeakLogEntry,
    S: WeakLogStorage<T>,
{
    pub async fn new(network: Network<Message, DeMon>, storage: Arc<RwLock<S>>) -> (Self, Receiver<WeakEvent<T>>) {
        let (sender, receiver) = channel(1000);

        // TODO: correctly initialize snapshots
        let current_snapshot = Arc::new(Mutex::new(Snapshot{vec: vec![]}));
        let quorum_replicated_snapshot = Arc::new(Mutex::new(Snapshot{vec: vec![]}));
        let peer_snapshots = Arc::new(Mutex::new(HashMap::new()));
        let quorum_size = (network.nodes().await.len() as u64) / 2 + 1;
        let weak_replication = Self {
            network,
            event_sender: sender,
            quorum_replicated_snapshot,
            current_snapshot,
            peer_snapshots,
            storage,
            quorum_size,
            _phantom: PhantomData,
        };
        tokio::task::spawn(weak_replication.clone().run_gossip());
        (weak_replication, receiver)
    }

    /// Handle an incoming message.
    pub async fn handle_msg(&self, from: NodeId, data: Vec<u8>) {
        let msg: WeakMsg<T> = bincode::deserialize(&data).unwrap();
        match msg {
            WeakMsg::Snapshot(s) => {
                let mut entries = vec![];
                for i in 0..s.vec.len() {
                    entries.extend(self.storage.read().await.read(NodeId(i as u32), s.vec[i]))
                }
                let payload = bincode::serialize(&WeakMsg::Entries(entries)).unwrap();
                let msg = Message{component: Component::WeakReplication, payload};
                self.network.send(from, msg).await;
                self.peer_snapshots.lock().await.insert(from, s);
                if let Some(snapshot) = self.update_quorum_replicated_snapshot().await {
                    self.event_sender.send(WeakEvent::QuorumReplicated(snapshot)).await.unwrap();
                }
            },
            WeakMsg::Entries(e) => {
                let mut latch = self.current_snapshot.lock().await;
                for entry in e {
                    let new_entry = entry.update_snapshot(&mut latch);
                    if new_entry {
                        self.event_sender.send(WeakEvent::Deliver(entry)).await.unwrap();
                    }
                }
            },
        }
    }

    /// Re-computes the quorum_replicated_snapshot.
    ///
    /// Returns `Some(snapshot)` iff the snapshot increased.
    async fn update_quorum_replicated_snapshot(&self) -> Option<Snapshot> {
        let mut qs_latch = self.quorum_replicated_snapshot.lock().await;
        let peer_latch = self.peer_snapshots.lock().await;
        let current_latch = self.current_snapshot.lock().await;
        let mut snapshot = vec![];
        for i in 0..qs_latch.vec.len() {
            let mut values = vec![current_latch.vec[i]];
            for peer in peer_latch.values() {
                values.push(peer.vec[i]);
            }
            values.sort_unstable();
            snapshot.push(values[(self.quorum_size - 1) as usize]);
        }
        let snapshot = Snapshot{vec: snapshot};
        if let Some(true) = snapshot.greater(&*qs_latch) {
            *qs_latch = snapshot.clone();
            Some(snapshot)
        } else {
            None
        }
    }

    /// Periodically broadcast current snapshot.
    ///
    /// Should be called with a cloned handle to the sequencer.
    async fn run_gossip(self) {
        loop {
            tokio::time::sleep(Duration::from_micros(100_000)).await;
            let payload = bincode::serialize(&*self.current_snapshot.lock().await).unwrap();
            let msg = Message{component: Component::WeakReplication, payload};
            self.network.broadcast(msg).await;
        }
    }
}
