use std::{collections::{HashMap, VecDeque}, marker::PhantomData, sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, Mutex, RwLock};

use crate::{protocols::{Component, Message}, network::{Network, NodeId}};

mod snapshot;
pub use snapshot::Snapshot;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedEntry<T> {
    pub causality: Snapshot,
    pub value: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CausalReplicationMsg<T> {
    Snapshot(Snapshot),
    Entries(Vec<TaggedEntry<T>>),
}

#[derive(Debug, Clone)]
pub enum CausalReplicationEvent<T> {
    /// Triggered for all entries that did not orininate at this node.
    Deliver(TaggedEntry<T>),
    /// Triggered every time the quorum-replicated snapshot increases.
    QuorumReplicated(Snapshot),
}

/// An eventually consistent replication layer that replicates one totally ordered log per
/// participant, and keeps track of which entries are quorum-replicated.
pub struct CausalReplication<T> {
    network: Network<Message>,
    event_sender: Sender<CausalReplicationEvent<T>>,
    log: Arc<RwLock<VecDeque<TaggedEntry<T>>>>,
    current_snapshot: Arc<Mutex<Snapshot>>,
    peer_snapshots: Arc<Mutex<HashMap<NodeId, Snapshot>>>,
    quorum_replicated_snapshot: Arc<Mutex<Snapshot>>,
    quorum_size: u64,
    _phantom: PhantomData<T>,
}

impl<T> Clone for CausalReplication<T> {
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            event_sender: self.event_sender.clone(),
            log: self.log.clone(),
            quorum_replicated_snapshot: self.quorum_replicated_snapshot.clone(),
            current_snapshot: self.current_snapshot.clone(),
            peer_snapshots: self.peer_snapshots.clone(),
            quorum_size: self.quorum_size,
            _phantom: PhantomData,
        }
    }
}

impl<T> CausalReplication<T>
where T: 'static + Clone + Serialize + DeserializeOwned + Send + Sync {
    pub async fn new(network: Network<Message>) -> (Self, Receiver<CausalReplicationEvent<T>>) {
        let (sender, receiver) = channel(1000);

        let nodes = network.nodes().await;
        let current_snapshot = Arc::new(Mutex::new(Snapshot::new(&nodes)));
        let quorum_replicated_snapshot = Arc::new(Mutex::new(Snapshot::new(&nodes)));
        let peer_snapshots = Arc::new(Mutex::new(HashMap::new()));
        let quorum_size = (nodes.len() as u64) / 2 + 1;
        let causal_replication = Self {
            network,
            event_sender: sender,
            log: Default::default(),
            quorum_replicated_snapshot,
            current_snapshot,
            peer_snapshots,
            quorum_size,
            _phantom: PhantomData,
        };
        tokio::task::spawn(causal_replication.clone().run_gossip());
        tokio::task::spawn(causal_replication.clone().run_gc());
        (causal_replication, receiver)
    }

    /// Handle an incoming message.
    pub async fn handle_msg(&self, from: NodeId, data: Vec<u8>) {
        let msg: CausalReplicationMsg<T> = bincode::deserialize(&data).unwrap();
        match msg {
            CausalReplicationMsg::Snapshot(s) => {
                let mut entries = vec![];
                {
                    let log = self.log.read().await;
                    for entry in log.iter() {
                        if entry.causality.greater(&s) {
                            entries.push(entry.clone());
                        }
                    }
                }
                if entries.len() > 0 {
                    let payload = bincode::serialize(&CausalReplicationMsg::Entries(entries)).unwrap();
                    let msg = Message{component: Component::WeakReplication, payload};
                    self.network.send(from, msg).await;
                }
                self.peer_snapshots.lock().await.insert(from, s);
                if let Some(snapshot) = self.update_quorum_replicated_snapshot().await {
                    self.event_sender.send(CausalReplicationEvent::QuorumReplicated(snapshot)).await.unwrap();
                }
            },
            CausalReplicationMsg::Entries(e) => {
                let mut current_snapshot = self.current_snapshot.lock().await;
                let mut log = self.log.write().await;
                for entry in e {
                    if entry.causality.included_in(&current_snapshot) {
                        continue
                    }
                    current_snapshot.merge_inplace(&entry.causality);
                    log.push_back(entry.clone());
                    self.event_sender.send(CausalReplicationEvent::Deliver(entry)).await.unwrap();
                }
            },
        }
    }

    /// Stores and starts replicating an entry in this node's log.
    pub async fn replicate(&self, entry: T) -> TaggedEntry<T> {
        let my_id = self.network.my_id().await;
        let mut snapshot = self.current_snapshot.lock().await;
        snapshot.increment(my_id, 1);
        let tagged_entry = TaggedEntry {
            causality: snapshot.clone(),
            value: entry,
        };
        self.log.write().await.push_back(tagged_entry.clone());
        tagged_entry
    }

    /// Performs garbage collection.
    async fn collect_garbage(&self) {
        let fully_replicated_snapshot = {
            let peer_latch = self.peer_snapshots.lock().await;
            let mut fully_replicated_snapshot = self.current_snapshot.lock().await.clone();
            for peer_snapshot in peer_latch.values() {
                fully_replicated_snapshot.greatest_lower_bound_inplace(peer_snapshot);
            }
            fully_replicated_snapshot
        };

        let mut log = self.log.write().await;
        while let Some(entry) = log.front() {
            if entry.causality.included_in(&fully_replicated_snapshot) {
                log.pop_front();
            } else {
                break
            }
        }
        // maybe shrink capacity
        let len = log.len();
        if log.capacity() > 1024.max(10 * len) {
            log.shrink_to(1024.max(2 * len))
        }
    }

    /// Re-computes the quorum_replicated_snapshot.
    ///
    /// Returns `Some(snapshot)` if the snapshot increased.
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
        if snapshot.greater(&*qs_latch) {
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
            tokio::time::sleep(Duration::from_millis(23)).await;
            let payload = bincode::serialize(&CausalReplicationMsg::<T>::Snapshot(self.current_snapshot.lock().await.clone())).unwrap();
            let msg = Message{component: Component::WeakReplication, payload};
            self.network.broadcast(msg).await;
        }
    }

    /// Periodically runs garbage collection.
    ///
    /// Should be called with a cloned handle to the sequencer.
    async fn run_gc(self) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            self.collect_garbage().await;
        }
    }
}
