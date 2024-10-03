use std::{collections::{HashMap, VecDeque}, marker::PhantomData, sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, Mutex, RwLock};

use crate::{network::{Network, NodeId}, protocols::{Component, Message}};

mod snapshot;
pub use snapshot::Snapshot;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedEntry<T> {
    pub causality: Snapshot,
    pub from: NodeId,
    pub value: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CausalReplicationMsg<T> {
    Snapshot(Snapshot),
    Entries(Vec<TaggedEntry<T>>),
    Missing{
        from_idx: u64,
        to_idx: u64,
    }
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
    waiting: Arc<Mutex<Vec<VecDeque<TaggedEntry<T>>>>>,
    to_send: Arc<Mutex<usize>>,
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
            waiting: self.waiting.clone(),
            to_send: self.to_send.clone(),
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
            waiting: Arc::new(Mutex::new(vec![VecDeque::new(); nodes.len()])),
            to_send: Default::default(),
            quorum_replicated_snapshot,
            current_snapshot,
            peer_snapshots,
            quorum_size,
            _phantom: PhantomData,
        };
        tokio::task::spawn(causal_replication.clone().flush_batch());
        tokio::task::spawn(causal_replication.clone().run_gossip());
        tokio::task::spawn(causal_replication.clone().run_gc());
        (causal_replication, receiver)
    }

    /// Handle an incoming message.
    pub async fn handle_msg(&self, from: NodeId, data: Vec<u8>) {
        let msg: CausalReplicationMsg<T> = bincode::deserialize(&data).unwrap();
        match msg {
            CausalReplicationMsg::Snapshot(s) => {
                self.peer_snapshots.lock().await.insert(from, s);
                if let Some(snapshot) = self.update_quorum_replicated_snapshot().await {
                    self.event_sender.send(CausalReplicationEvent::QuorumReplicated(snapshot)).await.unwrap();
                }
            },
            CausalReplicationMsg::Entries(e) => {
                let mut current_snapshot = self.current_snapshot.lock().await;
                for entry in e {
                    if entry.causality.included_in(&current_snapshot) && entry.causality.get(from) + 1 > current_snapshot.get(from) {
                        current_snapshot.increment(entry.from, 1);
                        self.event_sender.send(CausalReplicationEvent::Deliver(entry)).await.unwrap();
                    } else {
                        let mut waiting = self.waiting.lock().await;
                        let mut min = current_snapshot.get(from);
                        if min < entry.causality.get(from) {
                            if let Some(waiting_entry) = waiting[from.0 as usize - 1].back() {
                                let seq = waiting_entry.causality.get(from);
                                if seq > min && seq < entry.causality.get(from) {
                                    min = seq;
                                }
                            }
                            if min+1 < entry.causality.get(from) {
                                self.network.send(from, Message{
                                    component: Component::WeakReplication,
                                    payload: bincode::serialize(&CausalReplicationMsg::<T>::Missing{ from_idx: min+1, to_idx: entry.causality.get(from) }).unwrap(),
                                }).await;
                            }
                        }
                        let queue = &mut waiting[from.0 as usize - 1];
                        let mut i = queue.len();
                        while i > 0 {
                            if queue[i - 1].causality.get(from) < entry.causality.get(from) {
                                break
                            } else {
                                i -= 1;
                            }
                        }
                        queue.insert(i, entry);
                        drop(waiting);
                    }
                }
                let mut waiting = self.waiting.lock().await;
                self.deliver_waiting(&mut waiting, &mut current_snapshot).await;
            },
            CausalReplicationMsg::Missing { from_idx, to_idx } => {
                let my_id = self.network.my_id().await;
                let log = self.log.read().await;
                let mut entries = vec![];
                for entry in log.iter() {
                    if entry.from == my_id && entry.causality.get(my_id) < to_idx && entry.causality.get(my_id) + 1 >= from_idx {
                        entries.push(entry.clone());
                    }
                }
                self.network.send(from, Message{
                    component: Component::WeakReplication,
                    payload: bincode::serialize(&CausalReplicationMsg::<T>::Entries(entries)).unwrap(),
                }).await;
            }
        }
    }

    /// Stores and starts replicating an entry in this node's log.
    pub async fn replicate(&self, entry: T) -> TaggedEntry<T> {
        let mut log = self.log.write().await;
        let tagged_entry = {
            let mut snapshot = self.current_snapshot.lock().await;
            let my_id = self.network.my_id().await;
            let tagged = TaggedEntry {
                causality: snapshot.clone(),
                from: my_id,
                value: entry,
            };
            snapshot.increment(my_id, 1);
            tagged
        };
        log.push_back(tagged_entry.clone());
        *self.to_send.lock().await += 1;
        if log.len() >= 1024 * 1024 {
            drop(log);
            self.collect_garbage().await;
            while self.log.read().await.len() >= 1024 * 1024 {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
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
        if log.capacity() > (2 * len).max(1024 * 1024 * 2) {
            log.shrink_to((2 * len).max(1024 * 1024 * 2));
        }
    }

    async fn deliver_waiting(&self, waiting: &mut Vec<VecDeque<TaggedEntry<T>>>, snapshot: &mut Snapshot) {
        for queue in waiting {
            while let Some(entry) = queue.pop_front() {
                if entry.causality.included_in(&snapshot) {
                    if entry.causality.get(entry.from) + 1 > snapshot.get(entry.from) {
                        snapshot.increment(entry.from, 1);
                        self.event_sender.send(CausalReplicationEvent::Deliver(entry)).await.unwrap();
                    }
                } else {
                    queue.push_front(entry);
                    break
                }
            }
        }
    }

    /// Re-computes the quorum_replicated_snapshot.
    ///
    /// Returns `Some(snapshot)` if the snapshot increased.
    async fn update_quorum_replicated_snapshot(&self) -> Option<Snapshot> {
        let mut qs_latch = self.quorum_replicated_snapshot.lock().await;
        let peer_latch = self.peer_snapshots.lock().await;
        let current_latch = self.current_snapshot.lock().await;
        if peer_latch.len() < current_latch.vec.len() - 1 {
            return None
        }
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
    async fn run_gossip(self) {
        loop {
            tokio::time::sleep(Duration::from_millis(23)).await;
            let payload = bincode::serialize(&CausalReplicationMsg::<T>::Snapshot(self.current_snapshot.lock().await.clone())).unwrap();
            let msg = Message{component: Component::WeakReplication, payload};
            self.network.broadcast(msg).await;
        }
    }

    /// Sends new entries in a batch.
    async fn flush_batch(self) {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let log = self.log.write().await;
            let mut to_send = self.to_send.lock().await;
            let entries = log.range((log.len() - *to_send)..log.len()).map(|x| x.to_owned()).collect();
            *to_send = 0;
            let payload = bincode::serialize(&CausalReplicationMsg::<T>::Entries(entries)).unwrap();
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
