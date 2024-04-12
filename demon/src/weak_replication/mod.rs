use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, Mutex, RwLock};

use crate::{protocols::{Component, Message}, network::{Network, NodeId}};

mod snapshot;
pub use snapshot::Snapshot;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedEntry<T> {
    pub node: NodeId,
    pub idx: u64,
    pub value: T,
}

impl<T> TaggedEntry<T> {
    /// Checks if this entry is contained in the snapshot or not.
    /// If not, it updates the snapshot to include the entry.
    pub fn update_snapshot(&self, snapshot: &mut Snapshot) -> bool {
        if self.idx >= snapshot.get(self.node) {
            // idx + 1, because snapshots represent length, not the max index
            *snapshot.get_mut(self.node) = self.idx + 1;
            true
        } else {
            false
        }
    }

    /// Returns true, if this entry is contained in the snapshot.
    pub fn is_in_snapshot(&self, snapshot: &Snapshot) -> bool {
        self.idx < snapshot.get(self.node)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WeakMsg<T> {
    Snapshot(Snapshot),
    Entries(Vec<TaggedEntry<T>>),
}

#[derive(Debug, Clone)]
pub enum WeakEvent<T> {
    /// Triggered for all entries that did not orininate at this node.
    Deliver(TaggedEntry<T>),
    /// Triggered every time the quorum-replicated snapshot increases.
    QuorumReplicated(Snapshot),
}

struct LocallyOrderedEntry<T> {
    value: T,
    /// represents the order in which entries were locally received, to ensure causality within
    /// replication messages
    idx: usize,
}

/// An eventually consistent replication layer that replicates one totally ordered log per
/// participant, and keeps track of which entries are quorum-replicated.
///
/// TODO: garbage collection / log compaction (should be possible for fully replicated entries)
pub struct WeakReplication<T> {
    network: Network<Message>,
    event_sender: Sender<WeakEvent<T>>,
    /// The replicated logs, with an offset
    logs: Arc<RwLock<HashMap<NodeId, (usize, Vec<LocallyOrderedEntry<T>>)>>>,
    /// a counter that is incremented for each entry
    local_log_len: Arc<Mutex<usize>>,
    quorum_replicated_snapshot: Arc<Mutex<Snapshot>>,
    current_snapshot: Arc<Mutex<Snapshot>>,
    peer_snapshots: Arc<Mutex<HashMap<NodeId, Snapshot>>>,
    quorum_size: u64,
    _phantom: PhantomData<T>,
}

impl<T> Clone for WeakReplication<T> {
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            event_sender: self.event_sender.clone(),
            logs: self.logs.clone(),
            local_log_len: self.local_log_len.clone(),
            quorum_replicated_snapshot: self.quorum_replicated_snapshot.clone(),
            current_snapshot: self.current_snapshot.clone(),
            peer_snapshots: self.peer_snapshots.clone(),
            quorum_size: self.quorum_size,
            _phantom: PhantomData,
        }
    }
}

impl<T> WeakReplication<T>
where T: 'static + Clone + Serialize + DeserializeOwned + Send + Sync {
    pub async fn new(network: Network<Message>) -> (Self, Receiver<WeakEvent<T>>) {
        let (sender, receiver) = channel(1000);

        let nodes = network.nodes().await;
        let mut logs = HashMap::new();
        for id in &nodes {
            logs.insert(*id, (0, vec![]));
        }
        let current_snapshot = Arc::new(Mutex::new(Snapshot::new(&nodes)));
        let quorum_replicated_snapshot = Arc::new(Mutex::new(Snapshot::new(&nodes)));
        let peer_snapshots = Arc::new(Mutex::new(HashMap::new()));
        let quorum_size = (nodes.len() as u64) / 2 + 1;
        let weak_replication = Self {
            network,
            event_sender: sender,
            logs: Arc::new(RwLock::new(logs)),
            local_log_len: Default::default(),
            quorum_replicated_snapshot,
            current_snapshot,
            peer_snapshots,
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
                {
                    let logs_latch = self.logs.read().await;
                    for id in 1..=s.vec.len() {
                        let node_id = NodeId(id as u32);
                        // we only respond with entries, if we also replicate this log
                        if let Some((offset, log)) = logs_latch.get(&node_id) {
                            let range_start = s.get(node_id) as usize - *offset;
                            if log.len() > range_start {
                                let peer_offset = s.get(node_id);
                                let tagged_entries = log[range_start..].iter().enumerate().map(|(i, entry)| {
                                    LocallyOrderedEntry{
                                        idx: entry.idx,
                                        value: TaggedEntry {
                                            value: entry.value.clone(),
                                            node: node_id,
                                            idx: peer_offset + i as u64,
                                        }
                                    }
                                });
                                entries.extend(tagged_entries);
                            }
                        }
                    }
                }
                if entries.len() > 0 {
                    entries.sort_unstable_by_key(|e| e.idx);
                    let entries = entries.into_iter().map(|e| e.value).collect();
                    let payload = bincode::serialize(&WeakMsg::Entries(entries)).unwrap();
                    let msg = Message{component: Component::WeakReplication, payload};
                    self.network.send(from, msg).await;
                }
                self.peer_snapshots.lock().await.insert(from, s);
                if let Some(snapshot) = self.update_quorum_replicated_snapshot().await {
                    self.event_sender.send(WeakEvent::QuorumReplicated(snapshot)).await.unwrap();
                }
            },
            WeakMsg::Entries(e) => {
                let mut latch = self.current_snapshot.lock().await;
                let mut logs = self.logs.write().await;
                let mut local_log_len = self.local_log_len.lock().await;
                for entry in e {
                    let is_new_entry = entry.update_snapshot(&mut latch);
                    if is_new_entry {
                        let (_offset, log) = logs.get_mut(&entry.node).unwrap();
                        let locally_ordered_entry = LocallyOrderedEntry {
                            idx: *local_log_len,
                            value: entry.value.clone(),
                        };
                        *local_log_len += 1;
                        log.push(locally_ordered_entry);
                        self.event_sender.send(WeakEvent::Deliver(entry)).await.unwrap();
                    }
                }
            },
        }
    }

    /// Stores and starts replicating an entry in this node's log.
    pub async fn replicate(&self, entry: T) {
        let my_id = self.network.my_id().await;
        self.current_snapshot.lock().await.increment(my_id, 1);
        let mut log_latch = self.logs.write().await;
        let (_offset, log) = log_latch.get_mut(&my_id).unwrap();
        let mut local_log_len = self.local_log_len.lock().await;
        let locally_ordered_entry = LocallyOrderedEntry {
            idx: *local_log_len,
            value: entry,
        };
        *local_log_len += 1;
        log.push(locally_ordered_entry);
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
            tokio::time::sleep(Duration::from_micros(100_000)).await;
            let payload = bincode::serialize(&WeakMsg::<T>::Snapshot(self.current_snapshot.lock().await.clone())).unwrap();
            let msg = Message{component: Component::WeakReplication, payload};
            self.network.broadcast(msg).await;
        }
    }
}
