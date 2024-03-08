use crate::{api::API, network::{MsgHandler, Network, NodeId}, sequencer::{Sequencer, SequencerEvent}, storage::{counters::CounterOp, Response, Storage, Transaction}, weak_replication::{Snapshot, WeakEvent, WeakReplication}};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use tokio::{net::ToSocketAddrs, select, sync::{mpsc::Receiver, oneshot, Mutex, OnceCell, RwLock}};
use std::{collections::HashMap, sync::Arc};


#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TransactionId(NodeId, u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Component {
    Sequencer,
    WeakReplication,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub payload: Vec<u8>,
    pub component: Component,
}

/// The DeMon mixed consistency protocol.
/// 
/// DeMon is not `Clone` on purpose, so that not all of its components need to be as well.
/// DeMon is `Sync` however, and never requires mutable access, so an `Arc<DeMon>` will do the trick.
pub struct DeMon {
    network: Network<Message, Self>,
    storage: Storage<CounterOp>,
    sequencer: Sequencer<Transaction<CounterOp>>,
    weak_replication: WeakReplication<CounterOp>,
    next_transaction_id: Arc<Mutex<TransactionId>>,
    next_transaction_snapshot: Arc<RwLock<Snapshot>>,
    /// Strong client requests wait here for transaction completion.
    waiting_transactions: Arc<Mutex<HashMap<TransactionId, oneshot::Sender<Response<CounterOp>>>>>,
}

// TODO: this single message loop could become a point of contention...
// maybe instead send the messages to the components via channels?
// or just spawn a task for ones that block the loop...
#[async_trait]
impl MsgHandler<Message> for DeMon {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg.component {
            Component::Sequencer => {
                self.sequencer.handle_msg(msg.payload).await;
            },
            Component::WeakReplication => {
                self.weak_replication.handle_msg(from, msg.payload).await;
            }
        }
    }
}

impl DeMon {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<CounterOp>>) -> Arc<Self> {
        let demon_cell = Arc::new(OnceCell::const_new());
        let network = Network::connect(addrs, cluster_size, demon_cell.clone()).await.unwrap();
        let storage = Storage::new(network.nodes().await);
        let (sequencer, sequencer_events) = Sequencer::new(network.clone()).await;
        let (weak_replication, weak_replication_events) = WeakReplication::new(network.clone()).await;
        let my_id = network.my_id().await;
        let nodes = network.nodes().await;
        let demon = demon_cell.get_or_init(|| async move {
            Arc::new(Self {
                network,
                storage,
                sequencer,
                weak_replication,
                next_transaction_id: Arc::new(Mutex::new(TransactionId(my_id, 0))),
                next_transaction_snapshot: Arc::new(RwLock::new(Snapshot{vec:vec![0; nodes.len()]})),
                waiting_transactions: Default::default(),
            })
        }).await.clone();
        tokio::task::spawn(demon.clone().event_loop(
            sequencer_events,
            weak_replication_events,
            api,
        ));
        demon
    }

    /// Generates a new globally unique transaction Id.
    async fn generate_transaction_id(&self) -> TransactionId {
        let mut latch = self.next_transaction_id.lock().await;
        let id = *latch;
        latch.1 += 1;
        id
    }

    /// Chooses the snapshot for the next transaction proposed at this node.
    async fn choose_transaction_snapshot(&self) -> Snapshot {
        self.next_transaction_snapshot.read().await.clone()
    }

    /// Process events from the components.
    async fn event_loop(
        self: Arc<Self>,
        mut sequencer_events: Receiver<SequencerEvent>,
        mut weak_replication_events: Receiver<WeakEvent<CounterOp>>,
        api: Box<dyn API<CounterOp>>,
    ) {
        let my_id = self.network.my_id().await;
        let (mut weak_api_events, mut strong_api_events) = api.start().await;
        loop {
            select! {
                Some((query, result_sender)) = weak_api_events.recv() => {
                    let (result, entries_to_replicate) = self.storage.exec_weak_query(query, my_id).await;
                    result_sender.send(result).unwrap();
                    for entry in entries_to_replicate {
                        self.weak_replication.replicate(entry).await;
                    }
                },
                Some((query, result_sender)) = strong_api_events.recv() => {
                    // generate transaction ID, start task waiting for result, replicate, then execute
                    let id = self.generate_transaction_id().await;
                    let snapshot = self.choose_transaction_snapshot().await;
                    let transaction = Transaction { id, snapshot, query };
                    self.sequencer.append(transaction).await;
                    self.waiting_transactions.lock().await.insert(id, result_sender);
                },
                Some(e) = sequencer_events.recv() => {
                    match e {
                        SequencerEvent::Decided(from, to) => {
                            for transaction in self.sequencer.read(from..to).await {
                                let result_sender = self.waiting_transactions.lock().await.remove(&transaction.id);
                                let response = self.storage.exec_transaction(transaction).await;
                                if let Some(sender) = result_sender {
                                    // this node has a client waiting for this response
                                    sender.send(response).unwrap();
                                }
                            }
                        },
                    }
                },
                Some(e) = weak_replication_events.recv() => {
                    match e {
                        WeakEvent::Deliver(op) => {
                            self.storage.exec_remote_weak_query(op).await;
                        },
                        WeakEvent::QuorumReplicated(snapshot) => {
                            println!("{:?}", snapshot);
                            self.next_transaction_snapshot.write().await.merge_inplace(&snapshot);
                        },
                    }
                },
            }
        }
    }
}
