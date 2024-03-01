use crate::{network::{MsgHandler, Network, NodeId}, sequencer::{Sequencer, SequencerEvent}, storage::{counters::CounterOp, Storage, TaggedOperation}, weak_replication::{WeakEvent, WeakReplication}};
use async_trait::async_trait;
use futures::Future;
use serde::{Serialize, Deserialize};
use tokio::{net::ToSocketAddrs, select, sync::{mpsc::Receiver, OnceCell, RwLock}};
use std::{sync::Arc, pin::Pin};
use omnipaxos::storage::{Entry, NoSnapshot};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {

}

impl Entry for Transaction {
    type Snapshot = NoSnapshot;
}

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

/// A helper trait that we need to make rustc happy
trait AsyncMsgHandler: Clone + FnOnce(NodeId, Message) -> Pin<Box<dyn Send + Future<Output = ()>>> {}

/// The DeMon mixed consistency protocol.
/// 
/// DeMon is not `Clone` on purpose, so that not all of its components need to be as well.
/// DeMon is `Sync` however, and never requires mutable access, so an `Arc<DeMon>` will do the trick.
pub struct DeMon {
    network: Network<Message, Self>,
    storage: Arc<RwLock<Storage<CounterOp>>>,
    sequencer: Sequencer<Transaction>,
    weak_replication: WeakReplication<TaggedOperation<CounterOp>, Storage<CounterOp>>,
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
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32) -> Arc<Self> {
        let demon_cell = Arc::new(OnceCell::const_new());
        let network = Network::connect(addrs, cluster_size, demon_cell.clone()).await.unwrap();
        let storage = Arc::new(RwLock::new(Storage::new(network.nodes().await)));
        let (sequencer, sequencer_events) = Sequencer::new(network.clone()).await;
        let (weak_replication, weak_replication_events) = WeakReplication::new(network.clone(), storage.clone()).await;
        let demon = demon_cell.get_or_init(|| async move {
            Arc::new(Self {
                network,
                storage,
                sequencer,
                weak_replication,
            })
        }).await.clone();
        tokio::task::spawn(demon.clone().event_loop(
            sequencer_events,
            weak_replication_events,
        ));
        demon
    }

    /// Process events from the components.
    async fn event_loop(
        self: Arc<Self>,
        mut sequencer_events: Receiver<SequencerEvent>,
        mut weak_replication_events: Receiver<WeakEvent<TaggedOperation<CounterOp>>>,
    ) {
        loop {
            select! {
                Some(e) = sequencer_events.recv() => {
                    todo!("execute decided transactions")
                },
                Some(e) = weak_replication_events.recv() => {
                    todo!("execute decided transactions")
                },
            }
        }
    }
}
