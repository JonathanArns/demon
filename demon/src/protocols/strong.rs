use crate::{api::API, network::{MsgHandler, Network, NodeId}, sequencer::{Sequencer, SequencerEvent}, storage::{basic::Storage, Response, Transaction}, weak_replication::Snapshot};
use async_trait::async_trait;
use tokio::{net::ToSocketAddrs, sync::{mpsc::Receiver, oneshot, Mutex}};
use std::{collections::HashMap, sync::Arc};

use super::{Op, TransactionId, Component, Message};

/// A basic deterministic implementation of strictly serializable replication
pub struct Strong {
    network: Network<Message>,
    storage: Storage<Op>,
    sequencer: Sequencer<Transaction<Op>>,
    next_transaction_id: Arc<Mutex<TransactionId>>,
    /// Strong client requests wait here for transaction completion.
    waiting_transactions: Arc<Mutex<HashMap<TransactionId, oneshot::Sender<Response<Op>>>>>,
}

// TODO: this single message loop could become a point of contention...
// maybe instead send the messages to the components via channels?
// or just spawn a task for ones that block the loop...
#[async_trait]
impl MsgHandler<Message> for Strong {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg.component {
            Component::Sequencer => {
                self.sequencer.handle_msg(msg.payload).await;
            },
            Component::WeakReplication => {
                unreachable!()
            }
        }
    }
}

impl Strong {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<Op>>) -> Arc<Self> {
        let network = Network::connect(addrs, cluster_size).await.unwrap();
        let storage = Storage::new();
        let (sequencer, sequencer_events) = Sequencer::new(network.clone()).await;
        let my_id = network.my_id().await;
        let proto = Arc::new(Self {
            network: network.clone(),
            storage,
            sequencer,
            next_transaction_id: Arc::new(Mutex::new(TransactionId(my_id, 0))),
            waiting_transactions: Default::default(),
        });
        network.set_msg_handler(proto.clone()).await;
        tokio::task::spawn(proto.clone().event_loop(
            sequencer_events,
            api,
        ));
        proto
    }

    /// Generates a new globally unique transaction Id.
    async fn generate_transaction_id(&self) -> TransactionId {
        let mut latch = self.next_transaction_id.lock().await;
        let id = *latch;
        latch.1 += 1;
        id
    }

    /// Process events from the components.
    /// Spawns an individual task for each event stream.
    async fn event_loop(
        self: Arc<Self>,
        mut sequencer_events: Receiver<SequencerEvent<Transaction<Op>>>,
        api: Box<dyn API<Op>>,
    ) {
        let mut api_events = api.start().await;
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = api_events.recv().await.unwrap();
                let id = proto.generate_transaction_id().await;
                let snapshot = Snapshot::new(&[]); // dummy snapshot, because we don't need it
                let transaction = Transaction { id, snapshot, op: query };
                proto.sequencer.append(transaction).await;
                proto.waiting_transactions.lock().await.insert(id, result_sender);
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let e = sequencer_events.recv().await.unwrap();
                match e {
                    SequencerEvent::Decided(decided_entries) => {
                        for transaction in decided_entries {
                            let result_sender = proto.waiting_transactions.lock().await.remove(&transaction.id);
                            let response = proto.storage.exec(transaction.op).await;
                            if let Some(sender) = result_sender {
                                // this node has a client waiting for this response
                                sender.send(response).unwrap();
                            }
                        }
                    },
                }
            }
        });
    }
}
