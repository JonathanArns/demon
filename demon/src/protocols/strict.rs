use crate::{api::{instrumentation::{log_instrumentation, InstrumentationEvent}, API}, causal_replication::Snapshot, network::{MsgHandler, Network, NodeId}, rdts::Operation, sequencer::{Sequencer, SequencerEvent}, storage::{basic::Storage, QueryResult, Transaction}};
use async_trait::async_trait;
use tokio::{net::ToSocketAddrs, sync::{mpsc::Receiver, oneshot, Mutex}};
use std::{collections::HashMap, sync::Arc};

use super::{TransactionId, Component, Message};

/// A basic deterministic implementation of strictly serializable replication
pub struct Strict<O: Operation> {
    network: Network<Message>,
    storage: Storage<O>,
    sequencer: Sequencer<Transaction<O>>,
    next_transaction_id: Arc<Mutex<TransactionId>>,
    /// Strong client requests wait here for transaction completion.
    waiting_transactions: Arc<Mutex<HashMap<TransactionId, oneshot::Sender<QueryResult<O>>>>>,
}

// TODO: this single message loop could become a point of contention...
// maybe instead send the messages to the components via channels?
// or just spawn a task for ones that block the loop...
#[async_trait]
impl<O: Operation> MsgHandler<Message> for Strict<O> {
    async fn handle_msg(&self, _from: NodeId, msg: Message) {
        match msg.component {
            Component::Sequencer => {
                self.sequencer.handle_msg(msg.payload).await;
            },
            Component::WeakReplication => unreachable!(),
            Component::Protocol => unreachable!(),
        }
    }
}

impl<O: Operation> Strict<O> {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<O>>, name: Option<String>) -> Arc<Self> {
        let network = Network::connect(addrs, cluster_size, name).await.unwrap();
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
        mut sequencer_events: Receiver<SequencerEvent<Transaction<O>>>,
        api: Box<dyn API<O>>,
    ) {
        let mut api_events = api.start(self.network.clone()).await;
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = api_events.recv().await.unwrap();
                if query.is_writing() {
                    let id = proto.generate_transaction_id().await;
                    #[cfg(feature = "instrument")]
                    log_instrumentation(InstrumentationEvent{
                        kind: String::from("initiated"),
                        val: None,
                        meta: Some(id.to_string() + " " + &query.name()),
                    });
                    let snapshot = Snapshot::new(&[]); // dummy snapshot, because we don't need it
                    let transaction = Transaction { id, snapshot, op: Some(query) };
                    proto.waiting_transactions.lock().await.insert(id, result_sender);
                    proto.sequencer.append(transaction).await;
                } else {
                    let response = proto.storage.exec(query).await;
                    let _ = result_sender.send(response);
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let e = sequencer_events.recv().await.unwrap();
                match e {
                    SequencerEvent::Decided(decided_entries) => {
                        for transaction in decided_entries {
                            if let Some(op) = transaction.op {
                                let result_sender = proto.waiting_transactions.lock().await.remove(&transaction.id);
                                let name = op.name();
                                let response = proto.storage.exec(op).await;
                                #[cfg(feature = "instrument")]
                                log_instrumentation(InstrumentationEvent{
                                    kind: String::from("visible"),
                                    val: None,
                                    meta: Some(transaction.id.to_string() + " " + &name),
                                });
                                if let Some(sender) = result_sender {
                                    // this node has a client waiting for this response
                                    let _ = sender.send(response);
                                }
                            }
                        }
                    },
                }
            }
        });
    }
}
