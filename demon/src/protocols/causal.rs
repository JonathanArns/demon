use crate::{api::{instrumentation::{log_instrumentation, InstrumentationEvent}, API}, causal_replication::{CausalReplication, CausalReplicationEvent}, network::{MsgHandler, Network, NodeId}, rdts::Operation, storage::{basic::Storage, QueryResult}};
use async_trait::async_trait;
use tokio::{net::ToSocketAddrs, sync::{mpsc::Receiver, Mutex}};
use std::sync::Arc;

use super::{Component, Message, TransactionId};

/// A basic implementation of causal order RDTs
pub struct Causal<O: Operation> {
    network: Network<Message>,
    storage: Storage<O>,
    causal_replication: CausalReplication<(TransactionId, O)>,
    next_transaction_id: Arc<Mutex<TransactionId>>,
}

// TODO: this single message loop could become a point of contention...
// maybe instead send the messages to the components via channels?
// or just spawn a task for ones that block the loop...
#[async_trait]
impl<O: Operation> MsgHandler<Message> for Causal<O> {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg.component {
            Component::WeakReplication => {
                self.causal_replication.handle_msg(from, msg.payload).await;
            },
            Component::Protocol => unreachable!(),
            Component::Sequencer => unreachable!(),
        }
    }
}

impl<O: Operation> Causal<O> {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<O>>, name: Option<String>) -> Arc<Self> {
        let network = Network::connect(addrs, cluster_size, name).await.unwrap();
        let storage = Storage::new();
        let (causal_replication, causal_replication_events) = CausalReplication::new(network.clone()).await;
        let id = network.my_id().await;
        let proto = Arc::new(Self {
            network: network.clone(),
            storage,
            causal_replication,
            next_transaction_id: Arc::new(Mutex::new(TransactionId(id, 0))),
        });
        network.set_msg_handler(proto.clone()).await;
        tokio::task::spawn(proto.clone().event_loop(
            causal_replication_events,
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
        mut causal_replication_events: Receiver<CausalReplicationEvent<(TransactionId, O)>>,
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
                    if let Some(shadow) = proto.storage.generate_shadow(query).await {
                        let result = proto.storage.exec(shadow.clone()).await;
                        let _ = result_sender.send(result);
                        proto.causal_replication.replicate((id, shadow)).await;
                    } else {
                        let _ = result_sender.send(QueryResult { value: None });
                    }
                } else {
                    let result = proto.storage.exec(query).await;
                    let _ = result_sender.send(result);
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let e = causal_replication_events.recv().await.unwrap();
                match e {
                    CausalReplicationEvent::Deliver(op) => {
                        let name = op.value.1.name();
                        let id = op.value.0;
                        proto.storage.exec(op.value.1).await;
                        #[cfg(feature = "instrument")]
                        log_instrumentation(InstrumentationEvent{
                            kind: String::from("visible"),
                            val: None,
                            meta: Some(id.to_string() + " " + &name),
                        });
                    },
                    CausalReplicationEvent::QuorumReplicated(_snapshot) => (),
                }
            }
        });
    }
}
