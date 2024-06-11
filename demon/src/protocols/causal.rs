use crate::{api::API, network::{MsgHandler, Network, NodeId}, rdts::Operation, storage::{basic::Storage, QueryResult}, causal_replication::{CausalReplicationEvent, CausalReplication}};
use async_trait::async_trait;
use tokio::{net::ToSocketAddrs, sync::mpsc::Receiver};
use std::sync::Arc;

use super::{Component, Message};

/// A basic implementation of causal order RDTs
pub struct Causal<O: Operation> {
    network: Network<Message>,
    storage: Storage<O>,
    causal_replication: CausalReplication<O>,
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
        let proto = Arc::new(Self {
            network: network.clone(),
            storage,
            causal_replication,
        });
        network.set_msg_handler(proto.clone()).await;
        tokio::task::spawn(proto.clone().event_loop(
            causal_replication_events,
            api,
        ));
        proto
    }

    /// Process events from the components.
    /// Spawns an individual task for each event stream.
    async fn event_loop(
        self: Arc<Self>,
        mut causal_replication_events: Receiver<CausalReplicationEvent<O>>,
        api: Box<dyn API<O>>,
    ) {
        let mut api_events = api.start(self.network.clone()).await;
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = api_events.recv().await.unwrap();
                if query.is_writing() {
                    if let Some(shadow) = proto.storage.generate_shadow(query).await {
                        let result = proto.storage.exec(shadow.clone()).await;
                        let _ = result_sender.send(result);
                        proto.causal_replication.replicate(shadow).await;
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
                        proto.storage.exec(op.value).await;
                    },
                    CausalReplicationEvent::QuorumReplicated(_snapshot) => (),
                }
            }
        });
    }
}
