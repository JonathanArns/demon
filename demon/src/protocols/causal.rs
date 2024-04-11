use crate::{api::API, network::{MsgHandler, Network, NodeId}, storage::{Operation, basic::Storage}, weak_replication::{WeakEvent, WeakReplication}};
use async_trait::async_trait;
use tokio::{net::ToSocketAddrs, sync::mpsc::Receiver};
use std::sync::Arc;

use super::{Op, Component, Message};

/// A basic implementation of causal order RDTs
pub struct Causal {
    network: Network<Message>,
    storage: Storage<Op>,
    weak_replication: WeakReplication<Op>,
}

// TODO: this single message loop could become a point of contention...
// maybe instead send the messages to the components via channels?
// or just spawn a task for ones that block the loop...
#[async_trait]
impl MsgHandler<Message> for Causal {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg.component {
            Component::WeakReplication => {
                self.weak_replication.handle_msg(from, msg.payload).await;
            },
            Component::Sequencer => {
                unreachable!()
            },
        }
    }
}

impl Causal {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<Op>>) -> Arc<Self> {
        let network = Network::connect(addrs, cluster_size).await.unwrap();
        let storage = Storage::new();
        let (weak_replication, weak_replication_events) = WeakReplication::new(network.clone()).await;
        let proto = Arc::new(Self {
            network: network.clone(),
            storage,
            weak_replication,
        });
        network.set_msg_handler(proto.clone()).await;
        tokio::task::spawn(proto.clone().event_loop(
            weak_replication_events,
            api,
        ));
        proto
    }

    /// Process events from the components.
    /// Spawns an individual task for each event stream.
    async fn event_loop(
        self: Arc<Self>,
        mut weak_replication_events: Receiver<WeakEvent<Op>>,
        api: Box<dyn API<Op>>,
    ) {
        let mut api_events = api.start().await;
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = api_events.recv().await.unwrap();
                let result = proto.storage.exec(query.clone()).await;
                result_sender.send(result).unwrap();
                if query.is_writing() {
                    proto.weak_replication.replicate(query).await;
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let e = weak_replication_events.recv().await.unwrap();
                match e {
                    WeakEvent::Deliver(op) => {
                        proto.storage.exec(op.value).await;
                    },
                    WeakEvent::QuorumReplicated(_snapshot) => (),
                }
            }
        });
    }
}
