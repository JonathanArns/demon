use crate::{api::API, network::{MsgHandler, Network, NodeId}, rdts::Operation, storage::{basic::Storage, QueryResult}, causal_replication::{CausalReplicationEvent, CausalReplication}};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{net::ToSocketAddrs, sync::{mpsc::{self, Receiver}, oneshot, Mutex}};
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{Component, Message};

#[derive(Serialize, Deserialize, Clone)]
enum ShadowOp<O> {
    Blue(O),
    Red {
        op: O,
        seq: usize,
    },
}

#[derive(Serialize, Deserialize, Clone)]
struct TokenPass {
    next_red_sequence: usize,
}

pub struct Gemini<O: Operation> {
    network: Network<Message>,
    storage: Storage<O>,
    // sequencer: Sequencer<Transaction<O>>,
    causal_replication: CausalReplication<ShadowOp<O>>,
    /// is Some(seq), if this replica holds the unique red token
    next_red_sequence: Arc<Mutex<Option<usize>>>,
    /// Strong client requests wait here for transaction completion.
    waiting_red_clients: Arc<Mutex<HashMap<usize, oneshot::Sender<QueryResult<O>>>>>,
    /// (next_seq_to_apply, seq -> op)
    waiting_red_ops: Arc<Mutex<(usize, HashMap<usize, O>)>>,
}

#[async_trait]
impl<O: Operation> MsgHandler<Message> for Gemini<O> {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg.component {
            Component::Sequencer => unreachable!(),
            Component::WeakReplication => {
                self.causal_replication.handle_msg(from, msg.payload).await;
            },
            Component::Protocol => {
                let TokenPass { next_red_sequence } = bincode::deserialize(&msg.payload).unwrap();
                *self.next_red_sequence.lock().await = Some(next_red_sequence);
                tokio::task::spawn(Self::forward_token_after_duration(
                    self.next_red_sequence.clone(),
                    self.network.clone(),
                    Duration::from_millis(5)
                ));
            }
        }
    }
}

impl<O: Operation> Gemini<O> {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<O>>, name: Option<String>) -> Arc<Self> {
        let network = Network::connect(addrs, cluster_size, name).await.unwrap();
        let storage = Storage::new();
        let (causal_replication, causal_replication_events) = CausalReplication::new(network.clone()).await;
        let my_id = network.my_id().await;
        let seq = if my_id.0 == 1 {
            Some(0)
        } else {
            None
        };
        let proto = Arc::new(Self {
            network: network.clone(),
            storage,
            causal_replication,
            next_red_sequence: Arc::new(Mutex::new(seq)),
            waiting_red_clients: Default::default(),
            waiting_red_ops: Default::default(),
        });
        network.set_msg_handler(proto.clone()).await;
        tokio::task::spawn(proto.clone().event_loop(
            causal_replication_events,
            api,
        ));
        if seq != None {
            tokio::task::spawn(Self::forward_token_after_duration(
                proto.next_red_sequence.clone(),
                proto.network.clone(),
                Duration::from_millis(100)
            ));
        }
        proto
    }

    /// Generates a new globally unique transaction Id.
    /// Only returns once all previous red shadow ops are applied.
    async fn sequence_red(&self, op: O) -> Option<(usize, ShadowOp<O>)> {
        let id;
        loop {
            let mut latch = self.next_red_sequence.lock().await;
            if let Some(ref mut seq) = *latch {
                let (applied, _) = *self.waiting_red_ops.lock().await;
                if applied == *seq {
                    // now we are allowed to process red ops locally
                    if let Some(shadow) = self.storage.generate_shadow(op).await {
                        id = *seq;
                        *seq += 1;
                        return Some((id, ShadowOp::Red {
                            seq: id,
                            op: shadow,
                        }))
                    } else {
                        return None
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn forward_token_after_duration(token: Arc<Mutex<Option<usize>>>, network: Network<Message>, duration: Duration) {
        tokio::time::sleep(duration).await;
        let mut latch = token.lock().await;
        let msg = TokenPass { next_red_sequence: latch.expect("should have token") };
        *latch = None;
        let my_id = network.my_id().await;
        let mut nodes = network.nodes().await;
        nodes.sort_unstable();
        let idx = nodes.iter().position(|x| *x == my_id).unwrap();
        let next_id = nodes[(idx + 1) % nodes.len()];
        network.send(next_id, Message {
            payload: bincode::serialize(&msg).unwrap(),
            component: Component::Protocol
        }).await;
    }

    /// Queues a sequenced red operation for execution
    async fn process_red_shadow_op(&self, rbop: ShadowOp<O>) {
        if let ShadowOp::Red {op, seq} = rbop {
            let mut clients = self.waiting_red_clients.lock().await;
            let mut latch = self.waiting_red_ops.lock().await;
            latch.1.insert(seq, op);
            let mut next_seq = latch.0;
            while let Some(op) = latch.1.remove(&next_seq) {
                let output = self.storage.exec(op).await;
                if let Some(sender) = clients.remove(&next_seq) {
                    let _ = sender.send(output);
                }
                next_seq += 1;
            }
            latch.0 = next_seq;
        }
    }

    /// Process events from the components.
    /// Spawns an individual task for each event stream.
    async fn event_loop(
        self: Arc<Self>,
        mut causal_replication_events: Receiver<CausalReplicationEvent<ShadowOp<O>>>,
        api: Box<dyn API<O>>,
    ) {
        let (red_sender, mut red_receiver) = mpsc::channel(8000);
        let mut api_events = api.start(self.network.clone()).await;
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = api_events.recv().await.unwrap();
                if query.is_red() {
                    red_sender.send((query, result_sender)).await.unwrap();
                } else {
                    // weak operation
                    if query.is_writing() {
                        let result = if let Some(shadow) = proto.storage.generate_shadow(query).await {
                            if shadow.is_writing() {
                                let tagged_op = ShadowOp::Blue(shadow.clone());
                                proto.causal_replication.replicate(tagged_op).await;
                            }
                            proto.storage.exec(shadow).await
                        } else {
                            QueryResult { value: None }
                        };
                        let _ = result_sender.send(result);
                    } else {
                        let result = proto.storage.exec(query).await;
                        let _ = result_sender.send(result);
                    }
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = red_receiver.recv().await.unwrap();
                // wait here until we hold red token
                if let Some((seq, shadow)) = proto.sequence_red(query).await {
                    proto.waiting_red_clients.lock().await.insert(seq, result_sender);
                    proto.causal_replication.replicate(shadow.clone()).await;
                    proto.process_red_shadow_op(shadow).await;
                } else {
                    let _ = result_sender.send(QueryResult { value: None });
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let e = causal_replication_events.recv().await.unwrap();
                match e {
                    CausalReplicationEvent::QuorumReplicated(_snapshot) => (),
                    CausalReplicationEvent::Deliver(new_op) => {
                        match new_op.value {
                            ShadowOp::Blue(op) => {
                                proto.storage.exec(op).await;
                            },
                            ShadowOp::Red { .. } => {
                                proto.process_red_shadow_op(new_op.value).await;
                            },
                        }
                    },
                }
            }
        });
    }
}
