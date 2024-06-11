use crate::{api::API, network::{MsgHandler, Network, NodeId}, rdts::Operation, sequencer::{Sequencer, SequencerEvent}, storage::{deterministic_redblue::Storage, QueryResult, Transaction}, causal_replication::{Snapshot, TaggedEntry, CausalReplicationEvent, CausalReplication}};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{net::ToSocketAddrs, select, sync::{mpsc::Receiver, oneshot, Mutex, RwLock}};
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{TransactionId, Component, Message};

/// Used to track causality between red and blue ops
#[derive(Serialize, Deserialize, Clone)]
struct TaggedBlueOp<O> {
    op: O,
    /// the amount of decided red transactions that we need to wait for before executing this blue op
    transaction_count: usize,
}

pub struct DeterministicRedBlue<O: Operation> {
    network: Network<Message>,
    storage: Storage<O>,
    sequencer: Sequencer<Transaction<O>>,
    causal_replication: CausalReplication<TaggedBlueOp<O>>,
    next_transaction_id: Arc<Mutex<TransactionId>>,
    quorum_replicated_snapshot: Arc<RwLock<Snapshot>>,
    decided_transaction_count: Arc<RwLock<usize>>,
    /// Strong client requests wait here for transaction completion.
    waiting_transactions: Arc<Mutex<HashMap<TransactionId, oneshot::Sender<QueryResult<O>>>>>,
}

#[async_trait]
impl<O: Operation> MsgHandler<Message> for DeterministicRedBlue<O> {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg.component {
            Component::Sequencer => {
                self.sequencer.handle_msg(msg.payload).await;
            },
            Component::WeakReplication => {
                self.causal_replication.handle_msg(from, msg.payload).await;
            },
            Component::Protocol => unreachable!(),
        }
    }
}

impl<O: Operation> DeterministicRedBlue<O> {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<O>>, name: Option<String>) -> Arc<Self> {
        let network = Network::connect(addrs, cluster_size, name).await.unwrap();
        let storage = Storage::new(network.nodes().await);
        let (sequencer, sequencer_events) = Sequencer::new(network.clone()).await;
        let (causal_replication, causal_replication_events) = CausalReplication::new(network.clone()).await;
        let my_id = network.my_id().await;
        let nodes = network.nodes().await;
        let proto = Arc::new(Self {
            network: network.clone(),
            storage,
            sequencer,
            causal_replication,
            next_transaction_id: Arc::new(Mutex::new(TransactionId(my_id, 0))),
            quorum_replicated_snapshot: Arc::new(RwLock::new(Snapshot{vec:vec![0; nodes.len()]})),
            decided_transaction_count: Default::default(),
            waiting_transactions: Default::default(),
        });
        network.set_msg_handler(proto.clone()).await;
        tokio::task::spawn(proto.clone().event_loop(
            sequencer_events,
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

    async fn inc_transaction_count(&self, amount: usize) {
        *self.decided_transaction_count.write().await += amount;
    }

    /// Chooses the snapshot for the next transaction proposed at this node
    /// and waits until that snapshot is quorum replicated.
    /// This is required in RedBlue and PoR for liveness and causality.
    async fn snapshot_barrier(&self) -> Snapshot {
        let snapshot = self.storage.get_current_snapshot().await;
        while snapshot.greater(&*self.quorum_replicated_snapshot.read().await) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        snapshot
    }

    /// Process events from the components.
    /// Spawns an individual task for each event stream.
    async fn event_loop(
        self: Arc<Self>,
        mut sequencer_events: Receiver<SequencerEvent<Transaction<O>>>,
        mut causal_replication_events: Receiver<CausalReplicationEvent<TaggedBlueOp<O>>>,
        api: Box<dyn API<O>>,
    ) {
        let mut api_events = api.start(self.network.clone()).await;
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = api_events.recv().await.unwrap();
                if query.is_red() {
                    // red operation
                    let protocol = proto.clone();
                    tokio::spawn(async move {
                        let id = protocol.generate_transaction_id().await;
                        let snapshot = protocol.snapshot_barrier().await;
                        let transaction = Transaction { id, snapshot, op: Some(query) };
                        protocol.waiting_transactions.lock().await.insert(id, result_sender);
                        protocol.sequencer.append(transaction).await;
                    });
                } else {
                    // blue operation
                    if query.is_writing() {
                        if let Some(shadow) = proto.storage.generate_blue_shadow(query.clone()).await {
                            let tagged_op = TaggedBlueOp {
                                op: shadow,
                                transaction_count: *proto.decided_transaction_count.read().await,
                            };
                            let op = proto.causal_replication.replicate(tagged_op).await;
                            let result = proto.storage.exec_blue_shadow(op.value.op, op.causality).await;
                            let _ = result_sender.send(result);
                        } else {
                            let _ = result_sender.send(QueryResult { value: None });
                        }
                    } else {
                        // just directly execute the read-only query
                        let result = proto.storage.exec_blue_shadow(query, Snapshot::new(&[])).await;
                        let _ = result_sender.send(result);
                    }
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let e = sequencer_events.recv().await.unwrap();
                match e {
                    SequencerEvent::Decided(decided_entries) => {
                        // we might track causality from red to blue ops slightly over-conservatively,
                        // but better this, than losing convergence.
                        proto.inc_transaction_count(decided_entries.len()).await;
                        for transaction in decided_entries {
                            let result_sender = proto.waiting_transactions.lock().await.remove(&transaction.id);
                            let response = proto.storage.exec_red(transaction).await;
                            if let Some(sender) = result_sender {
                                // this node has a client waiting for this response
                                let _ = sender.send(response);
                            }
                        }
                    },
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            let mut waiting_blue_ops: Vec<TaggedEntry<TaggedBlueOp<O>>> = vec![];
            loop {
                select! {
                    e = causal_replication_events.recv() => {
                        match e.unwrap() {
                            CausalReplicationEvent::Deliver(new_op) => {
                                // first, execute waiting ops that this might depend on
                                let transaction_count = *proto.decided_transaction_count.read().await;
                                let mut i = 0;
                                while i < waiting_blue_ops.len() {
                                    let op = &waiting_blue_ops[i];
                                    if transaction_count >= op.value.transaction_count {
                                        let op = waiting_blue_ops.remove(i);
                                        proto.storage.exec_blue_shadow(op.value.op, op.causality).await;
                                    } else {
                                        i += 1;
                                    }
                                }
                                // then execute or queue this op, depending on if it needs to wait for a red operation
                                if new_op.value.transaction_count > transaction_count {
                                    waiting_blue_ops.push(new_op);
                                } else {
                                    proto.storage.exec_blue_shadow(new_op.value.op, new_op.causality).await;
                                }
                            },
                            CausalReplicationEvent::QuorumReplicated(snapshot) => {
                                proto.quorum_replicated_snapshot.write().await.merge_inplace(&snapshot);
                            },
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        // periodically run waiting blue ops to avoid deadlocks
                        let transaction_count = *proto.decided_transaction_count.read().await;
                        let mut i = 0;
                        while i < waiting_blue_ops.len() {
                            let op = &waiting_blue_ops[i];
                            if transaction_count >= op.value.transaction_count {
                                let op = waiting_blue_ops.remove(i);
                                proto.storage.exec_blue_shadow(op.value.op, op.causality).await;
                            } else {
                                i += 1;
                            }
                        }
                    },
                }
            }
        });
    }
}
