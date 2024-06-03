use crate::{api::API, network::{MsgHandler, Network, NodeId}, rdts::Operation, sequencer::{Sequencer, SequencerEvent}, storage::{redblue::Storage, QueryResult, Transaction}, weak_replication::{Snapshot, TaggedEntry, WeakEvent, WeakReplication}};
use async_trait::async_trait;
use omnipaxos::storage::{Entry, NoSnapshot};
use serde::{Deserialize, Serialize};
use tokio::{net::ToSocketAddrs, select, sync::{mpsc::Receiver, oneshot, Mutex, RwLock}};
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{TransactionId, Component, Message};

/// Used to track causality between red and blue ops
#[derive(Serialize, Deserialize, Clone)]
struct TaggedWeakOp<O> {
    op: O,
    /// the amount of decided red transactions that we need to wait for before executing this blue op
    transaction_count: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TaggedTransaction<O> {
    t: Transaction<O>,
    /// May not conflict with any transaction between this one and itself
    start_id: Option<TransactionId>,
    original_query: O,
}

impl<O: Operation> Entry for TaggedTransaction<O> {
    type Snapshot = NoSnapshot;
}

pub struct Unistore<O: Operation> {
    network: Network<Message>,
    storage: Storage<O>,
    sequencer: Sequencer<TaggedTransaction<O>>,
    weak_replication: WeakReplication<TaggedWeakOp<O>>,
    next_transaction_id: Arc<Mutex<TransactionId>>,
    quorum_replicated_snapshot: Arc<RwLock<Snapshot>>,
    decided_transaction_count: Arc<RwLock<usize>>,
    /// Strong client requests wait here for transaction completion. Includes a retry counter.
    waiting_transactions: Arc<Mutex<HashMap<TransactionId, (oneshot::Sender<QueryResult<O>>, usize)>>>,
    /// Keeps track of which transactions each replica has seen
    peer_start_ids: Arc<Mutex<Vec<(NodeId, TransactionId)>>>,
    transaction_log: Arc<Mutex<Vec<Transaction<O>>>>,
}

#[async_trait]
impl<O: Operation> MsgHandler<Message> for Unistore<O> {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg.component {
            Component::Sequencer => {
                self.sequencer.handle_msg(msg.payload).await;
            },
            Component::WeakReplication => {
                self.weak_replication.handle_msg(from, msg.payload).await;
            },
            Component::Protocol => unreachable!(),
        }
    }
}

impl<O: Operation> Unistore<O> {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, api: Box<dyn API<O>>, name: Option<String>) -> Arc<Self> {
        let network = Network::connect(addrs, cluster_size, name).await.unwrap();
        let storage = Storage::new(network.nodes().await);
        let (sequencer, sequencer_events) = Sequencer::new(network.clone()).await;
        let (weak_replication, weak_replication_events) = WeakReplication::new(network.clone()).await;
        let my_id = network.my_id().await;
        let nodes = network.nodes().await;
        let mut peer_start_ids = vec![];
        for id in nodes.iter() {
            peer_start_ids.push((*id, TransactionId(*id, 0)));
        }
        let proto = Arc::new(Self {
            network: network.clone(),
            storage,
            sequencer,
            weak_replication,
            next_transaction_id: Arc::new(Mutex::new(TransactionId(my_id, 0))),
            quorum_replicated_snapshot: Arc::new(RwLock::new(Snapshot{vec:vec![0; nodes.len()]})),
            decided_transaction_count: Default::default(),
            waiting_transactions: Default::default(),
            peer_start_ids: Arc::new(Mutex::new(peer_start_ids)),
            transaction_log: Default::default(),
        });
        network.set_msg_handler(proto.clone()).await;
        tokio::task::spawn(proto.clone().event_loop(
            sequencer_events,
            weak_replication_events,
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
    async fn snapshot_barrier(&self, snapshot: &Snapshot) {
        while snapshot.greater(&*self.quorum_replicated_snapshot.read().await) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Process events from the components.
    /// Spawns an individual task for each event stream.
    async fn event_loop(
        self: Arc<Self>,
        mut sequencer_events: Receiver<SequencerEvent<TaggedTransaction<O>>>,
        mut weak_replication_events: Receiver<WeakEvent<TaggedWeakOp<O>>>,
        api: Box<dyn API<O>>,
    ) {
        let my_id = self.network.my_id().await;
        let mut api_events = api.start(self.network.clone()).await;
        let proto = self.clone();
        tokio::spawn(async move {
            loop {
                let (query, result_sender) = api_events.recv().await.unwrap();
                if query.is_red() {
                    // strong operation
                    let protocol = proto.clone();
                    tokio::spawn(async move {
                        let start_id = protocol.transaction_log.lock().await.last().map(|x| x.id);
                        if let Some((shadow, snapshot)) = protocol.storage.generate_shadow(query.clone()).await {
                            let id = protocol.generate_transaction_id().await;
                            protocol.snapshot_barrier(&snapshot).await;
                            let transaction = Transaction { id, snapshot, op: Some(shadow) };
                            let tagged_transaction = TaggedTransaction { t: transaction, start_id, original_query: query };
                            protocol.waiting_transactions.lock().await.insert(id, (result_sender, 0));
                            protocol.sequencer.append(tagged_transaction).await;
                        } else {
                            let _ = result_sender.send(QueryResult { value: None });
                        }
                    });
                } else {
                    // weak operation
                    if let Some((shadow, _)) = proto.storage.generate_shadow(query).await {
                        let result = proto.storage.exec_blue(shadow.clone(), my_id).await;
                        let _ = result_sender.send(result);
                        if shadow.is_writing() {
                            let tagged_op = TaggedWeakOp {
                                op: shadow,
                                transaction_count: *proto.decided_transaction_count.read().await,
                            };
                            proto.weak_replication.replicate(tagged_op).await;
                        }
                    } else {
                        let _ = result_sender.send(QueryResult { value: None });
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
                        let mut transaction_log = proto.transaction_log.lock().await;
                        let mut peer_start_ids = proto.peer_start_ids.lock().await;
                        for tagged_transaction in decided_entries {
                            let transaction = tagged_transaction.t;

                            if let Some(start_id) = tagged_transaction.start_id {
                                // update node's start id
                                for (peer_id, t_id) in &mut peer_start_ids.iter_mut() {
                                    if *peer_id == transaction.id.0 {
                                        *t_id = start_id;
                                        break
                                    }
                                }

                                // check if this transaction may commit
                                let mut may_commit = true;
                                let mut check_now = false;
                                for t in transaction_log.iter() {
                                    if check_now {
                                        if transaction.op.as_ref().unwrap().is_por_conflicting(t.op.as_ref().unwrap()) {
                                            may_commit = false;
                                            break
                                        }
                                    } else if t.id == start_id {
                                        check_now = true;
                                    }
                                }
                                // re-sequence with new id, if a PoR conflict was detected
                                if !may_commit {
                                    let mut waiting_transactions = proto.waiting_transactions.lock().await;
                                    if let Some((sender, retry_counter)) = waiting_transactions.remove(&transaction.id) {
                                        if retry_counter >= 2 {
                                            // we cancel a transaction after 3 failed commit attempts
                                            drop(sender);
                                            continue
                                        }
                                        
                                        // generate shadow again
                                        let protocol = proto.clone();
                                        let start_id = transaction_log.last().map(|x| x.id);
                                        if let Some((shadow, snapshot)) = protocol.storage.generate_shadow(tagged_transaction.original_query.clone()).await {
                                            let new_id = protocol.generate_transaction_id().await;
                                            waiting_transactions.insert(new_id, (sender, retry_counter + 1));
                                            tokio::spawn(async move {
                                                protocol.snapshot_barrier(&snapshot).await;
                                                let new_transaction = Transaction { id: new_id, snapshot, op: Some(shadow) };
                                                let new_tagged_transaction = TaggedTransaction { t: new_transaction, start_id, original_query: tagged_transaction.original_query };
                                                protocol.sequencer.append(new_tagged_transaction).await;
                                            });
                                        } else {
                                            let _ = sender.send(QueryResult { value: None });
                                        }

                                    }
                                    continue
                                }
                            }


                            // execute and commit
                            transaction_log.push(transaction.clone());
                            let result_sender = proto.waiting_transactions.lock().await.remove(&transaction.id);
                            let response = proto.storage.exec_red(transaction).await;
                            if let Some((sender, _retry_counter)) = result_sender {
                                // this node has a client waiting for this response
                                let _ = sender.send(response);
                            }
                        }

                        // garbage collect transaction_log
                        // may only remove entries that are before all peers' highest known start_ids,
                        // so that they will not be needed anymore for transaction certification
                        let mut remove = 0;
                        'OUTER: for t in transaction_log.iter() {
                            for (_node_id, start_id) in peer_start_ids.iter() {
                                if t.id == *start_id {
                                    break 'OUTER
                                }
                            }
                            remove += 1;
                        }
                        transaction_log.drain(0..remove);
                    },
                }
            }
        });
        let proto = self.clone();
        tokio::spawn(async move {
            let mut waiting_blue_ops: Vec<TaggedEntry<TaggedWeakOp<O>>> = vec![];
            loop {
                select! {
                    e = weak_replication_events.recv() => {
                        match e.unwrap() {
                            WeakEvent::Deliver(new_op) => {
                                // first, execute waiting ops that this might depend on
                                let transaction_count = *proto.decided_transaction_count.read().await;
                                let mut i = 0;
                                while i < waiting_blue_ops.len() {
                                    let op = &waiting_blue_ops[i];
                                    if transaction_count >= op.value.transaction_count {
                                        let op = waiting_blue_ops.remove(i);
                                        proto.storage.exec_blue(op.value.op, op.node).await;
                                    } else {
                                        i += 1;
                                    }
                                }
                                // then execute or queue this op, depending on if it needs to wait for a red operation
                                if new_op.value.transaction_count > transaction_count {
                                    waiting_blue_ops.push(new_op);
                                } else {
                                    proto.storage.exec_blue(new_op.value.op, new_op.node).await;
                                }
                            },
                            WeakEvent::QuorumReplicated(snapshot) => {
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
                                proto.storage.exec_blue(op.value.op, op.node).await;
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
