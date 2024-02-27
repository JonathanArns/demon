use crate::{gossip::{GossipEvent, GossipMsg, Gossiper}, network::{MsgHandler, Network, NodeId}, sequencer::{Sequencer, SequencerEvent, SequencerMsg}};
use async_trait::async_trait;
use futures::Future;
use serde::{Serialize, Deserialize};
use tokio::{net::ToSocketAddrs, select, sync::{mpsc::Receiver, OnceCell}};
use std::{sync::Arc, pin::Pin};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Sequencer(SequencerMsg),
    Gossiper(GossipMsg),
}

/// A helper trait that we need to make rustc happy
trait AsyncMsgHandler: Clone + FnOnce(NodeId, Message) -> Pin<Box<dyn Send + Future<Output = ()>>> {}

/// The DeMon mixed consistency protocol.
/// 
/// DeMon is not `Clone` on purpose, so that not all of its components need to be as well.
/// DeMon is `Sync` however, and never requires mutable access, so an `Arc<DeMon>` will do the trick.
pub struct DeMon {
    network: Network<Message, Self>,
    sequencer: Sequencer,
    gossiper: Gossiper,
}

// TODO: this single message loop could become a point of contention...
// maybe instead send the messages to the components via channels?
// or just spawn a task for ones that block the loop...
#[async_trait]
impl MsgHandler<Message> for DeMon {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg {
            Message::Sequencer(m) => {
                self.sequencer.handle_msg(m).await;
            },
            Message::Gossiper(m) => {
                self.gossiper.handle_msg(from, m).await;
            }
        }
    }
}

impl DeMon {
    /// Creates and starts a new DeMon node.
    pub async fn new<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32) -> Arc<Self> {
        let demon_cell = Arc::new(OnceCell::const_new());
        let network = Network::connect(addrs, cluster_size, demon_cell.clone()).await.unwrap();
        let (sequencer, sequencer_events) = Sequencer::new(network.clone()).await;
        let (gossiper, gossip_events) = Gossiper::new(network.clone()).await;
        let demon = demon_cell.get_or_init(|| async move {
            Arc::new(Self {
                network,
                sequencer,
                gossiper,
            })
        }).await.clone();
        tokio::task::spawn(demon.clone().event_loop(
            sequencer_events,
            gossip_events,
        ));
        demon
    }

    /// Process events from the components.
    async fn event_loop(
        self: Arc<Self>,
        mut sequencer_events: Receiver<SequencerEvent>,
        mut gossip_events: Receiver<GossipEvent>,
    ) {
        loop {
            select! {
                Some(e) = sequencer_events.recv() => {
                    todo!("execute decided transactions")
                },
                Some(e) = gossip_events.recv() => {
                    todo!("execute decided transactions")
                },
            }
        }
    }
}
