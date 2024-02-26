use crate::{network::{MsgHandler, Network, NodeId}, sequencer::{Sequencer, SequencerMsg}};
use async_trait::async_trait;
use futures::Future;
use serde::{Serialize, Deserialize};
use tokio::sync::OnceCell;
use std::{sync::Arc, pin::Pin};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Sequencer(SequencerMsg),
}

/// A helper trait that we need to make rustc happy
trait AsyncMsgHandler: Clone + FnOnce(NodeId, Message) -> Pin<Box<dyn Send + Future<Output = ()>>> {}

pub struct DeMon {
    network: Network<Message, Self>,
    sequencer: Sequencer,
}

#[async_trait]
impl MsgHandler<Message> for DeMon {
    async fn handle_msg(&self, from: NodeId, msg: Message) {
        self.handle_msg(from, msg).await
    }
}

impl DeMon {
    pub async fn new() -> Arc<Self> {
        let demon = Arc::new(OnceCell::const_new());
        let network = Network::connect::<String>(None, demon.clone()).await.unwrap();
        let sequencer = Sequencer::new(network.clone()).await;
        demon.get_or_init(|| async move {
            Arc::new(Self {
                network,
                sequencer,
            })
        }).await.clone()
    }

    async fn handle_msg(&self, from: NodeId, msg: Message) {
        match msg {
            Message::Sequencer(m) => {
                self.sequencer.handle_msg(m).await;
            }
        }
    }
}
