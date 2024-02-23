use crate::network::{Network, NodeId, MsgHandler};
use async_trait::async_trait;
use futures::Future;
use serde::{Serialize, Deserialize};
use tokio::sync::OnceCell;
use std::{sync::Arc, pin::Pin};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Sequencer,
    Gossip,
}

/// A helper trait that we need to make rustc happy
trait AsyncMsgHandler: Clone + FnOnce(NodeId, Message) -> Pin<Box<dyn Send + Future<Output = ()>>> {}

pub struct DeMon {
    network: Network<Message, Self>,
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
        demon.get_or_init(|| async move {
            Arc::new(Self {
                network,
            })
        }).await.clone()
    }

    async fn handle_msg(&self, from: NodeId, msg: Message) {
        todo!("")
    }
}
