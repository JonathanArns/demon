use std::collections::HashMap;

use tokio::{net::{TcpListener, unix::SocketAddr, tcp::{OwnedWriteHalf, OwnedReadHalf}}, sync::Mutex};
use tokio_util::codec::{LengthDelimitedCodec, FramedWrite, FramedRead};
use futures::{Future, SinkExt, TryStreamExt};
use serde::{Serialize, Deserialize, de::DeserializeOwned};


#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

pub struct Peer {
    pub id: NodeId,
    pub addr: SocketAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum NetworkMsg<T> {
    Payload(T),
    Join(NodeId), // TODO: cluster membership
}

/// A network abstraction that asynchronously sends messages between peers.
/// Received messages are handled as incoming events.
///
/// Routing etc is handled transparently.
pub struct Network<T, F, FUT>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync,
    F: 'static + Send + Clone + Fn(T) -> FUT,
    FUT: Future<Output = ()> + Send,
{
    pub my_id: NodeId,
    pub peers: Vec<Peer>,

    streams: Mutex<HashMap<NodeId, FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>>>,
    outgoing_buffer: Vec<(NodeId, NetworkMsg<T>)>,
    handler: F,
}

impl<T, F, FUT> Network<T, F, FUT>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync,
    F: 'static + Send + Clone + Fn(T) -> FUT,
    FUT: Future<Output = ()> + Send,
{
    async fn new(msg_handler: F) -> Self {
        // let x = Framed::new()
        Self {
            my_id: NodeId(0), // TODO: assign unique ids
            peers: vec![],

            streams: Default::default(),
            outgoing_buffer: vec![],
            handler: msg_handler,
        }
    }

    /// Buffers an outgoing message for sending.
    ///
    /// TODO: potentially it might be better to remove the buffer and just feed msgs to the sender immediately
    fn send(&mut self, to: NodeId, msg: T) {
        self.outgoing_buffer.push((to, NetworkMsg::Payload(msg)));
    }

    /// Send buffered outgoing messages.
    async fn flush(&mut self) -> anyhow::Result<()> {
        let mut streams = self.streams.lock().await;
        for (to, msg) in self.outgoing_buffer.drain(0..) {
            let sender = streams.get_mut(&to).unwrap(); // TODO: create new connection if missing
            sender.feed(bincode::serialize(&msg).unwrap().into()).await.unwrap(); // TOOD: handle send error
        }
        for sender in streams.values_mut() {
            sender.flush().await.unwrap(); // TODO: handle send error
        }
        Ok(())
    }

    /// Starts listening and handling messages
    async fn listen(&self) -> anyhow::Result<()> {
        let mut listener = TcpListener::bind("0.0.0.0:1234").await.unwrap();
        loop {
            if let Ok((stream, peer_addr)) = listener.accept().await {
                let (reader, writer) = stream.into_split();
                let framed_writer = FramedWrite::new(writer, LengthDelimitedCodec::new());
                let framed_reader = FramedRead::new(reader, LengthDelimitedCodec::new());
                let handle_fn = self.handler.clone();
                tokio::task::spawn(async move { Self::handle_read_half(framed_reader, handle_fn).await });
                self.streams.lock().await.insert(NodeId(0), framed_writer);
            } else {
                todo!("handle listener error")
            }
        }
    }

    async fn handle_read_half(
        mut reader: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        handler: F,
    ) -> anyhow::Result<()> {
        while let Some(data) = reader.try_next().await? {
            let msg: NetworkMsg<T> = bincode::deserialize(&data).unwrap();
            match msg {
                NetworkMsg::Payload(msg) => {
                    let handle_fn = handler.clone();
                    tokio::task::spawn(async move { handle_fn(msg).await });
                },
                NetworkMsg::Join(node_id) => todo!(),
            }
        }
        Ok(())
    }
}

