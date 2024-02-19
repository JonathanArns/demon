use std::{collections::HashMap, net::{SocketAddr, IpAddr}, sync::Arc, time::Duration};

use anyhow::bail;
use tokio::{net::{TcpListener, tcp::{OwnedWriteHalf, OwnedReadHalf}, TcpStream, ToSocketAddrs, lookup_host}, sync::{Mutex, RwLock}};
use tokio_util::codec::{LengthDelimitedCodec, FramedWrite, FramedRead};
use futures::{Future, SinkExt, TryStreamExt};
use serde::{Serialize, Deserialize, de::DeserializeOwned};


/// The port we listen on.
const PORT: u16 = 1234;


#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Peer {
    pub id: NodeId,
    pub addr: IpAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NetworkMsg<T> {
    from: NodeId,
    msg: Message<T>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Message<T> {
    Payload(T),
    Join,
    GetPeers,
    Peers(Vec<Peer>),
}

/// A network abstraction that asynchronously sends messages between peers.
/// Received messages are handled as incoming events.
///
/// Routing etc is handled transparently.
///
/// Network handles an `Arc` internally, so it is safe to clone and share between threads.
pub struct Network<T, F, FUT>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    F: 'static + Send + Sync + Clone + Fn(NodeId, T) -> FUT,
    FUT: 'static + Future<Output = ()> + Send,
{
    inner: Arc<NetworkInner<T, F, FUT>>,
}

impl<T, F, FUT> Clone for Network<T, F, FUT>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    F: 'static + Send + Sync + Clone + Fn(NodeId, T) -> FUT,
    FUT: 'static + Future<Output = ()> + Send,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, F, FUT> Network<T, F, FUT>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    F: 'static + Send + Sync + Clone + Fn(NodeId, T) -> FUT,
    FUT: 'static + Future<Output = ()> + Send,
{
    /// Creates and starts the local network instance.
    /// Handles to this instance should be created with `clone`.
    pub async fn new(msg_handler: F) -> Self {
        let id = NodeId(rand::random());
        let network = Self {inner: Arc::new(NetworkInner::new(id, msg_handler).await)};
        tokio::task::spawn(network.inner.clone().listen());
        tokio::task::spawn(network.inner.clone().chatter());
        tokio::task::spawn(network.inner.clone().send_loop());
        network
    }

    /// Connects this node to an existing cluster.
    /// Can be called with the address of any existing node in the cluster.
    pub async fn connect<A: ToSocketAddrs>(&self, addrs: A) -> anyhow::Result<()> {
        for addr in lookup_host(addrs).await? {
            let r = self.inner.clone().connect(addr.ip()).await;
            if r.is_err() {
                continue
            }
            self.inner.streams.lock().await.get_mut(&addr.ip()).unwrap().send(
                bincode::serialize(&NetworkMsg{from: self.inner.my_id, msg: Message::<T>::Join}).unwrap().into()
            ).await?;
            return Ok(())
        }
        bail!("could not connect to cluster")
    }

    /// Get a list of currently known peers.
    pub async fn peers(&self) -> Vec<NodeId> {
        self.inner.peers.read().await.keys().map(|k| k.to_owned()).collect()
    }

    /// Buffers an outgoing message for sending.
    pub async fn send(&self, to: NodeId, msg: T) {
        self.inner.send(to, msg).await
    }

    /// Send buffered outgoing messages.
    pub async fn flush(&self) -> anyhow::Result<()> {
        self.inner.clone().flush().await
    }
}

pub struct NetworkInner<T, F, FUT>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    F: 'static + Send + Sync + Clone + Fn(NodeId, T) -> FUT,
    FUT: 'static + Future<Output = ()> + Send,
{
    my_id: NodeId,
    peers: RwLock<HashMap<NodeId, Peer>>,
    streams: Mutex<HashMap<IpAddr, FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>>>,
    outgoing_buffer: Mutex<Vec<(NodeId, NetworkMsg<T>)>>,
    handler: F,
}

impl<T, F, FUT> NetworkInner<T, F, FUT>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    F: 'static + Send + Sync + Clone + Fn(NodeId, T) -> FUT,
    FUT: 'static + Future<Output = ()> + Send,
{
    /// The codec we use for framing all messages.
    fn codec() -> LengthDelimitedCodec {
        LengthDelimitedCodec::new()
    }

    async fn new(id: NodeId, msg_handler: F) -> Self {
        Self {
            my_id: id,
            peers: Default::default(),

            streams: Default::default(),
            outgoing_buffer: Default::default(),
            handler: msg_handler,
        }
    }

    /// Wraps and buffers an outgoing message for sending.
    async fn send(&self, to: NodeId, msg: T) {
        self.internal_send(to, NetworkMsg{from: self.my_id, msg: Message::Payload(msg)}).await
    }

    /// Buffers an outgoing message for sending.
    async fn internal_send(&self, to: NodeId, msg: NetworkMsg<T>) {
        self.outgoing_buffer.lock().await.push((to, msg));
    }

    /// Looks up a peer's address.
    async fn lookup_peer(&self, id: NodeId) -> anyhow::Result<IpAddr> {
        if let Some(Peer{addr, ..}) = self.peers.read().await.get(&id) {
            Ok(*addr)
        } else {
            todo!("perform remote lookup")
        }
    }

    /// Send buffered outgoing messages.
    /// TODO: this locks and unlocks `self.streams` for each msg... seems not great maybe
    async fn flush(self: Arc<Self>) -> anyhow::Result<()> {
        let buf: Vec<(NodeId, NetworkMsg<T>)> = self.outgoing_buffer.lock().await.drain(0..).collect();
        for (to, msg) in buf {
            let addr = self.lookup_peer(to).await?;
            if let Some(sender) = self.streams.lock().await.get_mut(&addr) {
                sender.feed(bincode::serialize(&msg).unwrap().into()).await.unwrap(); // TOOD: handle send error
                continue
            }
            // create new connection if missing
            self.clone().connect(addr).await?;
            self.streams.lock().await.get_mut(&addr).unwrap().feed(bincode::serialize(&msg).unwrap().into()).await.unwrap(); // TOOD: handle send error
        }
        for sender in self.streams.lock().await.values_mut() {
            sender.flush().await.unwrap(); // TODO: handle send error
        }
        Ok(())
    }

    /// Creates and registers a new socket connection to `addr`.
    async fn connect(self: Arc<Self>, addr: IpAddr) -> anyhow::Result<()> {
        let stream = TcpStream::connect(SocketAddr::new(addr, PORT)).await?;
        self.clone().handle_new_stream(stream, addr).await?;
        Ok(())
    }

    /// Starts listening and handling messages.
    async fn listen(self: Arc<Self>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).await.unwrap();
        loop {
            if let Ok((stream, peer_addr)) = listener.accept().await {
                self.clone().handle_new_stream(stream, peer_addr.ip()).await.unwrap(); // TODO: real peer_id
            } else {
                todo!("handle listener error")
            }
        }
    }

    async fn send_loop(self: Arc<Self>) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            self.clone().flush().await?;
        }
    }

    /// We gossip our peers in a ring, so eventually everyone knows everyone.
    async fn chatter(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            // find the peer to talk to
            let mut next_peer = NodeId(u32::MAX);
            let mut smallest_peer = NodeId(u32::MAX);
            for peer in self.peers.read().await.keys() {
                if peer.0 > self.my_id.0 && peer.0 < next_peer.0 {
                    next_peer = *peer;
                }
                if peer.0 < smallest_peer.0 {
                    smallest_peer = *peer;
                }
                // self.send_peers(*peer).await;
            }

            if next_peer.0 != u32::MAX {
                self.send_peers(next_peer).await;
            } else if smallest_peer.0 != u32::MAX {
                self.send_peers(smallest_peer).await;
            }

            // // alternative: just broadcast
            // for peer in self.peers.read().await.keys() {
            //     self.send_peers(*peer).await;
            // }
        }
    }

    async fn handle_new_stream(self: Arc<Self>, stream: TcpStream, peer_addr: IpAddr) -> anyhow::Result<()> {
        let (reader, writer) = stream.into_split();
        let framed_writer = FramedWrite::new(writer, Self::codec());
        let framed_reader = FramedRead::new(reader, Self::codec());
        let handle_fn = self.handler.clone();
        let network_handle = self.clone();
        tokio::task::spawn(async move { network_handle.handle_read_half(framed_reader, peer_addr, handle_fn).await });
        self.streams.lock().await.insert(peer_addr, framed_writer);
        Ok(())
    }

    /// Sends `to` a list of known peers.
    async fn send_peers(&self, to: NodeId) {
        let peers = self.peers.read().await.values().map(|p| p.to_owned()).collect();
        let msg = NetworkMsg{from: self.my_id, msg: Message::Peers(peers)};
        self.internal_send(to, msg).await;
    }

    /// Sends `msg` to all known peers.
    async fn broadcast(&self, msg: NetworkMsg<T>) {
        for peer_id in self.peers.read().await.keys() {
            self.internal_send(*peer_id, msg.clone()).await;
        }
    }

    async fn handle_read_half(
        self: Arc<Self>,
        mut reader: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        addr: IpAddr,
        handler: F,
    ) -> anyhow::Result<()> {
        let mut known_peer = false;
        while let Some(data) = reader.try_next().await? {
            let NetworkMsg::<T>{ from, msg } = bincode::deserialize(&data).unwrap();
            if !known_peer {
                self.peers.write().await.insert(from, Peer {id: from, addr});
                known_peer = true;
            }
            match msg {
                Message::Payload(msg) => {
                    let handle_fn = handler.clone();
                    tokio::task::spawn(async move { handle_fn(from, msg).await });
                },
                Message::Join => {
                    self.send_peers(from).await;
                    let msg = NetworkMsg{
                        from: self.my_id,
                        msg: Message::<T>::Peers(vec![Peer{id: from, addr}]),
                    };
                    self.broadcast(msg).await;
                },
                Message::GetPeers => {
                    self.send_peers(from).await;
                },
                Message::Peers(peers) => {
                    let mut my_peers = self.peers.write().await;
                    for peer in peers {
                        if peer.id != self.my_id {
                            my_peers.insert(peer.id, peer);
                        }
                    }
                },
            }
        }
        Ok(())
    }
}
