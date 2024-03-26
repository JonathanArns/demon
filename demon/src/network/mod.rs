use std::{collections::HashMap, net::{SocketAddr, IpAddr}, sync::Arc, time::Duration};

use anyhow::bail;
use async_trait::async_trait;
use tokio::{net::{TcpListener, tcp::{OwnedWriteHalf, OwnedReadHalf}, TcpStream, ToSocketAddrs, lookup_host}, sync::{Mutex, RwLock, OnceCell}};
use tokio_util::{codec::{LengthDelimitedCodec, FramedWrite, FramedRead}, bytes::Bytes};
use futures::{SinkExt, TryStreamExt};
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
    /// A payload message by the application.
    Payload(T),
    /// A join request by a new node.
    Join,
    /// The unique ID assigned to a newly joined node by the root node 1.
    AssignId(NodeId),
    /// Information about peers' addresses in the network.
    Peers(Vec<Peer>),
    /// The number of peers this node knows.
    AckPeers(u32),
}

/// Handles incoming payload messages from the network.
#[async_trait]
pub trait MsgHandler<T>: Send + Sync + 'static {
    async fn handle_msg(&self, from: NodeId, msg: T);
}

/// A network abstraction that asynchronously sends messages between peers.
/// Received messages are handled as incoming events.
///
/// Routing etc is handled transparently.
///
/// Network handles an `Arc` internally, so it is safe to clone and share between threads.
pub struct Network<T, H>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    H: MsgHandler<T>,
{
    inner: Arc<NetworkInner<T, H>>,
}

impl<T, H> Clone for Network<T, H>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    H: MsgHandler<T>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, H> Network<T, H>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    H: MsgHandler<T>,
{
    /// Creates and connects the local network instance.
    /// Handles to this instance should be created with `clone`.
    ///
    /// The first node in a cluster should be created with `addrs = None`, all subsequent nodes
    /// should connect to that root node to join the cluster.
    pub async fn connect<A: ToSocketAddrs>(addrs: Option<A>, cluster_size: u32, msg_handler: Arc<OnceCell<Arc<H>>>) -> anyhow::Result<Self> {
        let network = Self {inner: Arc::new(NetworkInner::new(NodeId(0), msg_handler).await)};
        tokio::task::spawn(network.inner.clone().listen());
        tokio::task::spawn(network.inner.clone().send_loop());

        if let Some(addrs) = addrs {
            let mut connected = false;
            for addr in lookup_host(addrs).await? {
                let r = network.inner.clone().connect(addr.ip()).await;
                if r.is_err() {
                    continue
                }
                let join_msg: Bytes = bincode::serialize(&NetworkMsg{from: NodeId(0), msg: Message::<T>::Join}).unwrap().into();
                network.inner.streams.lock().await.get_mut(&addr.ip()).unwrap().send(join_msg.clone()).await?;
                // retry until we are assigned an ID
                loop {
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    if *network.inner.id.read().await != NodeId(0) {
                        break
                    } else {
                        network.inner.streams.lock().await.get_mut(&addr.ip()).unwrap().send(join_msg.clone()).await?;
                    }
                }
                connected = true;
                break;
            }
            if !connected {
                bail!("could not connect to cluster");
            }
        } else {
            *network.inner.id.write().await = NodeId(1);
            tokio::task::spawn(network.inner.clone().root_only_loop());
        }
        // wait until the whole cluster is connected
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if network.nodes().await.len() as u32 == cluster_size {
                break
            }
        }
        Ok(network)
    }

    pub async fn my_id(&self) -> NodeId {
        *self.inner.id.read().await
    }

    /// Get a list of currently known peers.
    pub async fn peers(&self) -> Vec<NodeId> {
        self.inner.peers.read().await.keys().map(|k| k.to_owned()).collect()
    }

    /// Get a list of currently known nodes, this includes self, as opposed to `peers()`.
    pub async fn nodes(&self) -> Vec<NodeId> {
        let mut nodes = self.peers().await;
        nodes.push(self.my_id().await);
        nodes
    }

    /// Buffers an outgoing message for sending.
    pub async fn send(&self, to: NodeId, msg: T) {
        self.inner.send(to, msg).await
    }

    /// Buffers an outgoing message for sending.
    pub async fn send_batch(&self, messages: Vec<(NodeId, T)>) {
        let my_id = self.my_id().await;
        let msgs = messages.into_iter().map(|(to, msg)| {
            let msg = NetworkMsg{
                from: my_id,
                msg: Message::Payload(msg),
            };
            (to, msg)
        });
        self.inner.outgoing_buffer.lock().await.extend(msgs);
    }

    pub async fn broadcast(&self, message: T) {
        let peers = self.peers().await;
        let msg = NetworkMsg{
            from: self.my_id().await,
            msg: Message::Payload(message),
        };
        let mut latch = self.inner.outgoing_buffer.lock().await;
        for peer in peers {
            latch.push((peer, msg.clone()));
        }
    }
}

pub struct NetworkInner<T, H>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    H: MsgHandler<T>,
{
    id: RwLock<NodeId>,
    peers: RwLock<HashMap<NodeId, Peer>>,
    streams: Mutex<HashMap<IpAddr, FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>>>,
    outgoing_buffer: Mutex<Vec<(NodeId, NetworkMsg<T>)>>,
    handler: Arc<OnceCell<Arc<H>>>,

    /// root node state
    acked_peers: Mutex<HashMap<NodeId, u32>>,
}

impl<T, H> NetworkInner<T, H>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync + Clone,
    H: MsgHandler<T>,
{
    /// The codec we use for framing all messages.
    fn codec() -> LengthDelimitedCodec {
        LengthDelimitedCodec::new()
    }

    async fn new(id: NodeId, msg_handler: Arc<OnceCell<Arc<H>>>) -> Self {
        Self {
            id: RwLock::new(id),
            peers: Default::default(),
            streams: Default::default(),
            outgoing_buffer: Default::default(),
            handler: msg_handler,

            acked_peers: Default::default(),
        }
    }

    /// Wraps and buffers an outgoing message for sending.
    async fn send(&self, to: NodeId, msg: T) {
        self.internal_send(to, NetworkMsg{from: *self.id.read().await, msg: Message::Payload(msg)}).await
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
            bail!("unknown peer")
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
        stream.set_nodelay(true)?;
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

    /// A loop only to be run on the root node.
    /// Ensures that all other peers eventually know each other.
    async fn root_only_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let peers: Vec<Peer> = self.peers.read().await.values().map(|p| p.to_owned()).collect();
            let num_peers = peers.len();
            let msg = NetworkMsg{from: *self.id.read().await, msg: Message::<T>::Peers(peers)};
            let mut buf = self.outgoing_buffer.lock().await;
            for (id, num) in self.acked_peers.lock().await.iter() {
                if (*num as usize) < num_peers {
                    buf.push((*id, msg.clone()));
                }
            }
        }
    }

    async fn handle_new_stream(self: Arc<Self>, stream: TcpStream, peer_addr: IpAddr) -> anyhow::Result<()> {
        let (reader, writer) = stream.into_split();
        let framed_writer = FramedWrite::new(writer, Self::codec());
        let framed_reader = FramedRead::new(reader, Self::codec());
        let msg_handler = self.handler.clone();
        let network_handle = self.clone();
        tokio::task::spawn(async move { network_handle.handle_read_half(framed_reader, peer_addr, msg_handler).await });
        self.streams.lock().await.insert(peer_addr, framed_writer);
        Ok(())
    }

    async fn broadcast_peers(&self) {
        let peers = self.peers.read().await.values().map(|p| p.to_owned()).collect();
        let msg = NetworkMsg{from: *self.id.read().await, msg: Message::Peers(peers)};
        for id in self.peers.read().await.keys() {
            self.outgoing_buffer.lock().await.push((*id, msg.clone()));
        }
    }

    /// Handles the receiver half of a TCP stream.
    async fn handle_read_half(
        self: Arc<Self>,
        mut reader: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        peer_addr: IpAddr,
        handler: Arc<OnceCell<Arc<H>>>,
    ) -> anyhow::Result<()> {
        while let Some(data) = reader.try_next().await? {
            let NetworkMsg::<T>{ from, msg } = bincode::deserialize(&data).unwrap();
            match msg {
                Message::Payload(msg) => {
                    if let Some(handler) = handler.get() {
                        let msg_handler = handler.clone();
                        tokio::task::spawn(async move { msg_handler.handle_msg(from, msg).await });
                    }
                },
                Message::Join => {
                    // only handle if we are the root node
                    if *self.id.read().await == NodeId(1) {
                        let mut new_id;
                        {
                            let mut peers = self.peers.write().await;
                            new_id = NodeId(peers.len() as u32 + 2);
                            for Peer{id, addr} in peers.values() {
                                if *addr == peer_addr {
                                    new_id = *id;
                                }
                            }
                            peers.insert(new_id, Peer{id: new_id, addr: peer_addr});
                        }
                        self.acked_peers.lock().await.insert(new_id, 0);
                        let msg = NetworkMsg{
                            from: NodeId(1),
                            msg: Message::<T>::AssignId(new_id),
                        };
                        self.internal_send(new_id, msg).await;
                        self.broadcast_peers().await;
                    }
                },
                Message::AssignId(id) => {
                    self.peers.write().await.insert(from, Peer{id: from, addr: peer_addr});
                    *self.id.write().await = id;
                },
                Message::Peers(peers) => {
                    let mut my_peers = self.peers.write().await;
                    let my_id = *self.id.read().await;
                    for peer in peers {
                        if peer.id != my_id {
                            my_peers.insert(peer.id, peer);
                        }
                    }
                    let msg = NetworkMsg{
                        from: *self.id.read().await,
                        msg: Message::AckPeers(my_peers.len() as u32),
                    };
                    self.internal_send(from, msg).await;
                },
                Message::AckPeers(num_ack) => {
                    self.acked_peers.lock().await.insert(from, num_ack);
                },
            }
        }
        Ok(())
    }
}
