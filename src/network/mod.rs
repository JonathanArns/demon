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

    Prepare{
        round: u32,
    },
    Promise{
        round: u32,
        peers: Vec<Peer>,
        accepted: Option<Peer>,
    },
    Propose{
        round: u32,
        proposal: Peer,
    },
    Accept{
        round: u32,
    },
    Decide{
        round: u32,
        proposal: Peer,
    },
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
    /// Creates and connects the local network instance.
    /// Handles to this instance should be created with `clone`.
    ///
    /// If called with no addrs, this node is the first one in the cluster.
    pub async fn connect<A: ToSocketAddrs>(addrs: Option<A>, msg_handler: F) -> anyhow::Result<Self> {
        let id = NodeId(rand::random());
        let network = Self {inner: Arc::new(NetworkInner::new(id, msg_handler).await)};
        tokio::task::spawn(network.inner.clone().listen());
        // tokio::task::spawn(network.inner.clone().chatter());
        tokio::task::spawn(network.inner.clone().send_loop());


        if let Some(addrs) = addrs {
            for addr in lookup_host(addrs).await? {
                let r = network.inner.clone().connect(addr.ip()).await;
                if r.is_err() {
                    bail!("could not connect to cluster");
                }
            }

            // we run paxos here to connect with a unique ID
            loop {
                let msg = NetworkMsg {
                    from: *network.inner.id.read().await,
                    msg: Message::Prepare{ round: *network.inner.round.lock().await },
                };
                network.inner.broadcast(msg).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } else { // we are the first node
            *network.inner.id.write().await = NodeId(0);
        }
        Ok(network)
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
    id: RwLock<NodeId>,
    streams: Mutex<HashMap<IpAddr, FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>>>,
    outgoing_buffer: Mutex<Vec<(NodeId, NetworkMsg<T>)>>,
    handler: F,

    peers: RwLock<HashMap<NodeId, Peer>>,

    // acceptor state
    promised: Mutex<NodeId>,
    accepted: Mutex<Option<Peer>>,

    // proposer state
    round: Mutex<u32>,
    promises: Mutex<u32>,
    acceptances: Mutex<u32>,
    proposal: Mutex<Option<Peer>>,
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
            id: RwLock::new(id),
            streams: Default::default(),
            outgoing_buffer: Default::default(),
            handler: msg_handler,

            peers: Default::default(),

            // acceptor state
            promised: Mutex::new(NodeId(0)),
            accepted: Default::default(),

            // proposer state
            round: Default::default(),
            promises: Default::default(),
            acceptances: Default::default(),
            proposal: Default::default(),
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
    // async fn chatter(self: Arc<Self>) {
    //     loop {
    //         tokio::time::sleep(Duration::from_secs(1)).await;

    //         // find the peer to talk to
    //         let mut next_peer = NodeId(u32::MAX);
    //         let mut smallest_peer = NodeId(u32::MAX);
    //         for peer in self.peers.read().await.keys() {
    //             if peer.0 > self.id.0 && peer.0 < next_peer.0 {
    //                 next_peer = *peer;
    //             }
    //             if peer.0 < smallest_peer.0 {
    //                 smallest_peer = *peer;
    //             }
    //             // self.send_peers(*peer).await;
    //         }

    //         if next_peer.0 != u32::MAX {
    //             self.send_peers(next_peer).await;
    //         } else if smallest_peer.0 != u32::MAX {
    //             self.send_peers(smallest_peer).await;
    //         }

    //         // // alternative: just broadcast
    //         // for peer in self.peers.read().await.keys() {
    //         //     self.send_peers(*peer).await;
    //         // }
    //     }
    // }

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

    // /// Sends `to` a list of known peers.
    // async fn send_peers(&self, to: NodeId) {
    //     let peers = self.peers.read().await.values().map(|p| p.to_owned()).collect();
    //     let msg = NetworkMsg{from: self.id, msg: Message::Peers(peers)};
    //     self.internal_send(to, msg).await;
    // }

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
                Message::Prepare{round} => {
                    let mut promised = self.promised.lock().await;
                    if promised.0 <= from.0 {
                        *promised = from;
                        let msg = NetworkMsg{
                            from: *self.id.read().await,
                            msg: Message::Promise{
                                round,
                                accepted: self.accepted.lock().await.clone(),
                                peers: self.peers.read().await.values().map(|p| p.to_owned()).collect(),
                            },
                        };
                        self.internal_send(from, msg).await;
                    }
                },
                Message::Promise{round, peers, accepted} => {
                    if round == *self.round.lock().await {
                        *self.promises.lock().await += 1;
                    }
                    if accepted.is_some() {
                        *self.proposal.lock().await = accepted;
                    }
                    let mut my_peers = self.peers.write().await;
                    for peer in peers {
                        if peer.id != *self.id.read().await {
                            my_peers.insert(peer.id, peer);
                        }
                    }
                },
                Message::Propose{round, proposal} => {
                    if self.promised.lock().await.0 <= from.0 {
                        *self.accepted.lock().await = Some(proposal);
                        let msg = NetworkMsg{
                            from: *self.id.read().await,
                            msg: Message::Accept{ round },
                        };
                        self.internal_send(from, msg).await;
                    }
                },
                Message::Accept{round} => {
                    if round == *self.round.lock().await {
                        *self.acceptances.lock().await += 1;
                    }
                },
                Message::Decide{round, proposal} => {
                    *self.promised.lock().await = NodeId(0);
                    *self.accepted.lock().await = None;
                    self.peers.write().await.insert(proposal.id, proposal);
                    self.internal_send(from, NetworkMsg{
                        from: *self.id.read().await,
                        msg: Message::Accept{ round },
                    }).await;
                },
                // Message::Join => {
                //     self.send_peers(from).await;
                //     let msg = NetworkMsg{
                //         from: self.id,
                //         msg: Message::<T>::Peers(vec![Peer{id: from, addr}]),
                //     };
                //     self.broadcast(msg).await;
                // },
                // Message::GetPeers => {
                //     self.send_peers(from).await;
                // },
                // Message::Peers(peers) => {
                //     let mut my_peers = self.peers.write().await;
                //     for peer in peers {
                //         if peer.id != self.id {
                //             my_peers.insert(peer.id, peer);
                //         }
                //     }
                // },
            }
        }
        Ok(())
    }
}
