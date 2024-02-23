/// client API, calls sequencer or executor
mod api;
/// The transaction sequencing layer (replicated Log)
mod sequencer;
/// The gossip replication layer for weak operations
mod gossip;

// local only components

/// Mixed consistency local query executor (multi-threaded, with concurrency control)
mod executor;
/// The storage layer (single-threaded, versioned CRDTs)
mod storage;


/// Demon is fully event-driven. All actions are triggered here.
mod core;

/// networking, cluster membership?
mod network;
mod demon;

use core::Message;
use std::{time::Duration, env};
use network::{NodeId, Network};

use lazy_static::lazy_static;

lazy_static! {
    static ref CLUSTER_ADDR: Option<String> = env::args().skip(1).next();
}

async fn handle_msg(from: NodeId, msg: Message) {
    println!("{:?}: {:?}", from, msg);
}

#[tokio::main]
async fn main() {
    // let network = Network::<Message, _, _>::connect(CLUSTER_ADDR.clone(), handle_msg).await.unwrap();

    // loop {
    //     tokio::time::sleep(Duration::from_secs(1)).await;
    //     for peer in network.peers().await {
    //         network.send(peer, Message::Gossip).await;
    //     }
    // }
}
