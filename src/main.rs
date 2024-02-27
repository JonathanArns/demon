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

use std::{env, time::Duration};
use demon::DeMon;

use lazy_static::lazy_static;

lazy_static! {
    static ref CLUSTER_SIZE: u32 = env::args().skip(1).next().map(|s| s.parse::<u32>().unwrap()).unwrap();
    static ref CLUSTER_ADDR: Option<String> = env::args().skip(2).next();
}

#[tokio::main]
async fn main() {
    let demon = DeMon::new(CLUSTER_ADDR.clone(), *CLUSTER_SIZE).await;
    println!("instantiated demon");
    tokio::time::sleep(Duration::from_secs(5)).await;
}
