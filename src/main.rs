/// client API, calls sequencer or executor
mod api;
/// The transaction sequencing layer (replicated Log)
mod sequencer;
/// The storage layer (versioned CRDTs)
mod storage;
/// networking with basic cluster membership
mod network;
/// The replication layer used for weak operations
mod weak_replication;
/// The DeMon protocol
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
