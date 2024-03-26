/// client API, calls sequencer or executor
mod api;
/// networking with basic cluster membership
mod network;
/// The transaction sequencing layer (replicated Log)
mod sequencer;
/// The replication layer used for weak operations
mod weak_replication;
/// The storage and query execution layer.
mod storage;
/// The DeMon protocol
mod demon;

use std::env;
use demon::DeMon;
use api::http::HttpApi;

use lazy_static::lazy_static;
use tokio::{select, signal::unix::{signal, SignalKind}, sync::watch};

lazy_static! {
    static ref CLUSTER_SIZE: u32 = env::args().skip(1).next().map(|s| s.parse::<u32>().unwrap()).unwrap();
    static ref CLUSTER_ADDR: Option<String> = env::args().skip(2).next();
}

#[tokio::main]
async fn main() {
    let _demon = DeMon::new(CLUSTER_ADDR.clone(), *CLUSTER_SIZE, Box::new(HttpApi{})).await;
    println!("Started DeMon.");


    // listen for termination signals
    let (terminate_tx, mut terminate_rx) = watch::channel(());
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        loop {
            select! {
                _ = sigterm.recv() => (),
                _ = sigint.recv() => (),
            };
            terminate_tx.send(()).expect("Failed to send internal termination signal.");
        }
    });

    terminate_rx.changed().await.expect("Failed to listen for internal termination signal.");
    println!("Shutting down. Goodbye.");
}
