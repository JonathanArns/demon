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
/// The different protocols
mod protocols;

use clap::Parser;

use api::http::HttpApi;
use storage::counters::CounterOp;

use tokio::{select, signal::unix::{signal, SignalKind}, sync::watch};

#[derive(Parser)]
#[command(version = "1.0", about = "jonathan's hybrid consistency prototype", long_about = None)]
struct Arguments {
    #[arg(short = 'c', long = "cluster-size")]
    cluster_size: u32,

    #[arg(short = 'a', long = "addr")]
    cluster_addr: Option<String>,

    #[arg(short = 'p', long = "proto")]
    protocol: String,
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();

    match &args.protocol[..] {
        "demon" => {
            protocols::demon::DeMon::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, Box::new(HttpApi{})).await;
        },
        "strict" => {
            protocols::strong::Strong::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, Box::new(HttpApi{})).await;
        },
        "causal" => {
            protocols::causal::Causal::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, Box::new(HttpApi{})).await;
        },
        "redblue" => {
            protocols::redblue::RedBlue::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, Box::new(HttpApi{})).await;
        },
        _ => panic!("unknown protocol {:?}", args.protocol.clone()),
    };

    println!("Started Server.");


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
