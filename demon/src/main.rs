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
/// RDT definitions.
mod rdts;
/// The different protocols
mod protocols;

use clap::Parser;

use api::http::HttpApi;
use rdts::{counters::CounterOp, non_negative_counter::NonNegativeCounterOp, tpcc::TpccOp};

use tokio::{select, signal::unix::{signal, SignalKind}, sync::watch};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(version = "1.0", about = "jonathan's hybrid consistency prototype", long_about = None)]
struct Arguments {
    #[arg(short = 'c', long = "cluster-size")]
    cluster_size: u32,

    #[arg(short = 'a', long = "addr")]
    cluster_addr: Option<String>,

    #[arg(short = 'p', long = "proto")]
    protocol: String,

    #[arg(short = 't', long = "datatype")]
    datatype: String,

    #[arg(short = 'n', long = "replica-name")]
    name: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();

    let api = Box::new(HttpApi{});
    match &args.datatype[..] {
        "non-neg-counter" => {
            match &args.protocol[..] {
                "demon" => {
                    protocols::demon::DeMon::<NonNegativeCounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "strict" => {
                    protocols::strict::Strict::<NonNegativeCounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "causal" => {
                    protocols::causal::Causal::<NonNegativeCounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "redblue" => {
                    protocols::redblue::RedBlue::<NonNegativeCounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "redblue-mod" => {
                    protocols::redblue_modified::RedBlueModified::<NonNegativeCounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "gemini" => {
                    protocols::gemini::Gemini::<NonNegativeCounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "unistore" => {
                    protocols::unistore::Unistore::<NonNegativeCounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                _ => panic!("unknown protocol {:?}", args.protocol.clone()),
            };
        },
        "counter" => {
            match &args.protocol[..] {
                "demon" => {
                    protocols::demon::DeMon::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "strict" => {
                    protocols::strict::Strict::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "causal" => {
                    protocols::causal::Causal::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "redblue" => {
                    protocols::redblue::RedBlue::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "redblue-mod" => {
                    protocols::redblue_modified::RedBlueModified::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "gemini" => {
                    protocols::gemini::Gemini::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "unistore" => {
                    protocols::unistore::Unistore::<CounterOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                _ => panic!("unknown protocol {:?}", args.protocol.clone()),
            };
        },
        "tpcc" => {
            match &args.protocol[..] {
                "demon" => {
                    protocols::demon::DeMon::<TpccOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "strict" => {
                    protocols::strict::Strict::<TpccOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "causal" => {
                    protocols::causal::Causal::<TpccOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "redblue" => {
                    protocols::redblue::RedBlue::<TpccOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "redblue-mod" => {
                    protocols::redblue_modified::RedBlueModified::<TpccOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "gemini" => {
                    protocols::gemini::Gemini::<TpccOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                "unistore" => {
                    protocols::unistore::Unistore::<TpccOp>::new(args.cluster_addr.clone(), args.cluster_size, api, args.name.clone()).await;
                },
                _ => panic!("unknown protocol {:?}", args.protocol.clone()),
            };
        },
        _ => panic!("bad datatype argument, options are: counter, non-neg-counter, tpcc")
    }

    println!("Started Server. Running version {}", VERSION);


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
