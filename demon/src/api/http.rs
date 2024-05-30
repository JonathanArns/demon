use std::time::Duration;

use axum::{async_trait, extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::{select, sync::{mpsc, oneshot, watch}, time::Instant};

use crate::{network::{Network, NodeId}, protocols::Message, rdts::Operation, storage::QueryResult};

use super::API;

pub struct HttpApi {}

#[derive(Debug, Serialize)]
struct Response<O: Operation> {
    data: QueryResult<O>,
    latency: Duration,
}

#[async_trait]
impl<O: Operation> API<O> for HttpApi {
    async fn start(self: Box<Self>, network: Network<Message>) -> mpsc::Receiver<(O, oneshot::Sender<QueryResult<O>>)> {
        let (query_sender, query_receiver) = mpsc::channel(1000);
        tokio::task::spawn(async move {
            let app = Router::new()
                .route("/", get(|| async { "Hello world!" }))
                .route("/query", post(query_endpoint))
                .route("/bench", post(bench_endpoint))
                .route("/measure_rtt_latency", get(latency_endpoint))
                .with_state((network, query_sender));
            let listener = tokio::net::TcpListener::bind("0.0.0.0:80").await.unwrap();
            axum::serve(listener, app).await.unwrap()
        });
        query_receiver
    }
}

async fn query_endpoint<O: Operation>(State((_network, query_sender)): State<(Network<Message>, mpsc::Sender<(O, oneshot::Sender<QueryResult<O>>)>)>, body: String) -> Result<Json<Response<O>>, StatusCode> {
    let start_time = Instant::now();
    let (result_sender, result_receiver) = oneshot::channel();
    let query = O::parse(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    query_sender.send((query, result_sender)).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let result = select! {
        res = result_receiver => {
            res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        },
        // request timeout
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            Err(StatusCode::INTERNAL_SERVER_ERROR)?
        },
    };
    let response = Response {
        data: result,
        latency: start_time.elapsed(),
    };
    Ok(Json(response))
}

/// Measures round trip latency to all peers
async fn latency_endpoint<O: Operation>(State((network, _query_sender)): State<(Network<Message>, mpsc::Sender<(O, oneshot::Sender<QueryResult<O>>)>)>) -> Result<Json<Vec<(NodeId, Option<String>, Duration)>>, StatusCode> {
    let latencies = network.measure_round_trips().await;
    Ok(Json(latencies))
}

#[derive(Clone, Debug)]
struct Measurement {
    latency: Duration,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BenchSettings {
    pub read_ratio: f64,
    pub strong_ratio: f64,
    pub num_clients: usize,
    pub key_range: usize,
    /// in seconds
    pub duration: u64,
}

#[derive(Clone, Debug, Serialize)]
struct BenchMetrics {
    /// in millis
    pub mean_latency: f64,
    /// ops per second
    pub throughput: u64,
}

async fn bench_endpoint<O: Operation>(State((_network, query_sender)): State<(Network<Message>, mpsc::Sender<(O, oneshot::Sender<QueryResult<O>>)>)>, Json(settings): Json<BenchSettings>) -> Result<Json<BenchMetrics>, StatusCode> {
    let (watch_sender, watcher) = watch::channel(false);

    // set up the clients
    let mut measurements = vec![];
    let mut futures = vec![];
    for _ in 0..settings.num_clients {
        futures.push(tokio::spawn(run_client(watcher.clone(), query_sender.clone(), settings.clone())));
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    // Run the clients for the set duration
    watch_sender.send(true).unwrap();
    tokio::time::sleep(Duration::from_secs(settings.duration)).await;
    watch_sender.send(false).unwrap();

    // collect the results
    for f in futures {
        let data = f.await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        measurements.extend_from_slice(&data);
    }

    let metrics = BenchMetrics {
        mean_latency: measurements.iter().map(|m| m.latency.as_millis() as f64).sum::<f64>() / measurements.len() as f64,
        throughput: measurements.len() as u64 / settings.duration,
    };
    Ok(Json(metrics))
}

async fn run_client<O: Operation>(
    mut watcher: watch::Receiver<bool>,
    query_sender: mpsc::Sender<(O, oneshot::Sender<QueryResult<O>>)>,
    settings: BenchSettings,
) -> anyhow::Result<Vec<Measurement>> {
    let mut measurements = vec![];
    
    // wait for the benchmark to start
    watcher.wait_for(|v| *v).await?;
    watcher.mark_unchanged();
    loop {
        let query = O::gen_query(&settings);
        let start_time = Instant::now();
        let (result_sender, result_receiver) = oneshot::channel();
        query_sender.send((query, result_sender)).await?;
        select! {
            _ = result_receiver => {
                measurements.push(Measurement{latency: start_time.elapsed()});
            },
            // request timeout
            _ = tokio::time::sleep(Duration::from_secs(3)) => (),
        } 

        if watcher.has_changed()? {
            break
        }
    }
    
    Ok(measurements)
}
