use std::time::Duration;

use axum::{async_trait, extract::State, routing::{get, post}, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::{sync::{watch, mpsc, oneshot}, time::Instant};
use rand::{Rng, thread_rng};

use crate::{rdts::Operation, storage::QueryResult};

use super::API;

pub struct HttpApi {}

#[derive(Debug, Serialize)]
struct Response<O: Operation> {
    data: QueryResult<O>,
    latency: Duration,
}

#[async_trait]
impl<O: Operation> API<O> for HttpApi {
    async fn start(self: Box<Self>) -> mpsc::Receiver<(O, oneshot::Sender<QueryResult<O>>)> {
        let (query_sender, query_receiver) = mpsc::channel(1000);
        tokio::task::spawn(async move {
            let app = Router::new()
                .route("/", get(|| async { "Hello world!" }))
                .route("/query", post(query_endpoint))
                .route("/bench", post(bench_endpoint))
                .with_state(query_sender);
            let listener = tokio::net::TcpListener::bind("0.0.0.0:80").await.unwrap();
            axum::serve(listener, app).await.unwrap()
        });
        query_receiver
    }
}

async fn query_endpoint<O: Operation>(State(query_sender): State<mpsc::Sender<(O, oneshot::Sender<QueryResult<O>>)>>, body: String) -> Json<Response<O>> {
    let start_time = Instant::now();
    let (result_sender, result_receiver) = oneshot::channel();
    let query = O::parse(&body).unwrap();
    query_sender.send((query, result_sender)).await.unwrap();
    let result = result_receiver.await.unwrap();
    let response = Response {
        data: result,
        latency: start_time.elapsed(),
    };
    Json(response)
}

#[derive(Clone, Debug)]
struct Measurement {
    latency: Duration,
}

#[derive(Clone, Debug, Deserialize)]
struct BenchSettings {
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

async fn bench_endpoint<O: Operation>(State(query_sender): State<mpsc::Sender<(O, oneshot::Sender<QueryResult<O>>)>>, Json(settings): Json<BenchSettings>) -> Json<BenchMetrics> {
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
        measurements.extend_from_slice(&f.await.unwrap());
    }

    let metrics = BenchMetrics {
        mean_latency: measurements.iter().map(|m| m.latency.as_millis() as f64).sum::<f64>() / measurements.len() as f64,
        throughput: measurements.len() as u64 / settings.duration,
    };
    Json(metrics)
}

async fn run_client<O: Operation>(
    mut watcher: watch::Receiver<bool>,
    query_sender: mpsc::Sender<(O, oneshot::Sender<QueryResult<O>>)>,
    settings: BenchSettings,
) -> Vec<Measurement> {
    let mut measurements = vec![];
    
    // wait for the benchmark to start
    watcher.wait_for(|v| *v).await.unwrap();
    watcher.mark_unchanged();
    loop {
        let query = generate_query(&settings);
        let start_time = Instant::now();
        let (result_sender, result_receiver) = oneshot::channel();
        let query = O::parse(&query).unwrap();
        query_sender.send((query, result_sender)).await.unwrap();
        let _result = result_receiver.await.unwrap();
        measurements.push(Measurement{latency: start_time.elapsed()});

        if watcher.has_changed().unwrap() {
            break
        }
    }
    
    measurements
}

fn generate_query(settings: &BenchSettings) -> String {
    let mut rng = thread_rng();
    let key = rng.gen_range(0..settings.key_range);
    let weak_ops = [format!("{}+1", key), format!("{}-1", key), format!("r{}", key)];
    let strong_ops = [format!("{}=0", key)];
    let strong = rng.gen_bool(settings.strong_ratio);
    let query = if strong {
        let i = rng.gen_range(0..strong_ops.len());
        strong_ops[i].to_owned()
    } else {
        let i = rng.gen_range(0..weak_ops.len());
        weak_ops[i].to_owned()
    };
    query
}
