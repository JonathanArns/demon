use std::{env, time::Duration};

use lazy_static::lazy_static;
use serde::Deserialize;
use tokio::{self, sync::watch, time::Instant};
use reqwest::{self, Client};
use rand::{Rng, thread_rng};

// benchmark settings
const STRONG_RATIO: f64 = 0.10;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const NUM_CLIENTS: usize = 300;
// How long the benchmark should run
const DURATION: Duration = Duration::from_secs(3);
/// Controls contention
const KEY_RANGE: usize = 100;

lazy_static!{
    static ref TARGET_DOMAINS: Vec<String> = env::args().skip(1).collect();
}

#[derive(Clone, Debug, Deserialize)]
struct DbResponse {
    latency: Duration,
}

#[derive(Clone, Debug, Deserialize)]
struct Measurement {
    db_latency: Duration,
    real_latency: Duration,
}

#[tokio::main]
async fn main() {
    if TARGET_DOMAINS.len() < 1 {
        panic!("missing arguments: target domain(s)");
    }

    let client = Client::new();
    let (sender, watcher) = watch::channel(false);

    // set up the clients
    let mut measurements = vec![];
    let mut futures = vec![];
    for i in 0..NUM_CLIENTS {
        let watcher_handle = watcher.clone();
        futures.push(tokio::spawn(run_client(watcher_handle, &TARGET_DOMAINS[i % TARGET_DOMAINS.len()], client.clone())));
    }

    // Run the clients for the set duration
    sender.send(true).unwrap();
    tokio::time::sleep(DURATION).await;
    sender.send(false).unwrap();

    // collect the results
    for f in futures {
        measurements.extend_from_slice(&f.await.unwrap());
    }

    println!(
        "mean db latency: {}ms, real latency {}ms, throughput: {} operations per second",
        measurements.iter().map(|m| m.db_latency.as_millis() as f64).sum::<f64>() / measurements.len() as f64,
        measurements.iter().map(|m| m.real_latency.as_millis() as f64).sum::<f64>() / measurements.len() as f64,
        measurements.len() / DURATION.as_secs() as usize,
    );
}

async fn run_client(mut watcher: watch::Receiver<bool>, domain: &str, client: Client) -> Vec<Measurement> {
    let mut measurements = vec![];
    
    // wait for the benchmark to start
    watcher.wait_for(|v| *v).await.unwrap();
    watcher.mark_unchanged();
    loop {
        let query = generate_query();
        let start_time = Instant::now();
        let response = client.post(format!("{}/query", domain))
            .body(query.clone())
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await;
        let resp: DbResponse = if let Ok(r) = response {
            serde_json::from_str(&r.text().await.unwrap()).unwrap()
        } else {
            println!("timed out on operation: {:?} with err: {:?}", query, response);
            return vec![]
        };
        measurements.push(Measurement{db_latency: resp.latency, real_latency: start_time.elapsed()});

        if watcher.has_changed().unwrap() {
            break
        }
    }
    
    measurements
}

fn generate_query() -> String {
    let mut rng = thread_rng();
    let key = rng.gen_range(0..KEY_RANGE);
    let weak_ops = [format!("{}+1", key), format!("{}-1", key), format!("r{}", key)];
    let strong_ops = [format!("{}=0", key)];
    let strong = rng.gen_bool(STRONG_RATIO);
    let query = if strong {
        let i = rng.gen_range(0..strong_ops.len());
        strong_ops[i].to_owned()
    } else {
        let i = rng.gen_range(0..weak_ops.len());
        weak_ops[i].to_owned()
    };
    query
}
