use std::{env, time::Duration};

use lazy_static::lazy_static;
use tokio::{self, time::Instant};
use reqwest;
use rand::{Rng, thread_rng};

// benchmark settings
const STRONG_RATIO: f64 = 0.3;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const NUM_CLIENTS: usize = 100;

lazy_static!{
    static ref TARGET_DOMAINS: Vec<String> = env::args().skip(1).collect();
}

struct Measurement {
    latency: Duration,
}

#[tokio::main]
async fn main() {
    if TARGET_DOMAINS.len() < 1 {
        panic!("missing arguments: target domain(s)");
    }
    let mut futures = vec![];
    for i in 0..NUM_CLIENTS {
        futures.push(tokio::spawn(run_client(&TARGET_DOMAINS[i % TARGET_DOMAINS.len()])));
    }
    for f in futures {
        f.await.unwrap();
    }
}

async fn run_client(domain: &str) {
    let client = reqwest::Client::new();
    let mut measurements = vec![];
    for _ in 0..1000 {
        let query = generate_query();
        let start_time = Instant::now();
        let response = client.post(format!("{}/query", domain))
            .body(query.clone())
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await;
        if response.is_err() {
            println!("timed out on operation: {:?}", query);
            return
        }
        let latency = start_time.elapsed();
        measurements.push(Measurement{ latency });
    }
    
    println!("mean latency: {}ms", measurements.iter().map(|m| m.latency.as_millis() as f64).sum::<f64>() / measurements.len() as f64);
}

fn generate_query() -> String {
    let mut rng = thread_rng();
    let weak_ops = ["1+1", "1-1", "r1"];
    let strong_ops = ["1=0"];
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
