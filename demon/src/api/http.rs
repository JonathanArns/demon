use std::time::Duration;

use axum::{async_trait, extract::State, routing::{get, post}, Json, Router};
use serde::Serialize;
use tokio::{sync::{mpsc, oneshot}, time::Instant};

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
