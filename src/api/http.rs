use axum::{async_trait, extract::State, routing::get, Router};
use tokio::sync::{mpsc, oneshot};

use crate::storage::{counters::CounterOp, Query, Response};

use super::API;

type Senders = (mpsc::Sender<(Query<CounterOp>, oneshot::Sender<Response<CounterOp>>)>, mpsc::Sender<(Query<CounterOp>, oneshot::Sender<Response<CounterOp>>)>);
type Receivers = (mpsc::Receiver<(Query<CounterOp>, oneshot::Sender<Response<CounterOp>>)>, mpsc::Receiver<(Query<CounterOp>, oneshot::Sender<Response<CounterOp>>)>);

pub struct HttpApi {}

#[async_trait]
impl API<CounterOp> for HttpApi {
    async fn start(self: Box<Self>) -> Receivers {
        let (weak_sender, weak_receiver) = mpsc::channel(1000);
        let (strong_sender, strong_receiver) = mpsc::channel(1000);
        tokio::task::spawn(async move {
            let app = Router::new()
                .route("/strong", get(strong_endpoint))
                .route("/weak", get(weak_endpoint))
                .with_state((weak_sender, strong_sender));
            let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
            axum::serve(listener, app).await.unwrap()
        });
        (weak_receiver, strong_receiver)
    }
}

async fn strong_endpoint(State(senders): State<Senders>) -> String {
    let (_, query_sender) = senders;
    let (result_sender, result_receiver) = oneshot::channel();
    let query = Query {ops:vec![]};
    query_sender.send((query, result_sender)).await.unwrap();
    let result = result_receiver.await.unwrap();
    todo!()
}

async fn weak_endpoint(State(senders): State<Senders>) -> String {
    let (query_sender, _) = senders;
    let (result_sender, result_receiver) = oneshot::channel();
    let query = Query {ops:vec![]};
    query_sender.send((query, result_sender)).await.unwrap();
    let result = result_receiver.await.unwrap();
    todo!()
}
