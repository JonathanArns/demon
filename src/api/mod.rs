use axum::async_trait;
use tokio::sync::{mpsc::Receiver, oneshot::Sender};

use crate::storage::{Operation, Query, Response};

pub mod http;

#[async_trait]
pub trait API<O: Operation>: Send {
    /// Runs the API and returns (weak, strong) receivers for new requests.
    /// New requests come with a oneshot channel sender for the response.
    async fn start(self: Box<Self>) -> (Receiver<(Query<O>, Sender<Response<O>>)>, Receiver<(Query<O>, Sender<Response<O>>)>);
}
