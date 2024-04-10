use axum::async_trait;
use tokio::sync::{mpsc::Receiver, oneshot::Sender};

use crate::storage::{Operation, Response};

pub mod http;
pub mod query_parser;

#[async_trait]
pub trait API<O: Operation>: Send {
    /// Runs the API and returns (weak, strong) receivers for new requests.
    /// New requests come with a oneshot channel sender for the response.
    async fn start(self: Box<Self>) -> Receiver<(O, Sender<Response<O>>)>;
}
