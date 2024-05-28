use axum::async_trait;
use tokio::sync::{mpsc::Receiver, oneshot::Sender};

use crate::{network::Network, protocols::Message, rdts::Operation, storage::QueryResult};

pub mod http;

#[async_trait]
pub trait API<O: Operation>: Send {
    /// Runs the API and returns (weak, strong) receivers for new requests.
    /// New requests come with a oneshot channel sender for the response.
    async fn start(self: Box<Self>, network: Network<Message>) -> Receiver<(O, Sender<QueryResult<O>>)>;
}
