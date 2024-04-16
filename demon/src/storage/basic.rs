use std::{fmt::Debug, sync::Arc};
use tokio::sync::RwLock;
use super::Response;
use crate::rdts::Operation;

/// A naive storage implementation meant for non-hybrid consistency models.
#[derive(Debug)]
pub struct Storage<O: Operation> {
    state: Arc<RwLock<O::State>>,
}

impl<O: Operation> Storage<O> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(O::State::default())),
        }
    }

    /// Executes a blue operation.
    pub async fn exec(&self, op: O) -> Response<O> {
        let output = op.apply(&mut *self.state.write().await);
        Response{ value: output }
    }
}
